/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package volume

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
)

// countingReader wraps an io.Reader and reports raw bytes read to onProgress
// incrementally, as the stream is consumed, so a byte-progress bar advances
// during FS file staging instead of jumping from 0% to 100% in one frame.
type countingReader struct {
	r          io.Reader
	onProgress func(n int)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if c.onProgress != nil && n > 0 {
		c.onProgress(n)
	}

	return n, err
}

// fsItem is one file-system entry collected from the data-exporter listing.
// It holds both the download URL (for file type) and the metadata needed to
// build a TarEntry for the final data.tar assembly.
type fsItem struct {
	relPath  string
	itemType string // "file", "dir", or "link"
	uri      string // download URL; non-empty only for itemType == "file"
	size     int64  // declared content size from the listing; meaningful only for itemType == "file"
	mode     fs.FileMode
	uid      int
	gid      int
	mtime    time.Time
	linkname string // symlink target; non-empty only for itemType == "link"
}

// DownloadFilesystemVolume downloads all files from the data-exporter filesystem
// volume at filesRootURL, stages each file as a compressed blob named
// <relPath><codec.Ext()> under stagingDir, then assembles a single uncompressed
// PAX tar at tarPath whose file entries carry the compressed names and bytes.
//
// If tarPath already exists the whole operation is skipped (resume: tar complete).
// An already-staged compressed file <relPath><ext> is not re-downloaded (partial
// resume). The stagingDir is removed on successful tar assembly.
//
// workers bounds the parallelism for file downloads; the first error cancels all
// in-flight downloads.
//
// chunkSize gates per-file chunking: a file whose declared size exceeds chunkSize
// is staged via Range-based chunks (reusing DownloadBlockChunks/MergeBlockChunks,
// the same machinery the block-volume path uses) so an interrupted download can
// resume at sub-file granularity instead of restarting the whole file; chunkSize
// <= 0 falls back to DefaultChunkSize. Files at or below chunkSize keep the
// original single-shot GET + codec.EncodeStream path unchanged.
//
// setTotal, when non-nil, is called exactly once with the summed declared size of
// all file items in the listing before staging begins, so a progress sink can show
// a real denominator (mirrors the block path's stream.SetTotal after HeadVolume).
func DownloadFilesystemVolume(
	ctx context.Context,
	log *slog.Logger,
	tarPath string,
	stagingDir string,
	filesRootURL string,
	workers int,
	chunkSize int64,
	fetcher *exporter.Fetcher,
	codec compress.Codec,
	setTotal func(total int64),
	onProgress func(n int),
) error {
	// Resume: completed tar → skip entirely.
	if _, err := os.Stat(tarPath); err == nil {
		log.Info("fs tar already present, skipping", slog.String("path", tarPath))

		return nil
	}

	if workers <= 0 {
		workers = 1
	}

	if err := archive.EnsureDir(stagingDir); err != nil {
		return fmt.Errorf("create staging dir %s: %w", stagingDir, err)
	}

	base, err := url.Parse(filesRootURL)
	if err != nil {
		return fmt.Errorf("parse files root URL %q: %w", filesRootURL, err)
	}

	items, err := collectAllFSItems(ctx, fetcher, filesRootURL, base, "")
	if err != nil {
		return fmt.Errorf("list filesystem volume: %w", err)
	}

	// Report the expected total now that the listing (with per-file sizes) is known,
	// before any staging begins, mirroring the block path's stream.SetTotal.
	if setTotal != nil {
		setTotal(sumFileSizes(items))
	}

	log.Info("staging filesystem volume",
		slog.String("tar", tarPath),
		slog.Int("items", len(items)),
		slog.Int("workers", workers))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)

	for _, it := range items {
		if it.itemType != "file" {
			continue
		}

		item := it

		g.Go(func() error {
			return stageCompressedFile(gctx, log, stagingDir, item, chunkSize, codec, fetcher, onProgress)
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("stage filesystem files: %w", err)
	}

	// File entries in the tar carry the compressed name <relPath><ext> so that
	// the tar container holds the already-compressed blobs (no re-compression).
	// Dir and link entries keep their original relPath (no extension suffix).
	ext := codec.Ext()
	entries := make([]TarEntry, 0, len(items))

	for _, it := range items {
		relPath := it.relPath
		if it.itemType == "file" {
			relPath += ext
		}

		entries = append(entries, TarEntry{
			RelPath:  relPath,
			Type:     it.itemType,
			Mode:     it.mode,
			UID:      it.uid,
			GID:      it.gid,
			Mtime:    it.mtime,
			Linkname: it.linkname,
		})
	}

	if err := WriteTar(ctx, tarPath, stagingDir, entries); err != nil {
		return fmt.Errorf("assemble tar %s: %w", tarPath, err)
	}

	log.Info("fs tar assembled", slog.String("path", tarPath))

	if err := os.RemoveAll(stagingDir); err != nil {
		log.Warn("failed to remove FS staging dir",
			slog.String("dir", stagingDir),
			slog.String("error", err.Error()))
	}

	return nil
}

// collectAllFSItems recursively walks the listing at dirURL and accumulates
// all items (file, dir, link) with their relative paths and metadata.
// filesRootURL is the absolute URL of the volume root used to resolve relative URIs.
// relPrefix is prepended to item names when building the relative path.
func collectAllFSItems(ctx context.Context, fetcher *exporter.Fetcher, dirURL string, base *url.URL, relPrefix string) ([]fsItem, error) {
	items, err := fetcher.ListDir(ctx, dirURL)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", dirURL, err)
	}

	var result []fsItem

	for _, item := range items {
		ref, err := url.Parse(item.URI)
		if err != nil {
			return nil, fmt.Errorf("parse item URI %q: %w", item.URI, err)
		}

		absURI := base.ResolveReference(ref).String()
		relPath := relPrefix + item.Name
		mode, uid, gid, mtime := parseItemAttrs(item.Attributes)

		switch item.Type {
		case "file":
			result = append(result, fsItem{
				relPath:  relPath,
				itemType: "file",
				uri:      absURI,
				size:     parseItemSize(item.Attributes),
				mode:     mode,
				uid:      uid,
				gid:      gid,
				mtime:    mtime,
			})

		case "dir":
			result = append(result, fsItem{
				relPath:  relPath,
				itemType: "dir",
				mode:     mode,
				uid:      uid,
				gid:      gid,
				mtime:    mtime,
			})

			subPrefix := relPath + "/"

			subItems, err := collectAllFSItems(ctx, fetcher, absURI, base, subPrefix)
			if err != nil {
				return nil, err
			}

			result = append(result, subItems...)

		case "link":
			result = append(result, fsItem{
				relPath:  relPath,
				itemType: "link",
				mode:     mode,
				uid:      uid,
				gid:      gid,
				mtime:    mtime,
				linkname: item.TargetPath,
			})

		default:
			// Unknown or error items: skip silently (forward-compatible).
		}
	}

	return result, nil
}

// stageCompressedFile stages one file to stagingDir/<relPath><codec.Ext()>,
// choosing between two staging strategies based on the item's declared size:
//
//   - item.size > chunkSize (chunkSize <= 0 falls back to DefaultChunkSize):
//     the file is staged via stageChunkedFile, which fetches it as independent
//     Range-based chunks so an interrupted download resumes at sub-file
//     granularity instead of restarting the whole file.
//   - otherwise: the file is staged via stageWholeFile, the original single-shot
//     GET + codec.EncodeStream path — unchanged for the common case of many
//     small files, where a chunk directory would add overhead with no resume
//     benefit.
//
// Already-complete destination files are skipped (resume) regardless of which
// strategy would otherwise apply; the skip still credits the item's declared
// size to onProgress so the numerator can reach the denominator that setTotal
// established from the same declared sizes (sumFileSizes) — otherwise a
// partially-staged resume could never advance the progress bar to 100% even
// though the tar assembles successfully. Stale <destPath>.tmp files are removed
// before either strategy runs.
func stageCompressedFile(
	ctx context.Context,
	log *slog.Logger,
	stagingDir string,
	item fsItem,
	chunkSize int64,
	codec compress.Codec,
	fetcher *exporter.Fetcher,
	onProgress func(n int),
) error {
	destPath := filepath.Join(stagingDir, filepath.FromSlash(item.relPath+codec.Ext()))

	if _, err := os.Stat(destPath); err == nil {
		log.Debug("staging file already present, skipping", slog.String("path", item.relPath))

		if onProgress != nil && item.size > 0 {
			onProgress(int(item.size))
		}

		return nil
	}

	tmpPath := destPath + ".tmp"

	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale tmp %s: %w", tmpPath, err)
	}

	parentDir := filepath.Dir(destPath)

	if err := archive.EnsureDir(parentDir); err != nil {
		return fmt.Errorf("create parent dir %s: %w", parentDir, err)
	}

	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	if item.size > chunkSize {
		return stageChunkedFile(ctx, log, stagingDir, destPath, item, chunkSize, codec, fetcher, onProgress)
	}

	return stageWholeFile(ctx, log, destPath, item, codec, fetcher, onProgress)
}

// stageChunkedFile downloads item as independent Range-based chunks into a
// per-file chunk directory and merges them into destPath, reusing the
// block-volume chunking machinery (DownloadBlockChunks/MergeBlockChunks)
// UNCHANGED. workers is pinned to 1: chunks within one file download
// sequentially, inside that file's own already-allocated slot in the outer
// per-file errgroup (DownloadFilesystemVolume's g.SetLimit(workers) loop) —
// this deliberately avoids adding a third multiplicative concurrency
// dimension on top of node-workers × PerVolumeConcurrency. Already-present
// chunks are skipped and their raw length is credited to onProgress by
// downloadChunk's existing resume-skip path; no progress-crediting logic is
// duplicated here.
func stageChunkedFile(
	ctx context.Context,
	log *slog.Logger,
	stagingDir string,
	destPath string,
	item fsItem,
	chunkSize int64,
	codec compress.Codec,
	fetcher *exporter.Fetcher,
	onProgress func(n int),
) error {
	chunkDirName := archive.FsFileChunksDirName(item.relPath, codec.Ext())
	chunkDir := filepath.Join(stagingDir, filepath.FromSlash(chunkDirName))

	if err := DownloadBlockChunks(ctx, log, chunkDir, item.uri, item.size, chunkSize, 1, fetcher, codec, onProgress); err != nil {
		return fmt.Errorf("download chunks for %s: %w", item.relPath, err)
	}

	if err := MergeBlockChunks(ctx, chunkDir, destPath, item.size, chunkSize, codec.Ext()); err != nil {
		return fmt.Errorf("merge chunks for %s: %w", item.relPath, err)
	}

	return nil
}

// stageWholeFile downloads item in a single GET, compresses it with codec, and
// writes the result atomically to destPath. Compression is streaming: the HTTP
// body is piped through codec.EncodeStream so no whole-file buffering occurs.
func stageWholeFile(
	ctx context.Context,
	log *slog.Logger,
	destPath string,
	item fsItem,
	codec compress.Codec,
	fetcher *exporter.Fetcher,
	onProgress func(n int),
) error {
	log.Debug("staging fs file", slog.String("path", item.relPath))

	body, err := fetcher.GetFile(ctx, item.uri)
	if err != nil {
		return fmt.Errorf("GET %s: %w", item.uri, err)
	}

	defer func() { _ = body.Close() }()

	cr := &countingReader{r: body, onProgress: onProgress}

	aw, err := archive.NewAtomicWriter(destPath)
	if err != nil {
		return fmt.Errorf("open atomic writer for %s: %w", destPath, err)
	}

	if err := codec.EncodeStream(aw, cr); err != nil {
		aw.Abort()

		return fmt.Errorf("stage %s: %w", item.relPath, err)
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit staging %s: %w", destPath, err)
	}

	log.Debug("staging file written", slog.String("path", item.relPath))

	return nil
}

// sumFileSizes returns the total declared content size across all "file" items in
// the collected listing. Items whose size is missing or zero contribute nothing.
func sumFileSizes(items []fsItem) int64 {
	var total int64

	for _, it := range items {
		if it.itemType == "file" {
			total += it.size
		}
	}

	return total
}

// parseItemSize extracts the "size" attribute from a data-exporter listing item.
// The real exporter emits it as a JSON number (decoded as float64 by encoding/json);
// json.Number is also handled in case a decoder is configured with UseNumber.
// Missing, negative, or non-numeric values yield 0 so the total degrades gracefully.
func parseItemSize(attrs map[string]any) int64 {
	v, ok := attrs["size"]
	if !ok {
		return 0
	}

	switch n := v.(type) {
	case float64:
		if n <= 0 {
			return 0
		}

		return int64(n)
	case json.Number:
		i, err := n.Int64()
		if err != nil || i < 0 {
			return 0
		}

		return i
	case int64:
		if n < 0 {
			return 0
		}

		return n
	case int:
		if n < 0 {
			return 0
		}

		return int64(n)
	default:
		return 0
	}
}

// parseItemAttrs extracts file metadata from the data-exporter listing attributes map.
// The real data-exporter (storage-volume-data-manager, images/data-exporter,
// prepareAttributesStat) emits these keys and types:
//   - "permissions": octal string via fmt.Sprintf("%#o", perm), e.g. "0644"
//   - "modtime":     RFC3339 string (time.RFC3339)
//   - "uid", "gid": JSON numbers (decoded as float64 by encoding/json)
//   - "size":        JSON number (files only; consumed via parseItemSize)
//   - "hash.md5":    optional hex string; not consumed here
//
// Missing or unrecognised attribute values produce zero values; sensible defaults
// are applied by WriteTar: 0644 for files, 0755 for dirs, 0777 for links.
func parseItemAttrs(attrs map[string]any) (fs.FileMode, int, int, time.Time) {
	var mode fs.FileMode

	var uid, gid int

	var mtime time.Time

	// "permissions" is an octal string, e.g. "0644". Accept float64 as a
	// forward-compat fallback for hypothetical future numeric encoding.
	if v, ok := attrs["permissions"]; ok {
		switch p := v.(type) {
		case string:
			if n, parseErr := strconv.ParseUint(p, 8, 32); parseErr == nil {
				mode = fs.FileMode(n)
			}
		case float64:
			mode = fs.FileMode(uint32(p))
		}
	}

	if v, ok := attrs["uid"]; ok {
		if n, ok := v.(float64); ok {
			uid = int(n)
		}
	}

	if v, ok := attrs["gid"]; ok {
		if n, ok := v.(float64); ok {
			gid = int(n)
		}
	}

	// "modtime" is an RFC3339 string. Accept RFC3339Nano as a fallback for
	// sub-second precision if the exporter ever emits it.
	if v, ok := attrs["modtime"]; ok {
		if s, ok := v.(string); ok {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				t, err = time.Parse(time.RFC3339Nano, s)
			}

			if err == nil {
				mtime = t
			}
		}
	}

	return mode, uid, gid, mtime
}
