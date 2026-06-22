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
	"fmt"
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

// fsItem is one file-system entry collected from the data-exporter listing.
// It holds both the download URL (for file type) and the metadata needed to
// build a TarEntry for the final data.tar assembly.
type fsItem struct {
	relPath  string
	itemType string // "file", "dir", or "link"
	uri      string // download URL; non-empty only for itemType == "file"
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
func DownloadFilesystemVolume(
	ctx context.Context,
	log *slog.Logger,
	tarPath string,
	stagingDir string,
	filesRootURL string,
	workers int,
	fetcher *exporter.Fetcher,
	codec compress.Codec,
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
			return stageCompressedFile(gctx, log, stagingDir, item, codec, fetcher)
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

	if err := WriteTar(tarPath, stagingDir, entries); err != nil {
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

// stageCompressedFile downloads one file, compresses it with codec, and writes
// the result atomically to stagingDir/<relPath><codec.Ext()>.
// Already-complete destination files are skipped (resume).
// Stale <destPath>.tmp files are removed before the download attempt.
// Compression is streaming: the HTTP body is piped through codec.EncodeStream
// so no whole-file buffering occurs.
func stageCompressedFile(
	ctx context.Context,
	log *slog.Logger,
	stagingDir string,
	item fsItem,
	codec compress.Codec,
	fetcher *exporter.Fetcher,
) error {
	destPath := filepath.Join(stagingDir, filepath.FromSlash(item.relPath+codec.Ext()))

	if _, err := os.Stat(destPath); err == nil {
		log.Info("staging file already present, skipping", slog.String("path", item.relPath))

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

	log.Info("staging fs file", slog.String("path", item.relPath))

	body, err := fetcher.GetFile(ctx, item.uri)
	if err != nil {
		return fmt.Errorf("GET %s: %w", item.uri, err)
	}

	defer func() { _ = body.Close() }()

	aw, err := archive.NewAtomicWriter(destPath)
	if err != nil {
		return fmt.Errorf("open atomic writer for %s: %w", destPath, err)
	}

	if err := codec.EncodeStream(aw, body); err != nil {
		aw.Abort()

		return fmt.Errorf("stage %s: %w", item.relPath, err)
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit staging %s: %w", destPath, err)
	}

	log.Info("staging file written", slog.String("path", item.relPath))

	return nil
}

// parseItemAttrs extracts file metadata from the data-exporter listing attributes map.
// The real data-exporter (storage-volume-data-manager, images/data-exporter,
// prepareAttributesStat) emits these keys and types:
//   - "permissions": octal string via fmt.Sprintf("%#o", perm), e.g. "0644"
//   - "modtime":     RFC3339 string (time.RFC3339)
//   - "uid", "gid": JSON numbers (decoded as float64 by encoding/json)
//   - "size":        JSON number (files only; not consumed here)
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
