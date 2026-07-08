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
	"bufio"
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // not used for security; matches the exporter's own hash.md5 attribute
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	kgzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"golang.org/x/sync/errgroup"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
)

// ErrSourceHashMismatch is returned when a staged filesystem file's raw (decompressed)
// bytes do not match the MD5 digest the data-exporter reported for the source file,
// indicating wire-level corruption, a torn resume append, or a source/CLI disagreement
// that the local, self-referential archive.VerifyNode checksum cannot detect on its own.
var ErrSourceHashMismatch = errors.New("staged file does not match source-provided MD5 digest")

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
	md5      string // exporter-provided hex MD5 of the plaintext; empty if not reported
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
// chunkSize bounds the size of each Range-based chunk used to stage a file whose
// declared size is known (item.size > 0): every such file is staged via
// stageChunkedFile, reusing DownloadBlockChunks/MergeBlockChunks (the same
// durable, ".part"-resumable machinery the block-volume path uses) — a single
// chunk when size <= chunkSize, multiple chunks otherwise — so an interrupted
// download of ANY known-size file resumes from its last durably-persisted
// offset instead of restarting from byte zero. chunkSize <= 0 falls back to
// DefaultChunkSize. A file whose declared size is unknown (item.size <= 0 —
// either genuinely empty or the listing omitted the "size" attribute, which
// parseItemSize cannot tell apart) keeps the original single-shot GET +
// codec.EncodeStream path: chunk geometry needs a trustworthy total size up
// front, and there is no meaningful partial to resume for zero declared bytes.
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

	// Persist the declared per-file sizes (and their sum) now that the listing
	// is known, BEFORE any staging begins, so a crash-and-resume can recover
	// them from disk on the next run without a network round-trip — see
	// ReadFSSizesSidecar/ScanFSStagingSizes and pipeline.seedStreamFromDisk,
	// which read this sidecar to seed a resumed stream's total and credit
	// already-staged flat blobs before the DataExport is even Ready.
	if err := writeFSSizesSidecar(stagingDir, items); err != nil {
		return fmt.Errorf("persist fs sizes sidecar: %w", err)
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
		mode, uid, gid, mtime, md5Hex := parseItemAttrs(item.Attributes)

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
				md5:      md5Hex,
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
// choosing between two staging strategies based on whether the item's
// declared size is known:
//
//   - item.size > 0: the file is staged via stageChunkedFile, which fetches it
//     as one or more independent Range-based chunks (reusing
//     DownloadBlockChunks/MergeBlockChunks) so an interrupted download always
//     resumes from its last durably-persisted offset instead of restarting the
//     whole file — this applies regardless of chunkSize, so even a file well
//     below the chunk-size threshold gets a durable single-chunk partial.
//   - item.size <= 0 (declared size unknown — a genuinely empty file and a
//     listing that omitted "size" are indistinguishable, see parseItemSize):
//     the file is staged via stageWholeFile, the original single-shot GET +
//     codec.EncodeStream path, since chunk geometry requires a trustworthy
//     total size up front and there is no meaningful partial to resume for
//     zero declared bytes.
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

	if item.size > 0 {
		return stageChunkedFile(ctx, log, stagingDir, destPath, item, chunkSize, codec, fetcher, onProgress)
	}

	return stageWholeFile(ctx, log, destPath, item, codec, fetcher, onProgress)
}

// stageChunkedFile downloads item as one or more independent Range-based
// chunks into a per-file chunk directory and merges them into destPath,
// reusing the block-volume chunking machinery (DownloadBlockChunks/
// MergeBlockChunks) UNCHANGED. It is used for every file with a known
// declared size, even one that fits in a single chunk — a single-chunk
// download still gets a durable ".part" partial via downloadChunk, so an
// interrupt anywhere in the file resumes from its persisted offset instead of
// restarting from byte zero. workers is pinned to 1: chunks within one file
// download sequentially, inside that file's own already-allocated slot in the outer
// per-file errgroup (DownloadFilesystemVolume's g.SetLimit(workers) loop) —
// this deliberately avoids adding a third multiplicative concurrency
// dimension on top of node-workers × PerVolumeConcurrency. Already-present
// chunks are skipped and their raw length is credited to onProgress by
// downloadChunk's existing resume-skip path; no progress-crediting logic is
// duplicated here.
//
// MergeBlockChunks only concatenates already-encoded chunk frames — the
// plaintext never exists as a single blob on disk for a chunked file (each
// chunk's raw ".part" is discarded once its frame is written) — so the
// source-provided digest can only be checked once destPath holds the merged,
// still-compressed artifact: verifyStagedFileMD5 decodes it back to plaintext
// and compares. On a mismatch destPath is removed so the next run re-fetches
// and re-verifies from scratch rather than resuming from corrupt output. If
// item.md5 is empty, verification is skipped with a single WARN.
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

	if item.md5 == "" {
		log.Warn("no source MD5 available for file, skipping integrity verification",
			slog.String("path", item.relPath))

		return nil
	}

	if err := verifyStagedFileMD5(destPath, codec.Ext(), item.md5); err != nil {
		if removeErr := os.Remove(destPath); removeErr != nil && !os.IsNotExist(removeErr) {
			log.Warn("failed to remove corrupt staged file after MD5 mismatch",
				slog.String("path", destPath),
				slog.String("error", removeErr.Error()))
		}

		return fmt.Errorf("verify %s: %w", item.relPath, err)
	}

	return nil
}

// verifyStagedFileMD5 decodes the codec-compressed file at destPath (ext is
// codec.Ext(): "", ".zst", ".gz", or ".lz4") back to its raw plaintext and
// compares the plaintext's MD5 against wantHex, the exporter-provided source
// digest. Comparison is case-insensitive since both sides are lowercase hex
// in practice but neither format is a hard contract.
func verifyStagedFileMD5(destPath, ext, wantHex string) error {
	f, err := os.Open(destPath)
	if err != nil {
		return fmt.Errorf("open staged file %s: %w", destPath, err)
	}

	defer func() { _ = f.Close() }()

	hasher := md5.New() //nolint:gosec // matches the exporter's own hash.md5 attribute, not a security control

	if err := decodeVolumeStream(hasher, f, ext); err != nil {
		return fmt.Errorf("decode staged file %s: %w", destPath, err)
	}

	got := hex.EncodeToString(hasher.Sum(nil))
	if !strings.EqualFold(got, wantHex) {
		return fmt.Errorf("got md5 %s, source reports %s: %w", got, wantHex, ErrSourceHashMismatch)
	}

	return nil
}

// decodeVolumeStream streams the decompressed bytes of src into dst. ext identifies the
// codec exactly as codec.Ext() returns it, so it matches the concatenated-frame layout
// EncodeFrame/DownloadBlockChunks produced: zstd and gzip readers consume concatenated
// frames natively, but lz4.Reader stops at the end of one frame, so lz4 frames are
// decoded one at a time over a buffered, peekable source.
func decodeVolumeStream(dst io.Writer, src io.Reader, ext string) error {
	switch ext {
	case ".zst":
		zr, err := zstd.NewReader(src)
		if err != nil {
			return fmt.Errorf("open zstd reader: %w", err)
		}
		defer zr.Close()

		_, err = io.Copy(dst, zr)

		return err
	case ".gz":
		gr, err := kgzip.NewReader(src)
		if err != nil {
			return fmt.Errorf("open gzip reader: %w", err)
		}

		defer func() { _ = gr.Close() }()

		_, err = io.Copy(dst, gr)

		return err
	case ".lz4":
		return decodeLZ4Frames(dst, src)
	default:
		_, err := io.Copy(dst, src)

		return err
	}
}

// decodeLZ4Frames decodes a concatenation of independent lz4 frames from src into dst.
// lz4.Reader consumes exactly one frame per call, so a fresh reader is created per frame;
// a buffered reader lets Peek detect end-of-stream without consuming the next frame's bytes.
func decodeLZ4Frames(dst io.Writer, src io.Reader) error {
	br := bufio.NewReader(src)

	for {
		if _, err := br.Peek(1); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("peek lz4 source: %w", err)
		}

		if _, err := io.Copy(dst, lz4.NewReader(br)); err != nil {
			return fmt.Errorf("decode lz4 frame: %w", err)
		}
	}
}

// stageWholeFile downloads item in a single GET, compresses it with codec, and
// writes the result atomically to destPath. Compression is streaming: the HTTP
// body is piped through codec.EncodeStream so no whole-file buffering occurs.
//
// This is the fallback path for items whose declared size is unknown
// (item.size <= 0): stageChunkedFile's durable Range-based resume needs a
// trustworthy total size up front to compute chunk geometry, which an unknown
// size cannot provide, and there is no meaningful partial-download story for
// zero declared bytes anyway. Every item with a known size (item.size > 0)
// uses stageChunkedFile instead, regardless of its relation to chunkSize, so
// that a resumable ".part" exists for it even when it is only a single chunk.
//
// The raw plaintext streamed off the HTTP body is MD5-summed via an
// io.TeeReader placed BEFORE codec.EncodeStream, matching the exporter's own
// prepareAttributesMd5 (which hashes the plaintext file, not its compressed
// form). If item.md5 is empty (older exporter, or the item genuinely carries
// no digest) verification is skipped with a single WARN — never a hard
// failure, to stay compatible with an exporter that does not emit hash.md5.
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

	if item.md5 == "" {
		log.Warn("no source MD5 available for file, skipping integrity verification",
			slog.String("path", item.relPath))
	}

	cr := &countingReader{r: body, onProgress: onProgress}
	hasher := md5.New() //nolint:gosec // matches the exporter's own hash.md5 attribute, not a security control
	src := io.TeeReader(cr, hasher)

	aw, err := archive.NewAtomicWriter(destPath)
	if err != nil {
		return fmt.Errorf("open atomic writer for %s: %w", destPath, err)
	}

	if err := codec.EncodeStream(aw, src); err != nil {
		aw.Abort()

		return fmt.Errorf("stage %s: %w", item.relPath, err)
	}

	if item.md5 != "" {
		if got := hex.EncodeToString(hasher.Sum(nil)); !strings.EqualFold(got, item.md5) {
			aw.Abort()

			return fmt.Errorf("stage %s: got md5 %s, source reports %s: %w", item.relPath, got, item.md5, ErrSourceHashMismatch)
		}
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit staging %s: %w", destPath, err)
	}

	log.Debug("staging file written", slog.String("path", item.relPath))

	return nil
}

// ScanFSStagingProgress computes durably-committed raw bytes across every
// still-open per-file chunk directory under stagingDir, purely from local
// state — no network call. A per-file chunk directory is identified by the
// presence of a readable chunks.meta sidecar — the same marker
// createChunkDir writes for both block volumes and per-file FS chunks (see
// stageChunkedFile, which reuses DownloadBlockChunks/MergeBlockChunks
// unchanged) — and its contribution is computed via the identical
// ScanBlockChunkProgress formula.
//
// Deliberately excluded: a file that is ALREADY fully staged (its chunk
// directory has already been merged away by MergeBlockChunks into a flat
// <relPath><ext> blob) contributes nothing here, because its original raw
// declared size is not recoverable from disk once the chunk dir — the only
// place that size was ever recorded (chunks.meta) — is gone; the merged
// blob's own on-disk length is a compressed/frame-concatenated size, not the
// raw size the rest of the progress accounting uses. Such a file keeps being
// credited exactly once, at its true declared size, by
// stageCompressedFile's existing resume-skip path once the listing confirms
// it; the caller must not double-count that credit against this scan (see
// pipeline.downloadFS, which cancels this exact seed back out via
// progress.Stream.SetCurrent(0) before staging begins).
func ScanFSStagingProgress(stagingDir, ext string) (int64, error) {
	if _, err := os.Stat(stagingDir); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}

		return 0, fmt.Errorf("stat staging dir %s: %w", stagingDir, err)
	}

	var committed int64

	walkErr := filepath.WalkDir(stagingDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if path == stagingDir || !d.IsDir() {
			return nil
		}

		_, found, metaErr := archive.ReadChunkMeta(path)
		if metaErr != nil || !found {
			return nil
		}

		fileCommitted, _, scanErr := ScanBlockChunkProgress(path, ext)
		if scanErr != nil {
			return scanErr
		}

		committed += fileCommitted

		return fs.SkipDir
	})
	if walkErr != nil {
		return 0, fmt.Errorf("scan fs staging progress in %s: %w", stagingDir, walkErr)
	}

	return committed, nil
}

// FSSizesSidecarName is the durable JSON sidecar recording per-file declared
// sizes for a filesystem volume, written to stagingDir as soon as the
// listing is first fetched. It intentionally does not end in ".tmp" so
// archive.resume.go's removeTmpFiles never deletes it, and it lives inside
// the FS staging dir (data.tar.d/) so it is removed along with the rest of
// the staging state once the tar is assembled and is excluded from the node
// checksum exactly like every other staging-dir file (see
// archive.ComputeNodeChecksum, which never walks the flat single-volume
// staging directory at all).
const FSSizesSidecarName = "sizes.json"

// FSSizesSidecar records the per-file declared content sizes and their sum
// for one filesystem volume, as known at listing time. relPath matches
// fsItem.relPath (the tar entry's source path, BEFORE the codec extension is
// appended) so it can be joined with the codec extension to locate a file's
// staged blob.
type FSSizesSidecar struct {
	Files map[string]int64 `json:"files"`
	Total int64            `json:"total"`
}

// writeFSSizesSidecar persists items' declared sizes to stagingDir as
// FSSizesSidecarName, fsynced via archive.WriteFileAtomic. Only "file" items
// with a known positive size are recorded — an unknown/zero declared size
// (see parseItemSize) already contributes nothing to sumFileSizes, so
// recording it would only bloat the sidecar without ever being credited.
func writeFSSizesSidecar(stagingDir string, items []fsItem) error {
	sizes := FSSizesSidecar{
		Files: make(map[string]int64, len(items)),
		Total: sumFileSizes(items),
	}

	for _, it := range items {
		if it.itemType == "file" && it.size > 0 {
			sizes.Files[it.relPath] = it.size
		}
	}

	data, err := json.Marshal(sizes)
	if err != nil {
		return fmt.Errorf("marshal fs sizes sidecar: %w", err)
	}

	path := filepath.Join(stagingDir, FSSizesSidecarName)

	if err := archive.WriteFileAtomic(path, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write fs sizes sidecar %s: %w", path, err)
	}

	return nil
}

// ReadFSSizesSidecar reads the sidecar written by writeFSSizesSidecar from
// stagingDir. found is false (with a nil error) when the sidecar does not
// exist — a from-scratch run, or a staging dir left by a run that predates
// this feature — which callers must treat as "no persisted sizes available",
// not as a legitimate zero total.
func ReadFSSizesSidecar(stagingDir string) (FSSizesSidecar, bool, error) {
	path := filepath.Join(stagingDir, FSSizesSidecarName)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return FSSizesSidecar{}, false, nil
		}

		return FSSizesSidecar{}, false, fmt.Errorf("read fs sizes sidecar %s: %w", path, err)
	}

	var sizes FSSizesSidecar

	if err := json.Unmarshal(data, &sizes); err != nil {
		return FSSizesSidecar{}, false, fmt.Errorf("parse fs sizes sidecar %s: %w", path, err)
	}

	return sizes, true, nil
}

// ScanFSStagingSizes reads the sizes sidecar from stagingDir and, for every
// file it records, credits its persisted declared size when that file has
// ALREADY been fully staged as a flat <relPath><ext> blob — i.e. its chunk
// directory was already merged away by MergeBlockChunks, or it was written
// whole by stageWholeFile. This is the complement to ScanFSStagingProgress,
// which by construction can only see STILL-OPEN chunk directories: once a
// chunk dir is merged away, chunks.meta — the only on-disk record of that
// file's raw declared size — goes with it, so the sidecar is the only way to
// credit an already-completed file without a network round-trip.
//
// found is false (with zero totals, no error) when no sidecar exists yet —
// a from-scratch run, or a staging dir predating this feature — so the
// caller knows to fall back to the network-driven total/credit path instead
// of trusting a zero total.
func ScanFSStagingSizes(stagingDir, ext string) (int64, int64, bool, error) {
	sizes, found, err := ReadFSSizesSidecar(stagingDir)
	if err != nil {
		return 0, 0, false, err
	}

	if !found {
		return 0, 0, false, nil
	}

	var staged int64

	for relPath, size := range sizes.Files {
		destPath := filepath.Join(stagingDir, filepath.FromSlash(relPath+ext))

		info, statErr := os.Stat(destPath)
		if statErr != nil || info.IsDir() {
			continue
		}

		staged += size
	}

	return sizes.Total, staged, true, nil
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
// prepareAttributesStat/prepareAttributesMd5) emits these keys and types:
//   - "permissions": octal string via fmt.Sprintf("%#o", perm), e.g. "0644"
//   - "modtime":     RFC3339 string (time.RFC3339)
//   - "uid", "gid": JSON numbers (decoded as float64 by encoding/json)
//   - "size":        JSON number (files only; consumed via parseItemSize)
//   - "hash.md5":    hex string, present only for regular files and only when the
//     listing request carries attribute=hash.md5 (see exporter.ListDir)
//
// Missing or unrecognised attribute values produce zero values; sensible defaults
// are applied by WriteTar: 0644 for files, 0755 for dirs, 0777 for links. The returned
// md5 is the empty string when the exporter reported no digest for this item
// (directories/links, or an older exporter that never emits hash.md5).
func parseItemAttrs(attrs map[string]any) (fs.FileMode, int, int, time.Time, string) {
	var mode fs.FileMode

	var uid, gid int

	var mtime time.Time

	var md5Hex string

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

	if v, ok := attrs["hash.md5"]; ok {
		if s, ok := v.(string); ok {
			md5Hex = s
		}
	}

	return mode, uid, gid, mtime, md5Hex
}
