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

// ErrUnsafePath is returned when a path, symlink target, or download URI supplied
// by the data-exporter listing fails safety validation. The listing (item.Name,
// item.TargetPath, item.URI) is untrusted input from a potentially compromised or
// buggy exporter: it MUST be validated before ever being used in a filepath.Join,
// written into a tar header, or turned into an HTTP request, or a malicious
// response could stage files outside the intended directory (path traversal /
// zip-slip), materialize a symlink that escapes the extracted tree on restore, or
// redirect a credential-bearing per-file GET to a foreign origin (token
// exfiltration / SSRF / bytes sourced from an attacker host). See collectAllFSItems
// (the name/relPath and same-origin URI ingestion checkpoint) and tar.go's
// writeLinkEntry (the symlink-target write guard).
var ErrUnsafePath = errors.New("server-provided path is unsafe")

// sanitizeRelPath validates that p is safe to treat as a "/"-separated path relative to
// the volume root: non-empty, free of NUL/control bytes and non-"/" OS separators, not
// absolute, containing no empty/"."/".." element, and not entering the reserved metadata
// namespace (its FIRST segment must not equal FSMetaDirName). A real data-exporter listing
// entry name is a literal directory entry — "", "." and ".." are not valid filenames on any
// filesystem — so rejecting them here can never reject a legitimate listing, only a
// malicious or corrupted one. The reserved-namespace check keys on the first segment
// because only a root-level ".d8-meta" collides with stagingDir/.d8-meta; a nested
// "sub/.d8-meta" stages under a user subtree and is harmless (inv. #10a). On success it
// returns p unchanged: the result is safe to convert with filepath.FromSlash for a
// filepath.Join. It is fed the FULL relative path (relPrefix + leaf) at the single
// ingestion checkpoint (collectAllFSItems), so the first-segment guard sees the whole path.
func sanitizeRelPath(p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("%w: empty name", ErrUnsafePath)
	}

	for _, r := range p {
		if r < 0x20 || r == 0x7f {
			return "", fmt.Errorf("%w: control byte in %q", ErrUnsafePath, p)
		}
	}

	if strings.ContainsRune(p, '\\') {
		return "", fmt.Errorf("%w: OS separator in %q", ErrUnsafePath, p)
	}

	if strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("%w: absolute path %q", ErrUnsafePath, p)
	}

	segs := strings.Split(p, "/")

	for _, seg := range segs {
		if seg == "" {
			return "", fmt.Errorf("%w: empty path element in %q", ErrUnsafePath, p)
		}

		if seg == "." || seg == ".." {
			return "", fmt.Errorf("%w: %q element in %q", ErrUnsafePath, seg, p)
		}
	}

	// The staging dir reserves FSMetaDirName for the download machinery's own
	// artifacts (the sizes sidecar); enforce that namespace at this single
	// ingestion checkpoint so no server-provided path can ever stage into it.
	// Only the FIRST segment collides: stagingDir/<relPath><ext> puts a
	// root-level ".d8-meta" entry at the exact path we own, whereas a deeper
	// "a/.d8-meta" lands under a user subtree and is harmless.
	if segs[0] == FSMetaDirName {
		return "", fmt.Errorf("%w: %q is the reserved metadata namespace in %q", ErrUnsafePath, FSMetaDirName, p)
	}

	return p, nil
}

// countingReader wraps an io.Reader and reports raw bytes read to onProgress
// incrementally, as the stream is consumed, so a byte-progress bar advances
// during FS file staging instead of jumping from 0% to 100% in one frame.
type countingReader struct {
	r          io.Reader
	onProgress func(n int)
	n          int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)

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
	size     int64  // declared content size from the listing; -1 means absent or invalid
	rawSize  int64  // exact plaintext size carried into the final tar PAX metadata
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
// DefaultChunkSize. A file whose declared size is unknown (item.size < 0) or
// exactly zero keeps the original single-shot GET +
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

	for i := range items {
		if items[i].itemType != "file" {
			continue
		}

		itemIndex := i
		item := items[itemIndex]

		g.Go(func() error {
			rawSize, stageErr := stageCompressedFile(gctx, log, stagingDir, item, chunkSize, codec, fetcher, onProgress)
			if stageErr != nil {
				return stageErr
			}

			items[itemIndex].rawSize = rawSize

			return nil
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
			RelPath:      relPath,
			Type:         it.itemType,
			Codec:        codec.Name(),
			OriginalPath: it.relPath,
			RawSize:      it.rawSize,
			Mode:         it.mode,
			UID:          it.uid,
			GID:          it.gid,
			Mtime:        it.mtime,
			Linkname:     it.linkname,
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
// all representable items (file, dir, link) with their relative paths and metadata.
// filesRootURL is the absolute URL of the volume root used to resolve relative URIs.
// relPrefix is prepended to item names when building the relative path.
func collectAllFSItems(ctx context.Context, fetcher *exporter.Fetcher, dirURL string, base *url.URL, relPrefix string) ([]fsItem, error) {
	items, err := fetcher.ListDir(ctx, dirURL)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", dirURL, err)
	}

	var result []fsItem

	for _, item := range items {
		// Validate the FULL relative path (relPrefix is already sanitized) so the
		// reserved-namespace guard keys on the whole path's first segment: a
		// root-level ".d8-meta" is rejected, a nested "sub/.d8-meta" allowed.
		relPath, nameErr := sanitizeRelPath(relPrefix + item.Name)
		if nameErr != nil {
			return nil, fmt.Errorf("listing %s: %w", dirURL, nameErr)
		}

		ref, err := url.Parse(item.URI)
		if err != nil {
			return nil, fmt.Errorf("parse item URI %q: %w", item.URI, err)
		}

		resolved := base.ResolveReference(ref)

		// The listing is untrusted input (same threat model as sanitizeRelPath):
		// per RFC 3986 an ABSOLUTE item.URI fully REPLACES base, so one hostile
		// entry could redirect a per-file GET (stageWholeFile's GetFile,
		// stageChunkedFile's RangeGet, or a recursive sub-listing) to an
		// attacker-controlled origin. Every such request rides the
		// credential-bearing SafeClient transport, and client-go's
		// bearer-token/basic-auth round-trippers attach the cluster credential to
		// EVERY request regardless of destination host — so a foreign-host entry
		// would exfiltrate the token, and source archive bytes (whose only
		// integrity proof, the MD5, comes from the same listing) from a foreign
		// origin. The real exporter only ever emits relative URIs under its own
		// server, so any resolved URI that leaves base's origin is rejected here,
		// at the single ingestion checkpoint, before any fetch is issued. This
		// covers file URIs and — because the guard precedes the type switch — the
		// recursive directory walk's absURI as well.
		if !sameOrigin(base, resolved) {
			return nil, fmt.Errorf("listing %s: %w: item URI %q resolves to %q, off the files-root origin %q",
				dirURL, ErrUnsafePath, item.URI, resolved.String(), base.String())
		}

		absURI := resolved.String()
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
			return nil, fmt.Errorf("filesystem listing item %q has unsupported wire type %q", relPath, item.Type)
		}
	}

	return result, nil
}

// sameOrigin reports whether resolved shares the DataExport files-root origin of
// base: same scheme and same host:port, with the scheme's default port
// normalized so http://h and http://h:80 (and https://h and https://h:443) are
// treated as the same origin. A relative item.URI always resolves to base's own
// scheme/host, so it passes unchanged; only an absolute URI that names a
// different scheme, host, or explicit non-default port is rejected. Scheme and
// host are compared case-insensitively (url.Parse lowercases the scheme but not
// the host).
func sameOrigin(base, resolved *url.URL) bool {
	return strings.EqualFold(base.Scheme, resolved.Scheme) &&
		strings.EqualFold(canonicalHostPort(base), canonicalHostPort(resolved))
}

// canonicalHostPort returns u's host with an explicit port, substituting the
// scheme's default port when u carries none, so a same-origin comparison is not
// fooled by an omitted-vs-explicit default port.
func canonicalHostPort(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = defaultPortForScheme(u.Scheme)
	}

	return u.Hostname() + ":" + port
}

// defaultPortForScheme returns the well-known default port for scheme, or "" for
// a scheme with no known default (in which case two portless URLs of that scheme
// still compare equal on the empty port).
func defaultPortForScheme(scheme string) string {
	switch strings.ToLower(scheme) {
	case "https":
		return "443"
	case "http":
		return "80"
	default:
		return ""
	}
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
//   - item.size <= 0: an unknown-size or empty file is staged via
//     stageWholeFile, the original single-shot GET + codec.EncodeStream path,
//     since chunk geometry requires a trustworthy positive total size and
//     there is no meaningful partial to resume for zero declared bytes.
//
// A destination file left by a prior run is reused (resume) only after its
// content is re-verified, never on os.Stat success alone: existence is not
// integrity. A same-named staged blob can be stale, foreign (left by a
// different snapshot of the same source object), or truncated by an unrelated
// crash, and it would otherwise be packed into data.tar verbatim (inv. #9).
// When the exporter advertised an MD5, the already-staged bytes are decoded
// and re-hashed via verifyStagedFileMD5 before the skip is trusted; on a
// mismatch the bad blob is removed and staging falls through to re-fetch it in
// this same run (a self-healing condition, not a hard error). When no MD5 is
// advertised the blob is still skipped, matching the fresh-path convention,
// with a one-line WARN. The verify costs one decode pass per already-staged
// file per resume run, bounded by staging size — the price of not trusting
// bytes we did not just write. A trusted skip still credits the item's
// declared size to onProgress so the numerator can reach the denominator that
// setTotal established from the same declared sizes (sumFileSizes) — otherwise
// a partially-staged resume could never advance the progress bar to 100% even
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
) (int64, error) {
	destPath := filepath.Join(stagingDir, filepath.FromSlash(item.relPath+codec.Ext()))

	if _, err := os.Stat(destPath); err == nil {
		var (
			verifyErr error
			rawSize   int64
		)

		if item.md5 == "" {
			log.Warn("no source MD5 available for file, skipping integrity verification",
				slog.String("path", item.relPath))

			if item.size >= 0 {
				rawSize = item.size
			} else {
				rawSize, verifyErr = stagedFileRawSize(destPath, codec.Ext())
			}
		} else {
			rawSize, verifyErr = verifyStagedFileMD5(destPath, codec.Ext(), item.md5)
		}

		if verifyErr == nil && item.size >= 0 && rawSize != item.size {
			verifyErr = fmt.Errorf("staged plaintext size %d differs from listing size %d", rawSize, item.size)
		}

		if verifyErr == nil {
			log.Debug("staging file already present, skipping", slog.String("path", item.relPath))

			if onProgress != nil && item.size > 0 {
				onProgress(int(item.size))
			}

			return rawSize, nil
		}

		// The staged bytes do not match the source digest: a stale, foreign, or
		// truncated blob. Drop it and fall through to re-stage in this same run
		// rather than failing the download — this is self-healing, not an error.
		log.Warn("staged file failed source MD5 re-check on resume, re-staging",
			slog.String("path", destPath),
			slog.String("error", verifyErr.Error()))

		if removeErr := os.Remove(destPath); removeErr != nil && !os.IsNotExist(removeErr) {
			return 0, fmt.Errorf("remove mismatched staged file %s: %w", destPath, removeErr)
		}
	}

	tmpPath := destPath + ".tmp"

	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("remove stale tmp %s: %w", tmpPath, err)
	}

	parentDir := filepath.Dir(destPath)

	if err := archive.EnsureDir(parentDir); err != nil {
		return 0, fmt.Errorf("create parent dir %s: %w", parentDir, err)
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
) (int64, error) {
	chunkDirName := archive.FsFileChunksDirName(item.relPath, codec.Ext())
	chunkDir := filepath.Join(stagingDir, filepath.FromSlash(chunkDirName))

	if err := DownloadBlockChunks(ctx, log, chunkDir, item.uri, item.size, chunkSize, 1, fetcher, codec, onProgress); err != nil {
		return 0, fmt.Errorf("download chunks for %s: %w", item.relPath, err)
	}

	if err := MergeBlockChunks(ctx, chunkDir, destPath, item.size, chunkSize, codec.Ext()); err != nil {
		return 0, fmt.Errorf("merge chunks for %s: %w", item.relPath, err)
	}

	if item.md5 == "" {
		log.Warn("no source MD5 available for file, skipping integrity verification",
			slog.String("path", item.relPath))

		return item.size, nil
	}

	rawSize, err := verifyStagedFileMD5(destPath, codec.Ext(), item.md5)
	if err != nil {
		if removeErr := os.Remove(destPath); removeErr != nil && !os.IsNotExist(removeErr) {
			log.Warn("failed to remove corrupt staged file after MD5 mismatch",
				slog.String("path", destPath),
				slog.String("error", removeErr.Error()))
		}

		return 0, fmt.Errorf("verify %s: %w", item.relPath, err)
	}

	if rawSize != item.size {
		return 0, fmt.Errorf("verify %s: staged plaintext size %d differs from listing size %d",
			item.relPath, rawSize, item.size)
	}

	return rawSize, nil
}

// verifyStagedFileMD5 decodes the codec-compressed file at destPath (ext is
// codec.Ext(): "", ".zst", ".gz", or ".lz4") back to its raw plaintext and
// compares the plaintext's MD5 against wantHex, the exporter-provided source
// digest. Comparison is case-insensitive since both sides are lowercase hex
// in practice but neither format is a hard contract.
func verifyStagedFileMD5(destPath, ext, wantHex string) (int64, error) {
	f, err := os.Open(destPath)
	if err != nil {
		return 0, fmt.Errorf("open staged file %s: %w", destPath, err)
	}

	defer func() { _ = f.Close() }()

	hasher := md5.New() //nolint:gosec // matches the exporter's own hash.md5 attribute, not a security control
	counter := &countingWriter{}

	if err := decodeVolumeStream(io.MultiWriter(hasher, counter), f, ext); err != nil {
		return 0, fmt.Errorf("decode staged file %s: %w", destPath, err)
	}

	got := hex.EncodeToString(hasher.Sum(nil))
	if !strings.EqualFold(got, wantHex) {
		return 0, fmt.Errorf("got md5 %s, source reports %s: %w", got, wantHex, ErrSourceHashMismatch)
	}

	return counter.n, nil
}

type countingWriter struct {
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))

	return len(p), nil
}

func stagedFileRawSize(destPath, ext string) (int64, error) {
	f, err := os.Open(destPath)
	if err != nil {
		return 0, fmt.Errorf("open staged file %s: %w", destPath, err)
	}

	counter := &countingWriter{}
	decodeErr := decodeVolumeStream(counter, f, ext)
	closeErr := f.Close()

	if decodeErr != nil {
		return 0, fmt.Errorf("decode staged file %s: %w", destPath, decodeErr)
	}

	if closeErr != nil {
		return 0, fmt.Errorf("close staged file %s: %w", destPath, closeErr)
	}

	return counter.n, nil
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
// This is the fallback path for items whose declared size is unknown or zero
// (item.size <= 0): stageChunkedFile's durable Range-based resume needs a
// trustworthy positive total size up front to compute chunk geometry, and
// there is no meaningful partial-download story for zero declared bytes.
// Every item with a known positive size (item.size > 0)
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
) (int64, error) {
	log.Debug("staging fs file", slog.String("path", item.relPath))

	body, err := fetcher.GetFile(ctx, item.uri)
	if err != nil {
		return 0, fmt.Errorf("GET %s: %w", item.uri, err)
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
		return 0, fmt.Errorf("open atomic writer for %s: %w", destPath, err)
	}

	if err := codec.EncodeStream(aw, src); err != nil {
		aw.Abort()

		return 0, fmt.Errorf("stage %s: %w", item.relPath, err)
	}

	if item.size >= 0 && cr.n != item.size {
		aw.Abort()

		return 0, fmt.Errorf("stage %s: observed plaintext size %d differs from listing size %d",
			item.relPath, cr.n, item.size)
	}

	if item.md5 != "" {
		if got := hex.EncodeToString(hasher.Sum(nil)); !strings.EqualFold(got, item.md5) {
			aw.Abort()

			return 0, fmt.Errorf("stage %s: got md5 %s, source reports %s: %w", item.relPath, got, item.md5, ErrSourceHashMismatch)
		}
	}

	if err := aw.Commit(); err != nil {
		return 0, fmt.Errorf("commit staging %s: %w", destPath, err)
	}

	log.Debug("staging file written", slog.String("path", item.relPath))

	return cr.n, nil
}

// ScanFSStagingProgress computes durably-committed raw bytes across every
// still-open per-file chunk directory, purely from local state — no network
// call. Per-file chunk dirs live under the reserved metadata namespace
// (stagingDir/.d8-meta/chunks/<relPath><ext>.d) so no server-provided path can
// alias one, so this scans ONLY that subtree. A per-file chunk directory is
// identified by the presence of a readable chunks.meta sidecar — the same
// marker createChunkDir writes for both block volumes and per-file FS chunks
// (see stageChunkedFile, which reuses DownloadBlockChunks/MergeBlockChunks
// unchanged) — and its contribution is computed via the identical
// ScanBlockChunkProgress formula.
//
// Deliberately excluded:
//   - The sizes sidecar (also under .d8-meta) carries no chunks.meta, so it is
//     naturally ignored; scanning only the chunks/ subtree makes that explicit.
//   - A file that is ALREADY fully staged (its chunk directory has already been
//     merged away by MergeBlockChunks into a flat <relPath><ext> blob at the
//     staging root) contributes nothing here, because its original raw declared
//     size is not recoverable from disk once the chunk dir — the only place that
//     size was ever recorded (chunks.meta) — is gone; the merged blob's own
//     on-disk length is a compressed/frame-concatenated size, not the raw size
//     the rest of the progress accounting uses. Such a file keeps being credited
//     exactly once, at its true declared size, by stageCompressedFile's existing
//     resume-skip path once the listing confirms it; the caller must not
//     double-count that credit against this scan (see pipeline.downloadFS, which
//     wraps its onProgress with pipeline.skipSeededBytes(seeded, ...) so that
//     later re-derived credit is discarded instead of double-counted, rather
//     than resetting the stream to 0 before staging begins).
//   - Legacy flat chunk dirs from trees written before the relocation
//     (stagingDir/<relPath><ext>.d) are not scanned; such a file re-downloads
//     once, which is acceptable and preferable to risking a user-blob alias.
func ScanFSStagingProgress(stagingDir, ext string) (int64, error) {
	chunksRoot := filepath.Join(stagingDir, FSMetaDirName, archive.FSChunksDirName)

	if _, err := os.Stat(chunksRoot); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}

		return 0, fmt.Errorf("stat fs chunks dir %s: %w", chunksRoot, err)
	}

	var committed int64

	walkErr := filepath.WalkDir(chunksRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
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
		return 0, fmt.Errorf("scan fs staging progress in %s: %w", chunksRoot, walkErr)
	}

	return committed, nil
}

// FSMetaDirName is the reserved metadata subdirectory of an FS staging dir
// (data.tar.d/) holding the download machinery's own internal artifacts: the
// sizes sidecar and, under archive.FSChunksDirName, every per-file chunk
// directory. It is dot-prefixed and clearly-internal, and sanitizeRelPath
// rejects any server-provided path whose FIRST segment equals it, so no
// user/server file can ever stage into this namespace — including at codec none
// (ext == ""), where a user file literally named "sizes.json" (or a "<x>.d"
// chunk-dir-shaped name) would otherwise stage into the staging root and be
// silently replaced by, or delete, an internal artifact (inv. #10a). Keeping
// internal artifacts under this dir makes the staged-blob namespace belong to
// server-provided paths only. Everything under it lives inside the staging dir
// so it is removed with the rest of the staging state on tar assembly, and is
// excluded from the node checksum exactly like every other staging-dir file
// (archive.ComputeNodeChecksum never walks the flat single-volume staging
// directory at all). The SSOT for the literal name is archive.FSMetaDirName;
// this is an alias so the volume package (sanitizeRelPath, the sidecar helpers)
// can reference it without importing it indirectly.
const FSMetaDirName = archive.FSMetaDirName

// FSSizesSidecarName is the durable JSON sidecar recording per-file declared
// sizes for a filesystem volume, written under FSMetaDirName as soon as the
// listing is first fetched (stagingDir/.d8-meta/sizes.json). Reads fall back to
// a legacy stagingDir/sizes.json only when the reserved-namespace file is
// absent (see ReadFSSizesSidecar).
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

// writeFSSizesSidecar persists items' declared sizes under stagingDir's
// reserved metadata dir (stagingDir/.d8-meta/sizes.json), fsynced via
// archive.WriteFileAtomic. Only "file" items with a known positive size are
// recorded — an unknown or zero declared size (see parseItemSize) already
// contributes nothing to sumFileSizes, so recording it would only bloat the
// sidecar without ever being credited. Writing under FSMetaDirName (never the
// staging root) guarantees the sidecar cannot shadow a user file named
// "sizes.json" at codec none (inv. #10a).
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

	metaDir := filepath.Join(stagingDir, FSMetaDirName)

	if err := archive.EnsureDir(metaDir); err != nil {
		return fmt.Errorf("create fs metadata dir %s: %w", metaDir, err)
	}

	path := filepath.Join(metaDir, FSSizesSidecarName)

	if err := archive.WriteFileAtomic(path, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write fs sizes sidecar %s: %w", path, err)
	}

	return nil
}

// ReadFSSizesSidecar reads the sidecar written by writeFSSizesSidecar. It first
// reads the reserved-namespace path (stagingDir/.d8-meta/sizes.json); only when
// that file is absent does it fall back to the legacy stagingDir/sizes.json
// written by runs predating the reserved metadata namespace. found is false
// (with a nil error) when no sidecar exists at either location — a from-scratch
// run, or a staging dir predating this feature — which callers must treat as
// "no persisted sizes available", not as a legitimate zero total.
//
// The sidecar is a best-effort display/seed aid only; correctness never depends
// on it (see pipeline.seedStreamFromDisk). That is what makes the conservative
// legacy handling safe: at codec none a user file literally named "sizes.json"
// could occupy the legacy path, so a legacy file that does not parse as an
// FSSizesSidecar is treated as possible user data — left untouched, reported as
// not-found — rather than risking a misread of (or worse, a write over) user
// bytes. A lost seed is the worst outcome; a wrong-bytes outcome never is.
func ReadFSSizesSidecar(stagingDir string) (FSSizesSidecar, bool, error) {
	path := filepath.Join(stagingDir, FSMetaDirName, FSSizesSidecarName)

	data, err := os.ReadFile(path)
	if err == nil {
		var sizes FSSizesSidecar

		if err := json.Unmarshal(data, &sizes); err != nil {
			return FSSizesSidecar{}, false, fmt.Errorf("parse fs sizes sidecar %s: %w", path, err)
		}

		return sizes, true, nil
	}

	if !os.IsNotExist(err) {
		return FSSizesSidecar{}, false, fmt.Errorf("read fs sizes sidecar %s: %w", path, err)
	}

	return readLegacyFSSizesSidecar(stagingDir)
}

// readLegacyFSSizesSidecar reads the pre-metadata-namespace sidecar location,
// stagingDir/sizes.json. It exists purely to keep resume-seeding working when
// resuming a tree written before the sidecar moved under FSMetaDirName. An
// unparseable legacy file is treated as possible user data (see
// ReadFSSizesSidecar): reported as not-found, never deleted or overwritten.
func readLegacyFSSizesSidecar(stagingDir string) (FSSizesSidecar, bool, error) {
	path := filepath.Join(stagingDir, FSSizesSidecarName)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return FSSizesSidecar{}, false, nil
		}

		return FSSizesSidecar{}, false, fmt.Errorf("read legacy fs sizes sidecar %s: %w", path, err)
	}

	var sizes FSSizesSidecar

	if err := json.Unmarshal(data, &sizes); err != nil {
		return FSSizesSidecar{}, false, nil
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
// the collected listing. Items whose size is unknown or zero contribute nothing.
func sumFileSizes(items []fsItem) int64 {
	var total int64

	for _, it := range items {
		if it.itemType == "file" && it.size > 0 {
			total += it.size
		}
	}

	return total
}

// parseItemSize extracts the "size" attribute from a data-exporter listing item.
// The real exporter emits it as a JSON number (decoded as float64 by encoding/json);
// json.Number is also handled in case a decoder is configured with UseNumber.
// Missing, negative, fractional, or non-numeric values yield -1. Zero is
// preserved because it is the exact size of an empty regular file.
func parseItemSize(attrs map[string]any) int64 {
	v, ok := attrs["size"]
	if !ok {
		return -1
	}

	switch n := v.(type) {
	case float64:
		if n < 0 || n != float64(int64(n)) {
			return -1
		}

		return int64(n)
	case json.Number:
		i, err := n.Int64()
		if err != nil || i < 0 {
			return -1
		}

		return i
	case int64:
		if n < 0 {
			return -1
		}

		return n
	case int:
		if n < 0 {
			return -1
		}

		return int64(n)
	default:
		return -1
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
