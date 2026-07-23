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
	"cmp"
	"context"
	"crypto/md5" //nolint:gosec // not used for security; matches the exporter's own hash.md5 attribute
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"slices"
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
// exfiltration / SSRF / bytes sourced from an attacker host). See
// inventoryItemFromListing (the name/relPath and same-origin URI ingestion
// checkpoint) and tar.go's
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
// ingestion checkpoint (inventoryItemFromListing), so the first-segment guard
// sees the whole path.
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
	mode     fs.FileMode
	uid      int
	gid      int
	mtime    time.Time
	linkname string // symlink target; non-empty only for itemType == "link"
	md5      string // exporter-provided hex MD5 fetched after inventory; empty if not reported
}

const (
	fsInventoryName          = "inventory.jsonl"
	fsInventoryWorkDirName   = "inventory.work"
	fsInventoryVersion       = 1
	fsInventorySortBatchSize = 256
	fsInventoryMergeFanIn    = 8
	fsInventoryMaxRecordSize = 64 << 10
	fsInventoryMaxPathSize   = 4 << 10
	fsInventoryMaxURISize    = 16 << 10
	fsSizesMaterializeBytes  = 16 << 20
)

var (
	errFSInventoryCorrupt  = errors.New("filesystem inventory spool is corrupt")
	errFSInventoryConflict = errors.New("filesystem inventory contains conflicting paths")
)

type fsInventoryRecord struct {
	Kind      string `json:"kind"`
	Version   int    `json:"version,omitempty"`
	CodecExt  string `json:"codecExt,omitempty"`
	RelPath   string `json:"relPath,omitempty"`
	ItemType  string `json:"type,omitempty"`
	URI       string `json:"uri,omitempty"`
	Size      int64  `json:"size,omitempty"`
	Mode      uint32 `json:"mode,omitempty"`
	UID       int    `json:"uid,omitempty"`
	GID       int    `json:"gid,omitempty"`
	Mtime     string `json:"mtime,omitempty"`
	Linkname  string `json:"linkname,omitempty"`
	Count     int64  `json:"count,omitempty"`
	Total     int64  `json:"total,omitempty"`
	SHA256Hex string `json:"sha256,omitempty"`
}

type fsInventorySummary struct {
	count int64
	total int64
}

type fsDirectoryRecord struct {
	URI       string `json:"uri"`
	RelPrefix string `json:"relPrefix"`
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
// workers bounds both active file downloads and the file-job queue. At most
// workers active fsItems plus workers queued fsItems are retained. Inventory
// sorting retains at most fsInventorySortBatchSize items; each merge opens at
// most fsInventoryMergeFanIn runs and retains one item per run. Every spool
// record is capped at fsInventoryMaxRecordSize. Total spool disk is linear in
// source metadata: one directory-queue record per directory plus at most two
// sorted-run copies during a merge pass. It lives under stagingDir/.d8-meta
// and is removed only after data.tar is durably committed. The first error
// cancels all in-flight downloads.
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

		if err := os.RemoveAll(stagingDir); err != nil {
			return fmt.Errorf("remove stale FS staging after completed tar %s: %w", stagingDir, err)
		}

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

	inventoryPath, summary, err := prepareFSInventory(ctx, stagingDir, filesRootURL, base, codec.Ext(), fetcher)
	if err != nil {
		return fmt.Errorf("prepare filesystem inventory: %w", err)
	}

	if err := writeFSSizesSidecar(ctx, stagingDir, inventoryPath, codec.Ext(), summary.total); err != nil {
		return fmt.Errorf("persist fs sizes sidecar: %w", err)
	}

	if setTotal != nil {
		setTotal(summary.total)
	}

	log.Info("staging filesystem volume",
		slog.String("tar", tarPath),
		slog.Int64("items", summary.count),
		slog.Int("workers", workers))

	if err := stageFSInventoryFiles(ctx, log, stagingDir, inventoryPath, base, workers, chunkSize, fetcher, codec, onProgress); err != nil {
		return fmt.Errorf("stage filesystem files: %w", err)
	}

	entries := tarEntriesFromInventory(ctx, inventoryPath, stagingDir, codec)
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

func prepareFSInventory(
	ctx context.Context,
	stagingDir string,
	filesRootURL string,
	base *url.URL,
	ext string,
	fetcher *exporter.Fetcher,
) (string, fsInventorySummary, error) {
	metaDir := filepath.Join(stagingDir, FSMetaDirName)
	if err := archive.EnsureDir(metaDir); err != nil {
		return "", fsInventorySummary{}, fmt.Errorf("create fs metadata dir %s: %w", metaDir, err)
	}

	inventoryPath := filepath.Join(metaDir, fsInventoryName)

	summary, err := validateFSInventory(ctx, inventoryPath, ext, nil)
	if err == nil {
		workDir := filepath.Join(metaDir, fsInventoryWorkDirName)
		if removeErr := os.RemoveAll(workDir); removeErr != nil {
			return "", fsInventorySummary{}, fmt.Errorf("remove stale inventory work dir %s: %w", workDir, removeErr)
		}

		return inventoryPath, summary, nil
	}

	if !os.IsNotExist(err) && !errors.Is(err, errFSInventoryCorrupt) {
		return "", fsInventorySummary{}, err
	}

	if removeErr := os.Remove(inventoryPath); removeErr != nil && !os.IsNotExist(removeErr) {
		return "", fsInventorySummary{}, fmt.Errorf("remove unusable inventory %s: %w", inventoryPath, removeErr)
	}

	summary, err = buildFSInventory(ctx, metaDir, inventoryPath, filesRootURL, base, ext, fetcher)
	if err != nil {
		return "", fsInventorySummary{}, err
	}

	validated, err := validateFSInventory(ctx, inventoryPath, ext, nil)
	if err != nil {
		return "", fsInventorySummary{}, fmt.Errorf("validate new filesystem inventory: %w", err)
	}

	if validated != summary {
		return "", fsInventorySummary{}, fmt.Errorf("%w: built summary %+v differs from validated summary %+v",
			errFSInventoryCorrupt, summary, validated)
	}

	return inventoryPath, summary, nil
}

func buildFSInventory(
	ctx context.Context,
	metaDir string,
	inventoryPath string,
	filesRootURL string,
	base *url.URL,
	ext string,
	fetcher *exporter.Fetcher,
) (fsInventorySummary, error) {
	workDir := filepath.Join(metaDir, fsInventoryWorkDirName)
	if err := os.RemoveAll(workDir); err != nil {
		return fsInventorySummary{}, fmt.Errorf("remove stale inventory work dir %s: %w", workDir, err)
	}

	if err := archive.EnsureDir(workDir); err != nil {
		return fsInventorySummary{}, fmt.Errorf("create inventory work dir %s: %w", workDir, err)
	}

	defer func() { _ = os.RemoveAll(workDir) }()

	builder := newFSRunBuilder(workDir, ext)

	summary, err := walkFSInventory(ctx, workDir, filesRootURL, base, fetcher, builder.add)
	if err != nil {
		return fsInventorySummary{}, err
	}

	runCount, err := builder.finish()
	if err != nil {
		return fsInventorySummary{}, err
	}

	finalRun, err := mergeFSInventoryRuns(ctx, workDir, ext, runCount)
	if err != nil {
		return fsInventorySummary{}, err
	}

	if err := writeFinalFSInventory(inventoryPath, finalRun, ext, summary); err != nil {
		return fsInventorySummary{}, err
	}

	return summary, nil
}

func walkFSInventory(
	ctx context.Context,
	workDir string,
	filesRootURL string,
	base *url.URL,
	fetcher *exporter.Fetcher,
	add func(fsItem) error,
) (fsInventorySummary, error) {
	queuePath := filepath.Join(workDir, "directories.jsonl")

	queueFile, err := os.OpenFile(queuePath, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fsInventorySummary{}, fmt.Errorf("create directory inventory queue: %w", err)
	}

	defer func() {
		_ = queueFile.Close()
	}()

	rootRef, err := url.Parse(filesRootURL)
	if err != nil {
		return fsInventorySummary{}, fmt.Errorf("parse files root URL %q: %w", filesRootURL, err)
	}

	root := fsDirectoryRecord{URI: rootRef.RequestURI()}
	if err := appendDirectoryRecord(queueFile, root); err != nil {
		return fsInventorySummary{}, err
	}

	readerFile, err := os.Open(queuePath)
	if err != nil {
		return fsInventorySummary{}, fmt.Errorf("open directory inventory queue: %w", err)
	}

	defer func() { _ = readerFile.Close() }()

	reader := bufio.NewReaderSize(readerFile, fsInventoryMaxRecordSize)
	queued := int64(1)

	var summary fsInventorySummary

	for processed := int64(0); processed < queued; processed++ {
		if err := ctx.Err(); err != nil {
			return fsInventorySummary{}, fmt.Errorf("filesystem inventory cancelled: %w", err)
		}

		record, err := readDirectoryRecord(reader)
		if err != nil {
			return fsInventorySummary{}, err
		}

		dirURL, err := resolveInventoryURI(base, record.URI)
		if err != nil {
			return fsInventorySummary{}, err
		}

		err = fetcher.ListDir(ctx, dirURL, func(item exporter.Item) error {
			inventoryItem, itemErr := inventoryItemFromListing(base, dirURL, record.RelPrefix, item)
			if itemErr != nil {
				return itemErr
			}

			if inventoryItem.itemType == "dir" {
				directory := fsDirectoryRecord{
					URI:       inventoryItem.uri,
					RelPrefix: inventoryItem.relPath + "/",
				}
				if err := appendDirectoryRecord(queueFile, directory); err != nil {
					return err
				}

				queued++
			}

			if inventoryItem.itemType == "file" && inventoryItem.size > 0 {
				if summary.total > math.MaxInt64-inventoryItem.size {
					return fmt.Errorf("filesystem declared size total overflows int64 at %q", inventoryItem.relPath)
				}

				summary.total += inventoryItem.size
			}

			summary.count++

			return add(inventoryItem)
		})
		if err != nil {
			return fsInventorySummary{}, fmt.Errorf("list %s: %w", dirURL, err)
		}
	}

	return summary, nil
}

func appendDirectoryRecord(f *os.File, record fsDirectoryRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal directory inventory record: %w", err)
	}

	if len(data) > fsInventoryMaxRecordSize {
		return fmt.Errorf("%w: directory record exceeds %d bytes", ErrUnsafePath, fsInventoryMaxRecordSize)
	}

	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("append directory inventory record: %w", err)
	}

	return nil
}

func readDirectoryRecord(reader *bufio.Reader) (fsDirectoryRecord, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return fsDirectoryRecord{}, fmt.Errorf("read directory inventory record: %w", err)
	}

	if len(line) > fsInventoryMaxRecordSize {
		return fsDirectoryRecord{}, fmt.Errorf("%w: directory record exceeds %d bytes", errFSInventoryCorrupt, fsInventoryMaxRecordSize)
	}

	var record fsDirectoryRecord
	if err := json.Unmarshal(line, &record); err != nil {
		return fsDirectoryRecord{}, fmt.Errorf("%w: parse directory record: %v", errFSInventoryCorrupt, err)
	}

	return record, nil
}

func inventoryItemFromListing(base *url.URL, dirURL, relPrefix string, item exporter.Item) (fsItem, error) {
	// The producer sets Name from fs.FileInfo.Name(), so it is one literal
	// directory-entry leaf. Rejecting "/" here prevents a malicious listing
	// from synthesizing descendants without the corresponding parent directory
	// records; hierarchy conflicts therefore reduce to exact stored-path
	// duplicates in the externally sorted inventory.
	if strings.Contains(item.Name, "/") {
		return fsItem{}, fmt.Errorf("%w: listing item name %q is not a single path element", ErrUnsafePath, item.Name)
	}

	relPath, err := sanitizeRelPath(relPrefix + item.Name)
	if err != nil {
		return fsItem{}, fmt.Errorf("listing %s: %w", dirURL, err)
	}

	if len(relPath) > fsInventoryMaxPathSize {
		return fsItem{}, fmt.Errorf("%w: path %q exceeds %d bytes", ErrUnsafePath, relPath, fsInventoryMaxPathSize)
	}

	ref, err := url.Parse(item.URI)
	if err != nil {
		return fsItem{}, fmt.Errorf("parse item URI %q: %w", item.URI, err)
	}

	resolved := base.ResolveReference(ref)
	if !sameOrigin(base, resolved) {
		return fsItem{}, fmt.Errorf("listing %s: %w: item URI %q resolves to %q, off the files-root origin %q",
			dirURL, ErrUnsafePath, item.URI, resolved.String(), base.String())
	}

	relativeURI := resolved.RequestURI()
	if len(relativeURI) > fsInventoryMaxURISize {
		return fsItem{}, fmt.Errorf("%w: item URI for %q exceeds %d bytes", ErrUnsafePath, relPath, fsInventoryMaxURISize)
	}

	mode, uid, gid, mtime := parseItemAttrs(item.Attributes)
	result := fsItem{
		relPath: relPath,
		uri:     relativeURI,
		mode:    mode,
		uid:     uid,
		gid:     gid,
		mtime:   mtime,
	}

	switch item.Type {
	case "file":
		result.itemType = "file"
		result.size = parseItemSize(item.Attributes)
	case "dir":
		result.itemType = "dir"
	case "link":
		result.itemType = "link"
		result.linkname = item.TargetPath
	default:
		return fsItem{}, fmt.Errorf("filesystem listing item %q has unsupported wire type %q", relPath, item.Type)
	}

	return result, nil
}

type fsRunBuilder struct {
	workDir string
	ext     string
	batch   []fsItem
	runs    int
}

func newFSRunBuilder(workDir, ext string) *fsRunBuilder {
	return &fsRunBuilder{
		workDir: workDir,
		ext:     ext,
		batch:   make([]fsItem, 0, fsInventorySortBatchSize),
	}
}

func (b *fsRunBuilder) add(item fsItem) error {
	b.batch = append(b.batch, item)
	if len(b.batch) < fsInventorySortBatchSize {
		return nil
	}

	return b.flush()
}

func (b *fsRunBuilder) finish() (int, error) {
	if err := b.flush(); err != nil {
		return 0, err
	}

	return b.runs, nil
}

func (b *fsRunBuilder) flush() error {
	if len(b.batch) == 0 {
		return nil
	}

	slices.SortFunc(b.batch, func(a, c fsItem) int {
		return cmp.Compare(fsStoredPath(a, b.ext), fsStoredPath(c, b.ext))
	})

	passDir := filepath.Join(b.workDir, "pass-000000")
	if err := archive.EnsureDir(passDir); err != nil {
		return fmt.Errorf("create inventory run dir %s: %w", passDir, err)
	}

	runPath := fsRunPath(b.workDir, 0, b.runs)
	if err := writeFSRun(runPath, b.batch); err != nil {
		return err
	}

	b.runs++
	b.batch = b.batch[:0]

	return nil
}

func fsRunPath(workDir string, pass, run int) string {
	return filepath.Join(workDir, fmt.Sprintf("pass-%06d", pass), fmt.Sprintf("run-%09d.jsonl", run))
}

func writeFSRun(path string, items []fsItem) error {
	aw, err := archive.NewAtomicWriter(path)
	if err != nil {
		return fmt.Errorf("open inventory run %s: %w", path, err)
	}

	for _, item := range items {
		if err := writeFSItemLine(aw, item); err != nil {
			aw.Abort()

			return fmt.Errorf("write inventory run %s: %w", path, err)
		}
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit inventory run %s: %w", path, err)
	}

	return nil
}

func mergeFSInventoryRuns(ctx context.Context, workDir, ext string, runCount int) (string, error) {
	if runCount == 0 {
		return "", nil
	}

	pass := 0

	for runCount > 1 {
		nextPass := pass + 1
		nextDir := filepath.Join(workDir, fmt.Sprintf("pass-%06d", nextPass))

		if err := archive.EnsureDir(nextDir); err != nil {
			return "", fmt.Errorf("create inventory merge dir %s: %w", nextDir, err)
		}

		outputCount := 0

		for first := 0; first < runCount; first += fsInventoryMergeFanIn {
			last := min(first+fsInventoryMergeFanIn, runCount)

			inputs := make([]string, 0, last-first)
			for run := first; run < last; run++ {
				inputs = append(inputs, fsRunPath(workDir, pass, run))
			}

			output := fsRunPath(workDir, nextPass, outputCount)
			if err := mergeFSRunGroup(ctx, ext, inputs, output); err != nil {
				return "", err
			}

			outputCount++
		}

		if err := os.RemoveAll(filepath.Join(workDir, fmt.Sprintf("pass-%06d", pass))); err != nil {
			return "", fmt.Errorf("remove merged inventory pass %d: %w", pass, err)
		}

		pass = nextPass
		runCount = outputCount
	}

	return fsRunPath(workDir, pass, 0), nil
}

type fsRunCursor struct {
	file    *os.File
	scanner *bufio.Scanner
	item    fsItem
	done    bool
}

func openFSRunCursor(path string) (*fsRunCursor, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open inventory run %s: %w", path, err)
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), fsInventoryMaxRecordSize)
	cursor := &fsRunCursor{file: f, scanner: scanner}

	if err := cursor.advance(); err != nil {
		_ = f.Close()

		return nil, fmt.Errorf("read inventory run %s: %w", path, err)
	}

	return cursor, nil
}

func (c *fsRunCursor) advance() error {
	if !c.scanner.Scan() {
		if err := c.scanner.Err(); err != nil {
			return fmt.Errorf("scan inventory run: %w", err)
		}

		c.done = true

		return nil
	}

	item, err := decodeFSItemLine(c.scanner.Bytes())
	if err != nil {
		return err
	}

	c.item = item

	return nil
}

func mergeFSRunGroup(ctx context.Context, ext string, inputs []string, output string) error {
	cursors := make([]*fsRunCursor, 0, len(inputs))
	for _, input := range inputs {
		cursor, err := openFSRunCursor(input)
		if err != nil {
			closeFSRunCursors(cursors)

			return err
		}

		cursors = append(cursors, cursor)
	}

	defer closeFSRunCursors(cursors)

	aw, err := archive.NewAtomicWriter(output)
	if err != nil {
		return fmt.Errorf("open merged inventory run %s: %w", output, err)
	}

	for {
		if err := ctx.Err(); err != nil {
			aw.Abort()

			return fmt.Errorf("merge filesystem inventory: %w", err)
		}

		selected := -1

		for index, cursor := range cursors {
			if cursor.done {
				continue
			}

			if selected < 0 ||
				fsStoredPath(cursor.item, ext) < fsStoredPath(cursors[selected].item, ext) {
				selected = index
			}
		}

		if selected < 0 {
			break
		}

		if err := writeFSItemLine(aw, cursors[selected].item); err != nil {
			aw.Abort()

			return fmt.Errorf("write merged inventory run %s: %w", output, err)
		}

		if err := cursors[selected].advance(); err != nil {
			aw.Abort()

			return fmt.Errorf("advance merged inventory run: %w", err)
		}
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit merged inventory run %s: %w", output, err)
	}

	return nil
}

func closeFSRunCursors(cursors []*fsRunCursor) {
	for _, cursor := range cursors {
		_ = cursor.file.Close()
	}
}

func writeFinalFSInventory(path, runPath, ext string, summary fsInventorySummary) error {
	aw, err := archive.NewAtomicWriter(path)
	if err != nil {
		return fmt.Errorf("open final filesystem inventory %s: %w", path, err)
	}

	header := fsInventoryRecord{Kind: "header", Version: fsInventoryVersion, CodecExt: ext}
	if err := writeInventoryRecord(aw, header, nil); err != nil {
		aw.Abort()

		return fmt.Errorf("write filesystem inventory header: %w", err)
	}

	hasher := sha256.New()

	if runPath != "" {
		cursor, err := openFSRunCursor(runPath)
		if err != nil {
			aw.Abort()

			return err
		}

		for !cursor.done {
			if err := writeInventoryItem(aw, cursor.item, hasher); err != nil {
				_ = cursor.file.Close()

				aw.Abort()

				return fmt.Errorf("write final filesystem inventory: %w", err)
			}

			if err := cursor.advance(); err != nil {
				_ = cursor.file.Close()

				aw.Abort()

				return fmt.Errorf("read final inventory run: %w", err)
			}
		}

		if err := cursor.file.Close(); err != nil {
			aw.Abort()

			return fmt.Errorf("close final inventory run: %w", err)
		}
	}

	footer := fsInventoryRecord{
		Kind:      "footer",
		Count:     summary.count,
		Total:     summary.total,
		SHA256Hex: hex.EncodeToString(hasher.Sum(nil)),
	}
	if err := writeInventoryRecord(aw, footer, nil); err != nil {
		aw.Abort()

		return fmt.Errorf("write filesystem inventory footer: %w", err)
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit filesystem inventory %s: %w", path, err)
	}

	return nil
}

func writeInventoryItem(w io.Writer, item fsItem, hasher io.Writer) error {
	record := fsInventoryRecordFromItem(item)

	return writeInventoryRecord(w, record, hasher)
}

func writeInventoryRecord(w io.Writer, record fsInventoryRecord, hasher io.Writer) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal inventory record: %w", err)
	}

	if len(data) > fsInventoryMaxRecordSize {
		return fmt.Errorf("%w: inventory record exceeds %d bytes", ErrUnsafePath, fsInventoryMaxRecordSize)
	}

	data = append(data, '\n')
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write inventory record: %w", err)
	}

	if hasher != nil {
		if _, err := hasher.Write(data); err != nil {
			return fmt.Errorf("hash inventory record: %w", err)
		}
	}

	return nil
}

func writeFSItemLine(w io.Writer, item fsItem) error {
	data, err := json.Marshal(fsInventoryRecordFromItem(item))
	if err != nil {
		return fmt.Errorf("marshal inventory item: %w", err)
	}

	if len(data) > fsInventoryMaxRecordSize {
		return fmt.Errorf("%w: inventory item %q exceeds %d bytes", ErrUnsafePath, item.relPath, fsInventoryMaxRecordSize)
	}

	data = append(data, '\n')
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write inventory item: %w", err)
	}

	return nil
}

func fsInventoryRecordFromItem(item fsItem) fsInventoryRecord {
	mtime := ""
	if !item.mtime.IsZero() {
		mtime = item.mtime.Format(time.RFC3339Nano)
	}

	return fsInventoryRecord{
		Kind:     "item",
		RelPath:  item.relPath,
		ItemType: item.itemType,
		URI:      item.uri,
		Size:     item.size,
		Mode:     uint32(item.mode),
		UID:      item.uid,
		GID:      item.gid,
		Mtime:    mtime,
		Linkname: item.linkname,
	}
}

func decodeFSItemLine(line []byte) (fsItem, error) {
	var record fsInventoryRecord
	if err := json.Unmarshal(line, &record); err != nil {
		return fsItem{}, fmt.Errorf("%w: parse inventory item: %v", errFSInventoryCorrupt, err)
	}

	if record.Kind != "item" {
		return fsItem{}, fmt.Errorf("%w: expected item record, got %q", errFSInventoryCorrupt, record.Kind)
	}

	mtime := time.Time{}

	if record.Mtime != "" {
		parsed, err := time.Parse(time.RFC3339Nano, record.Mtime)
		if err != nil {
			return fsItem{}, fmt.Errorf("%w: parse mtime for %q: %v", errFSInventoryCorrupt, record.RelPath, err)
		}

		mtime = parsed
	}

	return fsItem{
		relPath:  record.RelPath,
		itemType: record.ItemType,
		uri:      record.URI,
		size:     record.Size,
		mode:     fs.FileMode(record.Mode),
		uid:      record.UID,
		gid:      record.GID,
		mtime:    mtime,
		linkname: record.Linkname,
	}, nil
}

func validateFSInventory(
	ctx context.Context,
	path string,
	ext string,
	yield func(fsItem) error,
) (fsInventorySummary, error) {
	f, err := os.Open(path)
	if err != nil {
		return fsInventorySummary{}, err
	}

	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), fsInventoryMaxRecordSize)

	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return fsInventorySummary{}, fmt.Errorf("%w: read header: %v", errFSInventoryCorrupt, err)
		}

		return fsInventorySummary{}, fmt.Errorf("%w: inventory is empty", errFSInventoryCorrupt)
	}

	var header fsInventoryRecord
	if err := json.Unmarshal(scanner.Bytes(), &header); err != nil {
		return fsInventorySummary{}, fmt.Errorf("%w: parse header: %v", errFSInventoryCorrupt, err)
	}

	if header.Kind != "header" || header.Version != fsInventoryVersion || header.CodecExt != ext {
		return fsInventorySummary{}, fmt.Errorf("%w: header kind=%q version=%d codecExt=%q",
			errFSInventoryCorrupt, header.Kind, header.Version, header.CodecExt)
	}

	hasher := sha256.New()

	var (
		summary      fsInventorySummary
		previousPath string
		previousType string
		footer       *fsInventoryRecord
	)

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return fsInventorySummary{}, fmt.Errorf("validate filesystem inventory: %w", err)
		}

		line := scanner.Bytes()

		var record fsInventoryRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return fsInventorySummary{}, fmt.Errorf("%w: parse record: %v", errFSInventoryCorrupt, err)
		}

		if record.Kind == "footer" {
			copyRecord := record
			footer = &copyRecord

			break
		}

		item, err := decodeFSItemLine(line)
		if err != nil {
			return fsInventorySummary{}, err
		}

		if err := validateSpooledFSItem(item); err != nil {
			return fsInventorySummary{}, err
		}

		storedPath := fsStoredPath(item, ext)
		if previousPath != "" {
			if storedPath < previousPath {
				return fsInventorySummary{}, fmt.Errorf("%w: %q follows %q out of order",
					errFSInventoryCorrupt, storedPath, previousPath)
			}

			if storedPath == previousPath {
				return fsInventorySummary{}, fmt.Errorf("%w: duplicate stored path %q",
					errFSInventoryConflict, storedPath)
			}

			if previousType != "dir" && strings.HasPrefix(storedPath, previousPath+"/") {
				return fsInventorySummary{}, fmt.Errorf("%w: non-directory %q is an ancestor of %q",
					errFSInventoryConflict, previousPath, storedPath)
			}
		}

		previousPath = storedPath
		previousType = item.itemType
		summary.count++

		if item.itemType == "file" && item.size > 0 {
			if summary.total > math.MaxInt64-item.size {
				return fsInventorySummary{}, fmt.Errorf("%w: declared size total overflows int64", errFSInventoryCorrupt)
			}

			summary.total += item.size
		}

		if _, err := hasher.Write(line); err != nil {
			return fsInventorySummary{}, fmt.Errorf("hash inventory item: %w", err)
		}

		if _, err := hasher.Write([]byte{'\n'}); err != nil {
			return fsInventorySummary{}, fmt.Errorf("hash inventory delimiter: %w", err)
		}

		if yield != nil {
			if err := yield(item); err != nil {
				return fsInventorySummary{}, err
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fsInventorySummary{}, fmt.Errorf("%w: scan inventory: %v", errFSInventoryCorrupt, err)
	}

	if footer == nil {
		return fsInventorySummary{}, fmt.Errorf("%w: missing footer", errFSInventoryCorrupt)
	}

	if scanner.Scan() {
		return fsInventorySummary{}, fmt.Errorf("%w: records follow footer", errFSInventoryCorrupt)
	}

	if err := scanner.Err(); err != nil {
		return fsInventorySummary{}, fmt.Errorf("%w: scan inventory trailer: %v", errFSInventoryCorrupt, err)
	}

	gotHash := hex.EncodeToString(hasher.Sum(nil))
	if footer.Count != summary.count || footer.Total != summary.total || footer.SHA256Hex != gotHash {
		return fsInventorySummary{}, fmt.Errorf("%w: footer count=%d total=%d sha256=%s, calculated count=%d total=%d sha256=%s",
			errFSInventoryCorrupt, footer.Count, footer.Total, footer.SHA256Hex,
			summary.count, summary.total, gotHash)
	}

	return summary, nil
}

func validateSpooledFSItem(item fsItem) error {
	if _, err := sanitizeRelPath(item.relPath); err != nil {
		return fmt.Errorf("%w: invalid spooled path %q: %v", errFSInventoryCorrupt, item.relPath, err)
	}

	if len(item.relPath) > fsInventoryMaxPathSize || len(item.uri) > fsInventoryMaxURISize {
		return fmt.Errorf("%w: spooled path or URI exceeds limit for %q", errFSInventoryCorrupt, item.relPath)
	}

	switch item.itemType {
	case "file":
		if item.uri == "" {
			return fmt.Errorf("%w: file %q has empty URI", errFSInventoryCorrupt, item.relPath)
		}
	case "dir":
		if item.uri == "" {
			return fmt.Errorf("%w: directory %q has empty URI", errFSInventoryCorrupt, item.relPath)
		}
	case "link":
	default:
		return fmt.Errorf("%w: item %q has unsupported type %q", errFSInventoryCorrupt, item.relPath, item.itemType)
	}

	return nil
}

func fsStoredPath(item fsItem, ext string) string {
	if item.itemType == "file" {
		return item.relPath + ext
	}

	return item.relPath
}

func resolveInventoryURI(base *url.URL, rawURI string) (string, error) {
	ref, err := url.Parse(rawURI)
	if err != nil {
		return "", fmt.Errorf("%w: parse spooled URI %q: %v", errFSInventoryCorrupt, rawURI, err)
	}

	if ref.IsAbs() || ref.Host != "" {
		return "", fmt.Errorf("%w: spooled URI %q is not origin-relative", errFSInventoryCorrupt, rawURI)
	}

	resolved := base.ResolveReference(ref)
	if !sameOrigin(base, resolved) {
		return "", fmt.Errorf("%w: spooled URI %q leaves files-root origin", errFSInventoryCorrupt, rawURI)
	}

	return resolved.String(), nil
}

func stageFSInventoryFiles(
	ctx context.Context,
	log *slog.Logger,
	stagingDir string,
	inventoryPath string,
	base *url.URL,
	workers int,
	chunkSize int64,
	fetcher *exporter.Fetcher,
	codec compress.Codec,
	onProgress func(n int),
) error {
	g, gctx := errgroup.WithContext(ctx)
	jobs := make(chan fsItem, workers)

	for range workers {
		g.Go(func() error {
			for item := range jobs {
				uri, err := resolveInventoryURI(base, item.uri)
				if err != nil {
					return err
				}

				item.uri = uri

				sourceMD5, err := fetcher.SourceMD5(gctx, item.uri, item.size)
				if err != nil {
					return fmt.Errorf("fetch source MD5 for %s: %w", item.relPath, err)
				}

				item.md5 = sourceMD5

				if _, err := stageCompressedFile(gctx, log, stagingDir, item, chunkSize, codec, fetcher, onProgress); err != nil {
					return err
				}
			}

			return nil
		})
	}

	_, produceErr := validateFSInventory(gctx, inventoryPath, codec.Ext(), func(item fsItem) error {
		if item.itemType != "file" {
			return nil
		}

		select {
		case jobs <- item:
			return nil
		case <-gctx.Done():
			return gctx.Err()
		}
	})

	close(jobs)

	waitErr := g.Wait()

	if produceErr != nil {
		return produceErr
	}

	return waitErr
}

func tarEntriesFromInventory(
	ctx context.Context,
	inventoryPath string,
	stagingDir string,
	codec compress.Codec,
) TarEntrySource {
	return func(yield func(TarEntry) error) error {
		_, err := validateFSInventory(ctx, inventoryPath, codec.Ext(), func(item fsItem) error {
			rawSize := item.size
			if item.itemType == "file" && rawSize < 0 {
				destPath := filepath.Join(stagingDir, filepath.FromSlash(fsStoredPath(item, codec.Ext())))

				derived, err := stagedFileRawSize(destPath, codec.Ext())
				if err != nil {
					return fmt.Errorf("derive raw size for %s: %w", item.relPath, err)
				}

				rawSize = derived
			}

			return yield(TarEntry{
				RelPath:      fsStoredPath(item, codec.Ext()),
				Type:         item.itemType,
				Codec:        codec.Name(),
				OriginalPath: item.relPath,
				RawSize:      rawSize,
				Mode:         item.mode,
				UID:          item.uid,
				GID:          item.gid,
				Mtime:        item.mtime,
				Linkname:     item.linkname,
			})
		})

		return err
	}
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
// setTotal established from the inventory total — otherwise
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

// FSSizesSidecar is the compatibility materialized view returned by
// ReadFSSizesSidecar. Production progress scans use ScanFSStagingSizes, which
// streams the JSON object without building Files. Materialization is capped at
// fsSizesMaterializeBytes so even an accidental caller remains memory-bounded.
type FSSizesSidecar struct {
	Files map[string]int64 `json:"files"`
	Total int64            `json:"total"`
}

// writeFSSizesSidecar persists inventory file sizes under stagingDir's
// reserved metadata dir (stagingDir/.d8-meta/sizes.json), fsynced via
// archive.WriteFileAtomic. Only "file" items with a known positive size are
// recorded. The JSON object is emitted incrementally in inventory order; no
// map or whole-document byte slice proportional to the inode count is built.
func writeFSSizesSidecar(ctx context.Context, stagingDir, inventoryPath, ext string, total int64) error {
	metaDir := filepath.Join(stagingDir, FSMetaDirName)

	if err := archive.EnsureDir(metaDir); err != nil {
		return fmt.Errorf("create fs metadata dir %s: %w", metaDir, err)
	}

	path := filepath.Join(metaDir, FSSizesSidecarName)

	aw, err := archive.NewAtomicWriter(path)
	if err != nil {
		return fmt.Errorf("open fs sizes sidecar %s: %w", path, err)
	}

	if _, err := io.WriteString(aw, `{"files":{`); err != nil {
		aw.Abort()

		return fmt.Errorf("write fs sizes sidecar prefix: %w", err)
	}

	first := true

	_, err = validateFSInventory(ctx, inventoryPath, ext, func(item fsItem) error {
		if item.itemType != "file" || item.size <= 0 {
			return nil
		}

		name, err := json.Marshal(item.relPath)
		if err != nil {
			return fmt.Errorf("marshal fs size path %q: %w", item.relPath, err)
		}

		if !first {
			if _, err := io.WriteString(aw, ","); err != nil {
				return fmt.Errorf("write fs sizes separator: %w", err)
			}
		}

		first = false

		if _, err := fmt.Fprintf(aw, "%s:%d", name, item.size); err != nil {
			return fmt.Errorf("write fs size for %q: %w", item.relPath, err)
		}

		return nil
	})
	if err != nil {
		aw.Abort()

		return err
	}

	if _, err := fmt.Fprintf(aw, `},"total":%d}`, total); err != nil {
		aw.Abort()

		return fmt.Errorf("write fs sizes sidecar suffix: %w", err)
	}

	if err := aw.Commit(); err != nil {
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

	sizes, err := decodeFSSizesSidecar(path)
	if err == nil {
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

	sizes, err := decodeFSSizesSidecar(path)
	if err != nil {
		if os.IsNotExist(err) {
			return FSSizesSidecar{}, false, nil
		}

		return FSSizesSidecar{}, false, nil
	}

	return sizes, true, nil
}

func decodeFSSizesSidecar(path string) (FSSizesSidecar, error) {
	f, err := os.Open(path)
	if err != nil {
		return FSSizesSidecar{}, err
	}

	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return FSSizesSidecar{}, fmt.Errorf("stat fs sizes sidecar %s: %w", path, err)
	}

	if info.Size() > fsSizesMaterializeBytes {
		return FSSizesSidecar{}, fmt.Errorf("fs sizes sidecar %s exceeds %d-byte materialization limit", path, fsSizesMaterializeBytes)
	}

	dec := json.NewDecoder(io.LimitReader(f, fsSizesMaterializeBytes+1))

	var sizes FSSizesSidecar

	if err := dec.Decode(&sizes); err != nil {
		return FSSizesSidecar{}, fmt.Errorf("parse fs sizes sidecar %s: %w", path, err)
	}

	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return FSSizesSidecar{}, fmt.Errorf("parse fs sizes sidecar %s: trailing JSON value", path)
		}

		return FSSizesSidecar{}, fmt.Errorf("parse fs sizes sidecar %s trailer: %w", path, err)
	}

	return sizes, nil
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
	path := filepath.Join(stagingDir, FSMetaDirName, FSSizesSidecarName)

	total, staged, err := scanFSSizesFile(path, stagingDir, ext)
	if err == nil {
		return total, staged, true, nil
	}

	if !os.IsNotExist(err) {
		return 0, 0, false, err
	}

	legacyPath := filepath.Join(stagingDir, FSSizesSidecarName)

	total, staged, err = scanFSSizesFile(legacyPath, stagingDir, ext)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, false, nil
		}

		// The legacy location can be a real user file named sizes.json at
		// codec none. Preserve the established conservative fallback: ignore
		// unparseable bytes rather than treating or modifying them as metadata.
		return 0, 0, false, nil
	}

	return total, staged, true, nil
}

func scanFSSizesFile(path, stagingDir, ext string) (int64, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}

	defer func() { _ = f.Close() }()

	reader := bufio.NewReaderSize(f, 4<<10)
	if err := expectFSSizesJSONByte(reader, '{'); err != nil {
		return 0, 0, fmt.Errorf("parse fs sizes sidecar %s: %w", path, err)
	}

	var (
		total      int64
		staged     int64
		foundFiles bool
		foundTotal bool
	)

	firstField := true

	for {
		done, err := beforeNextFSSizesJSONValue(reader, firstField, '}')
		if err != nil {
			return 0, 0, fmt.Errorf("parse fs sizes sidecar %s field: %w", path, err)
		}

		if done {
			break
		}

		firstField = false

		key, err := readBoundedFSSizesJSONString(reader, fsInventoryMaxRecordSize)
		if err != nil {
			return 0, 0, fmt.Errorf("parse fs sizes sidecar %s field: %w", path, err)
		}

		if err := expectFSSizesJSONByte(reader, ':'); err != nil {
			return 0, 0, fmt.Errorf("parse fs sizes sidecar %s field %q separator: %w", path, key, err)
		}

		switch key {
		case "files":
			if foundFiles {
				return 0, 0, fmt.Errorf("parse fs sizes sidecar %s: duplicate files field", path)
			}

			foundFiles = true

			if err := expectFSSizesJSONByte(reader, '{'); err != nil {
				return 0, 0, fmt.Errorf("parse fs sizes sidecar %s files: %w", path, err)
			}

			firstFile := true

			for {
				filesDone, err := beforeNextFSSizesJSONValue(reader, firstFile, '}')
				if err != nil {
					return 0, 0, fmt.Errorf("parse fs sizes sidecar %s file path: %w", path, err)
				}

				if filesDone {
					break
				}

				firstFile = false

				relPath, err := readBoundedFSSizesJSONString(reader, fsInventoryMaxRecordSize)
				if err != nil {
					return 0, 0, fmt.Errorf("parse fs sizes sidecar %s file path: %w", path, err)
				}

				if len(relPath) > fsInventoryMaxPathSize {
					return 0, 0, fmt.Errorf(
						"parse fs sizes sidecar %s path exceeds %d-byte limit",
						path,
						fsInventoryMaxPathSize,
					)
				}

				if _, err := sanitizeRelPath(relPath); err != nil {
					return 0, 0, fmt.Errorf("parse fs sizes sidecar %s path %q: %w", path, relPath, err)
				}

				if err := expectFSSizesJSONByte(reader, ':'); err != nil {
					return 0, 0, fmt.Errorf("parse fs sizes sidecar %s size for %q separator: %w", path, relPath, err)
				}

				size, rawSize, err := readBoundedFSSizesJSONInt64(reader, fsInventoryMaxRecordSize)
				if err != nil {
					return 0, 0, fmt.Errorf("parse fs sizes sidecar %s size for %q: %w", path, relPath, err)
				}

				if size <= 0 {
					return 0, 0, fmt.Errorf("parse fs sizes sidecar %s size for %q: invalid %q", path, relPath, rawSize)
				}

				destPath := filepath.Join(stagingDir, filepath.FromSlash(relPath+ext))

				info, statErr := os.Stat(destPath)
				if statErr == nil && !info.IsDir() {
					if staged > math.MaxInt64-size {
						return 0, 0, fmt.Errorf("parse fs sizes sidecar %s: staged size overflows int64", path)
					}

					staged += size
				}
			}

		case "total":
			if foundTotal {
				return 0, 0, fmt.Errorf("parse fs sizes sidecar %s: duplicate total field", path)
			}

			foundTotal = true

			var rawTotal string

			total, rawTotal, err = readBoundedFSSizesJSONInt64(reader, fsInventoryMaxRecordSize)
			if err != nil {
				return 0, 0, fmt.Errorf("parse fs sizes sidecar %s total: %w", path, err)
			}

			if total < 0 {
				return 0, 0, fmt.Errorf("parse fs sizes sidecar %s: invalid total %q", path, rawTotal)
			}

		default:
			return 0, 0, fmt.Errorf("parse fs sizes sidecar %s: unknown field %q", path, key)
		}
	}

	if !foundFiles || !foundTotal || staged > total {
		return 0, 0, fmt.Errorf("parse fs sizes sidecar %s: incomplete or inconsistent metadata", path)
	}

	if err := expectFSSizesJSONEOF(reader); err != nil {
		return 0, 0, fmt.Errorf("parse fs sizes sidecar %s trailer: %w", path, err)
	}

	return total, staged, nil
}

// Sidecar limits are per encoded key/value, not per document, so inode count remains
// unlimited while one corrupt record cannot allocate attacker-controlled heap.
func readBoundedFSSizesJSONString(reader *bufio.Reader, limit int) (string, error) {
	if err := expectFSSizesJSONByte(reader, '"'); err != nil {
		return "", err
	}

	raw := make([]byte, 1, min(limit, 4<<10))
	raw[0] = '"'
	escaped := false

	for {
		b, err := reader.ReadByte()
		if err != nil {
			return "", err
		}

		if len(raw) >= limit {
			return "", fmt.Errorf("JSON string exceeds %d-byte encoded limit", limit)
		}

		raw = append(raw, b)

		if escaped {
			escaped = false

			continue
		}

		if b == '\\' {
			escaped = true

			continue
		}

		if b == '"' {
			break
		}
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("decode JSON string: %w", err)
	}

	return value, nil
}

func readBoundedFSSizesJSONInt64(reader *bufio.Reader, limit int) (int64, string, error) {
	first, err := readFSSizesJSONNonSpace(reader)
	if err != nil {
		return 0, "", err
	}

	if first == ',' || first == '}' || first == ']' || first == ':' {
		return 0, "", fmt.Errorf("unexpected %q at start of JSON number", first)
	}

	raw := make([]byte, 1, min(limit, 64))
	raw[0] = first

	for {
		next, err := reader.Peek(1)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return 0, "", err
		}

		if isFSSizesJSONBoundary(next[0]) {
			break
		}

		b, err := reader.ReadByte()
		if err != nil {
			return 0, "", err
		}

		if len(raw) >= limit {
			return 0, "", fmt.Errorf("JSON number exceeds %d-byte encoded limit", limit)
		}

		raw = append(raw, b)
	}

	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		return 0, string(raw), fmt.Errorf("invalid JSON number %q: %w", raw, err)
	}

	value, err := number.Int64()
	if err != nil {
		return 0, number.String(), fmt.Errorf("invalid integer %q: %w", number, err)
	}

	return value, number.String(), nil
}

func beforeNextFSSizesJSONValue(reader *bufio.Reader, first bool, end byte) (bool, error) {
	next, err := peekFSSizesJSONNonSpace(reader)
	if err != nil {
		return false, err
	}

	if next == end {
		_, _ = reader.ReadByte()

		return true, nil
	}

	if !first {
		if err := expectFSSizesJSONByte(reader, ','); err != nil {
			return false, err
		}
	}

	return false, nil
}

func expectFSSizesJSONByte(reader *bufio.Reader, want byte) error {
	got, err := readFSSizesJSONNonSpace(reader)
	if err != nil {
		return err
	}

	if got != want {
		return fmt.Errorf("expected %q, got %q", want, got)
	}

	return nil
}

func expectFSSizesJSONEOF(reader *bufio.Reader) error {
	if _, err := readFSSizesJSONNonSpace(reader); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}

		return err
	}

	return nil
}

func readFSSizesJSONNonSpace(reader *bufio.Reader) (byte, error) {
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}

		if !isFSSizesJSONSpace(b) {
			return b, nil
		}
	}
}

func peekFSSizesJSONNonSpace(reader *bufio.Reader) (byte, error) {
	for {
		next, err := reader.Peek(1)
		if err != nil {
			return 0, err
		}

		if !isFSSizesJSONSpace(next[0]) {
			return next[0], nil
		}

		_, _ = reader.ReadByte()
	}
}

func isFSSizesJSONBoundary(b byte) bool {
	return isFSSizesJSONSpace(b) || b == ',' || b == '}' || b == ']'
}

func isFSSizesJSONSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
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
// prepareAttributesStat) emits these keys and types:
//   - "permissions": octal string via fmt.Sprintf("%#o", perm), e.g. "0644"
//   - "modtime":     RFC3339 string (time.RFC3339)
//   - "uid", "gid": JSON numbers (decoded as float64 by encoding/json)
//   - "size":        JSON number (files only; consumed via parseItemSize)
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
