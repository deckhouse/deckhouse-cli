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

package snapimport

import (
	"archive/tar"
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"math"
	"net/http"
	neturl "net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

const (
	uploadFilesSubpath          = "api/v1/files"
	maxConsecutiveFileConflicts = 8
	maxFileConflictReplays      = 4 * maxConsecutiveFileConflicts

	fsTarMaxEntries          = 1_000_000_000
	fsTarMaxPathBytes        = 16 << 10
	fsTarRunMaxRecords       = 4096
	fsTarRunMaxPathBytes     = 4 << 20
	fsTarMergeFanIn          = 8
	fsTarMergeLevels         = 16
	fsTarIndexBufferBytes    = 64 << 10
	fsTarMaxDiagnostics      = 32
	fsTarCancellationCadence = 128
)

// The preflight inventory keeps one fixed-size in-memory run, merges at most eight runs at
// once, and retains a fixed sixteen-level run table. A merge therefore opens at most nine
// inventory descriptors (eight inputs plus one output), while sorted metadata occupies at
// most two generations on disk in addition to the fixed-width archive-order sequence.
// Files live in a 0700 OS temporary directory with mode 0600 and are never reused. Every
// normal/error/cancellation path removes the directory; a process crash can only leave that
// private, unreferenced directory for the host's standard temporary-directory scavenger.

// fileAttrs carries the filesystem metadata sent to the FS importer for each file.
// The importer's CheckRequiredHeaders middleware requires X-Attribute-Permissions,
// X-Attribute-Uid, and X-Attribute-Gid on every PUT; X-Attribute-ModTime is optional
// but always sent when non-zero so the importer can preserve source timestamps.
type fileAttrs struct {
	// Perm is the file permission bits (e.g. 0o644); formatted as octal in the header.
	Perm    os.FileMode
	UID     int
	GID     int
	ModTime time.Time
}

type fileBodyFactory func(ctx context.Context, offset, size int64) (io.ReadCloser, error)

type fsTarSource interface {
	io.ReaderAt
	Stat() (os.FileInfo, error)
}

type fileUploadProgress struct {
	onProgress func(int)
	credited   int64
}

func (p *fileUploadProgress) creditTo(offset int64) {
	if offset <= p.credited {
		return
	}

	if p.onProgress != nil {
		p.onProgress(int(offset - p.credited))
	}

	p.credited = offset
}

// fileConflictTracker bounds server-directed repositions without retaining attacker-controlled
// history. Successful PUTs compact the current offset path, while the lifetime counter limits
// compressed prefix replay to a constant number of whole-file traversals.
type fileConflictTracker struct {
	offsets [maxConsecutiveFileConflicts + 1]int64
	count   int
	total   int
}

func (t *fileConflictTracker) observe(from, to int64) error {
	if t.total == maxFileConflictReplays {
		return fmt.Errorf(
			"too many file upload conflict replays (%d); latest transition from %d to %d",
			maxFileConflictReplays,
			from,
			to,
		)
	}

	if t.count == 0 {
		t.offsets[0] = from
		t.count = 1
	}

	for _, offset := range t.offsets[:t.count] {
		if offset == to {
			return fmt.Errorf("server-directed file upload offset cycle from %d to %d", from, to)
		}
	}

	if t.count == len(t.offsets) {
		return fmt.Errorf("too many consecutive file upload conflicts (%d)", maxConsecutiveFileConflicts)
	}

	t.offsets[t.count] = to
	t.count++
	t.total++

	return nil
}

func (t *fileConflictTracker) reset() {
	t.count = 0
}

// putFile sends bounded requests using a fresh body positioned at every server-selected
// raw offset. Callers probe HEAD first and supply the validated durable offset.
func putFile(
	ctx context.Context,
	client httpDoer,
	baseURL, relPath string,
	totalSize, offset int64,
	attrs fileAttrs,
	newBody fileBodyFactory,
	progress *fileUploadProgress,
	activate func(),
) error {
	fileURL, err := fileUploadURL(baseURL, relPath)
	if err != nil {
		return err
	}

	if err := validateBlockOffset(offset, totalSize); err != nil {
		return fmt.Errorf("invalid initial file offset: %w", err)
	}

	var conflicts fileConflictTracker

	for {
		requestEnd := offset + min(blockPutPayloadLimit, totalSize-offset)
		requestSize := requestEnd - offset

		body := io.ReadCloser(http.NoBody)
		if requestSize > 0 {
			body, err = newBody(ctx, offset, requestSize)
			if err != nil {
				return fmt.Errorf("open body for %s at offset %d: %w", relPath, offset, err)
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, fileURL, body)
		if err != nil {
			return errors.Join(
				fmt.Errorf("build PUT for %s at offset %d: %w", relPath, offset, err),
				closeFileBody(body, relPath, offset),
			)
		}

		req.ContentLength = requestSize

		setFileHeaders(req, totalSize, offset, attrs)

		if activate != nil {
			activate()
		}

		next, reposition, requestErr := doFileChunk(client, req, offset, requestEnd, totalSize)
		closeErr := closeFileBody(body, relPath, offset)

		if requestErr != nil || closeErr != nil {
			return fmt.Errorf("upload %s at offset %d: %w", relPath, offset, errors.Join(requestErr, closeErr))
		}

		if reposition {
			if err := conflicts.observe(offset, next); err != nil {
				return err
			}
		} else {
			conflicts.reset()
		}

		if progress != nil {
			progress.creditTo(next)
		}

		offset = next

		if !reposition && offset == totalSize {
			return nil
		}
	}
}

func closeFileBody(body io.Closer, relPath string, offset int64) error {
	if err := body.Close(); err != nil {
		return fmt.Errorf("close body for %s at offset %d: %w", relPath, offset, err)
	}

	return nil
}

func fileUploadURL(baseURL, relPath string) (string, error) {
	if err := validateFSUploadPath(relPath); err != nil {
		return "", err
	}

	fileURL, err := neturl.JoinPath(baseURL, uploadFilesSubpath, relPath)
	if err != nil {
		return "", fmt.Errorf("build file URL for %s: %w", relPath, err)
	}

	return fileURL, nil
}

func validateFSUploadPath(originalPath string) error {
	if originalPath == "" || originalPath == "." {
		return fmt.Errorf("%w: original path %q is empty or dot", archive.ErrInvalidFSMetadata, originalPath)
	}

	if path.IsAbs(originalPath) || strings.HasPrefix(originalPath, "/") || strings.ContainsRune(originalPath, '\\') {
		return fmt.Errorf("%w: original path %q is not a portable slash-relative path",
			archive.ErrInvalidFSMetadata, originalPath)
	}

	if len(originalPath) >= 2 && isASCIILetter(originalPath[0]) && originalPath[1] == ':' {
		return fmt.Errorf("%w: original path %q is drive-like", archive.ErrInvalidFSMetadata, originalPath)
	}

	for _, r := range originalPath {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: original path %q contains a control byte", archive.ErrInvalidFSMetadata, originalPath)
		}
	}

	for _, element := range strings.Split(originalPath, "/") {
		if element == ".." {
			return fmt.Errorf("%w: original path %q contains '..'", archive.ErrInvalidFSMetadata, originalPath)
		}
	}

	cleaned := path.Clean(originalPath)
	if cleaned != originalPath {
		return fmt.Errorf("%w: original path %q changes under path.Clean to %q",
			archive.ErrInvalidFSMetadata, originalPath, cleaned)
	}

	return nil
}

func isASCIILetter(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

// setFileHeaders sets the headers the FS importer's CheckRequiredHeaders middleware
// requires on every PUT.
func setFileHeaders(req *http.Request, totalSize, offset int64, attrs fileAttrs) {
	req.Header.Set("X-Content-Length", strconv.FormatInt(totalSize, 10))
	req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))
	req.Header.Set("X-Attribute-Permissions", fmt.Sprintf("%04o", attrs.Perm))
	req.Header.Set("X-Attribute-Uid", strconv.Itoa(attrs.UID))
	req.Header.Set("X-Attribute-Gid", strconv.Itoa(attrs.GID))

	if !attrs.ModTime.IsZero() {
		req.Header.Set("X-Attribute-ModTime", attrs.ModTime.UTC().Format(time.RFC3339))
	}
}

// headFileOffset probes the file endpoint to determine the current upload state.
// Returns (offset, done, size) where done=true means the final file already exists and
// the upload should be skipped entirely; size is that final file's exact on-disk
// (decompressed) byte count, read from the HEAD response's Content-Length, and is only
// meaningful when done is true — it lets a caller credit progress for a skipped file
// without decompressing it. offset is the number of bytes already written to the
// server's temp file when done is false; 0 if no partial upload exists.
func headFileOffset(ctx context.Context, client httpDoer, fileURL string, totalSize int64) (int64, bool, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fileURL, nil)
	if err != nil {
		return 0, false, 0, err
	}

	resp, err := client.HTTPDo(req)
	if err != nil {
		return 0, false, 0, err
	}

	if err := drainAndCloseResponseBody(resp); err != nil {
		return 0, false, 0, fmt.Errorf("drain HEAD %s response: %w", fileURL, err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		// X-Next-Offset present → temp file exists with that many bytes (partial upload).
		if next := resp.Header.Get("X-Next-Offset"); next != "" {
			off, parseErr := strconv.ParseInt(next, 10, 64)
			if parseErr != nil {
				return 0, false, 0, fmt.Errorf("invalid X-Next-Offset %q from HEAD %s: %w", next, fileURL, parseErr)
			}

			if err := validateBlockOffset(off, totalSize); err != nil {
				return 0, false, 0, fmt.Errorf("invalid X-Next-Offset %q from HEAD %s: %w", next, fileURL, err)
			}

			return off, false, 0, nil
		}

		// No X-Next-Offset on a 200 → the final file already exists; skip the upload.
		// The importer sets Content-Length to the final file's exact decompressed size,
		// which net/http already parses into resp.ContentLength (-1 when absent/chunked).
		size := resp.ContentLength
		if size < 0 {
			size = 0
		}

		return 0, true, size, nil

	case http.StatusNotFound:
		return 0, false, 0, nil

	default:
		return 0, false, 0, fmt.Errorf("HEAD %s returned status %d (%s)", fileURL, resp.StatusCode, resp.Status)
	}
}

// doFileChunk performs one bounded PUT and validates the producer's exact status/header
// contract. A conflict returns a validated server-selected reposition offset.
func doFileChunk(client httpDoer, req *http.Request, offset, requestEnd, totalSize int64) (int64, bool, error) {
	if err := validateBlockOffset(offset, totalSize); err != nil {
		return 0, false, fmt.Errorf("invalid PUT start offset: %w", err)
	}

	if err := validateBlockOffset(requestEnd, totalSize); err != nil {
		return 0, false, fmt.Errorf("invalid PUT end offset: %w", err)
	}

	if requestEnd < offset || requestEnd == offset && requestEnd != totalSize {
		return 0, false, fmt.Errorf("invalid PUT range [%d,%d)", offset, requestEnd)
	}

	resp, bodyReport, err := doAttestedRequest(client, req, requestBodyRange{start: offset, end: requestEnd})
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}

	if err != nil {
		return 0, false, err
	}

	if resp == nil {
		return 0, false, errors.New("PUT returned neither a response nor an error")
	}

	if resp.StatusCode == http.StatusConflict {
		expected, parseErr := parseOffsetHeader(resp.Header, "X-Expected-Offset")
		if parseErr != nil {
			return 0, false, fmt.Errorf("409 missing valid X-Expected-Offset: %w", parseErr)
		}

		if err := validateBlockOffset(expected, totalSize); err != nil {
			return 0, false, fmt.Errorf("invalid X-Expected-Offset %d: %w", expected, err)
		}

		if expected == offset {
			return 0, false, fmt.Errorf("server returned non-progressing X-Expected-Offset %d for PUT at offset %d",
				expected, offset)
		}

		return expected, true, nil
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return 0, false, fmt.Errorf("server error at offset %d: status %d (%s)", offset, resp.StatusCode, resp.Status)
	}

	if err := bodyReport.validateExact(); err != nil {
		return 0, false, fmt.Errorf("attest successful PUT body: %w", err)
	}

	if resp.StatusCode == http.StatusCreated && requestEnd != totalSize {
		return 0, false, fmt.Errorf("server returned 201 Created for intermediate request end %d of %d",
			requestEnd, totalSize)
	}

	if resp.StatusCode == http.StatusNoContent && requestEnd == totalSize {
		return 0, false, fmt.Errorf("server returned 204 No Content for final request end %d", requestEnd)
	}

	nextValue := resp.Header.Get("X-Next-Offset")
	if resp.StatusCode == http.StatusCreated && nextValue == "" {
		return requestEnd, false, nil
	}

	next, parseErr := parseOffsetHeader(resp.Header, "X-Next-Offset")
	if parseErr != nil {
		return 0, false, fmt.Errorf("successful PUT missing valid X-Next-Offset: %w", parseErr)
	}

	if err := validateBlockOffset(next, totalSize); err != nil {
		return 0, false, fmt.Errorf("invalid X-Next-Offset %d: %w", next, err)
	}

	if next != requestEnd {
		return 0, false, fmt.Errorf("server returned X-Next-Offset %d, want exact request end %d", next, requestEnd)
	}

	return next, false, nil
}

// parseOffsetHeader parses a non-negative int64 from a named HTTP response header.
// Returns an error when the header is absent or its value is not a valid non-negative integer.
func parseOffsetHeader(h http.Header, name string) (int64, error) {
	val := h.Get(name)
	if val == "" {
		return 0, fmt.Errorf("header %q is missing or empty", name)
	}

	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %q value %q: %w", name, val, err)
	}

	if n < 0 {
		return 0, fmt.Errorf("invalid %q value %q", name, val)
	}

	return n, nil
}

type fsTarRecordKind byte

const (
	fsTarRecordRegular fsTarRecordKind = iota
	fsTarRecordDirectory
	fsTarRecordUnsupported
)

type fsTarRecord struct {
	Path     string
	Ordinal  uint64
	Kind     fsTarRecordKind
	Typeflag byte
}

type fsTarIndexOptions struct {
	tempRoot    string
	mkdirTemp   func(string, string) (string, error)
	createTemp  func(string, string) (*os.File, error)
	removeAll   func(string) error
	writeRecord func(io.Writer, fsTarRecord) error
}

type fsTarScan struct {
	tempDir                  string
	sequencePath             string
	entryCount               uint64
	regularCount             uint64
	StructuralDirectoryCount int
	removeAll                func(string) error
}

func (s *fsTarScan) Close() error {
	if s == nil || s.tempDir == "" {
		return nil
	}

	tempDir := s.tempDir
	if err := s.removeAll(tempDir); err != nil {
		return fmt.Errorf("remove filesystem tar preflight inventory %s: %w", tempDir, err)
	}

	s.tempDir = ""

	return nil
}

func defaultFSTarIndexOptions() fsTarIndexOptions {
	return fsTarIndexOptions{
		mkdirTemp:   os.MkdirTemp,
		createTemp:  os.CreateTemp,
		removeAll:   os.RemoveAll,
		writeRecord: writeFSTarRecord,
	}
}

func (o fsTarIndexOptions) withDefaults() fsTarIndexOptions {
	defaults := defaultFSTarIndexOptions()
	if o.mkdirTemp == nil {
		o.mkdirTemp = defaults.mkdirTemp
	}

	if o.createTemp == nil {
		o.createTemp = defaults.createTemp
	}

	if o.removeAll == nil {
		o.removeAll = defaults.removeAll
	}

	if o.writeRecord == nil {
		o.writeRecord = defaults.writeRecord
	}

	return o
}

type fsTarRunBuilder struct {
	ctx       context.Context
	tempDir   string
	options   fsTarIndexOptions
	records   []fsTarRecord
	pathBytes int
	levels    [fsTarMergeLevels][]string
}

func newFSTarRunBuilder(ctx context.Context, tempDir string, options fsTarIndexOptions) *fsTarRunBuilder {
	return &fsTarRunBuilder{
		ctx:     ctx,
		tempDir: tempDir,
		options: options,
		records: make([]fsTarRecord, 0, fsTarRunMaxRecords),
	}
}

func (b *fsTarRunBuilder) Add(record fsTarRecord) error {
	if len(record.Path) > fsTarMaxPathBytes {
		return fmt.Errorf("%w: tar entry path exceeds %d bytes",
			archive.ErrInvalidFSMetadata, fsTarMaxPathBytes)
	}

	if len(b.records) > 0 &&
		(len(b.records) == fsTarRunMaxRecords || b.pathBytes+len(record.Path) > fsTarRunMaxPathBytes) {
		if err := b.flush(); err != nil {
			return err
		}
	}

	b.records = append(b.records, record)
	b.pathBytes += len(record.Path)

	return nil
}

func (b *fsTarRunBuilder) Finalize() (string, error) {
	if err := b.flush(); err != nil {
		return "", err
	}

	runs := make([]string, 0, fsTarMergeLevels*(fsTarMergeFanIn-1))
	for level := range b.levels {
		runs = append(runs, b.levels[level]...)
		clear(b.levels[level])
		b.levels[level] = nil
	}

	if len(runs) == 0 {
		empty, err := createPrivateFSTarTemp(b.options, b.tempDir, "sorted-*")
		if err != nil {
			return "", err
		}

		path := empty.Name()
		if err := errors.Join(empty.Sync(), empty.Close()); err != nil {
			_ = os.Remove(path)

			return "", fmt.Errorf("finalize empty filesystem tar index: %w", err)
		}

		return path, nil
	}

	for len(runs) > 1 {
		next := make([]string, 0, (len(runs)+fsTarMergeFanIn-1)/fsTarMergeFanIn)
		for start := 0; start < len(runs); start += fsTarMergeFanIn {
			end := min(start+fsTarMergeFanIn, len(runs))
			if end-start == 1 {
				next = append(next, runs[start])

				continue
			}

			merged, err := mergeFSTarRuns(b.ctx, b.options, b.tempDir, runs[start:end])
			if err != nil {
				return "", err
			}

			next = append(next, merged)
		}

		clear(runs)
		runs = next
	}

	return runs[0], nil
}

func (b *fsTarRunBuilder) flush() error {
	if len(b.records) == 0 {
		return nil
	}

	if err := checkFSTarContext(b.ctx); err != nil {
		return err
	}

	sort.Slice(b.records, func(i, j int) bool {
		return lessFSTarRecord(b.records[i], b.records[j])
	})

	run, err := createPrivateFSTarTemp(b.options, b.tempDir, "run-*")
	if err != nil {
		return err
	}

	runPath := run.Name()
	writer := bufio.NewWriterSize(run, fsTarIndexBufferBytes)

	for i := range b.records {
		if i%fsTarCancellationCadence == 0 {
			if err := checkFSTarContext(b.ctx); err != nil {
				_ = run.Close()
				_ = os.Remove(runPath)

				return err
			}
		}

		if err := b.options.writeRecord(writer, b.records[i]); err != nil {
			_ = run.Close()
			_ = os.Remove(runPath)

			return fmt.Errorf("write filesystem tar sort run: %w", err)
		}
	}

	if err := errors.Join(writer.Flush(), run.Sync(), run.Close()); err != nil {
		_ = os.Remove(runPath)

		return fmt.Errorf("finalize filesystem tar sort run: %w", err)
	}

	if err := b.addRun(0, runPath); err != nil {
		return err
	}

	clear(b.records)
	b.records = b.records[:0]
	b.pathBytes = 0

	return nil
}

func (b *fsTarRunBuilder) addRun(level int, runPath string) error {
	if level >= len(b.levels) {
		return fmt.Errorf("%w: filesystem tar index exceeds %d merge levels",
			archive.ErrInvalidFSMetadata, fsTarMergeLevels)
	}

	b.levels[level] = append(b.levels[level], runPath)
	if len(b.levels[level]) < fsTarMergeFanIn {
		return nil
	}

	merged, err := mergeFSTarRuns(b.ctx, b.options, b.tempDir, b.levels[level])
	if err != nil {
		return err
	}

	clear(b.levels[level])
	b.levels[level] = b.levels[level][:0]

	return b.addRun(level+1, merged)
}

type fsTarRunReader struct {
	file    *os.File
	reader  *bufio.Reader
	current fsTarRecord
	hasNext bool
}

func mergeFSTarRuns(
	ctx context.Context,
	options fsTarIndexOptions,
	tempDir string,
	runPaths []string,
) (string, error) {
	if len(runPaths) < 2 || len(runPaths) > fsTarMergeFanIn {
		return "", fmt.Errorf("merge filesystem tar runs: invalid fan-in %d", len(runPaths))
	}

	readers := make([]fsTarRunReader, 0, len(runPaths))
	for _, runPath := range runPaths {
		file, err := os.Open(runPath)
		if err != nil {
			return "", errors.Join(
				fmt.Errorf("open filesystem tar sort run %s: %w", runPath, err),
				closeFSTarRunReaders(readers),
			)
		}

		reader := fsTarRunReader{
			file:   file,
			reader: bufio.NewReaderSize(file, fsTarIndexBufferBytes),
		}

		record, err := readFSTarRecord(reader.reader)
		if err != nil && !errors.Is(err, io.EOF) {
			readers = append(readers, reader)

			return "", errors.Join(
				fmt.Errorf("read filesystem tar sort run %s: %w", runPath, err),
				closeFSTarRunReaders(readers),
			)
		}

		if err == nil {
			reader.current = record
			reader.hasNext = true
		}

		readers = append(readers, reader)
	}

	output, err := createPrivateFSTarTemp(options, tempDir, "merge-*")
	if err != nil {
		return "", errors.Join(err, closeFSTarRunReaders(readers))
	}

	outputPath := output.Name()
	writer := bufio.NewWriterSize(output, fsTarIndexBufferBytes)
	written := 0

	var mergeErr error

	for {
		selected := -1

		for i := range readers {
			if !readers[i].hasNext {
				continue
			}

			if selected < 0 || lessFSTarRecord(readers[i].current, readers[selected].current) {
				selected = i
			}
		}

		if selected < 0 {
			break
		}

		if written%fsTarCancellationCadence == 0 {
			if err := checkFSTarContext(ctx); err != nil {
				mergeErr = err

				break
			}
		}

		if err := options.writeRecord(writer, readers[selected].current); err != nil {
			mergeErr = fmt.Errorf("write merged filesystem tar index: %w", err)

			break
		}

		written++

		record, err := readFSTarRecord(readers[selected].reader)
		if errors.Is(err, io.EOF) {
			readers[selected].hasNext = false

			continue
		}

		if err != nil {
			mergeErr = fmt.Errorf("read filesystem tar sort run %s: %w", readers[selected].file.Name(), err)

			break
		}

		readers[selected].current = record
	}

	finalizeErr := errors.Join(writer.Flush(), output.Sync(), output.Close(), closeFSTarRunReaders(readers))
	if err := errors.Join(mergeErr, finalizeErr); err != nil {
		_ = os.Remove(outputPath)

		return "", err
	}

	for _, runPath := range runPaths {
		if err := os.Remove(runPath); err != nil {
			_ = os.Remove(outputPath)

			return "", fmt.Errorf("remove merged filesystem tar sort run %s: %w", runPath, err)
		}
	}

	return outputPath, nil
}

func closeFSTarRunReaders(readers []fsTarRunReader) error {
	closeErrs := make([]error, 0, len(readers))
	for i := range readers {
		if err := readers[i].file.Close(); err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("close filesystem tar sort run %s: %w",
				readers[i].file.Name(), err))
		}
	}

	return errors.Join(closeErrs...)
}

func lessFSTarRecord(left, right fsTarRecord) bool {
	if pathOrder := compareFSTarPaths(left.Path, right.Path); pathOrder != 0 {
		return pathOrder < 0
	}

	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}

	return left.Ordinal < right.Ordinal
}

// compareFSTarPaths orders slash-delimited component sequences so every path's
// descendants form one contiguous range before the next sibling component.
// Comparing UTF-8 bytes and a literal slash keeps the index independent of host
// path conventions and locale.
func compareFSTarPaths(left, right string) int {
	for {
		leftComponent, leftRest, leftHasSeparator := strings.Cut(left, "/")
		rightComponent, rightRest, rightHasSeparator := strings.Cut(right, "/")

		if leftComponent < rightComponent {
			return -1
		}

		if leftComponent > rightComponent {
			return 1
		}

		if leftHasSeparator != rightHasSeparator {
			if leftHasSeparator {
				return 1
			}

			return -1
		}

		if !leftHasSeparator {
			return 0
		}

		left = leftRest
		right = rightRest
	}
}

func isFSTarPathAncestor(ancestor, descendant string) bool {
	for {
		ancestorComponent, ancestorRest, ancestorHasSeparator := strings.Cut(ancestor, "/")
		descendantComponent, descendantRest, descendantHasSeparator := strings.Cut(descendant, "/")

		if ancestorComponent != descendantComponent {
			return false
		}

		if !ancestorHasSeparator {
			return descendantHasSeparator
		}

		if !descendantHasSeparator {
			return false
		}

		ancestor = ancestorRest
		descendant = descendantRest
	}
}

func createPrivateFSTarTemp(options fsTarIndexOptions, dir, pattern string) (*os.File, error) {
	file, err := options.createTemp(dir, pattern)
	if err != nil {
		return nil, fmt.Errorf("create private filesystem tar preflight file: %w", err)
	}

	if err := file.Chmod(0o600); err != nil {
		path := file.Name()
		closeErr := file.Close()
		removeErr := os.Remove(path)

		return nil, errors.Join(
			fmt.Errorf("set private permissions on filesystem tar preflight file %s: %w", path, err),
			closeErr,
			removeErr,
		)
	}

	return file, nil
}

func writeFSTarRecord(writer io.Writer, record fsTarRecord) error {
	if len(record.Path) > fsTarMaxPathBytes {
		return fmt.Errorf("%w: filesystem tar index path exceeds %d bytes",
			archive.ErrInvalidFSMetadata, fsTarMaxPathBytes)
	}

	var header [14]byte

	header[0] = byte(record.Kind)
	header[1] = record.Typeflag
	binary.BigEndian.PutUint64(header[2:10], record.Ordinal)
	binary.BigEndian.PutUint32(header[10:14], uint32(len(record.Path)))

	if _, err := writer.Write(header[:]); err != nil {
		return err
	}

	if _, err := io.WriteString(writer, record.Path); err != nil {
		return err
	}

	return nil
}

func readFSTarRecord(reader io.Reader) (fsTarRecord, error) {
	var header [14]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return fsTarRecord{}, err
	}

	pathLength := binary.BigEndian.Uint32(header[10:14])
	if pathLength > fsTarMaxPathBytes {
		return fsTarRecord{}, fmt.Errorf("%w: filesystem tar index path length %d exceeds %d",
			archive.ErrInvalidFSMetadata, pathLength, fsTarMaxPathBytes)
	}

	pathBytes := make([]byte, int(pathLength))
	if _, err := io.ReadFull(reader, pathBytes); err != nil {
		return fsTarRecord{}, err
	}

	kind := fsTarRecordKind(header[0])
	if kind > fsTarRecordUnsupported {
		return fsTarRecord{}, fmt.Errorf("%w: filesystem tar index has invalid record kind %d",
			archive.ErrInvalidFSMetadata, kind)
	}

	return fsTarRecord{
		Path:     string(pathBytes),
		Ordinal:  binary.BigEndian.Uint64(header[2:10]),
		Kind:     kind,
		Typeflag: header[1],
	}, nil
}

// importFSFromTar uploads regular data.tar entries without materializing plaintext
// on disk. A header-only preflight validates every regular entry before the first
// HEAD or PUT. Upload path, decoder, and exact X-Content-Length then come only from
// checksum-covered PAX metadata; Header.Name is never interpreted by suffix.
//
// Structural directory headers are not uploaded because the importer creates parent
// directories implicitly on the first child file write. Their mode, uid, gid, and mtime
// therefore cannot be restored; one bounded warning reports the number of affected
// directories without listing archive-controlled paths. Empty directories and every other
// non-regular entry are rejected by the preflight because the importer cannot reproduce them.
// onProgress, when non-nil, is called with the decompressed byte count after each file is
// successfully uploaded (or, for an already-fully-uploaded entry, credited without any
// decompression at all).
//
// setTotal, when non-nil (nil disables reporting, matching onProgress's convention), is
// called with a running sum of exact PAX raw sizes as entries are walked.
//
// activate, when non-nil, is called once per entry that actually needs a real PUT (the
// NOT-done branch), never inside the `if done` server-side-skip branch above it. This is
// deliberately NOT the same signal as onProgress: onProgress also fires on a skipped entry
// (crediting its bytes to the bar per invariant #7), but a leaf whose every entry is
// server-side-skipped is a genuine full resume-skip and must never activate the caller's
// progress stream — activate only distinguishes "at least one file was genuinely
// transferred" from "nothing was transferred".
func importFSFromTar(ctx context.Context, client httpDoer, baseURL, tarPath string, log *slog.Logger, setTotal func(int64), onProgress func(int), activate func()) error {
	file, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", tarPath, err)
	}

	uploadErr := importFSFromTarSource(ctx, client, baseURL, tarPath, file, log, setTotal, onProgress, activate)
	closeErr := file.Close()

	return errors.Join(uploadErr, closeTarFileError(tarPath, closeErr))
}

func importFSFromTarSource(ctx context.Context, client httpDoer, baseURL, tarPath string, source fsTarSource, log *slog.Logger, setTotal func(int64), onProgress func(int), activate func()) error {
	scan, err := scanFSTarSource(ctx, source, tarPath)
	if err != nil {
		return fmt.Errorf("filesystem tar metadata preflight: %w", err)
	}

	if err := revalidateFSTarAgainstScan(ctx, source, tarPath, &scan); err != nil {
		cleanupErr := scan.Close()

		return errors.Join(fmt.Errorf("filesystem tar identity preflight: %w", err), cleanupErr)
	}

	uploadErr := uploadFSTarFromScan(ctx, client, baseURL, tarPath, source, log, setTotal, onProgress, activate, &scan)
	cleanupErr := scan.Close()

	return errors.Join(uploadErr, cleanupErr)
}

func revalidateFSTarAgainstScan(
	ctx context.Context,
	source fsTarSource,
	tarPath string,
	scan *fsTarScan,
) error {
	info, err := source.Stat()
	if err != nil {
		return fmt.Errorf("inspect %s: %w", tarPath, err)
	}

	resetAuthenticatedRead(source)

	section := io.NewSectionReader(source, 0, info.Size())
	tr := tar.NewReader(section)

	sequence, err := os.Open(scan.sequencePath)
	if err != nil {
		return fmt.Errorf("open filesystem tar preflight sequence: %w", err)
	}

	sequenceReader := bufio.NewReaderSize(sequence, fsTarIndexBufferBytes)

	var (
		entryIndex   uint64
		regularIndex uint64
	)

	for {
		if entryIndex%fsTarCancellationCadence == 0 {
			if err := checkFSTarContext(ctx); err != nil {
				return errors.Join(err, closeFSTarSequence(sequence))
			}
		}

		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return errors.Join(
				fmt.Errorf("read tar entry from %s: %w", tarPath, err),
				closeFSTarSequence(sequence),
			)
		}

		expectedDigest, err := readFSTarSequenceDigest(sequenceReader)
		if errors.Is(err, io.EOF) {
			return errors.Join(
				fmt.Errorf("%w: tar entry count changed after preflight (unexpected entry %q at index %d)",
					archive.ErrInvalidFSMetadata, hdr.Name, entryIndex),
				closeFSTarSequence(sequence),
			)
		}

		if err != nil {
			return errors.Join(
				fmt.Errorf("read filesystem tar preflight sequence at entry %d: %w", entryIndex, err),
				closeFSTarSequence(sequence),
			)
		}

		actualDigest, err := digestFSTarHeader(hdr)
		if err != nil {
			return errors.Join(
				fmt.Errorf("revalidate tar entry %q: %w", hdr.Name, err),
				closeFSTarSequence(sequence),
			)
		}

		if actualDigest != expectedDigest {
			return errors.Join(
				fmt.Errorf("%w: tar entry identity or order changed after preflight at index %d (%q)",
					archive.ErrInvalidFSMetadata, entryIndex, hdr.Name),
				closeFSTarSequence(sequence),
			)
		}

		entryIndex++

		if hdr.Typeflag == tar.TypeReg || hdr.Typeflag == 0 {
			regularIndex++
		}
	}

	if entryIndex != scan.entryCount || regularIndex != scan.regularCount {
		return errors.Join(
			fmt.Errorf("%w: tar entry count changed after preflight (entries %d/%d, regular entries %d/%d)",
				archive.ErrInvalidFSMetadata, entryIndex, scan.entryCount, regularIndex, scan.regularCount),
			closeFSTarSequence(sequence),
		)
	}

	if _, err := readFSTarSequenceDigest(sequenceReader); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("%w: filesystem tar preflight sequence has an unexpected trailing record",
				archive.ErrInvalidFSMetadata)
		}

		return errors.Join(err, closeFSTarSequence(sequence))
	}

	return closeFSTarSequence(sequence)
}

func uploadFSTarFromScan(
	ctx context.Context,
	client httpDoer,
	baseURL, tarPath string,
	source fsTarSource,
	log *slog.Logger,
	setTotal func(int64),
	onProgress func(int),
	activate func(),
	scan *fsTarScan,
) error {
	if scan.StructuralDirectoryCount > 0 {
		log.Warn("filesystem import creates structural parent directories implicitly; directory mode, uid, gid, and mtime cannot be restored",
			slog.Int("directory_count", scan.StructuralDirectoryCount))
	}

	info, err := source.Stat()
	if err != nil {
		return fmt.Errorf("inspect %s: %w", tarPath, err)
	}

	resetAuthenticatedRead(source)

	section := io.NewSectionReader(source, 0, info.Size())
	tr := tar.NewReader(section)

	sequence, err := os.Open(scan.sequencePath)
	if err != nil {
		return fmt.Errorf("open filesystem tar preflight sequence: %w", err)
	}

	sequenceReader := bufio.NewReaderSize(sequence, fsTarIndexBufferBytes)

	var (
		entryIndex   uint64
		regularIndex uint64
		runningTotal int64
	)

	for {
		if entryIndex%fsTarCancellationCadence == 0 {
			if err := checkFSTarContext(ctx); err != nil {
				return errors.Join(err, closeFSTarSequence(sequence))
			}
		}

		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return errors.Join(
				fmt.Errorf("read tar entry from %s: %w", tarPath, err),
				closeFSTarSequence(sequence),
			)
		}

		expectedDigest, err := readFSTarSequenceDigest(sequenceReader)
		if errors.Is(err, io.EOF) {
			return errors.Join(
				fmt.Errorf("%w: tar entry count changed after preflight (unexpected entry %q at index %d)",
					archive.ErrInvalidFSMetadata, hdr.Name, entryIndex),
				closeFSTarSequence(sequence),
			)
		}

		if err != nil {
			return errors.Join(
				fmt.Errorf("read filesystem tar preflight sequence at entry %d: %w", entryIndex, err),
				closeFSTarSequence(sequence),
			)
		}

		actualDigest, err := digestFSTarHeader(hdr)
		if err != nil {
			return errors.Join(
				fmt.Errorf("revalidate tar entry %q: %w", hdr.Name, err),
				closeFSTarSequence(sequence),
			)
		}

		if actualDigest != expectedDigest {
			return errors.Join(
				fmt.Errorf("%w: tar entry identity or order changed after preflight at index %d (%q)",
					archive.ErrInvalidFSMetadata, entryIndex, hdr.Name),
				closeFSTarSequence(sequence),
			)
		}

		entryIndex++

		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != 0 {
			continue
		}

		metadata, err := archive.ParseFSMetadata(hdr)
		if err != nil {
			return errors.Join(
				fmt.Errorf("revalidate tar entry %q: %w", hdr.Name, err),
				closeFSTarSequence(sequence),
			)
		}

		if err := validateFSUploadPath(metadata.OriginalPath); err != nil {
			return errors.Join(
				fmt.Errorf("revalidate tar entry %q: %w", hdr.Name, err),
				closeFSTarSequence(sequence),
			)
		}

		regularIndex++

		ext, err := archive.FSCodecExtension(metadata.Codec)
		if err != nil {
			return errors.Join(
				fmt.Errorf("resolve codec for tar entry %q: %w", hdr.Name, err),
				closeFSTarSequence(sequence),
			)
		}

		relPath := metadata.OriginalPath

		payloadStart, err := section.Seek(0, io.SeekCurrent)
		if err != nil {
			return errors.Join(
				fmt.Errorf("determine tar payload offset for %s: %w", hdr.Name, err),
				closeFSTarSequence(sequence),
			)
		}

		runningTotal, err = addRawSize(runningTotal, metadata.RawSize)
		if err != nil {
			return errors.Join(
				fmt.Errorf("account tar entry %s: %w", relPath, err),
				closeFSTarSequence(sequence),
			)
		}

		if setTotal != nil {
			setTotal(runningTotal)
		}

		fileURL, err := fileUploadURL(baseURL, relPath)
		if err != nil {
			return errors.Join(err, closeFSTarSequence(sequence))
		}

		offset, done, doneSize, err := headFileOffset(ctx, client, fileURL, metadata.RawSize)
		if err != nil {
			return errors.Join(
				fmt.Errorf("probe upload state for %s: %w", relPath, err),
				closeFSTarSequence(sequence),
			)
		}

		progress := &fileUploadProgress{onProgress: onProgress}

		if done {
			if doneSize != metadata.RawSize {
				return errors.Join(
					fmt.Errorf("probe upload state for %s: completed size %d differs from PAX raw size %d",
						relPath, doneSize, metadata.RawSize),
					closeFSTarSequence(sequence),
				)
			}

			progress.creditTo(metadata.RawSize)

			continue
		}

		progress.creditTo(offset)

		attrs := fileAttrs{
			Perm:    os.FileMode(hdr.Mode & 0o777),
			UID:     hdr.Uid,
			GID:     hdr.Gid,
			ModTime: hdr.ModTime,
		}

		newBody := tarEntryBodyFactoryFromSource(source, tarPath, ext, payloadStart, hdr.Size, metadata.RawSize)
		if err := putFile(ctx, client, baseURL, relPath, metadata.RawSize, offset, attrs,
			newBody, progress, activate); err != nil {
			return errors.Join(
				fmt.Errorf("upload %s: %w", relPath, err),
				closeFSTarSequence(sequence),
			)
		}

		if err := verifyTarEntryRawSizeFromSource(ctx, source, tarPath, ext, payloadStart, hdr.Size, metadata.RawSize); err != nil {
			return errors.Join(
				fmt.Errorf("verify plaintext size for %s: %w", relPath, err),
				closeFSTarSequence(sequence),
			)
		}
	}

	if entryIndex != scan.entryCount || regularIndex != scan.regularCount {
		return errors.Join(
			fmt.Errorf("%w: tar entry count changed after preflight (entries %d/%d, regular entries %d/%d)",
				archive.ErrInvalidFSMetadata, entryIndex, scan.entryCount, regularIndex, scan.regularCount),
			closeFSTarSequence(sequence),
		)
	}

	if _, err := readFSTarSequenceDigest(sequenceReader); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("%w: filesystem tar preflight sequence has an unexpected trailing record",
				archive.ErrInvalidFSMetadata)
		}

		return errors.Join(err, closeFSTarSequence(sequence))
	}

	return closeFSTarSequence(sequence)
}

func scanFSTar(ctx context.Context, tarPath string) (fsTarScan, error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return fsTarScan{}, fmt.Errorf("open %s: %w", tarPath, err)
	}

	scan, scanErr := scanFSTarSource(ctx, f, tarPath)

	closeErr := f.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close %s: %w", tarPath, closeErr)
	}

	if err := errors.Join(scanErr, closeErr); err != nil {
		return fsTarScan{}, errors.Join(err, scan.Close())
	}

	return scan, nil
}

func scanVerifiedFSTar(ctx context.Context, node PlannedNode) error {
	if node.archiveView == nil || node.payloadFile == nil {
		scan, err := scanFSTar(ctx, node.TarFile)
		if err != nil {
			return err
		}

		return scan.Close()
	}

	handle, err := node.archiveView.OpenVerifiedFile(ctx, node.payloadFile)
	if err != nil {
		return err
	}

	scan, scanErr := scanFSTarSource(ctx, handle, node.TarFile)
	verifyErr := handle.Verify(ctx)
	closeErr := handle.Close()
	cleanupErr := scan.Close()

	return errors.Join(
		scanErr,
		verifyErr,
		closeTarFileError(node.TarFile, closeErr),
		cleanupErr,
	)
}

func scanFSTarSource(ctx context.Context, source fsTarSource, tarPath string) (fsTarScan, error) {
	info, err := source.Stat()
	if err != nil {
		return fsTarScan{}, fmt.Errorf("inspect %s: %w", tarPath, err)
	}

	resetAuthenticatedRead(source)

	return scanFSTarReader(ctx, tar.NewReader(io.NewSectionReader(source, 0, info.Size())), tarPath)
}

func scanFSTarReader(ctx context.Context, tr *tar.Reader, tarPath string) (fsTarScan, error) {
	return scanFSTarReaderWithOptions(ctx, tr, tarPath, defaultFSTarIndexOptions())
}

func scanFSTarReaderWithOptions(
	ctx context.Context,
	tr *tar.Reader,
	tarPath string,
	options fsTarIndexOptions,
) (fsTarScan, error) {
	options = options.withDefaults()

	tempDir, err := options.mkdirTemp(options.tempRoot, "d8-fs-tar-preflight-*")
	if err != nil {
		return fsTarScan{}, fmt.Errorf("create private filesystem tar preflight directory: %w", err)
	}

	if err := os.Chmod(tempDir, 0o700); err != nil {
		cleanupErr := options.removeAll(tempDir)

		return fsTarScan{}, errors.Join(
			fmt.Errorf("set private permissions on filesystem tar preflight directory %s: %w", tempDir, err),
			cleanupErr,
		)
	}

	cleanup := func(scanErr error) (fsTarScan, error) {
		cleanupErr := options.removeAll(tempDir)

		return fsTarScan{}, errors.Join(scanErr, cleanupErr)
	}

	sequence, err := createPrivateFSTarTemp(options, tempDir, "sequence-*")
	if err != nil {
		return cleanup(err)
	}

	sequencePath := sequence.Name()
	sequenceWriter := bufio.NewWriterSize(sequence, fsTarIndexBufferBytes)
	builder := newFSTarRunBuilder(ctx, tempDir, options)

	var (
		entryCount   uint64
		regularCount uint64
	)

	for {
		if entryCount%fsTarCancellationCadence == 0 {
			if err := checkFSTarContext(ctx); err != nil {
				_ = sequence.Close()

				return cleanup(err)
			}
		}

		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			_ = sequence.Close()

			return cleanup(fmt.Errorf("read tar entry from %s: %w", tarPath, err))
		}

		if entryCount == fsTarMaxEntries {
			_ = sequence.Close()

			return cleanup(fmt.Errorf("%w: filesystem tar exceeds %d entries",
				archive.ErrInvalidFSMetadata, fsTarMaxEntries))
		}

		digest, err := digestFSTarHeader(hdr)
		if err != nil {
			_ = sequence.Close()

			return cleanup(fmt.Errorf("entry %q: %w", hdr.Name, err))
		}

		if err := writeFSTarSequenceDigest(sequenceWriter, digest); err != nil {
			_ = sequence.Close()

			return cleanup(fmt.Errorf("write filesystem tar preflight sequence: %w", err))
		}

		record := fsTarRecord{
			Ordinal:  entryCount,
			Typeflag: hdr.Typeflag,
		}

		switch hdr.Typeflag {
		case tar.TypeReg, 0:
			if originalPath, ok := hdr.PAXRecords[archive.PAXFSOriginalPath]; ok {
				if err := validateFSTarPathLength(originalPath); err != nil {
					_ = sequence.Close()

					return cleanup(fmt.Errorf("entry %q: %w", hdr.Name, err))
				}

				if err := validateFSUploadPath(originalPath); err != nil {
					_ = sequence.Close()

					return cleanup(fmt.Errorf("entry %q: %w", hdr.Name, err))
				}
			}

			metadata, err := archive.ParseFSMetadata(hdr)
			if err != nil {
				_ = sequence.Close()

				return cleanup(fmt.Errorf("entry %q: %w", hdr.Name, err))
			}

			if err := validateFSTarPathLength(metadata.OriginalPath); err != nil {
				_ = sequence.Close()

				return cleanup(fmt.Errorf("entry %q: %w", hdr.Name, err))
			}

			if err := validateFSUploadPath(metadata.OriginalPath); err != nil {
				_ = sequence.Close()

				return cleanup(fmt.Errorf("entry %q: %w", hdr.Name, err))
			}

			record.Kind = fsTarRecordRegular
			record.Path = path.Clean(metadata.OriginalPath)
			regularCount++
		case tar.TypeDir:
			directoryPath, err := normalizeFSTarDirectoryPath(hdr.Name)
			if err != nil {
				_ = sequence.Close()

				return cleanup(fmt.Errorf("directory entry %q: %w", hdr.Name, err))
			}

			record.Kind = fsTarRecordDirectory
			record.Path = directoryPath
		default:
			if err := validateFSTarPathLength(hdr.Name); err != nil {
				_ = sequence.Close()

				return cleanup(fmt.Errorf("entry %q: %w", hdr.Name, err))
			}

			record.Kind = fsTarRecordUnsupported
			record.Path = hdr.Name
		}

		if err := builder.Add(record); err != nil {
			_ = sequence.Close()

			return cleanup(err)
		}

		entryCount++
	}

	if err := errors.Join(sequenceWriter.Flush(), sequence.Sync(), sequence.Close()); err != nil {
		return cleanup(fmt.Errorf("finalize filesystem tar preflight sequence: %w", err))
	}

	sortedPath, err := builder.Finalize()
	if err != nil {
		return cleanup(err)
	}

	structuralDirectoryCount, err := validateSortedFSTar(ctx, sortedPath)
	if err != nil {
		return cleanup(err)
	}

	return fsTarScan{
		tempDir:                  tempDir,
		sequencePath:             sequencePath,
		entryCount:               entryCount,
		regularCount:             regularCount,
		StructuralDirectoryCount: structuralDirectoryCount,
		removeAll:                options.removeAll,
	}, nil
}

func normalizeFSTarDirectoryPath(headerPath string) (string, error) {
	directoryPath := strings.TrimSuffix(headerPath, "/")
	if err := validateFSTarPathLength(directoryPath); err != nil {
		return "", err
	}

	if err := validateFSUploadPath(directoryPath); err != nil {
		return "", err
	}

	return directoryPath, nil
}

type fsTarDiagnosticCollector struct {
	count    uint64
	messages []string
}

func (c *fsTarDiagnosticCollector) Add(message string) {
	c.count++
	if len(c.messages) < fsTarMaxDiagnostics {
		c.messages = append(c.messages, message)
	}
}

func (c *fsTarDiagnosticCollector) Err() error {
	if c.count == 0 {
		return nil
	}

	message := strings.Join(c.messages, "; ")
	if omitted := c.count - uint64(len(c.messages)); omitted > 0 {
		message += fmt.Sprintf("; ... %d additional entries omitted", omitted)
	}

	return fmt.Errorf("unsupported filesystem tar entries (%d): %s", c.count, message)
}

func validateSortedFSTar(ctx context.Context, sortedPath string) (int, error) {
	diagnostics := &fsTarDiagnosticCollector{
		messages: make([]string, 0, fsTarMaxDiagnostics),
	}

	if err := inspectSortedFSTarConflicts(ctx, sortedPath, diagnostics); err != nil {
		return 0, err
	}

	structuralDirectoryCount, err := classifySortedFSTarDirectories(ctx, sortedPath, diagnostics)
	if err != nil {
		return 0, err
	}

	if err := diagnostics.Err(); err != nil {
		return 0, err
	}

	return structuralDirectoryCount, nil
}

func inspectSortedFSTarConflicts(
	ctx context.Context,
	sortedPath string,
	diagnostics *fsTarDiagnosticCollector,
) error {
	file, err := os.Open(sortedPath)
	if err != nil {
		return fmt.Errorf("open sorted filesystem tar preflight index: %w", err)
	}

	reader := bufio.NewReaderSize(file, fsTarIndexBufferBytes)

	var (
		previousRegular   string
		previousDirectory string
		recordIndex       uint64
	)

	for {
		if recordIndex%fsTarCancellationCadence == 0 {
			if err := checkFSTarContext(ctx); err != nil {
				return errors.Join(err, closeFSTarIndex(file))
			}
		}

		record, err := readFSTarRecord(reader)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return errors.Join(
				fmt.Errorf("read sorted filesystem tar preflight index: %w", err),
				closeFSTarIndex(file),
			)
		}

		switch record.Kind {
		case fsTarRecordRegular:
			if record.Path == previousRegular {
				return errors.Join(
					fmt.Errorf("%w: duplicate normalized original path %q",
						archive.ErrInvalidFSMetadata, record.Path),
					closeFSTarIndex(file),
				)
			}

			if previousRegular != "" && isFSTarPathAncestor(previousRegular, record.Path) {
				return errors.Join(
					fmt.Errorf("%w: regular path %q is an ancestor of regular path %q",
						archive.ErrInvalidFSMetadata, previousRegular, record.Path),
					closeFSTarIndex(file),
				)
			}

			previousRegular = record.Path
		case fsTarRecordDirectory:
			if record.Path == previousDirectory {
				return errors.Join(
					fmt.Errorf("%w: duplicate normalized directory path %q",
						archive.ErrInvalidFSMetadata, record.Path),
					closeFSTarIndex(file),
				)
			}

			if previousRegular == record.Path ||
				previousRegular != "" && isFSTarPathAncestor(previousRegular, record.Path) {
				return errors.Join(
					fmt.Errorf("%w: regular path %q conflicts with directory path %q",
						archive.ErrInvalidFSMetadata, previousRegular, record.Path),
					closeFSTarIndex(file),
				)
			}

			previousDirectory = record.Path
		case fsTarRecordUnsupported:
			diagnostics.Add(fmt.Sprintf(
				"entry %q (%s): unsupported filesystem entry semantics",
				record.Path,
				fsTarTypeName(record.Typeflag),
			))
		default:
			return errors.Join(
				fmt.Errorf("%w: unknown filesystem tar index record kind %d",
					archive.ErrInvalidFSMetadata, record.Kind),
				closeFSTarIndex(file),
			)
		}

		recordIndex++
	}

	return closeFSTarIndex(file)
}

type fsTarKindCursor struct {
	ctx       context.Context
	file      *os.File
	reader    *bufio.Reader
	kind      fsTarRecordKind
	readCount uint64
}

func openFSTarKindCursor(
	ctx context.Context,
	sortedPath string,
	kind fsTarRecordKind,
) (*fsTarKindCursor, error) {
	file, err := os.Open(sortedPath)
	if err != nil {
		return nil, fmt.Errorf("open sorted filesystem tar preflight index: %w", err)
	}

	return &fsTarKindCursor{
		ctx:    ctx,
		file:   file,
		reader: bufio.NewReaderSize(file, fsTarIndexBufferBytes),
		kind:   kind,
	}, nil
}

func (c *fsTarKindCursor) Next() (fsTarRecord, error) {
	for {
		if c.readCount%fsTarCancellationCadence == 0 {
			if err := checkFSTarContext(c.ctx); err != nil {
				return fsTarRecord{}, err
			}
		}

		record, err := readFSTarRecord(c.reader)
		if err != nil {
			return fsTarRecord{}, err
		}

		c.readCount++

		if record.Kind == c.kind {
			return record, nil
		}
	}
}

func (c *fsTarKindCursor) Close() error {
	return closeFSTarIndex(c.file)
}

func classifySortedFSTarDirectories(
	ctx context.Context,
	sortedPath string,
	diagnostics *fsTarDiagnosticCollector,
) (int, error) {
	directories, err := openFSTarKindCursor(ctx, sortedPath, fsTarRecordDirectory)
	if err != nil {
		return 0, err
	}

	regulars, err := openFSTarKindCursor(ctx, sortedPath, fsTarRecordRegular)
	if err != nil {
		return 0, errors.Join(err, directories.Close())
	}

	regular, regularErr := regulars.Next()

	structuralDirectoryCount := 0

	for {
		directory, err := directories.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return 0, errors.Join(err, directories.Close(), regulars.Close())
		}

		for regularErr == nil && compareFSTarPaths(regular.Path, directory.Path) <= 0 {
			regular, regularErr = regulars.Next()
		}

		if regularErr != nil && !errors.Is(regularErr, io.EOF) {
			return 0, errors.Join(regularErr, directories.Close(), regulars.Close())
		}

		if regularErr == nil && isFSTarPathAncestor(directory.Path, regular.Path) {
			structuralDirectoryCount++

			continue
		}

		diagnostics.Add(fmt.Sprintf(
			"entry %q (directory): unsupported empty directory has no regular-file descendant",
			directory.Path,
		))
	}

	return structuralDirectoryCount, errors.Join(directories.Close(), regulars.Close())
}

func validateFSTarPathLength(value string) error {
	if len(value) > fsTarMaxPathBytes {
		return fmt.Errorf("%w: tar entry path exceeds %d bytes",
			archive.ErrInvalidFSMetadata, fsTarMaxPathBytes)
	}

	return nil
}

func writeFSTarSequenceDigest(writer io.Writer, digest [sha256.Size]byte) error {
	if _, err := writer.Write(digest[:]); err != nil {
		return err
	}

	return nil
}

func readFSTarSequenceDigest(reader io.Reader) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if _, err := io.ReadFull(reader, digest[:]); err != nil {
		return [sha256.Size]byte{}, err
	}

	return digest, nil
}

func digestFSTarHeader(header *tar.Header) ([sha256.Size]byte, error) {
	if header == nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: nil tar header", archive.ErrInvalidFSMetadata)
	}

	if err := validateFSTarPathLength(header.Name); err != nil {
		return [sha256.Size]byte{}, err
	}

	digest := sha256.New()
	if err := hashFSTarByte(digest, header.Typeflag); err != nil {
		return [sha256.Size]byte{}, err
	}

	for _, value := range []string{
		header.Name,
		header.Linkname,
		header.Uname,
		header.Gname,
		header.ModTime.UTC().Format(time.RFC3339Nano),
		header.AccessTime.UTC().Format(time.RFC3339Nano),
		header.ChangeTime.UTC().Format(time.RFC3339Nano),
	} {
		if err := hashFSTarString(digest, value); err != nil {
			return [sha256.Size]byte{}, err
		}
	}

	for _, value := range []int64{
		header.Size,
		header.Mode,
		int64(header.Uid),
		int64(header.Gid),
		header.Devmajor,
		header.Devminor,
		int64(header.Format),
	} {
		if err := hashFSTarInt64(digest, value); err != nil {
			return [sha256.Size]byte{}, err
		}
	}

	if err := hashFSTarMap(digest, header.PAXRecords); err != nil {
		return [sha256.Size]byte{}, err
	}

	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))

	return result, nil
}

func hashFSTarMap(digest hash.Hash, values map[string]string) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	if err := hashFSTarInt64(digest, int64(len(keys))); err != nil {
		return err
	}

	for _, key := range keys {
		if err := hashFSTarString(digest, key); err != nil {
			return err
		}

		if err := hashFSTarString(digest, values[key]); err != nil {
			return err
		}
	}

	return nil
}

func hashFSTarString(digest hash.Hash, value string) error {
	if err := hashFSTarInt64(digest, int64(len(value))); err != nil {
		return err
	}

	if _, err := io.WriteString(digest, value); err != nil {
		return err
	}

	return nil
}

func hashFSTarInt64(digest hash.Hash, value int64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))

	if _, err := digest.Write(encoded[:]); err != nil {
		return err
	}

	return nil
}

func hashFSTarByte(digest hash.Hash, value byte) error {
	if _, err := digest.Write([]byte{value}); err != nil {
		return err
	}

	return nil
}

func checkFSTarContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("filesystem tar preflight cancelled: %w", err)
	}

	return nil
}

func closeFSTarIndex(file *os.File) error {
	if err := file.Close(); err != nil {
		return fmt.Errorf("close filesystem tar preflight index %s: %w", file.Name(), err)
	}

	return nil
}

func closeFSTarSequence(file *os.File) error {
	if err := file.Close(); err != nil {
		return fmt.Errorf("close filesystem tar preflight sequence %s: %w", file.Name(), err)
	}

	return nil
}

func fsTarTypeName(typeflag byte) string {
	switch typeflag {
	case tar.TypeSymlink:
		return "symlink"
	case tar.TypeLink:
		return "hardlink"
	case tar.TypeChar:
		return "character device"
	case tar.TypeBlock:
		return "block device"
	case tar.TypeFifo:
		return "FIFO"
	default:
		return fmt.Sprintf("unknown typeflag %#x", typeflag)
	}
}

func addRawSize(total, size int64) (int64, error) {
	if size > math.MaxInt64-total {
		return 0, fmt.Errorf("raw-size total overflows int64")
	}

	return total + size, nil
}

type fsUploadBody struct {
	reader    io.Reader
	closers   []io.Closer
	closeOnce sync.Once
	closeErr  error
}

func (b *fsUploadBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *fsUploadBody) Close() error {
	b.closeOnce.Do(func() {
		closeErrs := make([]error, 0, len(b.closers))
		for _, closer := range b.closers {
			if err := closer.Close(); err != nil {
				closeErrs = append(closeErrs, err)
			}
		}

		b.closeErr = errors.Join(closeErrs...)
	})

	return b.closeErr
}

// tarEntryBodyFactoryFromSource deliberately preserves the source's authenticated chunk cache
// across adjacent entries and body reopens. Raw bytes are traversed once per requested range;
// compressed bytes are traversed once per request plus any decoded-prefix replay for reposition.
func tarEntryBodyFactoryFromSource(source io.ReaderAt, tarPath, ext string, payloadStart, storedSize, rawSize int64) fileBodyFactory {
	return func(ctx context.Context, offset, size int64) (io.ReadCloser, error) {
		if err := validateBlockOffset(offset, rawSize); err != nil {
			return nil, fmt.Errorf("validate body offset: %w", err)
		}

		if size < 0 || size > rawSize-offset {
			return nil, fmt.Errorf("body size %d at offset %d exceeds raw size %d", size, offset, rawSize)
		}

		if payloadStart > math.MaxInt64-offset {
			return nil, fmt.Errorf("tar payload offset %d plus raw offset %d overflows int64", payloadStart, offset)
		}

		if ext == "" {
			section := io.NewSectionReader(source, payloadStart+offset, size)

			return &fsUploadBody{reader: section}, nil
		}

		stored := io.NewSectionReader(source, payloadStart, storedSize)

		decoder, err := compress.NewReader(ext, stored)
		if err != nil {
			return nil, fmt.Errorf("open decompressor for %s: %w", tarPath, err)
		}

		discarded, err := discardDecoded(ctx, decoder, offset)
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("discard decoded prefix (got %d of %d bytes): %w", discarded, offset, err),
				closeTarEntryDecoder(decoder),
			)
		}

		return &fsUploadBody{
			reader:  io.LimitReader(decoder, size),
			closers: []io.Closer{decoder},
		}, nil
	}
}

func verifyTarEntryRawSizeFromSource(ctx context.Context, source io.ReaderAt, tarPath, ext string, payloadStart, storedSize, rawSize int64) error {
	if ext == "" {
		if storedSize != rawSize {
			return fmt.Errorf("stored size %d differs from declared raw size %d", storedSize, rawSize)
		}

		return nil
	}

	stored := io.NewSectionReader(source, payloadStart, storedSize)

	decoder, err := compress.NewReader(ext, stored)
	if err != nil {
		return fmt.Errorf("open decompressor for %s: %w", tarPath, err)
	}

	discarded, err := discardDecoded(ctx, decoder, rawSize)
	if err != nil {
		return errors.Join(
			fmt.Errorf("discard decoded payload (got %d of %d bytes): %w", discarded, rawSize, err),
			closeTarEntryDecoder(decoder),
		)
	}

	var probe [1]byte

	n, readErr := decoder.Read(probe[:])

	switch {
	case n > 0:
		readErr = fmt.Errorf("decoded stream exceeds declared PAX raw size %d", rawSize)
	case errors.Is(readErr, io.EOF):
		readErr = nil
	case readErr != nil:
		readErr = fmt.Errorf("probe decoded stream end: %w", readErr)
	}

	closeErr := closeTarEntryDecoder(decoder)

	if readErr != nil || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}

	return nil
}

func closeTarEntryDecoder(decoder io.Closer) error {
	if err := decoder.Close(); err != nil {
		return fmt.Errorf("close decoder: %w", err)
	}

	return nil
}

func closeTarFileError(path string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("close tar %s: %w", path, err)
}
