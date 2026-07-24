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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	neturl "net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

const uploadFilesSubpath = "api/v1/files"

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

	conflicts := make(map[[2]int64]struct{})

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
			transition := [2]int64{offset, next}
			if _, repeated := conflicts[transition]; repeated {
				return fmt.Errorf("server-directed file upload offset loop from %d to %d", offset, next)
			}

			conflicts[transition] = struct{}{}
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

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

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

	resp, err := client.HTTPDo(req)
	if err != nil {
		return 0, false, err
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

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

type fsTarNonRegularEntry struct {
	Typeflag   byte
	HeaderPath string
}

type fsTarScan struct {
	RegularPaths             []string
	StructuralDirectoryCount int
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
	scan, err := scanFSTarSource(source, tarPath)
	if err != nil {
		return fmt.Errorf("filesystem tar metadata preflight: %w", err)
	}

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
	regularIndex := 0

	var runningTotal int64

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("read tar entry from %s: %w", tarPath, err)
		}

		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != 0 {
			continue
		}

		metadata, err := archive.ParseFSMetadata(hdr)
		if err != nil {
			return fmt.Errorf("revalidate tar entry %q: %w", hdr.Name, err)
		}

		if err := validateFSUploadPath(metadata.OriginalPath); err != nil {
			return fmt.Errorf("revalidate tar entry %q: %w", hdr.Name, err)
		}

		if regularIndex >= len(scan.RegularPaths) || scan.RegularPaths[regularIndex] != metadata.OriginalPath {
			return fmt.Errorf("%w: tar regular-entry order changed after preflight at entry %q",
				archive.ErrInvalidFSMetadata, hdr.Name)
		}

		regularIndex++

		ext, err := archive.FSCodecExtension(metadata.Codec)
		if err != nil {
			return fmt.Errorf("resolve codec for tar entry %q: %w", hdr.Name, err)
		}

		relPath := metadata.OriginalPath

		payloadStart, err := section.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("determine tar payload offset for %s: %w", hdr.Name, err)
		}

		runningTotal, err = addRawSize(runningTotal, metadata.RawSize)
		if err != nil {
			return fmt.Errorf("account tar entry %s: %w", relPath, err)
		}

		if setTotal != nil {
			setTotal(runningTotal)
		}

		fileURL, err := fileUploadURL(baseURL, relPath)
		if err != nil {
			return err
		}

		offset, done, doneSize, err := headFileOffset(ctx, client, fileURL, metadata.RawSize)
		if err != nil {
			return fmt.Errorf("probe upload state for %s: %w", relPath, err)
		}

		progress := &fileUploadProgress{onProgress: onProgress}

		if done {
			if doneSize != metadata.RawSize {
				return fmt.Errorf("probe upload state for %s: completed size %d differs from PAX raw size %d",
					relPath, doneSize, metadata.RawSize)
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
			return fmt.Errorf("upload %s: %w", relPath, err)
		}

		if err := verifyTarEntryRawSizeFromSource(ctx, source, tarPath, ext, payloadStart, hdr.Size, metadata.RawSize); err != nil {
			return fmt.Errorf("verify plaintext size for %s: %w", relPath, err)
		}
	}

	if regularIndex != len(scan.RegularPaths) {
		return fmt.Errorf("%w: tar regular-entry count changed after preflight (got %d, want %d)",
			archive.ErrInvalidFSMetadata, regularIndex, len(scan.RegularPaths))
	}

	return nil
}

func scanFSTar(tarPath string) (fsTarScan, error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return fsTarScan{}, fmt.Errorf("open %s: %w", tarPath, err)
	}

	scan, scanErr := scanFSTarSource(f, tarPath)

	closeErr := f.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close %s: %w", tarPath, closeErr)
	}

	if err := errors.Join(scanErr, closeErr); err != nil {
		return fsTarScan{}, err
	}

	return scan, nil
}

func scanVerifiedFSTar(ctx context.Context, node PlannedNode) error {
	if node.archiveView == nil || node.payloadFile == nil {
		_, err := scanFSTar(node.TarFile)

		return err
	}

	handle, err := node.archiveView.OpenVerifiedFile(ctx, node.payloadFile)
	if err != nil {
		return err
	}

	_, scanErr := scanFSTarSource(handle, node.TarFile)
	verifyErr := handle.Verify(ctx)
	closeErr := handle.Close()

	return errors.Join(
		scanErr,
		verifyErr,
		closeTarFileError(node.TarFile, closeErr),
	)
}

func scanFSTarSource(source fsTarSource, tarPath string) (fsTarScan, error) {
	info, err := source.Stat()
	if err != nil {
		return fsTarScan{}, fmt.Errorf("inspect %s: %w", tarPath, err)
	}

	resetAuthenticatedRead(source)

	return scanFSTarReader(tar.NewReader(io.NewSectionReader(source, 0, info.Size())), tarPath)
}

func scanFSTarReader(tr *tar.Reader, tarPath string) (fsTarScan, error) {
	scan := fsTarScan{}
	originalPaths := make(map[string]struct{})

	var nonRegular []fsTarNonRegularEntry

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fsTarScan{}, fmt.Errorf("read tar entry from %s: %w", tarPath, err)
		}

		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != 0 {
			nonRegular = append(nonRegular, fsTarNonRegularEntry{
				Typeflag:   hdr.Typeflag,
				HeaderPath: hdr.Name,
			})

			continue
		}

		if originalPath, ok := hdr.PAXRecords[archive.PAXFSOriginalPath]; ok {
			if err := validateFSUploadPath(originalPath); err != nil {
				return fsTarScan{}, fmt.Errorf("entry %q: %w", hdr.Name, err)
			}
		}

		metadata, err := archive.ParseFSMetadata(hdr)
		if err != nil {
			return fsTarScan{}, fmt.Errorf("entry %q: %w", hdr.Name, err)
		}

		if err := validateFSUploadPath(metadata.OriginalPath); err != nil {
			return fsTarScan{}, fmt.Errorf("entry %q: %w", hdr.Name, err)
		}

		normalized := path.Clean(metadata.OriginalPath)
		if _, exists := originalPaths[normalized]; exists {
			return fsTarScan{}, fmt.Errorf("%w: duplicate normalized original path %q",
				archive.ErrInvalidFSMetadata, normalized)
		}

		originalPaths[normalized] = struct{}{}
		scan.RegularPaths = append(scan.RegularPaths, normalized)
	}

	entryErrs := make([]error, 0, len(nonRegular))

	for _, entry := range nonRegular {
		if entry.Typeflag == tar.TypeDir {
			directoryPath, err := normalizeFSTarDirectoryPath(entry.HeaderPath)
			if err != nil {
				entryErrs = append(entryErrs, fmt.Errorf("directory entry %q: %w", entry.HeaderPath, err))

				continue
			}

			if hasRegularDescendant(directoryPath, scan.RegularPaths) {
				scan.StructuralDirectoryCount++

				continue
			}

			entryErrs = append(entryErrs, fmt.Errorf(
				"entry %q (directory): unsupported empty directory has no regular-file descendant",
				directoryPath,
			))

			continue
		}

		entryErrs = append(entryErrs, fmt.Errorf(
			"entry %q (%s): unsupported filesystem entry semantics",
			entry.HeaderPath,
			fsTarTypeName(entry.Typeflag),
		))
	}

	if len(entryErrs) > 0 {
		return fsTarScan{}, fmt.Errorf("unsupported filesystem tar entries (%d): %w",
			len(entryErrs), errors.Join(entryErrs...))
	}

	return scan, nil
}

func normalizeFSTarDirectoryPath(headerPath string) (string, error) {
	directoryPath := strings.TrimSuffix(headerPath, "/")
	if err := validateFSUploadPath(directoryPath); err != nil {
		return "", err
	}

	return directoryPath, nil
}

func hasRegularDescendant(directoryPath string, regularPaths []string) bool {
	prefix := directoryPath + "/"

	for _, regularPath := range regularPaths {
		if strings.HasPrefix(regularPath, prefix) {
			return true
		}
	}

	return false
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

		resetAuthenticatedRead(source)

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

	resetAuthenticatedRead(source)

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
