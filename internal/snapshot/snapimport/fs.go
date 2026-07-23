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
	"strconv"
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

// putFile PUTs body — already positioned at offset in the file's original (decompressed)
// byte stream, with totalSize its exact total length — to the FS importer at baseURL under
// relPath, preserving the given file attributes. putFile performs no HEAD probing of its
// own: the caller (importFSFromTar) already made the single HEAD call that determines both
// whether the file is done and, when not, its resume offset, and is responsible for
// positioning body accordingly (e.g. via discard-and-fast-forward). Responses:
//   - 201 Created: file is complete.
//   - 204 No Content + X-Next-Offset: partial; the next chunk starts at that offset.
//   - 409 Conflict + X-Expected-Offset: offset mismatch; the loop retries from the
//     server-reported position.
//
// Callers are responsible for calling postFinished after all files are uploaded.
func putFile(ctx context.Context, client httpDoer, baseURL, relPath string, body io.Reader, totalSize, offset int64, attrs fileAttrs) error {
	fileURL, err := neturl.JoinPath(baseURL, uploadFilesSubpath, relPath)
	if err != nil {
		return fmt.Errorf("build file URL for %s: %w", relPath, err)
	}

	if totalSize == 0 {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, fileURL, http.NoBody)
		if err != nil {
			return err
		}

		setFileHeaders(req, 0, 0, attrs)

		if _, err = doFileChunk(client, req, 0, 0); err != nil {
			return fmt.Errorf("upload %s at offset 0: %w", relPath, err)
		}

		return nil
	}

	// A resume offset already equal to the total means a prior call already durably
	// transferred every byte (the finalize step just never confirmed it); nothing remains
	// to stream, so skip the loop entirely rather than issuing a spurious zero-length PUT.
	if offset >= totalSize {
		return nil
	}

	for offset < totalSize {
		remain := totalSize - offset
		limited := io.LimitReader(body, remain)

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, fileURL, io.NopCloser(limited))
		if err != nil {
			return err
		}

		// net/http only auto-detects Content-Length for *bytes.Buffer/*bytes.Reader/
		// *strings.Reader bodies; an io.LimitReader-wrapped stream needs it set
		// explicitly, or the request silently falls back to chunked transfer encoding.
		req.ContentLength = remain

		setFileHeaders(req, totalSize, offset, attrs)

		next, err := doFileChunk(client, req, offset, totalSize)
		if err != nil {
			return fmt.Errorf("upload %s at offset %d: %w", relPath, offset, err)
		}

		offset = next
	}

	return nil
}

// setFileHeaders sets the headers the FS importer's CheckRequiredHeaders middleware
// requires on every PUT.
func setFileHeaders(req *http.Request, totalSize, offset int64, attrs fileAttrs) {
	req.Header.Set("X-Content-Length", strconv.FormatInt(totalSize, 10))
	req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))
	req.Header.Set("X-Attribute-Permissions", fmt.Sprintf("%04o", attrs.Perm))
	req.Header.Set("X-Attribute-Uid", strconv.Itoa(attrs.UID))
	req.Header.Set("X-Attribute-Gid", strconv.Itoa(attrs.GID))
	req.Header.Set("X-Attribute-ModTime", attrs.ModTime.UTC().Format(time.RFC3339))
}

// headFileOffset probes the file endpoint to determine the current upload state.
// Returns (offset, done, size) where done=true means the final file already exists and
// the upload should be skipped entirely; size is that final file's exact on-disk
// (decompressed) byte count, read from the HEAD response's Content-Length, and is only
// meaningful when done is true — it lets a caller credit progress for a skipped file
// without decompressing it. offset is the number of bytes already written to the
// server's temp file when done is false; 0 if no partial upload exists.
func headFileOffset(ctx context.Context, client httpDoer, fileURL string) (int64, bool, int64, error) {
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
			if parseErr != nil || off < 0 {
				return 0, false, 0, fmt.Errorf("invalid X-Next-Offset %q from HEAD %s", next, fileURL)
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

// doFileChunk performs one PUT to the FS importer and returns the next offset to resume
// from. On 201 it returns totalSize (upload complete). On 204 it advances to X-Next-Offset.
// On 409 it returns X-Expected-Offset so the caller can retry from the correct position.
func doFileChunk(client httpDoer, req *http.Request, offset, totalSize int64) (int64, error) {
	resp, err := client.HTTPDo(req)
	if err != nil {
		return 0, err
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusCreated:
		return totalSize, nil

	case http.StatusNoContent:
		next, parseErr := parseOffsetHeader(resp.Header, "X-Next-Offset")
		if parseErr != nil {
			return 0, fmt.Errorf("204 missing valid X-Next-Offset: %w", parseErr)
		}

		if next <= offset {
			return 0, fmt.Errorf("server returned non-advancing X-Next-Offset (%d <= %d)", next, offset)
		}

		return next, nil

	case http.StatusConflict:
		exp, parseErr := parseOffsetHeader(resp.Header, "X-Expected-Offset")
		if parseErr != nil {
			return 0, fmt.Errorf("409 missing valid X-Expected-Offset: %w", parseErr)
		}

		// Prevent tight loop: if the expected offset equals the one we sent, the mismatch
		// is permanent and retrying would spin forever.
		if exp == offset {
			return 0, fmt.Errorf("server 409: X-Expected-Offset %d equals sent offset — offset mismatch is unrecoverable", exp)
		}

		return exp, nil

	default:
		return 0, fmt.Errorf("server error at offset %d: status %d (%s)", offset, resp.StatusCode, resp.Status)
	}
}

// parseOffsetHeader parses a non-negative int64 from a named HTTP response header.
// Returns an error when the header is absent or its value is not a valid non-negative integer.
func parseOffsetHeader(h http.Header, name string) (int64, error) {
	val := h.Get(name)
	if val == "" {
		return 0, fmt.Errorf("header %q is missing or empty", name)
	}

	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid %q value %q", name, val)
	}

	return n, nil
}

// importFSFromTar uploads regular data.tar entries without materializing plaintext
// on disk. A header-only preflight validates every regular entry before the first
// HEAD or PUT. Upload path, decoder, and exact X-Content-Length then come only from
// checksum-covered PAX metadata; Header.Name is never interpreted by suffix.
//
// Directory and symlink entries are skipped — the importer creates parent directories
// implicitly on the first child file write. onProgress, when non-nil, is called with the
// decompressed byte count after each file is successfully uploaded (or, for an
// already-fully-uploaded entry, credited without any decompression at all).
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
//
// TODO(follow-up): reproduce empty-directory and symlink entries when needed.
func importFSFromTar(ctx context.Context, client httpDoer, baseURL, tarPath string, log *slog.Logger, setTotal func(int64), onProgress func(int), activate func()) error {
	if err := validateFSTarMetadata(tarPath); err != nil {
		return fmt.Errorf("filesystem tar metadata preflight: %w", err)
	}

	f, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", tarPath, err)
	}

	defer func() { _ = f.Close() }()

	skipped := 0
	tr := tar.NewReader(f)

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
			skipped++

			continue
		}

		metadata, err := archive.ParseFSMetadata(hdr)
		if err != nil {
			return fmt.Errorf("revalidate tar entry %q: %w", hdr.Name, err)
		}

		ext, err := archive.FSCodecExtension(metadata.Codec)
		if err != nil {
			return fmt.Errorf("resolve codec for tar entry %q: %w", hdr.Name, err)
		}

		relPath := metadata.OriginalPath

		fileURL, err := neturl.JoinPath(baseURL, uploadFilesSubpath, relPath)
		if err != nil {
			return fmt.Errorf("build file URL for %s: %w", relPath, err)
		}

		offset, done, doneSize, err := headFileOffset(ctx, client, fileURL)
		if err != nil {
			return fmt.Errorf("probe upload state for %s: %w", relPath, err)
		}

		if offset > metadata.RawSize {
			return fmt.Errorf("probe upload state for %s: offset %d exceeds PAX raw size %d",
				relPath, offset, metadata.RawSize)
		}

		if done {
			if doneSize != metadata.RawSize {
				return fmt.Errorf("probe upload state for %s: completed size %d differs from PAX raw size %d",
					relPath, doneSize, metadata.RawSize)
			}

			runningTotal, err = addRawSize(runningTotal, metadata.RawSize)
			if err != nil {
				return fmt.Errorf("account tar entry %s: %w", relPath, err)
			}

			if setTotal != nil {
				setTotal(runningTotal)
			}

			if onProgress != nil && metadata.RawSize > 0 {
				onProgress(int(metadata.RawSize))
			}

			continue
		}

		if activate != nil {
			activate()
		}

		attrs := fileAttrs{
			Perm:    os.FileMode(hdr.Mode & 0o777),
			UID:     hdr.Uid,
			GID:     hdr.Gid,
			ModTime: hdr.ModTime,
		}

		runningTotal, err = addRawSize(runningTotal, metadata.RawSize)
		if err != nil {
			return fmt.Errorf("account tar entry %s: %w", relPath, err)
		}

		if setTotal != nil {
			setTotal(runningTotal)
		}

		if metadata.Codec == "none" {
			counted := &rawCountingReader{r: tr}
			if offset > 0 {
				if _, ffErr := io.CopyN(io.Discard, counted, offset); ffErr != nil {
					return fmt.Errorf("fast-forwarding %s to resume offset %d: %w", relPath, offset, ffErr)
				}
			}

			if err := putFile(ctx, client, baseURL, relPath, counted, metadata.RawSize, offset, attrs); err != nil {
				return fmt.Errorf("upload %s: %w", relPath, err)
			}

			if err := verifyRawStreamSize(counted, metadata.RawSize); err != nil {
				return fmt.Errorf("verify plaintext size for %s: %w", relPath, err)
			}
		} else {
			payloadStart, seekErr := f.Seek(0, io.SeekCurrent)
			if seekErr != nil {
				return fmt.Errorf("determine tar payload offset for %s: %w", hdr.Name, seekErr)
			}

			if err := streamCompressedEntry(ctx, client, baseURL, tarPath, relPath, ext, payloadStart,
				hdr.Size, offset, metadata.RawSize, attrs); err != nil {
				return fmt.Errorf("upload %s: %w", relPath, err)
			}
		}

		if onProgress != nil && metadata.RawSize > 0 {
			onProgress(int(metadata.RawSize))
		}
	}

	if skipped > 0 {
		log.Info("skipped non-regular tar entries (directories and symlinks are not reproduced)",
			slog.Int("count", skipped),
			slog.String("tar", tarPath))
	}

	return nil
}

func validateFSTarMetadata(tarPath string) (retErr error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", tarPath, err)
	}

	defer func() {
		if err := f.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("close %s: %w", tarPath, err)
		}
	}()

	tr := tar.NewReader(f)
	originalPaths := make(map[string]struct{})

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("read tar entry from %s: %w", tarPath, err)
		}

		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != 0 {
			continue
		}

		metadata, err := archive.ParseFSMetadata(hdr)
		if err != nil {
			return fmt.Errorf("entry %q: %w", hdr.Name, err)
		}

		if _, exists := originalPaths[metadata.OriginalPath]; exists {
			return fmt.Errorf("%w: duplicate original path %q", archive.ErrInvalidFSMetadata, metadata.OriginalPath)
		}

		originalPaths[metadata.OriginalPath] = struct{}{}
	}
}

func addRawSize(total, size int64) (int64, error) {
	if size > math.MaxInt64-total {
		return 0, fmt.Errorf("raw-size total overflows int64")
	}

	return total + size, nil
}

type rawCountingReader struct {
	r io.Reader
	n int64
}

func (r *rawCountingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)

	return n, err
}

func verifyRawStreamSize(r *rawCountingReader, expected int64) error {
	var probe [1]byte

	n, err := r.Read(probe[:])
	if n > 0 {
		return fmt.Errorf("decoded stream exceeds declared PAX raw size %d", expected)
	}

	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("probe decoded stream end: %w", err)
	}

	if r.n != expected {
		return fmt.Errorf("decoded stream size %d differs from declared PAX raw size %d", r.n, expected)
	}

	return nil
}

// streamCompressedEntry reopens one stored payload and streams its decoded bytes
// exactly once. PAX rawSize supplies the request length, avoiding a measure pass.
func streamCompressedEntry(ctx context.Context, client httpDoer, baseURL, tarPath, relPath, ext string, payloadStart, storedSize, offset, exactSize int64, attrs fileAttrs) error {
	f2, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("reopen %s for streaming upload: %w", tarPath, err)
	}

	defer func() { _ = f2.Close() }()

	if _, err := f2.Seek(payloadStart, io.SeekStart); err != nil {
		return fmt.Errorf("seek to payload offset for %s: %w", relPath, err)
	}

	limited := io.LimitReader(f2, storedSize)

	decodeReader, err := compress.NewReader(ext, limited)
	if err != nil {
		return fmt.Errorf("open decompressor for %s: %w", relPath, err)
	}

	defer func() { _ = decodeReader.Close() }()

	counted := &rawCountingReader{r: decodeReader}

	if offset > 0 {
		if _, err := io.CopyN(io.Discard, counted, offset); err != nil {
			return fmt.Errorf("fast-forwarding %s to resume offset %d: %w", relPath, offset, err)
		}
	}

	if err := putFile(ctx, client, baseURL, relPath, counted, exactSize, offset, attrs); err != nil {
		return err
	}

	if err := verifyRawStreamSize(counted, exactSize); err != nil {
		return err
	}

	return nil
}
