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
	"context"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"strconv"
	"time"
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

// putFile uploads localPath to the FS importer at baseURL under relPath, preserving
// the given file attributes. It probes the server with HEAD to seed the resume offset,
// then PUTs the remaining bytes. Responses:
//   - 201 Created: file is complete.
//   - 204 No Content + X-Next-Offset: partial; the next chunk starts at that offset.
//   - 409 Conflict + X-Expected-Offset: offset mismatch; the loop retries from the
//     server-reported position.
//
// Callers are responsible for calling postFinished after all files are uploaded.
func putFile(ctx context.Context, client httpDoer, baseURL, relPath, localPath string, attrs fileAttrs) error {
	fileURL, err := neturl.JoinPath(baseURL, uploadFilesSubpath, relPath)
	if err != nil {
		return fmt.Errorf("build file URL for %s: %w", relPath, err)
	}

	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", localPath, err)
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", localPath, err)
	}

	totalSize := fi.Size()

	offset, done, err := headFileOffset(ctx, client, fileURL)
	if err != nil {
		return err
	}

	if done {
		return nil
	}

	for offset < totalSize {
		section := io.NewSectionReader(f, offset, totalSize-offset)

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, fileURL, io.NopCloser(section))
		if err != nil {
			return err
		}

		req.Header.Set("X-Content-Length", strconv.FormatInt(totalSize, 10))
		req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))
		req.Header.Set("X-Attribute-Permissions", fmt.Sprintf("%04o", attrs.Perm))
		req.Header.Set("X-Attribute-Uid", strconv.Itoa(attrs.UID))
		req.Header.Set("X-Attribute-Gid", strconv.Itoa(attrs.GID))
		req.Header.Set("X-Attribute-ModTime", attrs.ModTime.UTC().Format(time.RFC3339))

		next, err := doFileChunk(client, req, offset, totalSize)
		if err != nil {
			return fmt.Errorf("upload %s at offset %d: %w", relPath, offset, err)
		}

		offset = next
	}

	return nil
}

// headFileOffset probes the file endpoint to determine the current upload state.
// Returns (offset, done) where done=true means the final file already exists and the
// upload should be skipped entirely. offset is the number of bytes already written to
// the server's temp file; 0 if no partial upload exists.
func headFileOffset(ctx context.Context, client httpDoer, fileURL string) (int64, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fileURL, nil)
	if err != nil {
		return 0, false, err
	}

	resp, err := client.HTTPDo(req)
	if err != nil {
		return 0, false, err
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
				return 0, false, fmt.Errorf("invalid X-Next-Offset %q from HEAD %s", next, fileURL)
			}

			return off, false, nil
		}

		// No X-Next-Offset on a 200 → the final file already exists; skip the upload.
		return 0, true, nil

	case http.StatusNotFound:
		return 0, false, nil

	default:
		return 0, false, fmt.Errorf("HEAD %s returned status %d (%s)", fileURL, resp.StatusCode, resp.Status)
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
