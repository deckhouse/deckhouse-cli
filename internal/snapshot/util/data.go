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

package util

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"time"

	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// dataChunkBytes is the per-PUT body size for data-pod block uploads.
const dataChunkBytes = 4 * 1024 * 1024

// dataMaxAttempts bounds transient-error retries when establishing a data-pod request.
const dataMaxAttempts = 4

// dataMaxConverge bounds how many times a resumable upload may follow a server 409 resync before
// giving up (defends against a server that never converges).
const dataMaxConverge = 1000

// httpDoer is the subset of *safeClient.SafeClient used by the data-pod transfer cores. Splitting it
// out lets the resumable download/upload state machines be unit-tested against an httptest server.
type httpDoer interface {
	HTTPDo(req *http.Request) (*http.Response, error)
}

// VolumeModeBlock / VolumeModeFilesystem mirror the index volume mode values.
const (
	VolumeModeBlock      = "Block"
	VolumeModeFilesystem = "Filesystem"
)

// dataPodClient returns a SafeClient that trusts the per-endpoint CA (status.ca, base64 PEM) in
// addition to the system + kubeconfig trust. An empty CA keeps the default trust (published endpoints
// carry an externally-trusted cert).
//
// NOTE: the kubeconfig bearer token is intentionally carried to the data-pod endpoint: the
// storage-volume-data-manager data-exporter/importer authorizes the transfer with a
// SubjectAccessReview on that token (create dataexports/dataimports download subresource), so it is a
// required credential, not an accidental leak. When the endpoint is exposed externally via --publish,
// the operator opts into sending that token over the published route; the TLS trust above still
// fails closed.
func dataPodClient(sc *safeClient.SafeClient, caB64 string) (*safeClient.SafeClient, error) {
	sub := sc.Copy()
	if caB64 == "" {
		sub.SetTLSCAData(nil)
		return sub, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(caB64)
	if err != nil {
		return nil, fmt.Errorf("decode endpoint CA: %w", err)
	}
	sub.SetTLSCAData(decoded)
	return sub, nil
}

// DownloadBlock downloads the whole block device served at <base>/api/v1/block into outPath. It
// resumes from the current local file size using an HTTP Range request, so re-running continues an
// interrupted transfer. The local partial file is the resume "state".
func DownloadBlock(ctx context.Context, sc *safeClient.SafeClient, base, caB64, outPath string) error {
	client, err := dataPodClient(sc, caB64)
	if err != nil {
		return err
	}
	url, err := neturl.JoinPath(base, "api/v1/block")
	if err != nil {
		return err
	}
	return downloadBlock(ctx, client, url, outPath)
}

// downloadBlock is the testable core of DownloadBlock against an already-resolved client + URL.
func downloadBlock(ctx context.Context, client httpDoer, url, outPath string) error {
	total, err := headContentLength(ctx, client, url)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	offset := fi.Size()

	// A local file larger than the remote object cannot be a valid partial of it: it is stale/corrupt
	// (e.g. left over from a different export). Restart from scratch rather than reporting success.
	if total > 0 && offset > total {
		if terr := f.Truncate(0); terr != nil {
			return terr
		}
		offset = 0
	}
	// Exact-size local file: treat as already complete (the data pod exposes no checksum to verify
	// content further, so size equality is the strongest available signal).
	if total > 0 && offset == total {
		return nil
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	for total <= 0 || offset < total {
		n, status, derr := downloadRange(ctx, client, url, f, offset)
		if derr != nil {
			return derr
		}
		if status == http.StatusOK {
			// The server ignored Range and streamed the whole object from byte 0; downloadRange has
			// already rewound + truncated the file so the body landed at offset 0.
			offset = n
			break
		}
		offset += n
		if n == 0 {
			break
		}
	}

	// Verify the transfer is complete: a short stream (truncated download) must not be reported as
	// success. When the size is unknown (total<=0) we can only trust EOF.
	if total > 0 && offset != total {
		return fmt.Errorf("download %s incomplete: wrote %d of %d bytes (re-run to resume)", url, offset, total)
	}
	return nil
}

// downloadRange performs one ranged GET from offset and copies the body into f. It returns the bytes
// written, the HTTP status (200 means Range was ignored and the file was rewound to 0), and any error.
// Establishing the request is retried on transient (connection) errors; a mid-stream copy error is
// returned so the caller resumes on re-run.
func downloadRange(ctx context.Context, client httpDoer, url string, f *os.File, offset int64) (int64, int, error) {
	var resp *http.Response
	var lastErr error
	for attempt := 0; attempt < dataMaxAttempts; attempt++ {
		if attempt > 0 {
			if serr := sleep(ctx, backoff(attempt)); serr != nil {
				return 0, 0, serr
			}
		}
		req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if rerr != nil {
			return 0, 0, rerr
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		r, derr := client.HTTPDo(req)
		if derr != nil {
			lastErr = derr
			continue
		}
		resp = r
		break
	}
	if resp == nil {
		return 0, 0, fmt.Errorf("download %s: %w", url, lastErr)
	}
	defer drainClose(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK:
		// Range ignored: rewind + truncate so the full body lands at offset 0 (fixes silent corruption
		// when resuming at offset>0 against a Range-ignoring server/proxy).
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return 0, 0, err
		}
		if err := f.Truncate(0); err != nil {
			return 0, 0, err
		}
	case http.StatusPartialContent:
		if err := validateContentRangeStart(resp.Header.Get("Content-Range"), offset); err != nil {
			return 0, 0, err
		}
		// f is already positioned at offset by the caller's Seek / the previous iteration's copy.
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, resp.StatusCode, fmt.Errorf("download %s: %s: %s", url, resp.Status, string(body))
	}

	n, cerr := io.Copy(f, resp.Body)
	return n, resp.StatusCode, cerr
}

// validateContentRangeStart parses "bytes <start>-<end>/<total>" and verifies start == want, so a
// misaligned 206 (server resuming at the wrong byte) is rejected instead of corrupting the file.
func validateContentRangeStart(header string, want int64) error {
	if header == "" {
		return nil // best-effort: some servers omit Content-Range on 206
	}
	v := strings.TrimSpace(strings.TrimPrefix(header, "bytes "))
	dash := strings.IndexByte(v, '-')
	if dash <= 0 {
		return fmt.Errorf("malformed Content-Range %q", header)
	}
	start, err := strconv.ParseInt(strings.TrimSpace(v[:dash]), 10, 64)
	if err != nil {
		return fmt.Errorf("malformed Content-Range %q: %w", header, err)
	}
	if start != want {
		return fmt.Errorf("server resumed at byte %d but expected %d (Content-Range %q)", start, want, header)
	}
	return nil
}

func backoff(attempt int) time.Duration {
	return time.Duration(attempt) * 500 * time.Millisecond
}

func drainClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
}

// UploadBlock uploads outPath (a block-volume image) to the importer served at <base>/api/v1/block.
// It is resumable (HEAD X-Next-Offset, PUT X-Offset) and signals completion with POST /api/v1/finished,
// which flips the DataImport UploadFinished condition the SnapshotImport controller waits on.
func UploadBlock(ctx context.Context, sc *safeClient.SafeClient, base, caB64, inPath string) error {
	client, err := dataPodClient(sc, caB64)
	if err != nil {
		return err
	}
	return uploadBlock(ctx, client, base, inPath)
}

// uploadBlock is the testable core of UploadBlock against an already-resolved client + base URL.
func uploadBlock(ctx context.Context, client httpDoer, base, inPath string) error {
	url, err := neturl.JoinPath(base, "api/v1/block")
	if err != nil {
		return err
	}

	f, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	total := fi.Size()

	offset, err := dataHeadOffset(ctx, client, url)
	if err != nil {
		offset = 0
	}
	if offset > total {
		offset = total
	}

	converges := 0
	for offset < total {
		sendLen := int64(dataChunkBytes)
		if offset+sendLen > total {
			sendLen = total - offset
		}
		next, conflicted, uerr := putBlock(ctx, client, url, f, offset, sendLen, total)
		if uerr != nil {
			return uerr
		}
		if conflicted {
			// The server rejected our offset and reported its authoritative resume point; converge to
			// it (it may point backward when the server lost a tail). Bound the resyncs.
			converges++
			if converges > dataMaxConverge {
				return fmt.Errorf("upload to %s did not converge after %d server conflicts", url, dataMaxConverge)
			}
			if next < 0 || next > total {
				return fmt.Errorf("upload to %s: server returned out-of-range offset %d (size %d)", url, next, total)
			}
			offset = next
			continue
		}
		if next <= offset {
			return fmt.Errorf("upload to %s stalled at offset %d", url, offset)
		}
		offset = next
	}

	return finishUpload(ctx, client, base)
}

// putBlock PUTs one chunk [offset,offset+sendLen) of f. It returns the next offset, whether the server
// reported a 409 resync (offset mismatch), and any error. A 409 without X-Next-Offset is an error: the
// only offset we know is wrong is the one we just sent, so we must not optimistically advance past it.
func putBlock(ctx context.Context, client httpDoer, url string, f *os.File, offset, sendLen, total int64) (int64, bool, error) {
	req, rerr := http.NewRequestWithContext(ctx, http.MethodPut, url, io.NopCloser(io.NewSectionReader(f, offset, sendLen)))
	if rerr != nil {
		return 0, false, rerr
	}
	req.ContentLength = sendLen
	// GetBody lets the transport replay the body on redirect/retry (a bare SectionReader cannot rewind).
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(io.NewSectionReader(f, offset, sendLen)), nil
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Content-Length", strconv.FormatInt(total, 10))
	req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))
	resp, derr := client.HTTPDo(req)
	if derr != nil {
		return 0, false, derr
	}
	defer drainClose(resp.Body)
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

	next := offset + sendLen
	hasNext := false
	if v := resp.Header.Get("X-Next-Offset"); v != "" {
		if n, perr := strconv.ParseInt(v, 10, 64); perr == nil && n >= 0 {
			next = n
			hasNext = true
		}
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
		return next, false, nil
	case http.StatusConflict:
		if !hasNext {
			return 0, false, fmt.Errorf("upload %s (offset %d): 409 without X-Next-Offset, cannot converge: %s", url, offset, string(body))
		}
		return next, true, nil
	default:
		return 0, false, fmt.Errorf("upload %s (offset %d): %s: %s", url, offset, resp.Status, string(body))
	}
}

// finishUpload POSTs /api/v1/finished so the importer marks the DataImport upload complete.
func finishUpload(ctx context.Context, client httpDoer, base string) error {
	url, err := neturl.JoinPath(base, "api/v1/finished")
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.HTTPDo(req)
	if err != nil {
		return err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("finish upload %s: %s: %s", url, resp.Status, string(body))
	}
	return nil
}

func headContentLength(ctx context.Context, client httpDoer, url string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.HTTPDo(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, nil
	}
	if v := resp.Header.Get("Content-Length"); v != "" {
		if n, perr := strconv.ParseInt(v, 10, 64); perr == nil && n >= 0 {
			return n, nil
		}
	}
	return 0, nil
}

func dataHeadOffset(ctx context.Context, client httpDoer, url string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.HTTPDo(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, nil
	}
	if v := resp.Header.Get("X-Next-Offset"); v != "" {
		if n, perr := strconv.ParseInt(v, 10, 64); perr == nil && n >= 0 {
			return n, nil
		}
	}
	return 0, nil
}
