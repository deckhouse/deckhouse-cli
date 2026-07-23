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

// Package exporter provides typed HTTP helpers for the data-exporter API exposed by a
// running DataExport. The API has two endpoints: api/v1/block (block volumes served via
// http.ServeContent with Range support) and api/v1/files (filesystem volumes: trailing-
// slash paths return a JSON directory listing; other paths stream file bytes).
package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Doer executes a single HTTP request and returns the response.
// *http.Client and pkg/libsaferequest.SafeClient both satisfy this interface.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// ErrContentRangeMismatch is returned when a 206 response's Content-Range header does not
// cover the byte range the caller requested, so the body must not be trusted at the
// caller's intended offset.
var ErrContentRangeMismatch = errors.New("server Content-Range does not match requested range")

// ErrDataPlaneIdle is reported by a Fetcher-issued response body when no bytes
// arrive for the configured idle window (see idleReadCloser). The data plane
// deliberately runs without an overall request deadline — volume transfers are
// long — so a TCP connection that stops delivering bytes WITHOUT erroring
// (NAT/LB half-close, wedged exporter pod) would otherwise block a Read (and the
// whole download) forever. It is wrapped with %w so the chunk/file
// retry+resume machinery treats a silent stall as an ordinary fetch error.
var ErrDataPlaneIdle = errors.New("data-plane read stalled: no bytes within idle timeout")

// DefaultIdleReadTimeout is the conservative default idle window applied to every
// data-plane response body a Fetcher hands out or consumes. It bounds how long a
// single Read may block WITHOUT receiving any bytes; it is not an overall
// transfer deadline, so a slow-but-flowing stream is never aborted. Override per
// Fetcher with WithIdleReadTimeout (tests use a short window).
const DefaultIdleReadTimeout = 2 * time.Minute

const (
	sourceHashHeader            = "X-Attribute-Hash-Md5"
	sourceHashMinimumThroughput = int64(1 << 20)
	sourceHashTimeoutFloor      = 5 * time.Minute
	sourceHashTimeoutSlack      = 1 * time.Minute
	sourceHashTimeoutCeiling    = 7 * 24 * time.Hour
)

// Fetcher wraps a Doer and exposes typed methods for the data-exporter HTTP API.
type Fetcher struct {
	doer              Doer
	sourceHashDoer    Doer
	idleTimeout       time.Duration
	sourceHashTimeout func(size int64) time.Duration
}

// FetcherOption customizes a Fetcher at construction time.
type FetcherOption func(*Fetcher)

// WithIdleReadTimeout sets the idle-read watchdog window for response bodies this
// Fetcher issues. A value <= 0 disables the watchdog (bodies are returned
// unwrapped). When unset, DefaultIdleReadTimeout applies.
func WithIdleReadTimeout(d time.Duration) FetcherOption {
	return func(f *Fetcher) { f.idleTimeout = d }
}

// WithSourceHashDoer sets the transport used for source-hash HEAD requests.
// Production uses a separate transport because source hashing may legitimately
// take longer than the ordinary data-plane response-header timeout.
func WithSourceHashDoer(doer Doer) FetcherOption {
	return func(f *Fetcher) {
		if doer != nil {
			f.sourceHashDoer = doer
		}
	}
}

// NewFetcher creates a Fetcher backed by the given Doer. Unless overridden via
// WithIdleReadTimeout, response bodies carry an idle-read watchdog with
// DefaultIdleReadTimeout.
func NewFetcher(doer Doer, opts ...FetcherOption) *Fetcher {
	f := &Fetcher{
		doer:              doer,
		sourceHashDoer:    doer,
		idleTimeout:       DefaultIdleReadTimeout,
		sourceHashTimeout: sourceHashTimeout,
	}
	for _, opt := range opts {
		opt(f)
	}

	return f
}

// guardBody wraps body in an idle-read watchdog unless the watchdog is disabled
// (idleTimeout <= 0), in which case the raw body is returned unchanged.
func (f *Fetcher) guardBody(ctx context.Context, body io.ReadCloser) io.ReadCloser {
	if f.idleTimeout <= 0 {
		return body
	}

	return newIdleReadCloser(ctx, body, f.idleTimeout)
}

// BlockURL returns the block-volume endpoint for a DataExport base URL.
// The block volume is served at api/v1/block.
func BlockURL(baseURL string) (string, error) {
	return url.JoinPath(baseURL, "api/v1/block")
}

// FilesURL returns the filesystem root-listing endpoint for a DataExport base URL.
// The trailing slash instructs the server to return a directory listing.
func FilesURL(baseURL string) (string, error) {
	u, err := url.JoinPath(baseURL, "api/v1/files")
	if err != nil {
		return "", err
	}

	if len(u) == 0 || u[len(u)-1] != '/' {
		u += "/"
	}

	return u, nil
}

// HeadVolume issues a HEAD request to blockURL and returns the total content length in bytes.
func (f *Fetcher) HeadVolume(ctx context.Context, blockURL string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, blockURL, nil)
	if err != nil {
		return 0, fmt.Errorf("build HEAD request: %w", err)
	}

	resp, err := f.doer.Do(req)
	if err != nil {
		return 0, fmt.Errorf("HEAD %s: %w", blockURL, err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HEAD %s: unexpected status %s", blockURL, resp.Status)
	}

	if resp.ContentLength < 0 {
		return 0, fmt.Errorf("HEAD %s: Content-Length header absent or negative", blockURL)
	}

	return resp.ContentLength, nil
}

// RangeGet issues a GET request with a Range: bytes=start-end header to blockURL and
// returns the response body. The caller must close the returned ReadCloser.
// Returns an error unless the server responds with 206 Partial Content AND its
// Content-Range header confirms the returned body actually covers [start, end]: the
// block exporter (storage-volume-data-manager images/data-exporter/internal/export_block/
// handler.go HandleGetMethod) serves the block device via stdlib http.ServeContent, which
// always sets Content-Range on a 206 response, so a missing or mismatched header means a
// misbehaving server/proxy returned bytes from the wrong offset (or the whole object) and
// must not be trusted at the caller's intended offset.
func (f *Fetcher) RangeGet(ctx context.Context, blockURL string, start, end int64) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, blockURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET request: %w", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	resp, err := f.doer.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", blockURL, err)
	}

	if resp.StatusCode != http.StatusPartialContent {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("GET %s (range %d-%d): expected 206, got %s", blockURL, start, end, resp.Status)
	}

	if err := validateContentRange(resp.Header.Get("Content-Range"), start, end); err != nil {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("GET %s (range %d-%d): %w", blockURL, start, end, err)
	}

	return f.guardBody(ctx, resp.Body), nil
}

// validateContentRange checks that a 206 response's Content-Range header value confirms
// the body covers exactly the requested [start, end] byte range (RFC 9110 §14.4,
// "bytes start-end/total"; total is "*" when the complete length is unknown). It fails
// closed: an absent or malformed header, a start/end that differs from what was
// requested, or a present total that cannot possibly hold byte end are all rejected as
// ErrContentRangeMismatch, since any of them means the body cannot be trusted at the
// caller's intended offset.
func validateContentRange(header string, start, end int64) error {
	if header == "" {
		return fmt.Errorf("%w: 206 response has no Content-Range header", ErrContentRangeMismatch)
	}

	gotStart, gotEnd, total, err := parseContentRange(header)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrContentRangeMismatch, err)
	}

	if gotStart != start || gotEnd != end {
		return fmt.Errorf("%w: requested bytes %d-%d, server returned Content-Range %q",
			ErrContentRangeMismatch, start, end, header)
	}

	if total >= 0 && total <= end {
		return fmt.Errorf("%w: Content-Range %q reports total %d not greater than end %d",
			ErrContentRangeMismatch, header, total, end)
	}

	return nil
}

// parseContentRange parses a "bytes start-end/total" Content-Range header value.
// The returned total is -1 when the server sent "*" for an unknown complete length.
func parseContentRange(header string) (int64, int64, int64, error) {
	const unitPrefix = "bytes "

	spec, ok := strings.CutPrefix(header, unitPrefix)
	if !ok {
		return 0, 0, 0, fmt.Errorf("unsupported Content-Range unit in %q", header)
	}

	rangePart, totalPart, ok := strings.Cut(spec, "/")
	if !ok {
		return 0, 0, 0, fmt.Errorf("missing total in Content-Range %q", header)
	}

	startPart, endPart, ok := strings.Cut(rangePart, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("missing '-' in Content-Range %q", header)
	}

	start, err := strconv.ParseInt(startPart, 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse start in Content-Range %q: %w", header, err)
	}

	end, err := strconv.ParseInt(endPart, 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse end in Content-Range %q: %w", header, err)
	}

	if totalPart == "*" {
		return start, end, -1, nil
	}

	total, err := strconv.ParseInt(totalPart, 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse total in Content-Range %q: %w", header, err)
	}

	return start, end, total, nil
}

// Item is one entry returned by the data-exporter filesystem listing API.
type Item struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	URI        string         `json:"uri"`
	TargetPath string         `json:"targetPath,omitempty"`
	Attributes map[string]any `json:"attributes"`
}

// ListDir GETs filesURL (which must end with a trailing slash for directory
// semantics), requesting only the inexpensive stat attributes, and returns the
// stream-decoded list of directory entries. Source hashes are fetched separately
// after each regular file's declared size is known because the producer computes
// hash.md5 synchronously before emitting that listing item.
func (f *Fetcher) ListDir(ctx context.Context, filesURL string) ([]Item, error) {
	reqURL, err := withAttributes(filesURL, "stat")
	if err != nil {
		return nil, fmt.Errorf("build listing URL for %s: %w", filesURL, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET request: %w", err)
	}

	resp, err := f.doer.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", filesURL, err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: unexpected status %s", filesURL, resp.Status)
	}

	// Guard the bounded listing body too: a stalled listing must not hang the
	// run any more than a stalled data stream. Closing the watchdog stops its
	// timer; the underlying resp.Body is still closed by the defer above.
	body := f.guardBody(ctx, resp.Body)

	defer func() { _ = body.Close() }()

	items, err := decodeItems(body)
	if err != nil {
		return nil, fmt.Errorf("decode listing from %s: %w", filesURL, err)
	}

	return items, nil
}

// SourceMD5 retrieves the producer-computed plaintext MD5 for one regular file
// through the filesystem exporter's HEAD attribute contract. The producer must
// read the complete source file before it can send this response header, so the
// request uses an overall size-derived deadline rather than the transfer body's
// progress-based idle watchdog.
//
// The budget assumes the source can be hashed at no less than 1 MiB/s, adds one
// minute of fixed scheduling slack, floors small or unknown sizes at five
// minutes, and caps untrusted declared sizes at seven days. It is finite for
// every int64 size. An empty result means an older exporter returned 200 without
// the optional hash header.
func (f *Fetcher) SourceMD5(ctx context.Context, fileURL string, size int64) (string, error) {
	reqURL, err := withAttributes(fileURL, "hash.md5")
	if err != nil {
		return "", fmt.Errorf("build source-hash URL for %s: %w", fileURL, err)
	}

	timeout := f.sourceHashTimeout(size)

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodHead, reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("build source-hash HEAD request: %w", err)
	}

	resp, err := f.sourceHashDoer.Do(req)
	if err != nil {
		if ctxErr := reqCtx.Err(); ctxErr != nil {
			return "", fmt.Errorf("HEAD source hash for %s after %s: %w", fileURL, timeout, ctxErr)
		}

		return "", fmt.Errorf("HEAD source hash for %s: %w", fileURL, err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HEAD source hash for %s: unexpected status %s", fileURL, resp.Status)
	}

	return resp.Header.Get(sourceHashHeader), nil
}

func sourceHashTimeout(size int64) time.Duration {
	if size <= 0 {
		return sourceHashTimeoutFloor
	}

	seconds := size / sourceHashMinimumThroughput
	if size%sourceHashMinimumThroughput != 0 {
		seconds++
	}

	maxSeconds := int64((sourceHashTimeoutCeiling - sourceHashTimeoutSlack) / time.Second)
	if seconds >= maxSeconds {
		return sourceHashTimeoutCeiling
	}

	timeout := time.Duration(seconds)*time.Second + sourceHashTimeoutSlack
	if timeout < sourceHashTimeoutFloor {
		return sourceHashTimeoutFloor
	}

	return timeout
}

// GetFile GETs fileURL and returns the response body for streaming.
// The caller must close the returned ReadCloser.
func (f *Fetcher) GetFile(ctx context.Context, fileURL string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET request: %w", err)
	}

	resp, err := f.doer.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", fileURL, err)
	}

	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("GET %s: unexpected status %s", fileURL, resp.Status)
	}

	return f.guardBody(ctx, resp.Body), nil
}

// withAttributes appends one "attribute" query parameter per entry in attrs to rawURL.
// The data-exporter filesystem endpoint only includes an attribute in the response
// (X-Attribute-* headers for a single file, or the listing item's Attributes map for a
// directory) when the request explicitly asks for it via ?attribute=<name>, repeated
// once per requested attribute (storage-volume-data-manager's
// export_filesystem/handler.go getRequestedAttributes); "stat" is otherwise harmless to
// request since the server already emits it unconditionally.
func withAttributes(rawURL string, attrs ...string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse URL %q: %w", rawURL, err)
	}

	q := u.Query()
	for _, a := range attrs {
		q.Add("attribute", a)
	}

	u.RawQuery = q.Encode()

	return u.String(), nil
}

// decodeItems stream-decodes the JSON response body from a directory listing.
// The response format is {"apiVersion": "...", "items": [{...}, ...]}.
func decodeItems(r io.Reader) ([]Item, error) {
	dec := json.NewDecoder(r)

	// Scan tokens until the "items" key is found.
	for {
		t, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("scan for items key: %w", err)
		}

		if s, ok := t.(string); ok && s == "items" {
			break
		}
	}

	// Expect the opening '[' of the items array.
	t, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("read items array delimiter: %w", err)
	}

	if t != json.Delim('[') {
		return nil, errors.New("items field is not a JSON array")
	}

	var items []Item

	for dec.More() {
		var item Item
		if err := dec.Decode(&item); err != nil {
			return nil, fmt.Errorf("decode item: %w", err)
		}

		items = append(items, item)
	}

	return items, nil
}

// idleReadCloser wraps a response body with a per-Read idle watchdog. A single
// timer is (re)armed immediately before each Read and stopped when that Read
// returns, so a stream that keeps delivering bytes — even slowly, in many small
// reads — never trips it, because no individual Read blocks for the whole window.
// A connection that goes silent WITHOUT erroring, however, leaves one Read
// blocked for the full window; the timer then fires and closes the underlying
// body, which unblocks that Read, after which reads report ErrDataPlaneIdle so
// the caller's retry/resume machinery takes over.
//
// It holds exactly one timer per body and starts no long-lived goroutine: the
// timer's callback runs only if it fires, and is stopped on every Read return
// and on Close, so nothing leaks. ctx cancellation is honored two ways: Read
// short-circuits when ctx is already done, and the request-scoped body (built
// via http.NewRequestWithContext) aborts an in-flight Read promptly on
// cancellation — the watchdog never masks that.
type idleReadCloser struct {
	ctx     context.Context
	body    io.ReadCloser
	timeout time.Duration

	mu      sync.Mutex
	timer   *time.Timer
	closed  bool
	tripped bool
}

// newIdleReadCloser wraps body with an idle watchdog of the given timeout,
// scoped to ctx. timeout is expected to be > 0 (callers use guardBody, which
// skips wrapping otherwise).
func newIdleReadCloser(ctx context.Context, body io.ReadCloser, timeout time.Duration) *idleReadCloser {
	return &idleReadCloser{ctx: ctx, body: body, timeout: timeout}
}

func (r *idleReadCloser) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	r.mu.Lock()

	if r.closed {
		r.mu.Unlock()
		return 0, os.ErrClosed
	}

	if r.tripped {
		r.mu.Unlock()
		return 0, fmt.Errorf("%w after %s", ErrDataPlaneIdle, r.timeout)
	}

	if r.timer == nil {
		r.timer = time.AfterFunc(r.timeout, r.onIdle)
	} else {
		r.timer.Reset(r.timeout)
	}

	r.mu.Unlock()

	n, err := r.body.Read(p)

	r.mu.Lock()

	if r.timer != nil {
		r.timer.Stop()
	}

	tripped := r.tripped
	r.mu.Unlock()

	if tripped {
		// The watchdog closed the body mid-Read; surface the idle sentinel
		// rather than the incidental "read on closed body" error. Any bytes
		// read before the close (n) are still valid and returned to the caller.
		return n, fmt.Errorf("%w after %s", ErrDataPlaneIdle, r.timeout)
	}

	return n, err
}

// onIdle runs when the timer fires: it marks the body as tripped and closes the
// underlying body to unblock the stuck Read. It closes the body while holding
// the mutex so it can never race a concurrent Close on the same body.
func (r *idleReadCloser) onIdle() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed || r.tripped {
		return
	}

	r.tripped = true
	_ = r.body.Close()
}

// Close stops the watchdog timer and closes the underlying body exactly once.
// It is safe to call concurrently with an in-flight Read (the normal way a
// caller aborts a stream) and after the watchdog has already tripped.
func (r *idleReadCloser) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true

	if r.timer != nil {
		r.timer.Stop()
	}

	if r.tripped {
		// onIdle already closed the body.
		return nil
	}

	return r.body.Close()
}
