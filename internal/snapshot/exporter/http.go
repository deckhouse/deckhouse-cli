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
	"strconv"
	"strings"
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

// Fetcher wraps a Doer and exposes typed methods for the data-exporter HTTP API.
type Fetcher struct {
	doer Doer
}

// NewFetcher creates a Fetcher backed by the given Doer.
func NewFetcher(doer Doer) *Fetcher {
	return &Fetcher{doer: doer}
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

	return resp.Body, nil
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

// ListDir GETs filesURL (which must end with a trailing slash for directory semantics),
// requesting the "stat" and "hash.md5" attributes so each file item's Attributes map
// carries a source-provided digest for downstream integrity verification, and returns
// the stream-decoded list of directory entries. The body is consumed and closed before
// returning.
func (f *Fetcher) ListDir(ctx context.Context, filesURL string) ([]Item, error) {
	reqURL, err := withAttributes(filesURL, "stat", "hash.md5")
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

	items, err := decodeItems(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode listing from %s: %w", filesURL, err)
	}

	return items, nil
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

	return resp.Body, nil
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
