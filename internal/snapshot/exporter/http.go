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
)

// Doer executes a single HTTP request and returns the response.
// *http.Client and pkg/libsaferequest.SafeClient both satisfy this interface.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

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
// Returns an error unless the server responds with 206 Partial Content.
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

	return resp.Body, nil
}

// Item is one entry returned by the data-exporter filesystem listing API.
type Item struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	URI        string         `json:"uri"`
	TargetPath string         `json:"targetPath,omitempty"`
	Attributes map[string]any `json:"attributes"`
}

// ListDir GETs filesURL (which must end with a trailing slash for directory semantics) and
// returns the stream-decoded list of directory entries. The body is consumed and closed
// before returning.
func (f *Fetcher) ListDir(ctx context.Context, filesURL string) ([]Item, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, filesURL, nil)
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
