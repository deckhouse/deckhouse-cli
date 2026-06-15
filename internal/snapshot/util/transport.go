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

// Package util implements the transport and CR helpers for the d8 snapshot export/import CLI.
//
// There are two transports:
//   - APIClient talks to the kube-apiserver aggregated subresources (index / per-node manifests)
//     using AbsPaths published in the SnapshotExport/SnapshotImport status. Auth + TLS come from the
//     kubeconfig, so no per-object CA is needed.
//   - the data-pod helpers (DownloadBlock / UploadBlock) talk directly to the DataExport/DataImport
//     pod endpoints, trusting the per-entry CA surfaced in status (DataCA / UploadCA).
package util

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"k8s.io/client-go/rest"

	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// apiUploadChunkBytes bounds a single aggregated-subresource append. The server caps a chunk at
// 512 KiB (maxImportUploadChunkBytes), so stay comfortably below that.
const apiUploadChunkBytes = 256 * 1024

// apiMaxConverge bounds how many times an upload may follow a server 409 resync before giving up.
const apiMaxConverge = 1000

// APIClient performs GET/HEAD/PUT against kube-apiserver AbsPaths with full header control (Range,
// X-Offset, X-Next-Offset), which the controller-runtime / typed clients do not expose.
type APIClient struct {
	httpClient *http.Client
	baseURL    string
}

// NewAPIClient builds an apiserver client from the SafeClient's rest.Config (auth + cluster CA).
func NewAPIClient(sc *safeClient.SafeClient) (*APIClient, error) {
	cfg := sc.RESTConfig()
	if cfg == nil {
		return nil, fmt.Errorf("no REST config")
	}
	hc, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("build apiserver HTTP client: %w", err)
	}
	return &APIClient{httpClient: hc, baseURL: strings.TrimSuffix(cfg.Host, "/")}, nil
}

// Get returns the body of a GET on an apiserver AbsPath. Compression is disabled so the raw JSON is
// returned verbatim.
func (a *APIClient) Get(ctx context.Context, absPath string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL+absPath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s: %s", absPath, resp.Status, truncate(body))
	}
	return body, nil
}

// headOffset returns the resume offset (X-Next-Offset) for an upload AbsPath.
func (a *APIClient) headOffset(ctx context.Context, absPath string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, a.baseURL+absPath, nil)
	if err != nil {
		return 0, err
	}
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, nil
	}
	if v := resp.Header.Get("X-Next-Offset"); v != "" {
		n, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil || n < 0 {
			return 0, nil
		}
		return n, nil
	}
	return 0, nil
}

// UploadBlob uploads data to an apiserver upload AbsPath (resumable, idempotent) and, when finalize
// is set, commits the blob (?finalize=true on the last request). pathBase may carry an existing query
// (e.g. ?node=<id>). It follows server 409 resyncs (forward or backward) and, when finalize is set,
// guarantees a positive commit confirmation: a 409 on the finalize request resumes the missing bytes
// instead of silently reporting success.
func (a *APIClient) UploadBlob(ctx context.Context, pathBase string, data []byte, finalize bool) error {
	offset, err := a.headOffset(ctx, pathBase)
	if err != nil {
		offset = 0
	}
	if offset > int64(len(data)) {
		offset = int64(len(data))
	}

	converges := 0
	for {
		if offset >= int64(len(data)) {
			if !finalize {
				return nil
			}
			// Body fully persisted (fresh, resumed, or empty): send a finalize-only commit. If the
			// server 409s it wants more bytes from `next`, so resume there rather than swallow it.
			next, conflicted, aerr := a.appendChunk(ctx, withQuery(pathBase, "finalize", "true"), offset, nil)
			if aerr != nil {
				return aerr
			}
			if !conflicted {
				return nil
			}
			converges++
			if converges > apiMaxConverge {
				return fmt.Errorf("upload to %s did not converge after %d server conflicts", pathBase, apiMaxConverge)
			}
			if next < 0 || next > int64(len(data)) {
				return fmt.Errorf("upload to %s: server returned out-of-range offset %d (size %d)", pathBase, next, len(data))
			}
			offset = next
			continue
		}

		end := offset + apiUploadChunkBytes
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		last := end == int64(len(data))
		path := pathBase
		if last && finalize {
			path = withQuery(pathBase, "finalize", "true")
		}
		next, conflicted, aerr := a.appendChunk(ctx, path, offset, data[offset:end])
		if aerr != nil {
			return aerr
		}
		if conflicted {
			converges++
			if converges > apiMaxConverge {
				return fmt.Errorf("upload to %s did not converge after %d server conflicts", pathBase, apiMaxConverge)
			}
			if next < 0 || next > int64(len(data)) {
				return fmt.Errorf("upload to %s: server returned out-of-range offset %d (size %d)", pathBase, next, len(data))
			}
			offset = next
			continue
		}
		if next <= offset {
			return fmt.Errorf("upload to %s stalled at offset %d", pathBase, offset)
		}
		offset = next
		if last && finalize {
			// The finalize=true chunk was accepted: the blob is committed.
			return nil
		}
	}
}

// appendChunk PUTs one chunk at offset and returns the next offset, whether the server reported a 409
// resync (offset mismatch), and any error. A 409 without X-Next-Offset is an error: the only offset we
// know is wrong is the one we just sent, so we must not optimistically advance past it.
func (a *APIClient) appendChunk(ctx context.Context, path string, offset int64, body []byte) (int64, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, a.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer drainClose(resp.Body)
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))

	next := offset + int64(len(body))
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
			return 0, false, fmt.Errorf("PUT %s (offset %d): 409 without X-Next-Offset, cannot converge: %s", path, offset, truncate(respBody))
		}
		return next, true, nil
	default:
		return 0, false, fmt.Errorf("PUT %s (offset %d): %s: %s", path, offset, resp.Status, truncate(respBody))
	}
}

// ManifestsNodePath appends the ?node=<id> selector to a manifests AbsPath (export retrieval and
// import upload share the per-node selector).
func ManifestsNodePath(base, nodeID string) string {
	return withQuery(base, "node", nodeID)
}

func withQuery(path, key, value string) string {
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + key + "=" + url.QueryEscape(value)
}

func truncate(b []byte) string {
	const max = 512
	if len(b) > max {
		return string(b[:max]) + "..."
	}
	return string(b)
}
