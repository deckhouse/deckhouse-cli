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

package aggapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	restfake "k8s.io/client-go/rest/fake"
)

// testMapper resolves the kinds used in the path-building tests to their plurals.
func testMapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper(nil)
	m.Add(schema.GroupVersionKind{Group: StorageGroup, Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Group: "demo.deckhouse.io", Version: "v1alpha1", Kind: "VirtualDiskSnapshot"}, meta.RESTScopeNamespace)
	// Real producer group for demo domain snapshot kinds (demo.state-snapshotter.deckhouse.io/v1alpha1).
	m.Add(schema.GroupVersionKind{Group: "demo.state-snapshotter.deckhouse.io", Version: "v1alpha1", Kind: "DemoVirtualDiskSnapshot"}, meta.RESTScopeNamespace)

	return m
}

func TestIsVolumeSnapshotLeaf(t *testing.T) {
	cases := []struct {
		name string
		ref  NodeRef
		want bool
	}{
		{
			name: "csi volume snapshot leaf",
			ref:  NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot"},
			want: true,
		},
		{
			name: "core snapshot",
			ref:  NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot"},
			want: false,
		},
		{
			name: "domain snapshot",
			ref:  NodeRef{APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "VirtualDiskSnapshot"},
			want: false,
		},
		{
			name: "wrong kind in vs group",
			ref:  NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshotContent"},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.ref.IsVolumeSnapshotLeaf(); got != tc.want {
				t.Errorf("IsVolumeSnapshotLeaf() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDownloadPath verifies that manifests-download is always served by the core
// subresources group for non-leaf nodes, and by the VS-connector group for CSI
// VolumeSnapshot leaves.
func TestDownloadPath(t *testing.T) {
	c := NewClient(nil, testMapper())

	cases := []struct {
		name string
		ref  NodeRef
		want string
	}{
		{
			name: "core snapshot",
			ref:  NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot", Name: "my-snap", Namespace: "ns"},
			want: "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/snapshots/my-snap/manifests-download",
		},
		{
			name: "domain snapshot still uses core group",
			ref:  NodeRef{APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "VirtualDiskSnapshot", Name: "vds-1", Namespace: "ns"},
			want: "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/virtualdisksnapshots/vds-1/manifests-download",
		},
		{
			name: "csi volume snapshot leaf uses vs-connector group",
			ref:  NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "vs-1", Namespace: "ns"},
			want: "/apis/subresources.snapshot.storage.k8s.io/v1/namespaces/ns/volumesnapshots/vs-1/manifests-download",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.downloadPath(tc.ref)
			if err != nil {
				t.Fatalf("downloadPath: %v", err)
			}

			if got != tc.want {
				t.Errorf("downloadPath:\n got  %q\n want %q", got, tc.want)
			}
		})
	}
}

// TestSubresourcePath verifies that the manifests-with-data-restoration subresource
// is served by the node's OWN subresource group (core group for core Snapshot,
// domain-prefixed group for domain CRs, VS-connector group for CSI leaves).
func TestSubresourcePath(t *testing.T) {
	c := NewClient(nil, testMapper())

	cases := []struct {
		name string
		ref  NodeRef
		sub  string
		want string
	}{
		{
			name: "core snapshot restore",
			ref:  NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot", Name: "my-snap", Namespace: "ns"},
			sub:  SubManifestsRestore,
			want: "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/snapshots/my-snap/manifests-with-data-restoration",
		},
		{
			name: "domain snapshot restore uses domain-prefixed group",
			ref:  NodeRef{APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "VirtualDiskSnapshot", Name: "vds-1", Namespace: "ns"},
			sub:  SubManifestsRestore,
			want: "/apis/subresources.demo.deckhouse.io/v1alpha1/namespaces/ns/virtualdisksnapshots/vds-1/manifests-with-data-restoration",
		},
		{
			name: "csi volume snapshot leaf restore uses vs-connector group",
			ref:  NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "vs-1", Namespace: "ns"},
			sub:  SubManifestsRestore,
			want: "/apis/subresources.snapshot.storage.k8s.io/v1/namespaces/ns/volumesnapshots/vs-1/manifests-with-data-restoration",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.subresourcePath(tc.ref, tc.sub)
			if err != nil {
				t.Fatalf("subresourcePath: %v", err)
			}

			if got != tc.want {
				t.Errorf("subresourcePath:\n got  %q\n want %q", got, tc.want)
			}
		})
	}
}

// TestUploadPath verifies that manifests-and-children-refs-upload is always served by
// the CORE subresources group for non-VS kinds (core Snapshot and domain CRs alike),
// and by the VS-connector group for CSI VolumeSnapshot leaves.
// Case (a) exposes the pre-fix regression: before the fix, domain kinds were routed to
// the domain-prefixed group (e.g. subresources.demo.state-snapshotter.deckhouse.io),
// which only implements GET and returns 405 for POST.
func TestUploadPath(t *testing.T) {
	c := NewClient(nil, testMapper())

	cases := []struct {
		name string
		ref  NodeRef
		want string
	}{
		{
			name: "domain DemoVirtualDiskSnapshot upload uses core group",
			ref:  NodeRef{APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1", Kind: "DemoVirtualDiskSnapshot", Name: "vds-1", Namespace: "ns"},
			want: "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/demovirtualdisksnapshots/vds-1/manifests-and-children-refs-upload",
		},
		{
			name: "core Snapshot upload uses core group",
			ref:  NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot", Name: "my-snap", Namespace: "ns"},
			want: "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/snapshots/my-snap/manifests-and-children-refs-upload",
		},
		{
			name: "csi volume snapshot leaf upload uses vs-connector group",
			ref:  NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "vs-1", Namespace: "ns"},
			want: "/apis/subresources.snapshot.storage.k8s.io/v1/namespaces/ns/volumesnapshots/vs-1/manifests-and-children-refs-upload",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.uploadPath(tc.ref)
			if err != nil {
				t.Fatalf("uploadPath: %v", err)
			}

			if got != tc.want {
				t.Errorf("uploadPath:\n got  %q\n want %q", got, tc.want)
			}
		})
	}
}

// TestSubresourceGroupVersion verifies the group/version selection for restore/upload.
func TestSubresourceGroupVersion(t *testing.T) {
	cases := []struct {
		name        string
		ref         NodeRef
		wantGroup   string
		wantVersion string
	}{
		{
			name:        "core snapshot",
			ref:         NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot"},
			wantGroup:   CoreSubresourcesGroup,
			wantVersion: CoreSubresourcesVersion,
		},
		{
			name:        "domain snapshot",
			ref:         NodeRef{APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "VirtualDiskSnapshot"},
			wantGroup:   "subresources.demo.deckhouse.io",
			wantVersion: "v1alpha1",
		},
		{
			name:        "csi volume snapshot leaf",
			ref:         NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot"},
			wantGroup:   VSConnectorGroup,
			wantVersion: VSConnectorVersion,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			group, version, err := subresourceGroupVersion(tc.ref)
			if err != nil {
				t.Fatalf("subresourceGroupVersion: %v", err)
			}

			if group != tc.wantGroup {
				t.Errorf("group: got %q, want %q", group, tc.wantGroup)
			}

			if version != tc.wantVersion {
				t.Errorf("version: got %q, want %q", version, tc.wantVersion)
			}
		})
	}
}

// TestResourceFor_NoMapper verifies a clear error when a non-leaf ref must be resolved
// without a configured RESTMapper.
func TestResourceFor_NoMapper(t *testing.T) {
	c := NewClient(nil, nil)

	if _, err := c.resourceFor(NodeRef{APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "VirtualDiskSnapshot"}); err == nil {
		t.Fatal("expected error when no RESTMapper is configured, got nil")
	}

	// CSI VolumeSnapshot leaves use a fixed plural and need no mapper.
	res, err := c.resourceFor(NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot"})
	if err != nil {
		t.Fatalf("resourceFor(VolumeSnapshot leaf): %v", err)
	}

	if res != VolumeSnapshotResource {
		t.Errorf("resourceFor(VolumeSnapshot leaf): got %q, want %q", res, VolumeSnapshotResource)
	}
}

// TestAggregatedAPIContract pins every aggregated-API path the CLI builds against the
// verified server contract. Each row specifies the subresource, the node kind, the
// exact absolute path, and the HTTP method the server requires. Inline comments
// reference the producer handler that serves each combination so any future drift
// (like the domain-upload 405 fixed in fix-import-upload-core-group-for-domain) fails
// fast here.
//
// Server contract summary (verified against state-snapshotter source):
//   - manifests-download (GET): core group for Snapshot+domain, VS-connector for VS leaf.
//     Every path is addressed by the node's own namespaced CR — the CLI never reads
//     cluster-scoped snapshotcontents; restore_handler.go SetupRoutes.
//   - manifests-with-data-restoration (GET): core group for Snapshot; domain-prefixed
//     group for domain CRs (domainapi/handler.go, GET-only); VS-connector for VS leaf.
//   - manifests-and-children-refs-upload (POST): CORE group for Snapshot AND domain CRs
//     (routeGenericSnapshotSubresource handles both); VS-connector for VS leaf.
func TestAggregatedAPIContract(t *testing.T) {
	c := NewClient(nil, testMapper())

	coreRef := NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot", Name: "snap-1", Namespace: "ns"}
	domainRef := NodeRef{APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1", Kind: "DemoVirtualDiskSnapshot", Name: "vds-1", Namespace: "ns"}
	vsRef := NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "vs-1", Namespace: "ns"}

	cases := []struct {
		name       string
		pathFn     func(*Client) (string, error)
		wantPath   string
		wantMethod string // HTTP method the server contract requires for this subresource
	}{
		// ── manifests-download (GET) ──────────────────────────────────────────────────
		// restore_handler.go routeCoreSnapshotSubresource -> HandleCoreSnapshotManifestsDownload
		{
			name:       "core Snapshot: manifests-download -> core group",
			pathFn:     func(c *Client) (string, error) { return c.downloadPath(coreRef) },
			wantPath:   "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/snapshots/snap-1/manifests-download",
			wantMethod: http.MethodGet,
		},
		// restore_handler.go routeGenericSnapshotSubresource -> HandleGenericSnapshotManifestsDownload
		{
			name:       "domain CR: manifests-download -> core group",
			pathFn:     func(c *Client) (string, error) { return c.downloadPath(domainRef) },
			wantPath:   "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/demovirtualdisksnapshots/vds-1/manifests-download",
			wantMethod: http.MethodGet,
		},
		// volumesnapshot_connector.go handleVolumeSnapshotNamespaced -> handleVolumeSnapshotManifestsDownload
		{
			name:       "VS leaf: manifests-download -> VS-connector group",
			pathFn:     func(c *Client) (string, error) { return c.downloadPath(vsRef) },
			wantPath:   "/apis/subresources.snapshot.storage.k8s.io/v1/namespaces/ns/volumesnapshots/vs-1/manifests-download",
			wantMethod: http.MethodGet,
		},

		// ── manifests-with-data-restoration (GET) ─────────────────────────────────────
		// restore_handler.go routeCoreSnapshotSubresource -> HandleGetSnapshotManifestsWithDataRestoration
		{
			name:       "core Snapshot: manifests-with-data-restoration -> core group",
			pathFn:     func(c *Client) (string, error) { return c.subresourcePath(coreRef, SubManifestsRestore) },
			wantPath:   "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/snapshots/snap-1/manifests-with-data-restoration",
			wantMethod: http.MethodGet,
		},
		// domainapi/handler.go handleSubtree: GET-only; "manifests-with-data-restoration" -> ManifestsWithDataRestoration
		// Served by the domain-prefixed group (subresources.demo.state-snapshotter.deckhouse.io).
		{
			name:       "domain CR: manifests-with-data-restoration -> domain-prefixed group",
			pathFn:     func(c *Client) (string, error) { return c.subresourcePath(domainRef, SubManifestsRestore) },
			wantPath:   "/apis/subresources.demo.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/demovirtualdisksnapshots/vds-1/manifests-with-data-restoration",
			wantMethod: http.MethodGet,
		},
		// volumesnapshot_connector.go handleVolumeSnapshotNamespaced -> handleVolumeSnapshotManifestsWithDataRestoration
		{
			name:       "VS leaf: manifests-with-data-restoration -> VS-connector group",
			pathFn:     func(c *Client) (string, error) { return c.subresourcePath(vsRef, SubManifestsRestore) },
			wantPath:   "/apis/subresources.snapshot.storage.k8s.io/v1/namespaces/ns/volumesnapshots/vs-1/manifests-with-data-restoration",
			wantMethod: http.MethodGet,
		},

		// ── manifests-and-children-refs-upload (POST) ─────────────────────────────────
		// restore_handler.go routeCoreSnapshotSubresource -> HandleSnapshotManifestsAndChildrenUpload
		{
			name:       "core Snapshot: manifests-and-children-refs-upload -> core group",
			pathFn:     func(c *Client) (string, error) { return c.uploadPath(coreRef) },
			wantPath:   "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/snapshots/snap-1/manifests-and-children-refs-upload",
			wantMethod: http.MethodPost,
		},
		// restore_handler.go routeGenericSnapshotSubresource -> HandleGenericSnapshotManifestsAndChildrenUpload
		// CORE group: domainapi/handler.go handleSubtree only implements GET; POST must go to core.
		{
			name:       "domain CR: manifests-and-children-refs-upload -> CORE group",
			pathFn:     func(c *Client) (string, error) { return c.uploadPath(domainRef) },
			wantPath:   "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/demovirtualdisksnapshots/vds-1/manifests-and-children-refs-upload",
			wantMethod: http.MethodPost,
		},
		// volumesnapshot_connector.go handleVolumeSnapshotNamespaced -> handleManifestsAndChildrenUpload (verb: create/POST)
		{
			name:       "VS leaf: manifests-and-children-refs-upload -> VS-connector group",
			pathFn:     func(c *Client) (string, error) { return c.uploadPath(vsRef) },
			wantPath:   "/apis/subresources.snapshot.storage.k8s.io/v1/namespaces/ns/volumesnapshots/vs-1/manifests-and-children-refs-upload",
			wantMethod: http.MethodPost,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantMethod != http.MethodGet && tc.wantMethod != http.MethodPost {
				t.Errorf("contract table error: wantMethod=%q must be GET or POST", tc.wantMethod)
			}

			got, err := tc.pathFn(c)
			if err != nil {
				t.Fatalf("path build: %v", err)
			}

			if got != tc.wantPath {
				t.Errorf("path mismatch:\n got  %q\n want %q", got, tc.wantPath)
			}
		})
	}
}

// ── manifests-download transient-error retry ────────────────────────────────────────
//
// These tests exercise getManifestsDownload (used by NodeManifestsDownload) against a
// stubbed rest.Interface. They deliberately do NOT use t.Parallel(): withFastBackoff
// mutates the package-level manifestsDownloadBackoff var for the duration of the test,
// which would race under parallel execution.

// coreSnapshotRef returns a core Snapshot node ref for the retry tests below.
func coreSnapshotRef() NodeRef {
	return NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot", Name: "my-snap", Namespace: "ns"}
}

// withFastBackoff overrides manifestsDownloadBackoff for the duration of the test with
// a small, deterministic backoff so retry tests exercise real (non-mocked) sleeps
// without slowing down the suite. Restored via t.Cleanup.
func withFastBackoff(t *testing.T, b wait.Backoff) {
	t.Helper()

	orig := manifestsDownloadBackoff
	manifestsDownloadBackoff = b
	t.Cleanup(func() { manifestsDownloadBackoff = orig })
}

// statusResponse builds an HTTP response factory returning a well-formed
// metav1.Status body, matching what the real kube-apiserver aggregation layer sends
// for a non-2xx response (this is what apierrors.FromObject/IsServiceUnavailable etc.
// classify against).
func statusResponse(t *testing.T, code int32, reason metav1.StatusReason, message string) func() *http.Response {
	t.Helper()

	raw, err := json.Marshal(metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure,
		Message:  message,
		Reason:   reason,
		Code:     code,
	})
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}

	return func() *http.Response {
		return &http.Response{
			StatusCode: int(code),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(string(raw))),
		}
	}
}

// okResponse builds an HTTP 200 response factory returning body.
func okResponse(body string) func() *http.Response {
	return func() *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}
	}
}

// countingRESTClient returns a fake rest.Interface serving respFns in order. A call
// beyond len(respFns) fails the test immediately, so a test can assert "no further
// attempt was made" simply by supplying exactly the expected number of responses.
func countingRESTClient(t *testing.T, respFns ...func() *http.Response) (*restfake.RESTClient, *int) {
	t.Helper()

	calls := 0
	rc := &restfake.RESTClient{
		NegotiatedSerializer: scheme.Codecs,
		GroupVersion:         schema.GroupVersion{Group: StorageGroup, Version: "v1alpha1"},
		VersionedAPIPath:     "/",
		Client: restfake.CreateHTTPClient(func(_ *http.Request) (*http.Response, error) {
			if calls >= len(respFns) {
				t.Fatalf("unexpected extra HTTP call #%d (want at most %d)", calls+1, len(respFns))
			}

			resp := respFns[calls]()
			calls++

			return resp, nil
		}),
	}

	return rc, &calls
}

// TestNodeManifestsDownload_RetriesTransientThenSucceeds verifies that a 503
// ServiceUnavailable ("the server is currently unable to handle the request" -- the
// exact message an aggregated APIService backend returns while briefly restarting) is
// retried and the call ultimately succeeds once the backend recovers.
func TestNodeManifestsDownload_RetriesTransientThenSucceeds(t *testing.T) {
	withFastBackoff(t, wait.Backoff{Steps: 5, Duration: time.Millisecond, Factor: 1.0, Cap: 5 * time.Millisecond})

	rc, calls := countingRESTClient(t,
		statusResponse(t, http.StatusServiceUnavailable, metav1.StatusReasonServiceUnavailable, "the server is currently unable to handle the request"),
		statusResponse(t, http.StatusServiceUnavailable, metav1.StatusReasonServiceUnavailable, "the server is currently unable to handle the request"),
		okResponse(`[]`),
	)

	c := NewClient(rc, testMapper())

	body, err := c.NodeManifestsDownload(context.Background(), coreSnapshotRef())
	if err != nil {
		t.Fatalf("NodeManifestsDownload: %v", err)
	}

	if string(body) != `[]` {
		t.Errorf("body: got %q, want %q", body, `[]`)
	}

	if *calls != 3 {
		t.Errorf("calls: got %d, want 3", *calls)
	}
}

// TestNodeManifestsDownload_NonTransientErrorNotRetried verifies that a genuine client
// error (Forbidden) surfaces on the first attempt with no retry.
func TestNodeManifestsDownload_NonTransientErrorNotRetried(t *testing.T) {
	withFastBackoff(t, wait.Backoff{Steps: 5, Duration: time.Millisecond, Factor: 1.0, Cap: 5 * time.Millisecond})

	rc, calls := countingRESTClient(t,
		statusResponse(t, http.StatusForbidden, metav1.StatusReasonForbidden, "not allowed"),
	)

	c := NewClient(rc, testMapper())

	_, err := c.NodeManifestsDownload(context.Background(), coreSnapshotRef())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !apierrors.IsForbidden(err) {
		t.Errorf("expected a Forbidden error, got: %v", err)
	}

	if *calls != 1 {
		t.Errorf("calls: got %d, want 1 (no retry expected)", *calls)
	}
}

// TestNodeManifestsDownload_ExhaustsRetriesOnPersistentTransientError verifies that a
// backend that never recovers fails after the bounded attempt count instead of hanging,
// and that the returned error still classifies as the transient reason it saw.
func TestNodeManifestsDownload_ExhaustsRetriesOnPersistentTransientError(t *testing.T) {
	backoff := wait.Backoff{Steps: 3, Duration: time.Millisecond, Factor: 1.0, Cap: 5 * time.Millisecond}
	withFastBackoff(t, backoff)

	respFn := statusResponse(t, http.StatusServiceUnavailable, metav1.StatusReasonServiceUnavailable, "the server is currently unable to handle the request")
	rc, calls := countingRESTClient(t, respFn, respFn, respFn)

	c := NewClient(rc, testMapper())

	_, err := c.NodeManifestsDownload(context.Background(), coreSnapshotRef())
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}

	if !apierrors.IsServiceUnavailable(err) {
		t.Errorf("expected the exhausted error to still classify as ServiceUnavailable, got: %v", err)
	}

	if *calls != backoff.Steps {
		t.Errorf("calls: got %d, want exactly %d (bounded attempts)", *calls, backoff.Steps)
	}
}

// TestNodeManifestsDownload_ContextCancelledAbortsRetryPromptly verifies that
// cancelling ctx during the retry loop's backoff wait aborts immediately and returns
// ctx.Err(), instead of waiting out the remaining attempt budget.
func TestNodeManifestsDownload_ContextCancelledAbortsRetryPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	rc, calls := countingRESTClient(t, func() *http.Response {
		// Cancel before returning control to the retry loop: by the time
		// ExponentialBackoffWithContext reaches its post-attempt select, ctx.Done()
		// is already closed, so it returns ctx.Err() without ever sleeping out the
		// (unmodified, multi-second) production backoff -- keeping this test fast
		// without needing to fake the clock.
		defer cancel()

		return statusResponse(t, http.StatusServiceUnavailable, metav1.StatusReasonServiceUnavailable, "the server is currently unable to handle the request")()
	})

	c := NewClient(rc, testMapper())

	_, err := c.NodeManifestsDownload(ctx, coreSnapshotRef())
	if err == nil {
		t.Fatal("expected error after ctx cancellation, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected error to wrap context.Canceled, got: %v", err)
	}

	if *calls != 1 {
		t.Errorf("calls: got %d, want 1 (aborted before a second attempt)", *calls)
	}
}
