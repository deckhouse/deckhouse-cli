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

package pipeline_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vbauerster/mpb/v8/decor"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/progress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
)

const (
	testNS        = "test-ns"
	rootSnapshot  = "my-snap"
	diskSnapName  = "disk-snap"
	sourcePVCName = "pvc-disk-source"

	storageAPIVersion = "storage.deckhouse.io/v1alpha1"
	childAPIVersion   = "demo.deckhouse.io/v1alpha1"
	childKind         = "VirtualDiskSnapshot"
)

// TestPipeline_HappyPath verifies the full download pipeline against a fake
// Kubernetes client and an httptest block-volume server.
//
// disk-snap has one OwnDataRef (non-aggregator), so it downloads its volume data
// directly into its own node directory using the flat layout.
//
// Layout after the run:
//
//	outputDir/ (root Snapshot node)
//	  manifests/configmap_test-cfg.yaml
//	  snapshots/
//	    virtualdisksnapshot_disk-snap/ (non-aggregator; 1 OwnDataRef → flat layout)
//	      manifests/
//	      data.bin.zst
//	      snapshot.yaml
//	  snapshot.yaml
func TestPipeline_HappyPath(t *testing.T) {
	// Raw block data for the child disk snapshot.
	rawBlock := bytes.Repeat([]byte("B"), 600)

	srv := makeBlockServer(t, rawBlock)
	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	// Root node must be complete.
	assertNodeComplete(t, outputDir)

	// Root must have a manifests/ dir with one ConfigMap file.
	manifestsDir := filepath.Join(outputDir, archive.ManifestsDirName)
	entries, err := os.ReadDir(manifestsDir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "expected one manifest file under root/manifests/")

	// Root must have a snapshots/ dir (because it has a child).
	_, err = os.Stat(filepath.Join(outputDir, archive.SnapshotsDirName))
	require.NoError(t, err, "root snapshots/ directory must exist")

	// disk-snap is a non-aggregator: complete, with data.bin.zst in its own dir.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)

	_, err = os.Stat(filepath.Join(diskSnapDir, archive.DataBlockName(".zst")))
	require.NoError(t, err, "non-aggregator node must have data.bin.zst directly")

	// disk-snap has no children, so no snapshots/ subdir.
	_, noSnapErr := os.Stat(filepath.Join(diskSnapDir, archive.SnapshotsDirName))
	require.True(t, os.IsNotExist(noSnapErr),
		"non-aggregator node must not have a snapshots/ subdir")

	// Second run must be a no-op: snapshot.yaml mtime must not change.
	rootYAML := filepath.Join(outputDir, archive.SnapshotYAMLName)
	diskSnapYAML := filepath.Join(diskSnapDir, archive.SnapshotYAMLName)

	rootMod := statMtime(t, rootYAML)
	diskSnapMod := statMtime(t, diskSnapYAML)

	// Sleep briefly so that any writes would produce a different mtime.
	time.Sleep(20 * time.Millisecond)

	err = runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	require.Equal(t, rootMod, statMtime(t, rootYAML),
		"root snapshot.yaml must not be rewritten on second run")
	require.Equal(t, diskSnapMod, statMtime(t, diskSnapYAML),
		"disk-snap snapshot.yaml must not be rewritten on second run")
}

// TestPipeline_OpenExportErrorReleasesCleanly is a regression guard for a
// live-reproduced leak: OpenExport's production implementation creates the
// DataExport CR (EnsureDataExport) BEFORE waiting for it to become Ready
// (WaitReady), so a cancellation/error during that wait can leave a DataExport
// behind even though OpenExport itself returns an error and no *exporter.Export
// value. The fake OpenExport below simulates exactly that: the DataExport is
// pre-seeded in the fake client (as if EnsureDataExport already created it),
// then OpenExport still fails (as if WaitReady errored). The pipeline must
// release the pre-seeded DataExport by its deterministic name even though it
// never received an *exporter.Export to call Release through.
func TestPipeline_OpenExportErrorReleasesCleanly(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	deName := exporter.DataExportName(diskSnapName)

	de := &deapi.DataExport{
		TypeMeta:   metav1.TypeMeta{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "DataExport"},
		ObjectMeta: metav1.ObjectMeta{Name: deName, Namespace: testNS},
	}
	require.NoError(t, c.Create(context.Background(), de))

	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    outputDir,
		Workers:      1,
		KubeClient:   c,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return nil, errors.New("simulated WaitReady cancellation after EnsureDataExport created the CR")
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.Error(t, err, "expected pipeline to fail when OpenExport errors")

	got := &deapi.DataExport{}
	getErr := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: deName}, got)
	require.Truef(t, apierrors.IsNotFound(getErr),
		"pre-seeded DataExport %q must be released even though OpenExport failed before returning an *exporter.Export, got err=%v", deName, getErr)
}

// ctxDeadlineClient wraps a client.Client and, on Get, returns ctx.Err() wrapped
// as a rate-limiter-style failure whenever ctx is already done, before ever
// delegating to the underlying client. This reproduces what client-go's rate
// limiter Wait(ctx) does against an already-expired context in production (the
// live incident WARN read "client rate limiter Wait returned an error: context
// deadline exceeded") — behavior the in-memory fake client does not exhibit on
// its own, since it never inspects ctx.
type ctxDeadlineClient struct {
	client.Client
}

// Get returns ctx's own error, wrapped, if ctx is already done; otherwise it
// delegates to the wrapped client unchanged.
func (c ctxDeadlineClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("client rate limiter Wait returned an error: %w", err)
	}

	return c.Client.Get(ctx, key, obj, opts...)
}

// TestPipeline_ReleaseGetsFreshTimeoutAfterSlowOpenExport is a regression guard
// for a live-reproduced leak distinct from TestPipeline_OpenExportErrorReleasesCleanly
// above: a FULLY SUCCESSFUL download whose OpenExport (EnsureDataExport +
// WaitReady) plus volume transfer together take longer than the release
// timeout used to leak its DataExport. The prior fix computed that timeout
// ONCE, before calling OpenExport, but the release defer only actually runs at
// function return — by which point the clock had often already run out on any
// real-sized volume. This test pins that the timeout budget is instead derived
// FRESH at the moment the release defer executes, so it is unaffected by how
// long the preceding work took.
//
// cfg.ReleaseTimeout is set to a short duration and OpenExport is stubbed to
// sleep past it before returning success — no real 30-second wait is needed;
// only the relative ordering (OpenExport's delay exceeds ReleaseTimeout)
// matters. ctxDeadlineClient supplies the "already-expired context fails the
// very next call" behavior that a real rate-limited client exhibits, which is
// what actually distinguishes the fixed and pre-fix implementations here.
func TestPipeline_ReleaseGetsFreshTimeoutAfterSlowOpenExport(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("B"), 600)

	srv := makeBlockServer(t, rawBlock)
	defer srv.Close()

	c := ctxDeadlineClient{buildFakeClient(t)}
	outputDir := t.TempDir()

	const releaseTimeout = 20 * time.Millisecond

	deName := exporter.DataExportName(diskSnapName)

	de := &deapi.DataExport{
		TypeMeta:   metav1.TypeMeta{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "DataExport"},
		ObjectMeta: metav1.ObjectMeta{Name: deName, Namespace: testNS},
	}
	require.NoError(t, c.Create(context.Background(), de))

	cfg := pipeline.Config{
		Namespace:      testNS,
		RootSnapshot:   rootSnapshot,
		OutputDir:      outputDir,
		Workers:        1,
		KubeClient:     c,
		ReleaseTimeout: releaseTimeout,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			// Simulate WaitReady taking longer than ReleaseTimeout, mirroring the
			// live repro where WaitReady alone took ~30s against a fixed 30s
			// budget. A pre-fix cleanupCtx created before this sleep would
			// already be expired by the time release runs.
			time.Sleep(3 * releaseTimeout)

			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err, "expected the download to succeed despite the slow OpenExport")

	got := &deapi.DataExport{}
	getErr := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: deName}, got)
	require.Truef(t, apierrors.IsNotFound(getErr),
		"DataExport %q must be released on a fully successful download even though OpenExport+transfer took longer than ReleaseTimeout, got err=%v", deName, getErr)
}

// TestPipeline_BlockResumeAfterMerge verifies that when data.bin.zst already exists
// in a node directory (crash-after-merge-before-snapshot.yaml window), the pipeline
// skips DataExport creation entirely and only calls FinalizeNode.
func TestPipeline_BlockResumeAfterMerge(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	// Pre-create disk-snap's directory with data.bin.zst but no snapshot.yaml,
	// simulating a crash after block chunks were merged but before FinalizeNode ran.
	// disk-snap is a non-aggregator: it downloads its OwnDataRef flat into its own dir.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))

	require.NoError(t, os.MkdirAll(filepath.Join(diskSnapDir, archive.ManifestsDirName), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(diskSnapDir, archive.DataBlockName(".zst")),
		[]byte("pre-merged-block-data"),
		0o644,
	))

	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    outputDir,
		Workers:      1,
		KubeClient:   c,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when data.bin.zst already exists")
			return nil, errors.New("unexpected OpenExport call")
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	// FinalizeNode must have been called: disk-snap directory must now be complete.
	assertNodeComplete(t, diskSnapDir)
}

// TestPipeline_FSResumeAfterTar verifies that when data.tar already exists in a
// node directory (crash-after-tar-assembly-before-snapshot.yaml window), the
// pipeline skips DataExport creation entirely and only calls FinalizeNode.
func TestPipeline_FSResumeAfterTar(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	// Pre-create disk-snap's directory with data.tar but no snapshot.yaml,
	// simulating a crash after the FS tar was assembled but before FinalizeNode ran.
	// disk-snap is a non-aggregator: it downloads its OwnDataRef flat into its own dir.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))

	require.NoError(t, os.MkdirAll(filepath.Join(diskSnapDir, archive.ManifestsDirName), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(diskSnapDir, archive.FsTarName),
		[]byte("pre-assembled-fs-tar"),
		0o644,
	))

	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    outputDir,
		Workers:      1,
		KubeClient:   c,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when data.tar already exists")

			return nil, errors.New("unexpected OpenExport call")
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	// FinalizeNode must have been called: disk-snap directory must now be complete.
	assertNodeComplete(t, diskSnapDir)
}

// assertNodeComplete checks that snapshot.yaml exists in dir and VerifyNode passes.
func assertNodeComplete(t *testing.T, dir string) {
	t.Helper()

	yamlPath := filepath.Join(dir, archive.SnapshotYAMLName)
	_, err := os.Stat(yamlPath)
	require.NoError(t, err, "snapshot.yaml must exist in %s", dir)

	require.NoError(t, archive.VerifyNode(dir), "VerifyNode must pass for %s", dir)
}

// statMtime returns the modification time of path.
func statMtime(t *testing.T, path string) time.Time {
	t.Helper()

	fi, err := os.Stat(path)
	require.NoError(t, err)

	return fi.ModTime()
}

// makeBlockServer creates an httptest.Server that serves rawData at /api/v1/block.
// It supports HEAD (Content-Length) and Range GET requests.
func makeBlockServer(t *testing.T, rawData []byte) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(rawData))
	})

	return httptest.NewServer(mux)
}

// buildFakeClient constructs a controller-runtime fake client pre-populated with
// all objects needed for the pipeline test.
func buildFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := buildScheme(t)

	// Root Snapshot (typed) with one child reference.
	rootSnap := &snapshotapi.Snapshot{
		TypeMeta: metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "Snapshot"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      rootSnapshot,
			Namespace: testNS,
		},
		Status: snapshotapi.SnapshotStatus{
			BoundSnapshotContentName: "sc-root",
			ChildrenSnapshotRefs: []snapshotapi.SnapshotChildRef{
				{APIVersion: childAPIVersion, Kind: childKind, Name: diskSnapName},
			},
		},
	}

	// Root SnapshotContent: own-node manifests are served by the stub ManifestSource,
	// keyed by node ref; the content itself carries no volume DataRefs here.
	rootContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-root"},
	}

	// Child snapshot (unstructured — domain-specific kind not in the scheme).
	childSnap := makeUnstructuredSnap(childAPIVersion, childKind, testNS, diskSnapName, "sc-disk")

	// Child SnapshotContent: one block DataRef pointing at the source PVC, no manifests.
	// This DataRef materialises disk-snap as a non-aggregator OwnDataRef node.
	childContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-disk"},
		Status: snapshotapi.SnapshotContentStatus{
			DataRef: &snapshotapi.SnapshotDataBinding{
				TargetUID: "uid-disk",
				Target: snapshotapi.SnapshotSubjectRef{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Namespace:  testNS,
					Name:       sourcePVCName,
				},
				Artifact: snapshotapi.SnapshotDataArtifactRef{
					APIVersion: "snapshot.storage.k8s.io/v1",
					Kind:       "VolumeSnapshotContent",
					Name:       "vsc-disk",
				},
			},
		},
	}

	typed := []client.Object{rootSnap, rootContent, childContent}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(typed...).
		WithObjects(childSnap).
		Build()
}

// buildScheme registers all types needed by the pipeline test.
func buildScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, snapshotapi.AddToScheme(scheme))
	require.NoError(t, deapi.AddToScheme(scheme))

	return scheme
}

// TestPipeline_LeafTargetRef verifies that OpenExport receives the correct snapshot
// leaf NodeRef (not a shadow VS name) when a domain snapshot node downloads its
// OwnDataRef volume.
func TestPipeline_LeafTargetRef(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("B"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	var capturedRef aggapi.NodeRef

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, leafRef aggapi.NodeRef, _ string) (*exporter.Export, error) {
			capturedRef = leafRef
			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	// OpenExport must receive the disk-snap domain snapshot ref, not a shadow VS.
	require.Equal(t, childAPIVersion, capturedRef.APIVersion,
		"OpenExport must receive the domain snapshot APIVersion")
	require.Equal(t, childKind, capturedRef.Kind,
		"OpenExport must receive the domain snapshot Kind")
	require.Equal(t, diskSnapName, capturedRef.Name,
		"OpenExport must receive the domain snapshot Name")
	require.Equal(t, testNS, capturedRef.Namespace,
		"OpenExport must receive the correct Namespace")
}

// TestPipeline_SubtreeSelection verifies that when SelectedNodeKind/SelectedNodeName
// identify a direct child of the root, only that node (and its descendants) is
// downloaded. The root directory gets content-free scaffold directories (snapshots/)
// but no snapshot.yaml or manifests/.
//
// Tree used by buildFakeClient:
//
//	outputDir/                         ← root Snapshot (scaffold only)
//	  snapshots/
//	    virtualdisksnapshot_disk-snap/ ← selected node (fully downloaded)
//	      manifests/
//	      data.bin.zst
//	      snapshot.yaml
func TestPipeline_SubtreeSelection(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("S"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		SelectedNodeKind:     childKind,
		SelectedNodeName:     diskSnapName,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-subtree", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	// Root dir must NOT have a snapshot.yaml — it was not processed, only scaffolded.
	_, err := os.Stat(filepath.Join(outputDir, archive.SnapshotYAMLName))
	require.True(t, os.IsNotExist(err),
		"root snapshot.yaml must not exist when only a subtree was selected")

	// Root dir must NOT have a manifests/ directory.
	_, err = os.Stat(filepath.Join(outputDir, archive.ManifestsDirName))
	require.True(t, os.IsNotExist(err),
		"root manifests/ must not exist when only a subtree was selected")

	// The selected node must be fully complete at its real path.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)

	// The selected node must have its block-volume data.
	_, err = os.Stat(filepath.Join(diskSnapDir, archive.DataBlockName(".zst")))
	require.NoError(t, err, "selected node must have data.bin.zst")

	// Resume: a second run must not overwrite the completed node.
	diskYAML := filepath.Join(diskSnapDir, archive.SnapshotYAMLName)
	diskMod := statMtime(t, diskYAML)

	time.Sleep(20 * time.Millisecond)

	require.NoError(t, runPipeline(context.Background(), cfg))
	require.Equal(t, diskMod, statMtime(t, diskYAML),
		"disk-snap snapshot.yaml must not be rewritten on second run")
}

// TestPipeline_SubtreeRootSelection verifies that selecting the root node by kind
// and name produces the same result as a full-tree download (both root and child
// nodes are fully processed).
func TestPipeline_SubtreeRootSelection(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("R"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		SelectedNodeKind:     "Snapshot",
		SelectedNodeName:     rootSnapshot,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-root-sel", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	// Root node must be complete (same as full-tree download).
	assertNodeComplete(t, outputDir)

	// Child node must also be complete.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)
}

// makeUnstructuredSnap builds an unstructured snapshot object for kinds not
// registered in the scheme (e.g. VirtualDiskSnapshot).
func makeUnstructuredSnap(apiVersion, kind, namespace, name, contentName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"status": map[string]interface{}{
				"boundSnapshotContentName": contentName,
			},
		},
	}
}

// TestPipeline_NoneCompression verifies that when Compression is set to the
// "none" codec the pipeline produces data.bin (no extension) for block volumes.
func TestPipeline_NoneCompression(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("N"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	noneCodec, err := compress.New("none", 0)
	require.NoError(t, err, "compress.New(none, 0)")

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Compression:          noneCodec,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-none", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)

	// none codec → no extension: data.bin (not data.bin.zst)
	noneBlockPath := filepath.Join(diskSnapDir, archive.DataBlockName(""))
	_, statErr := os.Stat(noneBlockPath)
	require.NoError(t, statErr, "none-compressed block must produce data.bin (no extension)")

	// The compressed file with .zst extension must NOT exist.
	_, statZstErr := os.Stat(filepath.Join(diskSnapDir, archive.DataBlockName(".zst")))
	require.True(t, os.IsNotExist(statZstErr),
		"none-compression must not produce data.bin.zst")

	got, readErr := os.ReadFile(noneBlockPath)
	require.NoError(t, readErr)
	require.Equal(t, rawBlock, got, "none-compressed block data must match original")
}

// TestPipeline_Progress_NonTTYFallback verifies that injecting a non-TTY progress.Sink
// into pipeline.Config causes the aggregate "downloaded X / total Y" line to be emitted
// to the configured io.Writer after the run completes, using a known block-volume total.
//
// The Sink is constructed with a very long tick interval so only sink.Wait() emits output,
// making the assertion fully deterministic.
func TestPipeline_Progress_NonTTYFallback(t *testing.T) {
	t.Parallel()

	// 600-byte block payload gives a known per-volume total for the assertion.
	rawBlock := bytes.Repeat([]byte("P"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	var buf bytes.Buffer

	// Long interval ensures no periodic tick fires during the test; only Wait() emits.
	sink := progress.New(&buf, false, progress.WithInterval(time.Hour))

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Progress:             sink,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-progress", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	sink.Wait()

	got := buf.String()

	// The non-TTY sink emits "downloaded X / total Y (N/M volumes)" using
	// decor.SizeB1024 with the "% .1f" verb — replicate the same format to pin the
	// exact expected line. This run has exactly one volume stream (the root's
	// single block leaf), so N/M settles at 1/1.
	total := int64(len(rawBlock))
	want := fmt.Sprintf("downloaded % .1f / total % .1f (1/1 volumes)\n",
		decor.SizeB1024(total), decor.SizeB1024(total))

	require.True(t, strings.Contains(got, want),
		"non-TTY Sink must emit the aggregate line after pipeline completes\ngot:  %q\nwant (contained): %q",
		got, want)
}

// TestPipeline_Progress_NilSinkIsNoop verifies that nil Progress in Config does not
// change pipeline behavior: the download completes normally and no progress output is
// produced.
func TestPipeline_Progress_NilSinkIsNoop(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("Q"), 300)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		// Progress deliberately left nil to test the no-op path.
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-nil-progress", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)
}

// TestPipeline_PartialChunkResume verifies the block_partial resume path: when a node's
// data.bin.d/ chunk directory already holds some (but not all) chunk files and there is
// no snapshot.yaml, the pipeline fetches only the missing byte ranges, merges all chunks,
// removes data.bin.d/, and finalizes the node.
func TestPipeline_PartialChunkResume(t *testing.T) {
	t.Parallel()

	const (
		testChunkSize int64 = 100 // 3 × 100 = 300 bytes → 3 chunks
		testTotalSize int64 = 300
	)

	rawBlock := bytes.Repeat([]byte("Z"), int(testTotalSize))

	// Track which Range GET headers the server receives.
	var (
		mu            sync.Mutex
		fetchedRanges []string
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			mu.Lock()
			fetchedRanges = append(fetchedRanges, r.Header.Get("Range"))
			mu.Unlock()
		}

		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(rawBlock))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	codec, err := compress.New("zstd", 0)
	require.NoError(t, err)

	// Pre-seed chunk 0 as a real zstd frame, simulating a crash after the first
	// chunk was downloaded but before the remaining chunks were fetched.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	chunkDir := filepath.Join(diskSnapDir, archive.BlockChunksDirName)
	require.NoError(t, os.MkdirAll(chunkDir, 0o755))

	chunk0Frame, err := codec.EncodeFrame(rawBlock[:testChunkSize])
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(
		filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())),
		chunk0Frame,
		0o644,
	))

	// A real interrupted run always has a chunks.meta recording the geometry
	// (written before the first chunk is even fetched — see the
	// chunk-size-mismatch-resume-corruption-guard fix), so seed one matching
	// this run's geometry; otherwise the geometry guard cannot distinguish
	// this partial dir from one left by a different --chunk-size and would
	// (correctly) purge and re-fetch chunk 0 too.
	require.NoError(t, archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: testChunkSize, TotalSize: testTotalSize}))

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		ChunkSize:            testChunkSize,
		KubeClient:           c,
		Compression:          codec,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-partial-resume", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	// (a) Chunk 0 must not have been re-fetched; chunks 1 and 2 must have been fetched.
	mu.Lock()
	gotRanges := append([]string(nil), fetchedRanges...)
	mu.Unlock()

	for _, hdr := range gotRanges {
		require.NotEqual(t, "bytes=0-99", hdr,
			"chunk 0 was pre-seeded and must not be re-fetched")
	}

	require.Contains(t, gotRanges, "bytes=100-199", "chunk 1 must be fetched")
	require.Contains(t, gotRanges, "bytes=200-299", "chunk 2 must be fetched")

	// (b) Merged data.bin.zst must decode to the original rawBlock.
	blockFile := filepath.Join(diskSnapDir, archive.DataBlockName(codec.Ext()))
	compressed, readErr := os.ReadFile(blockFile)
	require.NoError(t, readErr)
	require.Equal(t, rawBlock, decodeZstdBlock(t, compressed),
		"merged block must decode to original bytes")

	// (c) The node must be fully finalized.
	assertNodeComplete(t, diskSnapDir)

	// (d) The chunk directory must have been removed after merge.
	_, statErr := os.Stat(chunkDir)
	require.True(t, os.IsNotExist(statErr), "data.bin.d/ must be removed after merge")
}

// TestPipeline_FS_ChunkSizeThreadsToDownloadFilesystemVolume verifies that
// downloadFS passes cfg.ChunkSize through to volume.DownloadFilesystemVolume
// (fs-large-file-chunked-range-resume): a Filesystem-mode volume whose single
// file exceeds cfg.ChunkSize must be fetched via multiple Range GETs, not one
// plain GET, proving the pipeline-level config value — not just the
// volume package's own default — governs per-file chunking.
func TestPipeline_FS_ChunkSizeThreadsToDownloadFilesystemVolume(t *testing.T) {
	t.Parallel()

	const testChunkSize int64 = 100

	content := bytes.Repeat([]byte("F"), 250) // 3 chunks: 100, 100, 50

	var (
		mu     sync.Mutex
		ranges []string
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"big.bin","type":"file","uri":"big.bin","attributes":{"size":`+strconv.Itoa(len(content))+`}}`+
				`]}`)

		case "/api/v1/files/big.bin":
			mu.Lock()
			ranges = append(ranges, r.Header.Get("Range"))
			mu.Unlock()

			http.ServeContent(w, r, "big.bin", time.Time{}, bytes.NewReader(content))

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	codec, err := compress.New("zstd", 0)
	require.NoError(t, err)

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		ChunkSize:            testChunkSize,
		KubeClient:           c,
		Compression:          codec,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-fs-chunk", "Filesystem", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	mu.Lock()
	gotRanges := append([]string(nil), ranges...)
	mu.Unlock()

	require.GreaterOrEqual(t, len(gotRanges), 2,
		"cfg.ChunkSize must have been threaded to DownloadFilesystemVolume, forcing a chunked (multi Range GET) download")

	for _, hdr := range gotRanges {
		require.NotEmpty(t, hdr, "every request must carry a Range header once per-file chunking is active")
	}

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)

	f, err := os.Open(filepath.Join(diskSnapDir, archive.FsTarName))
	require.NoError(t, err)

	defer func() { _ = f.Close() }()

	tr := tar.NewReader(f)

	var found bool

	for {
		hdr, nextErr := tr.Next()
		if nextErr == io.EOF {
			break
		}

		require.NoError(t, nextErr)

		if hdr.Name != "big.bin"+codec.Ext() {
			continue
		}

		compressed, readErr := io.ReadAll(tr)
		require.NoError(t, readErr)
		require.Equal(t, content, decodeZstdBlock(t, compressed),
			"merged big.bin tar entry must decode to the original content")

		found = true
	}

	require.True(t, found, "tar entry for big.bin not found")
}

// ── recording progress helpers ────────────────────────────────────────────────

// recordedStream is a progress.Stream stub that counts Activate, Done, and Fail
// calls. It mirrors the real progress.Stream contract's "first terminal call
// wins" semantics (see ttyStream.finalize / plainStream.finalize in
// internal/progress/multibar.go): once Done or Fail has been called once, a
// later call to either is a no-op on the counters. This matters for
// pipeline.Run's post-g.Wait() defensive sweep, which calls Fail() on every
// pre-created stream unconditionally — against the real sinks that is a safe
// no-op for already-Done streams, and this stub must behave the same way for
// tests exercising the sweep to assert anything meaningful. All methods are
// safe for concurrent use.
type recordedStream struct {
	name        string
	mu          sync.Mutex
	activateCnt int
	doneCnt     int
	failCnt     int
	settled     bool
}

func (s *recordedStream) IncrBy(_ int)     {}
func (s *recordedStream) SetTotal(_ int64) {}

func (s *recordedStream) Activate() {
	s.mu.Lock()
	s.activateCnt++
	s.mu.Unlock()
}

func (s *recordedStream) Done() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.settled {
		return
	}

	s.settled = true
	s.doneCnt++
}

func (s *recordedStream) Fail() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.settled {
		return
	}

	s.settled = true
	s.failCnt++
}

// recordingSink is a progress.Sink stub that captures NewStream calls in creation
// order. All methods are safe for concurrent use.
type recordingSink struct {
	mu   sync.Mutex
	seen []*recordedStream
}

func (s *recordingSink) NewStream(name string, _ int64) progress.Stream {
	rs := &recordedStream{name: name}
	s.mu.Lock()
	s.seen = append(s.seen, rs)
	s.mu.Unlock()

	return rs
}

func (s *recordingSink) SetVolumeTotal(int)   {}
func (s *recordingSink) Wait()                {}
func (s *recordingSink) LogWriter() io.Writer { return io.Discard }

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.seen)
}

func (s *recordingSink) snapshot() []*recordedStream {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*recordedStream, len(s.seen))
	copy(out, s.seen)

	return out
}

// TestPipeline_Progress_PrecreateStreams verifies that the pipeline pre-creates
// exactly one progress.Stream per volume leaf BEFORE any download starts, and
// creates no stream for aggregator/manifest-only nodes.
//
// Two leaf shapes are exercised:
//   - single-OwnDataRef (non-aggregator snapshot node)
//   - Binding (orphan VolumeSnapshot leaf)
func TestPipeline_Progress_PrecreateStreams(t *testing.T) {
	t.Parallel()

	t.Run("SingleOwnDataRef", func(t *testing.T) {
		t.Parallel()

		rawBlock := bytes.Repeat([]byte("X"), 300)
		srv := makeBlockServer(t, rawBlock)

		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()
		rec := &recordingSink{}

		var (
			once               sync.Once
			streamsAtFirstCall int
		)

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				once.Do(func() { streamsAtFirstCall = rec.count() })

				return exporter.NewExport(namespace, "de-precreate", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		// Exactly one stream for the single-OwnDataRef leaf; none for the root.
		require.Equal(t, 1, rec.count(), "exactly 1 stream for 1 volume leaf")
		require.Equal(t, 1, streamsAtFirstCall,
			"all streams must be pre-created before the first OpenExport call")

		streams := rec.snapshot()
		require.Equal(t, diskSnapName, streams[0].name, "stream name = node ref name")
		require.Equal(t, 1, streams[0].activateCnt, "leaf stream must be Activated exactly once")
		require.Equal(t, 1, streams[0].doneCnt, "leaf stream must be Done exactly once")
		require.Equal(t, 0, streams[0].failCnt, "a successful download must never call Fail")
	})

	t.Run("BindingLeaf", func(t *testing.T) {
		t.Parallel()

		rawBlock := bytes.Repeat([]byte("Y"), 300)
		srv := makeBlockServer(t, rawBlock)

		defer srv.Close()

		c := buildOrphanLeafFakeClient(t)
		outputDir := t.TempDir()
		rec := &recordingSink{}

		var (
			once               sync.Once
			streamsAtFirstCall int
		)

		cfg := pipeline.Config{
			Namespace:            e2eNS,
			RootSnapshot:         e2eAggRootSnap,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, leafRef aggapi.NodeRef, _ string) (*exporter.Export, error) {
				once.Do(func() { streamsAtFirstCall = rec.count() })

				if leafRef.Name != "nss-vs-agg-pvc" {
					return nil, fmt.Errorf("unexpected leaf %q", leafRef.Name)
				}

				return exporter.NewExport(namespace, "de-agg-leaf", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		// One stream for the binding leaf; none for root or aggregator nodes.
		require.Equal(t, 1, rec.count(),
			"exactly 1 stream for the binding leaf; aggregator/manifest-only nodes must not create streams")
		require.Equal(t, 1, streamsAtFirstCall,
			"all streams must be pre-created before the first OpenExport call")

		streams := rec.snapshot()
		require.Equal(t, "nss-vs-agg-pvc", streams[0].name,
			"binding stream name = VS CR name (node.Ref().Name)")
		require.Equal(t, 1, streams[0].activateCnt, "binding stream must be Activated exactly once")
		require.Equal(t, 1, streams[0].doneCnt, "binding stream must be Done exactly once")
		require.Equal(t, 0, streams[0].failCnt, "a successful download must never call Fail")
	})
}

// TestPipeline_Progress_ResumeSkip_NeverActivated verifies that when a leaf node is
// already complete (NodeStateDone), its pre-created stream is Done immediately in
// precreateStreams and is never Activated (OpenExport is not called).
func TestPipeline_Progress_ResumeSkip_NeverActivated(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("W"), 300)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	// First run: complete the pipeline so disk-snap reaches NodeStateDone.
	firstCfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-resume-first", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}
	require.NoError(t, runPipeline(context.Background(), firstCfg))

	// Second run: disk-snap is NodeStateDone; its stream must be Done immediately
	// (in precreateStreams) and must never be Activated.
	rec := &recordingSink{}

	secondCfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Progress:             rec,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when all nodes are already complete")
			return nil, errors.New("unexpected OpenExport call")
		},
	}
	require.NoError(t, runPipeline(context.Background(), secondCfg))

	require.Equal(t, 1, rec.count(), "one stream pre-created for the complete leaf node")

	streams := rec.snapshot()
	require.Equal(t, 0, streams[0].activateCnt,
		"resume-skipped stream must never be Activated")
	require.Equal(t, 1, streams[0].doneCnt,
		"resume-skipped stream must be Done exactly once (in precreateStreams)")
	require.Equal(t, 0, streams[0].failCnt, "a resume skip must never call Fail")
}

// TestPipeline_Progress_DownloadFailure_CallsFailNotDone verifies that when a
// volume download fails AFTER its DataExport opened and its stream was
// Activated (e.g. the block server errors mid-transfer or the connection is
// cut), downloadVolumeBinding calls stream.Fail() exactly once and
// stream.Done() zero times — the interrupted volume must never be counted
// toward "N/M volumes downloaded". Before the fix this test observed doneCnt
// == 1 (the unconditional `defer stream.Done()`), which is the exact live bug
// reported on a cluster: an interrupted download's own deferred Done() call
// incremented the completed-volume counter.
func TestPipeline_Progress_DownloadFailure_CallsFailNotDone(t *testing.T) {
	t.Parallel()

	// A block server whose HEAD response always errors, so downloadBlock fails
	// inside downloadVolumeBinding right after stream.Activate() has run.
	failingSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "simulated block volume failure", http.StatusInternalServerError)
	}))
	defer failingSrv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()
	rec := &recordingSink{}

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Progress:             rec,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-fail", "Block", failingSrv.URL, exporter.NewFetcher(failingSrv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.Error(t, err, "expected pipeline to fail when the block volume HEAD request errors")

	streams := rec.snapshot()
	require.Len(t, streams, 1, "exactly 1 stream for the single volume leaf")
	require.Equal(t, 1, streams[0].activateCnt, "stream must still be Activated before the failure")
	require.Equal(t, 0, streams[0].doneCnt, "a failed download must never call Done")
	require.Equal(t, 1, streams[0].failCnt, "a failed download must call Fail exactly once")
}

// TestPipeline_KeepExports verifies the --cleanup / Config.KeepExports gate on
// downloadVolumeBinding's DataExport release: with KeepExports false (default,
// today's behavior) the DataExport CR is deleted after the volume completes;
// with KeepExports true it is left in the cluster for debugging.
func TestPipeline_KeepExports(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		keepExports   bool
		wantRemaining bool
	}{
		{name: "default deletes DataExport", keepExports: false, wantRemaining: false},
		{name: "KeepExports leaves DataExport", keepExports: true, wantRemaining: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rawBlock := bytes.Repeat([]byte("K"), 300)
			srv := makeBlockServer(t, rawBlock)

			defer srv.Close()

			c := buildFakeClient(t)
			outputDir := t.TempDir()

			// The pipeline releases by the deterministic name derived from the leaf's
			// own node-ref name (exporter.DataExportName), not from whatever name the
			// OpenExport stub happens to hand back — release must find this object.
			deName := exporter.DataExportName(diskSnapName)

			de := &deapi.DataExport{
				TypeMeta:   metav1.TypeMeta{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "DataExport"},
				ObjectMeta: metav1.ObjectMeta{Name: deName, Namespace: testNS},
			}
			require.NoError(t, c.Create(context.Background(), de))

			cfg := pipeline.Config{
				Namespace:            testNS,
				RootSnapshot:         rootSnapshot,
				OutputDir:            outputDir,
				Workers:              1,
				PerVolumeConcurrency: 1,
				KubeClient:           c,
				KeepExports:          tc.keepExports,
				OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
					return exporter.NewExport(namespace, deName, "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
				},
			}

			require.NoError(t, runPipeline(context.Background(), cfg))

			got := &deapi.DataExport{}
			err := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: deName}, got)

			if tc.wantRemaining {
				require.NoError(t, err, "DataExport must remain in the cluster when KeepExports is true")
			} else {
				require.Truef(t, apierrors.IsNotFound(err),
					"DataExport must be deleted when KeepExports is false, got err=%v", err)
			}
		})
	}
}

// TestPipeline_Progress_OpenExportFailure_CallsFailNotDone verifies that when
// cfg.OpenExport itself returns an error (e.g. ctx cancelled while polling
// WaitReady, or the DataExport never becomes Ready), downloadVolumeBinding's
// stream.Fail()/Done() defer — now registered right after the stream semaphore
// is acquired, BEFORE cfg.OpenExport is even called — still settles the stream
// as Fail exactly once and Done zero times. Before the
// progress-finalize-streams-on-early-error-paths fix, the terminal defer was
// registered only after cfg.OpenExport returned successfully, so this exact
// path left the pre-created stream dangling (failCnt==0, doneCnt==0) and a real
// TTY sink's Wait() would block forever on it.
func TestPipeline_Progress_OpenExportFailure_CallsFailNotDone(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()
	rec := &recordingSink{}

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Progress:             rec,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return nil, errors.New("simulated OpenExport failure (e.g. ctx cancelled mid-WaitReady)")
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.Error(t, err, "expected pipeline to fail when OpenExport itself errors")

	streams := rec.snapshot()
	require.Len(t, streams, 1, "exactly 1 stream for the single volume leaf")
	require.Equal(t, 0, streams[0].activateCnt, "a stream must never be Activated before OpenExport succeeds")
	require.Equal(t, 0, streams[0].doneCnt, "an OpenExport failure must never call Done")
	require.Equal(t, 1, streams[0].failCnt, "an OpenExport failure must call Fail exactly once")
}

// TestPipeline_Progress_CancelDuringWait_DoesNotDeadlock is the end-to-end
// regression test for the live "had to press Ctrl-C twice" report: it drives a
// REAL progress.New(..., true) ttySink (not the recordingSink stub used
// elsewhere in this file) through a cancelled run and asserts sink.Wait() —
// the exact call cmd/download/download.go makes after pipeline.Run returns —
// completes promptly instead of blocking forever.
//
// The tree has two volume leaves and MaxParallelDownloads=1, so once the first
// leaf's goroutine is blocked inside OpenExport (holding the one stream-
// semaphore slot), the second leaf's goroutine is necessarily blocked on
// cfg.streamSem.Acquire. Cancelling ctx at that moment exercises BOTH early-
// return paths named in the task at once: the semaphore-acquire failure (only
// caught by Run's post-g.Wait() sweep) and the OpenExport failure (caught by
// downloadVolumeBinding's own relocated defer).
func TestPipeline_Progress_CancelDuringWait_DoesNotDeadlock(t *testing.T) {
	t.Parallel()

	const nVolumes = 2

	c := buildCapTestClient(t, nVolumes)
	outputDir := t.TempDir()
	sink := progress.New(&bytes.Buffer{}, true)

	arrived := make(chan struct{})

	var arrivedOnce sync.Once

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := pipeline.Config{
		Namespace:            capTestNS,
		RootSnapshot:         capTestRootSnap,
		OutputDir:            outputDir,
		Workers:              nVolumes,
		PerVolumeConcurrency: 1,
		MaxParallelDownloads: 1,
		KubeClient:           c,
		Progress:             sink,
		OpenExport: func(exportCtx context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			arrivedOnce.Do(func() { close(arrived) })

			<-exportCtx.Done()

			return nil, exportCtx.Err()
		},
	}

	runDone := make(chan error, 1)

	go func() {
		runDone <- runPipeline(ctx, cfg)
	}()

	// Wait until exactly one leaf is blocked inside OpenExport (holding the one
	// MaxParallelDownloads=1 slot); the other leaf is necessarily blocked on
	// cfg.streamSem.Acquire at this point.
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("timeout: no leaf reached OpenExport")
	}

	// Simulate a SIGINT: cancel the context both leaves are waiting on.
	cancel()

	var runErr error

	select {
	case runErr = <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("pipeline.Run did not return after ctx cancellation")
	}

	require.ErrorIs(t, runErr, context.Canceled,
		"a cancelled run must return ctx.Err(), not the per-node best-effort aggregate")

	// The critical regression assertion: sink.Wait() must return promptly. Before
	// the fix, the leaf blocked on streamSem.Acquire left its pre-created stream
	// permanently unsettled, and this call would hang forever.
	waitDone := make(chan struct{})

	go func() {
		sink.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("sink.Wait() deadlocked after a cancelled run — every pre-created stream must be terminally settled")
	}
}

// TestPipeline_BestEffort_OneNodeFailureDoesNotCancelSiblings is the regression
// test for the best-effort per-node download design: one node's permanent
// download failure must not cancel sibling nodes that are still downloading.
//
// Three independent leaf nodes (buildCapTestClient) start downloading
// concurrently (Workers == nVolumes, so every leaf's goroutine runs
// immediately). The failing leaf's OpenExport returns an error right away; the
// healthy leaves' OpenExport instead waits briefly while watching ctx — long
// enough that, under the OLD errgroup.WithContext(ctx) behavior (the first
// non-nil g.Go return cancels the shared derived context), a healthy leaf
// would observe ctx.Done() during that wait and fail too. Under the fixed
// best-effort behavior a per-node error never cancels the shared context, so
// the healthy leaves complete normally despite the sibling failure.
func TestPipeline_BestEffort_OneNodeFailureDoesNotCancelSiblings(t *testing.T) {
	t.Parallel()

	const (
		nVolumes = 3
		failIdx  = 1
	)

	failName := fmt.Sprintf("cap-disk-%d", failIdx)
	errPermanentFailure := errors.New("simulated permanent volume failure")

	c := buildCapTestClient(t, nVolumes)
	outputDir := t.TempDir()

	rawBlock := bytes.Repeat([]byte("Z"), 300)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	cfg := pipeline.Config{
		Namespace:            capTestNS,
		RootSnapshot:         capTestRootSnap,
		OutputDir:            outputDir,
		Workers:              nVolumes,
		PerVolumeConcurrency: 1,
		MaxParallelDownloads: nVolumes,
		KubeClient:           c,
		ManifestSource:       newManifestStub(),
		OpenExport: func(exportCtx context.Context, ns string, ref aggapi.NodeRef, _ string) (*exporter.Export, error) {
			if ref.Name == failName {
				return nil, fmt.Errorf("node %s: %w", ref.Name, errPermanentFailure)
			}

			// Give the failing leaf's goroutine time to return and, under the old
			// first-error-cancels-all behavior, cancel the shared context while
			// this healthy leaf is still "in flight".
			select {
			case <-time.After(150 * time.Millisecond):
			case <-exportCtx.Done():
				return nil, exportCtx.Err()
			}

			return exporter.NewExport(ns, "de-"+ref.Name, "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := pipeline.Run(context.Background(), cfg)
	require.Error(t, err, "expected an aggregated error naming the permanently failed node")
	require.ErrorIs(t, err, errPermanentFailure, "aggregated error must join the failed node's own error")
	require.Contains(t, err.Error(), failName, "aggregated error must identify the failed node")

	for i := 0; i < nVolumes; i++ {
		diskName := fmt.Sprintf("cap-disk-%d", i)
		nodeDir := filepath.Join(outputDir, archive.SnapshotsDirName, archive.NodeDirName(capTestKind, diskName))
		dataPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

		if i == failIdx {
			_, statErr := os.Stat(dataPath)
			require.True(t, os.IsNotExist(statErr), "the failed node %s must not have downloaded data", diskName)

			continue
		}

		assertNodeComplete(t, nodeDir)

		_, statErr := os.Stat(dataPath)
		require.NoError(t, statErr, "healthy node %s must have downloaded data despite the sibling failure", diskName)
	}
}

// ─── TestPipeline_MixedResumeStates_ConcurrentRun ────────────────────────────

// Namespace/name/geometry constants for the mixed-resume-states tree. Names
// carry a "mixed" prefix so they cannot be confused with any other test's
// fake-client fixtures in this package.
const (
	mixedNS       = "mixed-resume-ns"
	mixedRootSnap = "mixed-root"
	mixedVMSnap   = "mixed-vm-snap"

	mixedDiskDone          = "mixed-disk-done"
	mixedDiskBlockPartial  = "mixed-disk-block-partial"
	mixedDiskFSPartial     = "mixed-disk-fs-partial"
	mixedDiskManifestsOnly = "mixed-disk-manifests-only"
	mixedDiskPending       = "mixed-disk-pending"
)

// mixedChunkSize is the block/FS-file chunk size used throughout the mixed-
// resume-states test; 300-byte raw payloads split into exactly 3 chunks.
const mixedChunkSize int64 = 100

// mixedLeafNames lists every volume-leaf name in the mixed-resume tree, in
// the order the fake client wires them as mixed-vm-snap's children. Used to
// size the aggregate-counter assertion and to drive fixture construction.
var mixedLeafNames = []string{
	mixedDiskDone,
	mixedDiskBlockPartial,
	mixedDiskFSPartial,
	mixedDiskManifestsOnly,
	mixedDiskPending,
}

// stringRecorder is a small concurrency-safe log used by
// TestPipeline_MixedResumeStates_ConcurrentRun to record which leaf names or
// HTTP requests occurred during a given pipeline.Run call. reset() discards
// prior entries so run 1's activity cannot leak into the run-2-only
// assertions the test makes about resume behavior.
type stringRecorder struct {
	mu      sync.Mutex
	entries []string
}

func (r *stringRecorder) record(s string) {
	r.mu.Lock()
	r.entries = append(r.entries, s)
	r.mu.Unlock()
}

func (r *stringRecorder) reset() {
	r.mu.Lock()
	r.entries = nil
	r.mu.Unlock()
}

func (r *stringRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.entries...)
}

// makeTrackedBlockServer serves rawData at /api/v1/block like makeBlockServer,
// additionally recording every GET Range header into rec so a test can assert
// exactly which byte ranges were (or were not) re-fetched across a resume run.
func makeTrackedBlockServer(t *testing.T, rawData []byte, rec *stringRecorder) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			rec.record(r.Header.Get("Range"))
		}

		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(rawData))
	})

	srv := httptest.NewServer(mux)

	t.Cleanup(srv.Close)

	return srv
}

// makeTrackedFSServer serves a flat (no subdirectories) filesystem-volume
// listing of files at /api/v1/files/, recording every per-file GET into rec
// so a test can assert exactly which files were (or were not) re-fetched
// across a resume run. Modeled on makeE2EFSServer but flat and instrumented.
func makeTrackedFSServer(t *testing.T, files []fsE2EFile, rec *stringRecorder) *httptest.Server {
	t.Helper()

	fileMap := make(map[string][]byte, len(files))
	for _, f := range files {
		fileMap[f.rel] = f.content
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/files/" {
			items := make([]string, 0, len(files))

			for _, f := range files {
				items = append(items, fmt.Sprintf(
					`{"name":%q,"type":"file","uri":%q,"attributes":{"permissions":"0644","modtime":"2024-03-01T12:00:00Z","uid":0,"gid":0,"size":%d}}`,
					f.rel, f.rel, len(f.content)))
			}

			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+strings.Join(items, ",")+`]}`)

			return
		}

		name := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")

		content, ok := fileMap[name]
		if !ok {
			http.NotFound(w, r)

			return
		}

		rec.record(name)

		// The listing declares a "size" for every file, so each one downloads
		// via the durable chunked path (stageChunkedFile/DownloadBlockChunks),
		// which issues Range GETs — http.ServeContent (mirroring the real
		// data-exporter's sendFile idiom) is required to honor them.
		http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(content))
	})

	srv := httptest.NewServer(mux)

	t.Cleanup(srv.Close)

	return srv
}

// buildMixedResumeFakeClient constructs the fake kube client for the mixed-
// resume-states tree:
//
//	mixed-root (Snapshot)
//	  └─ mixed-vm-snap (VirtualMachineSnapshot, aggregator/intermediate node)
//	       ├─ mixed-disk-done            (VirtualDiskSnapshot, OwnDataRef → block)
//	       ├─ mixed-disk-block-partial   (VirtualDiskSnapshot, OwnDataRef → block)
//	       ├─ mixed-disk-fs-partial      (VirtualDiskSnapshot, OwnDataRef → fs)
//	       ├─ mixed-disk-manifests-only  (VirtualDiskSnapshot, OwnDataRef → block)
//	       └─ mixed-disk-pending         (VirtualDiskSnapshot, OwnDataRef → block)
//
// Every leaf is a non-aggregator with exactly one OwnDataRef, mirroring
// buildE2EFakeClient's disk-block/disk-fs leaves.
func buildMixedResumeFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := buildScheme(t)

	rootSnap := &snapshotapi.Snapshot{
		TypeMeta: metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "Snapshot"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      mixedRootSnap,
			Namespace: mixedNS,
		},
		Status: snapshotapi.SnapshotStatus{
			BoundSnapshotContentName: "sc-mixed-root",
			ChildrenSnapshotRefs: []snapshotapi.SnapshotChildRef{
				{APIVersion: e2eVMAPIVersion, Kind: e2eVMKind, Name: mixedVMSnap},
			},
		},
	}

	rootContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-mixed-root"},
	}

	vmChildren := make([]map[string]interface{}, 0, len(mixedLeafNames))
	for _, name := range mixedLeafNames {
		vmChildren = append(vmChildren, map[string]interface{}{
			"apiVersion": e2eVMAPIVersion, "kind": e2eDiskKind, "name": name,
		})
	}

	vmSnap := makeUnstructuredE2ENode(e2eVMAPIVersion, e2eVMKind, mixedNS, mixedVMSnap, "sc-mixed-vm", vmChildren)

	vmContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-mixed-vm"},
	}

	typed := []client.Object{rootSnap, rootContent, vmContent}
	unstructuredObjs := []client.Object{vmSnap}

	for _, name := range mixedLeafNames {
		contentName := "sc-mixed-" + name
		leafSnap := makeUnstructuredSnap(e2eVMAPIVersion, e2eDiskKind, mixedNS, name, contentName)

		leafContent := &snapshotapi.SnapshotContent{
			TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
			ObjectMeta: metav1.ObjectMeta{Name: contentName},
			Status: snapshotapi.SnapshotContentStatus{
				DataRef: &snapshotapi.SnapshotDataBinding{
					TargetUID: "uid-" + name,
					Target: snapshotapi.SnapshotSubjectRef{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Namespace:  mixedNS,
						Name:       "pvc-" + name,
					},
					Artifact: snapshotapi.SnapshotDataArtifactRef{
						APIVersion: "snapshot.storage.k8s.io/v1",
						Kind:       "VolumeSnapshotContent",
						Name:       "vsc-" + name,
					},
				},
			},
		}

		typed = append(typed, leafContent)
		unstructuredObjs = append(unstructuredObjs, leafSnap)
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(typed...).
		WithObjects(unstructuredObjs...).
		Build()
}

// TestPipeline_MixedResumeStates_ConcurrentRun exercises the concurrent
// collectNodeTasks/processNode resume path against a tree where sibling
// leaves sit in every NodeState simultaneously: Done, BlockPartial,
// FSPartial, ManifestsOnly, and Pending, all processed by ONE pipeline.Run
// with cfg.Workers=3. The existing single-state resume tests
// (TestPipeline_BlockResumeAfterMerge, TestPipeline_FSResumeAfterTar,
// TestPipeline_PartialChunkResume) each exercise exactly one resume state at
// a time with Workers=1; none combines mixed states across concurrently
// processed siblings, which is the gap this test closes.
//
// Fixture strategy: run the full tree ONCE to completion, so every leaf
// reaches NodeStateDone through the real download+finalize path (a genuinely
// valid checksum/snapshot.yaml, not a hand-rolled one). Then, mimicking a
// crash mid-run, four of the five leaves are rolled back to a specific
// partial/pending state by deleting their finished artifacts and — for the
// two partial cases — re-creating the exact staging layout a real
// interrupted download would have left (same technique as
// TestPipeline_PartialChunkResume for the block chunk dir); the fifth leaf
// (mixed-disk-done) is left untouched. A second pipeline.Run then resumes
// the whole tree concurrently, and the fake OpenExport plus the block/FS test
// servers are instrumented to prove each node resumed correctly rather than
// restarting from zero.
func TestPipeline_MixedResumeStates_ConcurrentRun(t *testing.T) {
	t.Parallel()

	// ── Fixture content ────────────────────────────────────────────────────
	rawBlockDone := bytes.Repeat([]byte("D"), 300)
	rawBlockPartial := bytes.Repeat([]byte("P"), 300) // 3 × mixedChunkSize
	rawBlockManifestsOnly := bytes.Repeat([]byte("M"), 300)
	rawBlockPending := bytes.Repeat([]byte("N"), 300)

	fsPartialStaged := fsE2EFile{rel: "one.txt", content: []byte("hello-one-content")}
	fsPartialMissing := fsE2EFile{rel: "two.txt", content: []byte("hello-two-content!!")}
	fsFiles := []fsE2EFile{fsPartialStaged, fsPartialMissing}

	codec, err := compress.New("zstd", 0)
	require.NoError(t, err)

	// ── Instrumentation. The two "tracked" recorders are reset right before
	// run 2 so only its requests are captured for the resume assertions. ────
	openExportCalls := &stringRecorder{}
	blockPartialRanges := &stringRecorder{}
	fsPartialRequests := &stringRecorder{}

	doneSrv := makeBlockServer(t, rawBlockDone)
	defer doneSrv.Close()

	blockPartialSrv := makeTrackedBlockServer(t, rawBlockPartial, blockPartialRanges)
	fsPartialSrv := makeTrackedFSServer(t, fsFiles, fsPartialRequests)

	manifestsOnlySrv := makeBlockServer(t, rawBlockManifestsOnly)
	defer manifestsOnlySrv.Close()

	pendingSrv := makeBlockServer(t, rawBlockPending)
	defer pendingSrv.Close()

	openExport := func(_ context.Context, namespace string, leafRef aggapi.NodeRef, _ string) (*exporter.Export, error) {
		openExportCalls.record(leafRef.Name)

		switch leafRef.Name {
		case mixedDiskDone:
			return exporter.NewExport(namespace, "de-mixed-done", "Block", doneSrv.URL, exporter.NewFetcher(doneSrv.Client())), nil
		case mixedDiskBlockPartial:
			return exporter.NewExport(namespace, "de-mixed-block-partial", "Block", blockPartialSrv.URL, exporter.NewFetcher(blockPartialSrv.Client())), nil
		case mixedDiskFSPartial:
			return exporter.NewExport(namespace, "de-mixed-fs-partial", "Filesystem", fsPartialSrv.URL, exporter.NewFetcher(fsPartialSrv.Client())), nil
		case mixedDiskManifestsOnly:
			return exporter.NewExport(namespace, "de-mixed-manifests-only", "Block", manifestsOnlySrv.URL, exporter.NewFetcher(manifestsOnlySrv.Client())), nil
		case mixedDiskPending:
			return exporter.NewExport(namespace, "de-mixed-pending", "Block", pendingSrv.URL, exporter.NewFetcher(pendingSrv.Client())), nil
		default:
			return nil, fmt.Errorf("mixed-resume: unexpected leaf %q", leafRef.Name)
		}
	}

	c := buildMixedResumeFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            mixedNS,
		RootSnapshot:         mixedRootSnap,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 2,
		ChunkSize:            mixedChunkSize,
		Compression:          codec,
		KubeClient:           c,
		OpenExport:           openExport,
	}

	// ── Run 1: complete the whole tree normally ─────────────────────────────
	require.NoError(t, runPipeline(context.Background(), cfg))

	vmDir := filepath.Join(outputDir, archive.SnapshotsDirName, archive.NodeDirName(e2eVMKind, mixedVMSnap))

	leafDir := func(name string) string {
		return filepath.Join(vmDir, archive.SnapshotsDirName, archive.NodeDirName(e2eDiskKind, name))
	}

	doneDir := leafDir(mixedDiskDone)
	blockPartialDir := leafDir(mixedDiskBlockPartial)
	fsPartialDir := leafDir(mixedDiskFSPartial)
	manifestsOnlyDir := leafDir(mixedDiskManifestsOnly)
	pendingDir := leafDir(mixedDiskPending)

	for _, d := range []string{doneDir, blockPartialDir, fsPartialDir, manifestsOnlyDir, pendingDir} {
		assertNodeComplete(t, d)
	}

	// ── Roll four of the five leaves back to distinct partial resume states,
	// simulating a crash mid-run. mixed-disk-done is left untouched. ────────

	// mixed-disk-block-partial: drop the merged block file and snapshot.yaml,
	// re-create data.bin.d/ with only chunk 0 present.
	require.NoError(t, os.Remove(filepath.Join(blockPartialDir, archive.DataBlockName(codec.Ext()))))
	require.NoError(t, os.Remove(filepath.Join(blockPartialDir, archive.SnapshotYAMLName)))

	blockChunkDir := filepath.Join(blockPartialDir, archive.BlockChunksDirName)
	require.NoError(t, os.MkdirAll(blockChunkDir, 0o755))

	chunk0Frame, err := codec.EncodeFrame(rawBlockPartial[:mixedChunkSize])
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(blockChunkDir, archive.ChunkFileName(0, codec.Ext())),
		chunk0Frame,
		0o644,
	))
	require.NoError(t, archive.WriteChunkMeta(blockChunkDir, archive.ChunkMeta{
		ChunkSize: mixedChunkSize,
		TotalSize: int64(len(rawBlockPartial)),
	}))

	// mixed-disk-fs-partial: drop data.tar and snapshot.yaml, re-create
	// data.tar.d/ with "one.txt.zst" already staged; "two.txt" is left
	// missing so only it must be re-fetched on resume.
	require.NoError(t, os.Remove(filepath.Join(fsPartialDir, archive.FsTarName)))
	require.NoError(t, os.Remove(filepath.Join(fsPartialDir, archive.SnapshotYAMLName)))

	fsStagingDir := filepath.Join(fsPartialDir, archive.FsTarStagingDirName)
	require.NoError(t, os.MkdirAll(fsStagingDir, 0o755))

	stagedFrame, err := codec.EncodeFrame(fsPartialStaged.content)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(fsStagingDir, fsPartialStaged.rel+codec.Ext()),
		stagedFrame,
		0o644,
	))

	// mixed-disk-manifests-only: drop the merged block file and
	// snapshot.yaml; manifests/ (created by run 1) is left in place with no
	// volume artifact and no staging dir of any kind.
	require.NoError(t, os.Remove(filepath.Join(manifestsOnlyDir, archive.DataBlockName(codec.Ext()))))
	require.NoError(t, os.Remove(filepath.Join(manifestsOnlyDir, archive.SnapshotYAMLName)))

	// mixed-disk-pending: remove the whole node directory so it starts from
	// nothing on the second run.
	require.NoError(t, os.RemoveAll(pendingDir))

	// Isolate run 2's instrumentation: everything captured so far belongs to
	// run 1's full download and must not pollute the resume assertions below.
	openExportCalls.reset()
	blockPartialRanges.reset()
	fsPartialRequests.reset()

	var buf bytes.Buffer

	sink := progress.New(&buf, false, progress.WithInterval(time.Hour))

	cfg.Workers = 3
	cfg.Progress = sink

	// ── Run 2: resume the whole tree concurrently from the mixed states ─────
	require.NoError(t, runPipeline(context.Background(), cfg))

	sink.Wait()

	// (a) Every node — root, the intermediate aggregator, and every leaf —
	// must be complete and pass VerifyNode; every leaf's decoded content must
	// match what its server actually holds, proving a correct download
	// occurred wherever one was needed.
	assertNodeComplete(t, outputDir)
	assertNodeComplete(t, vmDir)

	for _, d := range []string{doneDir, blockPartialDir, fsPartialDir, manifestsOnlyDir, pendingDir} {
		assertNodeComplete(t, d)
	}

	require.Equal(t, rawBlockDone, e2eDecodeZstdFile(t, filepath.Join(doneDir, archive.DataBlockName(codec.Ext()))),
		"mixed-disk-done data must be untouched by run 2")
	require.Equal(t, rawBlockPartial, e2eDecodeZstdFile(t, filepath.Join(blockPartialDir, archive.DataBlockName(codec.Ext()))),
		"mixed-disk-block-partial must decode to the original bytes after resume")
	require.Equal(t, rawBlockManifestsOnly, e2eDecodeZstdFile(t, filepath.Join(manifestsOnlyDir, archive.DataBlockName(codec.Ext()))),
		"mixed-disk-manifests-only must download correctly")
	require.Equal(t, rawBlockPending, e2eDecodeZstdFile(t, filepath.Join(pendingDir, archive.DataBlockName(codec.Ext()))),
		"mixed-disk-pending must download correctly from scratch")

	fsTarPath := filepath.Join(fsPartialDir, archive.FsTarName)
	for _, f := range fsFiles {
		compressed, tarErr := readTarEntry(t, fsTarPath, f.rel+codec.Ext())
		require.NoError(t, tarErr, "tar must have entry for %s", f.rel)
		require.Equal(t, f.content, e2eDecodeZstdBytes(t, compressed), "fs file %s content mismatch after resume", f.rel)
	}

	// (b) The already-Done leaf must never be handed to OpenExport again.
	calls := openExportCalls.snapshot()
	require.NotContains(t, calls, mixedDiskDone,
		"OpenExport must not be called for the already-complete leaf")

	for _, name := range []string{mixedDiskBlockPartial, mixedDiskFSPartial, mixedDiskManifestsOnly, mixedDiskPending} {
		require.Contains(t, calls, name,
			"OpenExport must be called for %s to fetch its missing data", name)
	}

	// (c) Partial nodes resumed from their pre-seeded progress instead of
	// restarting from zero.
	blockRanges := blockPartialRanges.snapshot()
	chunk0Range := fmt.Sprintf("bytes=0-%d", mixedChunkSize-1)

	require.NotContains(t, blockRanges, chunk0Range,
		"chunk 0 was pre-seeded and must not be re-fetched on resume")
	require.Contains(t, blockRanges, fmt.Sprintf("bytes=%d-%d", mixedChunkSize, 2*mixedChunkSize-1),
		"chunk 1 must be fetched on resume")
	require.Contains(t, blockRanges, fmt.Sprintf("bytes=%d-%d", 2*mixedChunkSize, 3*mixedChunkSize-1),
		"chunk 2 must be fetched on resume")

	fsRequests := fsPartialRequests.snapshot()
	require.NotContains(t, fsRequests, fsPartialStaged.rel,
		"the pre-staged fs file must not be re-fetched on resume")
	require.Contains(t, fsRequests, fsPartialMissing.rel,
		"the missing fs file must be fetched on resume")

	// (d) The aggregate volume counter equals the number of volume leaves in
	// the run (5), and by the end of run 2 every one of them has settled.
	require.Contains(t, buf.String(), fmt.Sprintf("(%d/%d volumes)", len(mixedLeafNames), len(mixedLeafNames)),
		"aggregate volume counter must reach N/M == total volume leaves")
}
