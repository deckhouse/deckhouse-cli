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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vbauerster/mpb/v8/decor"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

// TestPipeline_OpenExportErrorReleasesCleanly verifies that when OpenExport fails
// the pipeline returns an error and no DataExport objects linger.
func TestPipeline_OpenExportErrorReleasesCleanly(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    outputDir,
		Workers:      1,
		KubeClient:   c,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return nil, errors.New("simulated DataExport creation failure")
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.Error(t, err, "expected pipeline to fail when OpenExport errors")
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

	// The non-TTY sink emits "downloaded X / total Y" using decor.SizeB1024 with
	// the "% .1f" verb — replicate the same format to pin the exact expected line.
	total := int64(len(rawBlock))
	want := fmt.Sprintf("downloaded % .1f / total % .1f\n",
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

// ── recording progress helpers ────────────────────────────────────────────────

// recordedStream is a progress.Stream stub that counts Activate and Done calls.
// All methods are safe for concurrent use.
type recordedStream struct {
	name        string
	mu          sync.Mutex
	activateCnt int
	doneCnt     int
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
	s.doneCnt++
	s.mu.Unlock()
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
}
