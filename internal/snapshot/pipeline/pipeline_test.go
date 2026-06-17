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
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
)

// noopWaitShadowVS is a WaitShadowVS stub for tests that do not exercise the
// shadow VS readiness wait (e.g. happy-path tests using a fake kube client
// where no snapshot-controller runs to set status).
func noopWaitShadowVS(_ context.Context, _ client.Client, _ *slog.Logger, _, _, _ string) error {
	return nil
}

const (
	testNS        = "test-ns"
	rootSnapshot  = "my-snap"
	diskSnapName  = "disk-snap"
	diskVSCName   = "vsc-disk"
	sourcePVCName = "pvc-disk-source"

	storageAPIVersion     = "storage.deckhouse.io/v1alpha1"
	childAPIVersion       = "demo.deckhouse.io/v1alpha1"
	childKind             = "VirtualDiskSnapshot"
	snapshotterAPIVersion = "state-snapshotter.deckhouse.io/v1alpha1"
)

// TestPipeline_HappyPath verifies the full download pipeline against a fake
// Kubernetes client and an httptest block-volume server.
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
		WaitShadowVS:         noopWaitShadowVS,
		OpenExport: func(_ context.Context, namespace, _ string, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := pipeline.Run(context.Background(), cfg)
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

	// Child block-volume node must be complete and data.img.zst must exist.
	childDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, childDir)

	_, err = os.Stat(filepath.Join(childDir, archive.DataBlockName))
	require.NoError(t, err, "child data.img.zst must exist")

	// Second run must be a no-op: snapshot.yaml mtime must not change.
	rootYAML := filepath.Join(outputDir, archive.SnapshotYAMLName)
	childYAML := filepath.Join(childDir, archive.SnapshotYAMLName)

	rootMod := statMtime(t, rootYAML)
	childMod := statMtime(t, childYAML)

	// Sleep briefly so that any writes would produce a different mtime.
	time.Sleep(20 * time.Millisecond)

	err = pipeline.Run(context.Background(), cfg)
	require.NoError(t, err)

	require.Equal(t, rootMod, statMtime(t, rootYAML), "root snapshot.yaml must not be rewritten on second run")
	require.Equal(t, childMod, statMtime(t, childYAML), "child snapshot.yaml must not be rewritten on second run")
}

// TestPipeline_CleanupAfterError verifies that shadow VS/VSC are deleted even when
// the parent context is cancelled (e.g. by errgroup on sibling error or SIGINT).
// The deferred cleanup must use a non-cancellable context so it runs after ctx.Done().
func TestPipeline_CleanupAfterError(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	// OpenExport always fails, simulating a cluster-level download error.
	// The errgroup cancels the shared gctx when this error propagates;
	// cleanup defers must still delete shadow objects despite the cancelled ctx.
	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    outputDir,
		Workers:      1,
		KubeClient:   c,
		WaitShadowVS: noopWaitShadowVS,
		OpenExport: func(_ context.Context, _, _, _ string) (*exporter.Export, error) {
			return nil, errors.New("simulated DataExport creation failure")
		},
	}

	err := pipeline.Run(context.Background(), cfg)
	require.Error(t, err, "expected pipeline to fail when OpenExport errors")

	// Shadow VS and VSC must have been cleaned up despite the failed run.
	pairName := exporter.ShadowName(diskVSCName)

	var shadowVS snapv1.VolumeSnapshot
	vsErr := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: pairName}, &shadowVS)
	assert.True(t, kubeerrors.IsNotFound(vsErr),
		"shadow VS must be deleted after error cleanup; got err=%v", vsErr)

	var shadowVSC snapv1.VolumeSnapshotContent
	vscErr := c.Get(context.Background(), types.NamespacedName{Name: pairName}, &shadowVSC)
	assert.True(t, kubeerrors.IsNotFound(vscErr),
		"shadow VSC must be deleted after error cleanup; got err=%v", vscErr)
}

// TestPipeline_BlockResumeAfterMerge verifies that when data.img.zst already exists
// in a node directory (crash-after-merge-before-snapshot.yaml window), the pipeline
// skips shadow pair creation and DataExport entirely and only calls FinalizeNode.
func TestPipeline_BlockResumeAfterMerge(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	// Pre-create the child node directory with data.img.zst but no snapshot.yaml,
	// simulating a crash after block chunks were merged but before FinalizeNode ran.
	childDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	require.NoError(t, os.MkdirAll(childDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(childDir, archive.DataBlockName),
		[]byte("pre-merged-block-data"),
		0o644,
	))

	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    outputDir,
		Workers:      1,
		KubeClient:   c,
		WaitShadowVS: noopWaitShadowVS,
		OpenExport: func(_ context.Context, _, _, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when data.img.zst already exists")
			return nil, errors.New("unexpected OpenExport call")
		},
	}

	err := pipeline.Run(context.Background(), cfg)
	require.NoError(t, err)

	// FinalizeNode must have been called: child directory must now be complete.
	assertNodeComplete(t, childDir)
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

	// Root SnapshotContent: has manifests, no volume.
	rootContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-root"},
		Status: snapshotapi.SnapshotContentStatus{
			ManifestCheckpointName: "mcp-root",
		},
	}

	// ManifestCheckpoint for the root.
	mcp := &snapshotapi.ManifestCheckpoint{
		TypeMeta:   metav1.TypeMeta{APIVersion: snapshotterAPIVersion, Kind: "ManifestCheckpoint"},
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-root"},
		Spec:       snapshotapi.ManifestCheckpointSpec{SourceNamespace: testNS},
		Status: snapshotapi.ManifestCheckpointStatus{
			TotalObjects: 1,
			Chunks:       []snapshotapi.ChunkInfo{{Index: 0, Name: "mcp-root-chunk-0", ObjectsCount: 1}},
		},
	}

	// ManifestCheckpointContentChunk: one ConfigMap.
	mcpChunk := makeManifestChunk(t, "mcp-root-chunk-0", "mcp-root", 0)

	// Child snapshot (unstructured — domain-specific kind not in the scheme).
	childSnap := makeUnstructuredSnap(childAPIVersion, childKind, testNS, diskSnapName, "sc-disk")

	// Child SnapshotContent: one block DataRef pointing at the source PVC, no manifests.
	childContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-disk"},
		Status: snapshotapi.SnapshotContentStatus{
			DataRefs: []snapshotapi.SnapshotDataBinding{
				{
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
						Name:       diskVSCName,
					},
				},
			},
		},
	}

	// Real VolumeSnapshotContent — needed by EnsureShadowPair.
	snapshotHandle := "snap-handle-1"
	realVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: diskVSCName},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "test.driver",
			Source: snapv1.VolumeSnapshotContentSource{
				SnapshotHandle: &snapshotHandle,
			},
			VolumeSnapshotRef: corev1.ObjectReference{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       "placeholder",
				Namespace:  "default",
			},
		},
	}

	// Source PVC — needed by resolveShadowMeta.
	fsMode := corev1.PersistentVolumeFilesystem
	storageClass := "csi-ceph-rbd"
	sourcePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: sourcePVCName, Namespace: testNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClass,
			VolumeMode:       &fsMode,
		},
	}

	typed := []client.Object{rootSnap, rootContent, mcp, mcpChunk, childContent, realVSC, sourcePVC}

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
	require.NoError(t, snapv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return scheme
}

// makeManifestChunk creates a ManifestCheckpointContentChunk with one ConfigMap encoded
// as base64(gzip(json[])).
func makeManifestChunk(t *testing.T, name, checkpointName string, index int) *snapshotapi.ManifestCheckpointContentChunk {
	t.Helper()

	const jsonPayload = `[{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test-cfg","namespace":"test-ns"}}]`

	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)
	_, err := gz.Write([]byte(jsonPayload))
	require.NoError(t, err)
	require.NoError(t, gz.Close())

	encoded := base64.StdEncoding.EncodeToString(buf.Bytes())

	return &snapshotapi.ManifestCheckpointContentChunk{
		TypeMeta:   metav1.TypeMeta{APIVersion: snapshotterAPIVersion, Kind: "ManifestCheckpointContentChunk"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: snapshotapi.ManifestCheckpointContentChunkSpec{
			CheckpointName: checkpointName,
			Index:          index,
			Data:           encoded,
			ObjectsCount:   1,
		},
	}
}

// TestPipeline_ShadowMetaFromLivePVC verifies that downloadVolume injects
// storageClass and volumeMode annotations on the shadow VS when the source PVC
// is present live in the cluster.
func TestPipeline_ShadowMetaFromLivePVC(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("B"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	var capturedMeta exporter.ShadowMeta

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		WaitShadowVS:         noopWaitShadowVS,
		OpenExport: func(ctx context.Context, namespace, vsName string, ttl string) (*exporter.Export, error) {
			// Inspect the shadow VS that was created before OpenExport is called.
			var shadowVS snapv1.VolumeSnapshot
			if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: vsName}, &shadowVS); err == nil {
				capturedMeta = exporter.ShadowMeta{
					StorageClass: shadowVS.Annotations[exporter.AnnotationStorageClassName],
					VolumeMode:   shadowVS.Annotations[exporter.AnnotationVolumeMode],
				}
			}

			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := pipeline.Run(context.Background(), cfg)
	require.NoError(t, err)

	assert.Equal(t, "csi-ceph-rbd", capturedMeta.StorageClass,
		"shadow VS must carry storage-class annotation from source PVC")
	assert.Equal(t, "Filesystem", capturedMeta.VolumeMode,
		"shadow VS must carry volume-mode annotation from source PVC")
}

// TestPipeline_ShadowMetaFromManifest verifies that when the source PVC is gone
// from the cluster, storageClass and volumeMode are read from the captured PVC
// manifest already on disk under <nodeDir>/manifests/.
func TestPipeline_ShadowMetaFromManifest(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("B"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	// Build a fake client WITHOUT the source PVC (simulating a deleted PVC).
	scheme := buildScheme(t)
	snapshotHandle := "snap-handle-mf"
	realVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: diskVSCName},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "test.driver",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &snapshotHandle},
			VolumeSnapshotRef: corev1.ObjectReference{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       "placeholder",
				Namespace:  "default",
			},
		},
	}

	rootSnap := &snapshotapi.Snapshot{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "Snapshot"},
		ObjectMeta: metav1.ObjectMeta{Name: rootSnapshot, Namespace: testNS},
		Status: snapshotapi.SnapshotStatus{
			BoundSnapshotContentName: "sc-root-mf",
			ChildrenSnapshotRefs: []snapshotapi.SnapshotChildRef{
				{APIVersion: childAPIVersion, Kind: childKind, Name: diskSnapName},
			},
		},
	}
	rootContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-root-mf"},
	}
	childContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-disk"},
		Status: snapshotapi.SnapshotContentStatus{
			DataRefs: []snapshotapi.SnapshotDataBinding{
				{
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
						Name:       diskVSCName,
					},
				},
			},
		},
	}
	childSnap := makeUnstructuredSnap(childAPIVersion, childKind, testNS, diskSnapName, "sc-disk")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rootSnap, rootContent, childContent, realVSC).
		WithObjects(childSnap).
		Build()

	outputDir := t.TempDir()

	// Pre-create the child node manifests/ directory with a PVC manifest so that
	// resolveShadowMeta can fall back to it when the live PVC is absent.
	// processNode will call ensureNodeSubdirs (idempotent) and then WriteNodeManifests
	// (no-op: ManifestCheckpointName is empty), leaving the pre-written file intact.
	childDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	manifDir := filepath.Join(childDir, archive.ManifestsDirName)
	require.NoError(t, os.MkdirAll(manifDir, 0o755))

	pvcManifest := `apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ` + sourcePVCName + `
  namespace: ` + testNS + `
spec:
  storageClassName: csi-ceph-rbd-from-manifest
  volumeMode: Block
`
	manifestPath := filepath.Join(manifDir,
		archive.ManifestFileName("PersistentVolumeClaim", sourcePVCName, ""))
	require.NoError(t, os.WriteFile(manifestPath, []byte(pvcManifest), 0o644))

	var capturedMeta exporter.ShadowMeta

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		WaitShadowVS:         noopWaitShadowVS,
		OpenExport: func(ctx context.Context, namespace, vsName string, ttl string) (*exporter.Export, error) {
			var shadowVS snapv1.VolumeSnapshot
			if err2 := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: vsName}, &shadowVS); err2 == nil {
				capturedMeta = exporter.ShadowMeta{
					StorageClass: shadowVS.Annotations[exporter.AnnotationStorageClassName],
					VolumeMode:   shadowVS.Annotations[exporter.AnnotationVolumeMode],
				}
			}

			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := pipeline.Run(context.Background(), cfg)
	require.NoError(t, err)

	assert.Equal(t, "csi-ceph-rbd-from-manifest", capturedMeta.StorageClass,
		"shadow VS must carry storage-class annotation from PVC manifest")
	assert.Equal(t, "Block", capturedMeta.VolumeMode,
		"shadow VS must carry volume-mode annotation from PVC manifest")
}

// TestPipeline_WaitShadowVSCalledBeforeExport verifies that WaitShadowVS is
// invoked after EnsureShadowPair but before OpenExport so that the DataExport
// is only created once the shadow VS has a non-nil restoreSize.
func TestPipeline_WaitShadowVSCalledBeforeExport(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("B"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	var mu sync.Mutex
	var callOrder []string

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		WaitShadowVS: func(_ context.Context, _ client.Client, _ *slog.Logger, _, _, _ string) error {
			mu.Lock()
			callOrder = append(callOrder, "WaitShadowVS")
			mu.Unlock()

			return nil
		},
		OpenExport: func(_ context.Context, namespace, _ string, _ string) (*exporter.Export, error) {
			mu.Lock()
			callOrder = append(callOrder, "OpenExport")
			mu.Unlock()

			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := pipeline.Run(context.Background(), cfg)
	require.NoError(t, err)

	// WaitShadowVS must appear before OpenExport in the call log.
	require.Len(t, callOrder, 2, "expected exactly WaitShadowVS then OpenExport")
	assert.Equal(t, "WaitShadowVS", callOrder[0], "WaitShadowVS must be called first")
	assert.Equal(t, "OpenExport", callOrder[1], "OpenExport must be called after WaitShadowVS")
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
