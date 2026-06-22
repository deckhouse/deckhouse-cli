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
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
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

	err = pipeline.Run(context.Background(), cfg)
	require.NoError(t, err)

	require.Equal(t, rootMod, statMtime(t, rootYAML),
		"root snapshot.yaml must not be rewritten on second run")
	require.Equal(t, diskSnapMod, statMtime(t, diskSnapYAML),
		"disk-snap snapshot.yaml must not be rewritten on second run")
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

// TestPipeline_BlockResumeAfterMerge verifies that when data.bin.zst already exists
// in a node directory (crash-after-merge-before-snapshot.yaml window), the pipeline
// skips shadow pair creation and DataExport entirely and only calls FinalizeNode.
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
		WaitShadowVS: noopWaitShadowVS,
		OpenExport: func(_ context.Context, _, _, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when data.bin.zst already exists")
			return nil, errors.New("unexpected OpenExport call")
		},
	}

	err := pipeline.Run(context.Background(), cfg)
	require.NoError(t, err)

	// FinalizeNode must have been called: disk-snap directory must now be complete.
	assertNodeComplete(t, diskSnapDir)
}

// TestPipeline_FSResumeAfterTar verifies that when data.tar already exists in a
// node directory (crash-after-tar-assembly-before-snapshot.yaml window), the
// pipeline skips shadow pair creation and DataExport entirely and only calls
// FinalizeNode.
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
		WaitShadowVS: noopWaitShadowVS,
		OpenExport: func(_ context.Context, _, _, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when data.tar already exists")

			return nil, errors.New("unexpected OpenExport call")
		},
	}

	err := pipeline.Run(context.Background(), cfg)
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
	// With the new tree model this DataRef materialises as a VolumeSnapshot child node.
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

// TestPipeline_ShadowMetaFromManifest verifies that when the source PVC is gone from
// the cluster, storageClass and volumeMode are resolved from the node's ManifestCheckpoint
// via shadowMetaFromCheckpoint. The on-disk manifest is intentionally absent (excluded by
// the OwnDataRef rule); only the checkpoint contains the captured PVC object.
func TestPipeline_ShadowMetaFromManifest(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("B"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	// Build a fake client WITHOUT the source PVC (simulating a deleted PVC).
	// sc-disk carries a ManifestCheckpointName so shadowMetaFromCheckpoint can look
	// up the PVC object from the checkpoint. The PVC is captured in the checkpoint
	// with storageClass="csi-ceph-rbd-from-checkpoint" and volumeMode="Block".
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

	// sc-disk sets ManifestCheckpointName so that the deleted-PVC fallback path
	// in resolveShadowMeta can reach the captured PVC object via FetchNodeManifests.
	childContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-disk"},
		Status: snapshotapi.SnapshotContentStatus{
			ManifestCheckpointName: "mcp-disk-mf",
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

	// The checkpoint captures the PVC with the storageClass and volumeMode that
	// shadowMetaFromCheckpoint should surface when the live PVC is absent.
	pvcPayload := `[{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"` +
		sourcePVCName + `","namespace":"` + testNS + `"}` +
		`,"spec":{"storageClassName":"csi-ceph-rbd-from-checkpoint","volumeMode":"Block"}}]`

	diskMCP := makeMFManifestCheckpoint("mcp-disk-mf")
	diskChunk := makeMFManifestChunk(t, "mcp-disk-mf-chunk-0", "mcp-disk-mf", 0, pvcPayload)

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rootSnap, rootContent, childContent, realVSC, diskMCP, diskChunk).
		WithObjects(childSnap).
		Build()

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

	assert.Equal(t, "csi-ceph-rbd-from-checkpoint", capturedMeta.StorageClass,
		"shadow VS must carry storage-class annotation resolved from ManifestCheckpoint")
	assert.Equal(t, "Block", capturedMeta.VolumeMode,
		"shadow VS must carry volume-mode annotation resolved from ManifestCheckpoint")
}

// makeMFManifestCheckpoint builds a ManifestCheckpoint for the deleted-PVC manifest test.
func makeMFManifestCheckpoint(name string) *snapshotapi.ManifestCheckpoint {
	return &snapshotapi.ManifestCheckpoint{
		TypeMeta:   metav1.TypeMeta{APIVersion: snapshotterAPIVersion, Kind: "ManifestCheckpoint"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       snapshotapi.ManifestCheckpointSpec{SourceNamespace: testNS},
		Status: snapshotapi.ManifestCheckpointStatus{
			TotalObjects: 1,
			Chunks:       []snapshotapi.ChunkInfo{{Index: 0, Name: name + "-chunk-0", ObjectsCount: 1}},
		},
	}
}

// makeMFManifestChunk encodes an arbitrary JSON array into a ManifestCheckpointContentChunk.
func makeMFManifestChunk(t *testing.T, name, checkpointName string, index int, jsonPayload string) *snapshotapi.ManifestCheckpointContentChunk {
	t.Helper()

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

// TestPipeline_ShadowReadinessTimeout verifies that when the shadow VS wait
// exceeds ShadowReadinessTimeout the pipeline returns an error and still
// cleans up the shadow VS and VSC via the cancel-proof cleanupCtx.
func TestPipeline_ShadowReadinessTimeout(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:              testNS,
		RootSnapshot:           rootSnapshot,
		OutputDir:              outputDir,
		Workers:                1,
		KubeClient:             c,
		ShadowReadinessTimeout: 10 * time.Millisecond,
		WaitShadowVS: func(ctx context.Context, _ client.Client, _ *slog.Logger, _, _, _ string) error {
			<-ctx.Done()

			return ctx.Err()
		},
		OpenExport: func(_ context.Context, _, _, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when shadow VS wait times out")

			return nil, errors.New("unexpected OpenExport call")
		},
	}

	err := pipeline.Run(context.Background(), cfg)
	require.Error(t, err, "expected pipeline to fail when shadow VS wait times out")

	// Shadow VS and VSC must have been cleaned up despite the timeout.
	pairName := exporter.ShadowName(diskVSCName)

	var shadowVS snapv1.VolumeSnapshot

	vsErr := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: pairName}, &shadowVS)
	assert.True(t, kubeerrors.IsNotFound(vsErr),
		"shadow VS must be deleted after timeout cleanup; got err=%v", vsErr)

	var shadowVSC snapv1.VolumeSnapshotContent

	vscErr := c.Get(context.Background(), types.NamespacedName{Name: pairName}, &shadowVSC)
	assert.True(t, kubeerrors.IsNotFound(vscErr),
		"shadow VSC must be deleted after timeout cleanup; got err=%v", vscErr)
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
		WaitShadowVS:         noopWaitShadowVS,
		SelectedNodeKind:     childKind,
		SelectedNodeName:     diskSnapName,
		OpenExport: func(_ context.Context, namespace, _ string, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-subtree", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, pipeline.Run(context.Background(), cfg))

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

	require.NoError(t, pipeline.Run(context.Background(), cfg))
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
		WaitShadowVS:         noopWaitShadowVS,
		SelectedNodeKind:     "Snapshot",
		SelectedNodeName:     rootSnapshot,
		OpenExport: func(_ context.Context, namespace, _ string, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-root-sel", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, pipeline.Run(context.Background(), cfg))

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
		WaitShadowVS:         noopWaitShadowVS,
		OpenExport: func(_ context.Context, namespace, _ string, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-none", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, pipeline.Run(context.Background(), cfg))

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
		WaitShadowVS:         noopWaitShadowVS,
		OpenExport: func(_ context.Context, namespace, _ string, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-partial-resume", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, pipeline.Run(context.Background(), cfg))

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
