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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
)

// Constants for the e2e test tree.
const (
	e2eNS        = "e2e-ns"
	e2eRootSnap  = "e2e-snap"
	e2eVMSnap    = "vm-snap"
	e2eBlockDisk = "disk-block"
	e2eFSDisk    = "disk-fs"

	e2eBlockVSC = "vsc-e2e-block"
	e2eFSVSC    = "vsc-e2e-fs"

	e2eVMAPIVersion = "demo.deckhouse.io/v1alpha1"
	e2eVMKind       = "VirtualMachineSnapshot"
	e2eDiskKind     = "VirtualDiskSnapshot"
	e2eRootCMName   = "root-cm"
	e2eVMCMName     = "vm-cm"

	// rawBlockSize must be > DefaultChunkSize/2 to exercise multi-chunk download.
	// Keep small for unit-test speed.
	e2eBlockSize = 300

	// Multi-volume test constants.
	e2eMultiRootSnap = "e2e-multi-root"
	e2eMultiDisk     = "multi-disk"
	e2eMultiBlockVSC = "vsc-multi-block"
	e2eMultiFSVSC    = "vsc-multi-fs"
	e2eMultiBlockPVC = "pvc-multi-block"
	e2eMultiFSPVC    = "pvc-multi-fs"

	// Deleted-PVC test constants.
	e2eDelRootSnap = "e2e-del-root"
	e2eDelDisk     = "del-disk"
	e2eDelVSC      = "vsc-del-block"
	e2eDelPVC      = "del-pvc"
)

// TestPipeline_E2E_FullTree runs the full pipeline against a fake kube client and
// httptest servers. The tree is:
//
//	root (manifests: root-cm)
//	  └─ vm-snap (VirtualMachineSnapshot, manifests: vm-cm)
//	       ├─ disk-block (VirtualDiskSnapshot, snapshot node)
//	       │    └─ volumesnapshot_<shadow-block>/ (volume node, block data)
//	       └─ disk-fs   (VirtualDiskSnapshot, snapshot node)
//	            └─ volumesnapshot_<shadow-fs>/ (volume node, filesystem data)
//
// After the first run all nodes are complete. A second run (resume) must be a no-op.
func TestPipeline_E2E_FullTree(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("Z"), e2eBlockSize)
	fsFiles := []fsE2EFile{
		{rel: "alpha.txt", content: []byte("hello-alpha")},
		{rel: "subdir/beta.txt", content: []byte("hello-beta")},
	}

	blockSrv := makeE2EBlockServer(t, rawBlock)
	fsSrv := makeE2EFSServer(t, fsFiles)

	c := buildE2EFakeClient(t)
	outputDir := t.TempDir()

	blockShadow := exporter.ShadowName(e2eBlockVSC)
	fsShadow := exporter.ShadowName(e2eFSVSC)

	cfg := pipeline.Config{
		Namespace:            e2eNS,
		RootSnapshot:         e2eRootSnap,
		OutputDir:            outputDir,
		Workers:              2,
		PerVolumeConcurrency: 2,
		KubeClient:           c,
		WaitShadowVS:         noopWaitShadowVS,
		OpenExport: func(_ context.Context, namespace, shadowVSName, _ string) (*exporter.Export, error) {
			switch shadowVSName {
			case blockShadow:
				return exporter.NewExport(namespace, "de-block", "Block", blockSrv.URL, exporter.NewFetcher(blockSrv.Client())), nil
			case fsShadow:
				return exporter.NewExport(namespace, "de-fs", "Filesystem", fsSrv.URL, exporter.NewFetcher(fsSrv.Client())), nil
			default:
				return nil, fmt.Errorf("e2e: unknown shadow VS %q", shadowVSName)
			}
		},
	}

	require.NoError(t, pipeline.Run(context.Background(), cfg))

	// ── Root node assertions ──────────────────────────────────────────────────
	assertE2ENodeComplete(t, outputDir)

	// Root must have a manifests/ entry.
	rootManifests, err := os.ReadDir(filepath.Join(outputDir, archive.ManifestsDirName))
	require.NoError(t, err)
	require.NotEmpty(t, rootManifests, "root manifests/ must not be empty")

	// Root must have snapshots/ because it has a vm-snap child.
	_, err = os.Stat(filepath.Join(outputDir, archive.SnapshotsDirName))
	require.NoError(t, err, "root snapshots/ must exist")

	// ── vm-snap node assertions ───────────────────────────────────────────────
	vmDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eVMKind, e2eVMSnap))
	assertE2ENodeComplete(t, vmDir)

	vmManifests, err := os.ReadDir(filepath.Join(vmDir, archive.ManifestsDirName))
	require.NoError(t, err)
	require.NotEmpty(t, vmManifests, "vm-snap manifests/ must not be empty")

	// vm-snap must have snapshots/ for its disk children.
	_, err = os.Stat(filepath.Join(vmDir, archive.SnapshotsDirName))
	require.NoError(t, err, "vm-snap snapshots/ must exist")

	// ── disk-block node assertions ────────────────────────────────────────────
	// disk-block is now a snapshot node: no data.img.zst directly; the block
	// data lives inside its VolumeSnapshot child.
	blockDir := filepath.Join(vmDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eBlockDisk))
	assertE2ENodeComplete(t, blockDir)

	_, noBlockPayload := os.Stat(filepath.Join(blockDir, archive.DataBlockName))
	require.True(t, os.IsNotExist(noBlockPayload),
		"disk-block snapshot node must not carry data.img.zst directly")

	// disk-block must have a snapshots/ dir for its VolumeSnapshot child.
	_, err = os.Stat(filepath.Join(blockDir, archive.SnapshotsDirName))
	require.NoError(t, err, "disk-block snapshots/ must exist")

	blockVolumeDir := filepath.Join(blockDir, archive.SnapshotsDirName,
		archive.NodeDirName("VolumeSnapshot", blockShadow))
	assertE2ENodeComplete(t, blockVolumeDir)

	blockFile := filepath.Join(blockVolumeDir, archive.DataBlockName)
	_, err = os.Stat(blockFile)
	require.NoError(t, err, "disk-block volume node data.img.zst must exist")

	// Verify decoded block data equals the original.
	require.Equal(t, rawBlock, e2eDecodeZstdFile(t, blockFile))

	// ── disk-fs node assertions ───────────────────────────────────────────────
	// disk-fs is now a snapshot node: no data/ directly; the filesystem data
	// lives inside its VolumeSnapshot child.
	fsDir := filepath.Join(vmDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eFSDisk))
	assertE2ENodeComplete(t, fsDir)

	_, noFSPayload := os.Stat(filepath.Join(fsDir, archive.DataDirName))
	require.True(t, os.IsNotExist(noFSPayload),
		"disk-fs snapshot node must not carry data/ directly")

	// disk-fs must have a snapshots/ dir for its VolumeSnapshot child.
	_, err = os.Stat(filepath.Join(fsDir, archive.SnapshotsDirName))
	require.NoError(t, err, "disk-fs snapshots/ must exist")

	fsVolumeDir := filepath.Join(fsDir, archive.SnapshotsDirName,
		archive.NodeDirName("VolumeSnapshot", fsShadow))
	assertE2ENodeComplete(t, fsVolumeDir)

	dataDir := filepath.Join(fsVolumeDir, archive.DataDirName)
	_, err = os.Stat(dataDir)
	require.NoError(t, err, "disk-fs volume node data/ must exist")

	for _, f := range fsFiles {
		zstPath := filepath.Join(dataDir, f.rel+".zst")
		require.Equal(t, f.content, e2eDecodeZstdFile(t, zstPath),
			"disk-fs file %s content mismatch", f.rel)
	}

	// ── Resume: second run must be a no-op ────────────────────────────────────
	mtimes := e2eCollectMtimes(t, outputDir)
	time.Sleep(20 * time.Millisecond)

	require.NoError(t, pipeline.Run(context.Background(), cfg))

	for path, before := range mtimes {
		after := statMtime(t, path)
		require.Equal(t, before, after, "snapshot.yaml mtime changed on resume: %s", path)
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// fsE2EFile is one file in the fake filesystem volume.
type fsE2EFile struct {
	rel     string
	content []byte
}

// assertE2ENodeComplete checks that snapshot.yaml exists and passes VerifyNode.
func assertE2ENodeComplete(t *testing.T, dir string) {
	t.Helper()

	_, err := os.Stat(filepath.Join(dir, archive.SnapshotYAMLName))
	require.NoError(t, err, "snapshot.yaml must exist in %s", dir)
	require.NoError(t, archive.VerifyNode(dir), "VerifyNode must pass for %s", dir)
}

// e2eDecodeZstdFile reads a .zst file and returns its decompressed bytes.
func e2eDecodeZstdFile(t *testing.T, path string) []byte {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err, "read zst file %s", path)

	dec, err := zstd.NewReader(bytes.NewReader(raw))
	require.NoError(t, err, "zstd.NewReader for %s", path)

	defer dec.Close()

	out, err := io.ReadAll(dec)
	require.NoError(t, err, "decode zstd for %s", path)

	return out
}

// e2eCollectMtimes walks the output dir and collects the mtime of every
// snapshot.yaml file for the resume assertion.
func e2eCollectMtimes(t *testing.T, root string) map[string]time.Time {
	t.Helper()

	result := make(map[string]time.Time)

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.Name() == archive.SnapshotYAMLName {
			fi, statErr := os.Stat(path)
			if statErr != nil {
				return statErr
			}

			result[path] = fi.ModTime()
		}

		return nil
	})
	require.NoError(t, err, "WalkDir for mtime collection")
	require.NotEmpty(t, result, "no snapshot.yaml files found (tree incomplete?)")

	return result
}

// makeE2EBlockServer creates a test server serving rawData at /api/v1/block with
// HEAD and Range GET support.
func makeE2EBlockServer(t *testing.T, rawData []byte) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(rawData))
	})

	srv := httptest.NewServer(mux)

	t.Cleanup(srv.Close)

	return srv
}

// makeE2EFSServer creates a test server that exposes two files under /api/v1/files/:
//
//	alpha.txt          → "hello-alpha"
//	subdir/beta.txt    → "hello-beta"
//
// Directory listings are returned as the JSON format expected by ListDir.
func makeE2EFSServer(t *testing.T, files []fsE2EFile) *httptest.Server {
	t.Helper()

	fileMap := make(map[string][]byte, len(files))
	for _, f := range files {
		fileMap[f.rel] = f.content
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w,
				`{"apiVersion":"v1","items":[`+
					`{"name":"alpha.txt","type":"file","uri":"alpha.txt","attributes":{}},`+
					`{"name":"subdir","type":"dir","uri":"subdir/","attributes":{}}`+
					`]}`)

		case "/api/v1/files/subdir/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w,
				`{"apiVersion":"v1","items":[`+
					`{"name":"beta.txt","type":"file","uri":"subdir/beta.txt","attributes":{}}`+
					`]}`)

		case "/api/v1/files/alpha.txt":
			_, _ = w.Write(fileMap["alpha.txt"])

		case "/api/v1/files/subdir/beta.txt":
			_, _ = w.Write(fileMap["subdir/beta.txt"])

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)

	t.Cleanup(srv.Close)

	return srv
}

// buildE2EFakeClient constructs the fake kube client pre-seeded with all objects
// for the full-tree e2e test.
func buildE2EFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := buildScheme(t)

	// ── Root typed Snapshot ───────────────────────────────────────────────────
	rootSnap := &snapshotapi.Snapshot{
		TypeMeta: metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "Snapshot"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      e2eRootSnap,
			Namespace: e2eNS,
		},
		Status: snapshotapi.SnapshotStatus{
			BoundSnapshotContentName: "sc-e2e-root",
			ChildrenSnapshotRefs: []snapshotapi.SnapshotChildRef{
				{APIVersion: e2eVMAPIVersion, Kind: e2eVMKind, Name: e2eVMSnap},
			},
		},
	}

	// Root SnapshotContent: manifests, no volume.
	rootContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-e2e-root"},
		Status: snapshotapi.SnapshotContentStatus{
			ManifestCheckpointName: "mcp-e2e-root",
		},
	}

	// ManifestCheckpoint for root.
	rootMCP := &snapshotapi.ManifestCheckpoint{
		TypeMeta:   metav1.TypeMeta{APIVersion: snapshotterAPIVersion, Kind: "ManifestCheckpoint"},
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-e2e-root"},
		Spec:       snapshotapi.ManifestCheckpointSpec{SourceNamespace: e2eNS},
		Status: snapshotapi.ManifestCheckpointStatus{
			TotalObjects: 1,
			Chunks:       []snapshotapi.ChunkInfo{{Index: 0, Name: "chunk-e2e-root-0", ObjectsCount: 1}},
		},
	}

	rootChunk := makeE2EManifestChunk(t, "chunk-e2e-root-0", "mcp-e2e-root", 0, e2eRootCMName)

	// ── vm-snap unstructured ──────────────────────────────────────────────────
	vmSnap := makeUnstructuredE2ENode(e2eVMAPIVersion, e2eVMKind, e2eNS, e2eVMSnap, "sc-e2e-vm",
		[]map[string]interface{}{
			{"apiVersion": e2eVMAPIVersion, "kind": e2eDiskKind, "name": e2eBlockDisk},
			{"apiVersion": e2eVMAPIVersion, "kind": e2eDiskKind, "name": e2eFSDisk},
		})

	vmContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-e2e-vm"},
		Status: snapshotapi.SnapshotContentStatus{
			ManifestCheckpointName: "mcp-e2e-vm",
		},
	}

	vmMCP := &snapshotapi.ManifestCheckpoint{
		TypeMeta:   metav1.TypeMeta{APIVersion: snapshotterAPIVersion, Kind: "ManifestCheckpoint"},
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-e2e-vm"},
		Spec:       snapshotapi.ManifestCheckpointSpec{SourceNamespace: e2eNS},
		Status: snapshotapi.ManifestCheckpointStatus{
			TotalObjects: 1,
			Chunks:       []snapshotapi.ChunkInfo{{Index: 0, Name: "chunk-e2e-vm-0", ObjectsCount: 1}},
		},
	}

	vmChunk := makeE2EManifestChunk(t, "chunk-e2e-vm-0", "mcp-e2e-vm", 0, e2eVMCMName)

	// ── disk-block unstructured ───────────────────────────────────────────────
	blockSnap := makeUnstructuredE2ENode(e2eVMAPIVersion, e2eDiskKind, e2eNS, e2eBlockDisk, "sc-e2e-block", nil)

	blockContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-e2e-block"},
		Status: snapshotapi.SnapshotContentStatus{
			DataRefs: []snapshotapi.SnapshotDataBinding{
				{
					TargetUID: "uid-block",
					Target: snapshotapi.SnapshotSubjectRef{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Namespace:  e2eNS,
						Name:       "pvc-block-source",
					},
					Artifact: snapshotapi.SnapshotDataArtifactRef{
						APIVersion: "snapshot.storage.k8s.io/v1",
						Kind:       "VolumeSnapshotContent",
						Name:       e2eBlockVSC,
					},
				},
			},
		},
	}

	// ── disk-fs unstructured ──────────────────────────────────────────────────
	fsSnap := makeUnstructuredE2ENode(e2eVMAPIVersion, e2eDiskKind, e2eNS, e2eFSDisk, "sc-e2e-fs", nil)

	fsContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-e2e-fs"},
		Status: snapshotapi.SnapshotContentStatus{
			DataRefs: []snapshotapi.SnapshotDataBinding{
				{
					TargetUID: "uid-fs",
					Target: snapshotapi.SnapshotSubjectRef{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Namespace:  e2eNS,
						Name:       "pvc-fs-source",
					},
					Artifact: snapshotapi.SnapshotDataArtifactRef{
						APIVersion: "snapshot.storage.k8s.io/v1",
						Kind:       "VolumeSnapshotContent",
						Name:       e2eFSVSC,
					},
				},
			},
		},
	}

	// ── Real VolumeSnapshotContents (needed by EnsureShadowPair) ─────────────
	blockHandle := "handle-block-1"
	realBlockVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: e2eBlockVSC},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "test.driver",
			Source: snapv1.VolumeSnapshotContentSource{
				SnapshotHandle: &blockHandle,
			},
			VolumeSnapshotRef: corev1.ObjectReference{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       "placeholder-block",
				Namespace:  "default",
			},
		},
	}

	fsHandle := "handle-fs-1"
	realFSVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: e2eFSVSC},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "test.driver",
			Source: snapv1.VolumeSnapshotContentSource{
				SnapshotHandle: &fsHandle,
			},
			VolumeSnapshotRef: corev1.ObjectReference{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       "placeholder-fs",
				Namespace:  "default",
			},
		},
	}

	// ── Source PVCs (needed by resolveShadowMeta) ────────────────────────────
	blockVolumeMode := corev1.PersistentVolumeBlock
	blockStorageClass := "csi-e2e-block-sc"
	sourcePVCBlock := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-block-source", Namespace: e2eNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &blockStorageClass,
			VolumeMode:       &blockVolumeMode,
		},
	}

	fsVolumeMode := corev1.PersistentVolumeFilesystem
	fsStorageClass := "csi-e2e-fs-sc"
	sourcePVCFS := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-fs-source", Namespace: e2eNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &fsStorageClass,
			VolumeMode:       &fsVolumeMode,
		},
	}

	typed := []client.Object{
		rootSnap, rootContent, rootMCP, rootChunk,
		vmContent, vmMCP, vmChunk,
		blockContent,
		fsContent,
		realBlockVSC, realFSVSC,
		sourcePVCBlock, sourcePVCFS,
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(typed...).
		WithObjects(vmSnap, blockSnap, fsSnap).
		Build()
}

// makeE2EManifestChunk encodes a single ConfigMap named cmName into a
// ManifestCheckpointContentChunk as base64(gzip(json[])).
func makeE2EManifestChunk(t *testing.T, name, checkpointName string, index int, cmName string) *snapshotapi.ManifestCheckpointContentChunk {
	t.Helper()

	payload := fmt.Sprintf(
		`[{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":%q,"namespace":%q}}]`,
		cmName, e2eNS,
	)

	var buf bytes.Buffer

	gz := gzip.NewWriter(&buf)
	_, err := gz.Write([]byte(payload))
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

// makeUnstructuredE2ENode builds an unstructured snapshot object (for domain-specific
// kinds not registered in the scheme) with the given boundSnapshotContentName and an
// optional childrenSnapshotRefs slice.
func makeUnstructuredE2ENode(
	apiVersion, kind, namespace, name, contentName string,
	children []map[string]interface{},
) *unstructured.Unstructured {
	status := map[string]interface{}{
		"boundSnapshotContentName": contentName,
	}

	if len(children) > 0 {
		rawChildren := make([]interface{}, len(children))
		for i, c := range children {
			rawChildren[i] = c
		}

		status["childrenSnapshotRefs"] = rawChildren
	}

	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"status": status,
		},
	}
}

// makeE2EManifestChunkWithPayload encodes an arbitrary JSON array payload into a
// ManifestCheckpointContentChunk as base64(gzip(payload)).
func makeE2EManifestChunkWithPayload(t *testing.T, name, checkpointName string, index int, jsonPayload string) *snapshotapi.ManifestCheckpointContentChunk {
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

// TestPipeline_E2E_MultiVolume verifies that a single snapshot node with TWO dataRefs
// (one Block and one Filesystem) produces two separate VolumeSnapshot child nodes,
// each with the correct data payload and the captured PVC manifest, while the parent
// snapshot node's manifests/ excludes both DataRef PVCs.
//
// Tree:
//
//	e2e-multi-root (Snapshot, no manifests)
//	  └─ multi-disk (VirtualDiskSnapshot, manifests: multi-cfg + block-pvc + fs-pvc)
//	       ├─ volumesnapshot_<block-shadow>/ (volume node, block data)
//	       └─ volumesnapshot_<fs-shadow>/   (volume node, filesystem data)
func TestPipeline_E2E_MultiVolume(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("M"), e2eBlockSize)
	// Use the same file names that makeE2EFSServer hardcodes in its directory listing.
	fsFiles := []fsE2EFile{
		{rel: "alpha.txt", content: []byte("multi-alpha")},
		{rel: "subdir/beta.txt", content: []byte("multi-beta")},
	}

	blockSrv := makeE2EBlockServer(t, rawBlock)
	fsSrv := makeE2EFSServer(t, fsFiles)

	c := buildMultiVolumeFakeClient(t)
	outputDir := t.TempDir()

	blockShadow := exporter.ShadowName(e2eMultiBlockVSC)
	fsShadow := exporter.ShadowName(e2eMultiFSVSC)

	cfg := pipeline.Config{
		Namespace:            e2eNS,
		RootSnapshot:         e2eMultiRootSnap,
		OutputDir:            outputDir,
		Workers:              2,
		PerVolumeConcurrency: 2,
		KubeClient:           c,
		WaitShadowVS:         noopWaitShadowVS,
		OpenExport: func(_ context.Context, namespace, shadowVSName, _ string) (*exporter.Export, error) {
			switch shadowVSName {
			case blockShadow:
				return exporter.NewExport(namespace, "de-multi-block", "Block", blockSrv.URL, exporter.NewFetcher(blockSrv.Client())), nil
			case fsShadow:
				return exporter.NewExport(namespace, "de-multi-fs", "Filesystem", fsSrv.URL, exporter.NewFetcher(fsSrv.Client())), nil
			default:
				return nil, fmt.Errorf("e2e-multi: unknown shadow VS %q", shadowVSName)
			}
		},
	}

	require.NoError(t, pipeline.Run(context.Background(), cfg))

	// multi-disk snapshot node.
	multiDiskDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eMultiDisk))
	assertE2ENodeComplete(t, multiDiskDir)

	// multi-disk must have snapshots/ for its two volume children.
	_, err := os.Stat(filepath.Join(multiDiskDir, archive.SnapshotsDirName))
	require.NoError(t, err, "multi-disk snapshots/ must exist")

	// multi-disk manifests/ must have the ConfigMap but NOT the two DataRef PVCs.
	manifestsDir := filepath.Join(multiDiskDir, archive.ManifestsDirName)
	require.NoError(t, func() error {
		_, e := os.Stat(filepath.Join(manifestsDir, "configmap_multi-cfg.yaml"))
		return e
	}(), "multi-disk manifests/ must include ConfigMap")

	for _, pvcName := range []string{e2eMultiBlockPVC, e2eMultiFSPVC} {
		pvcFile := fmt.Sprintf("persistentvolumeclaim_%s.yaml", pvcName)
		_, statErr := os.Stat(filepath.Join(manifestsDir, pvcFile))
		require.True(t, os.IsNotExist(statErr),
			"multi-disk manifests/ must NOT include DataRef PVC %s", pvcName)
	}

	// multi-disk must have NO direct data payload.
	_, noBlock := os.Stat(filepath.Join(multiDiskDir, archive.DataBlockName))
	require.True(t, os.IsNotExist(noBlock), "multi-disk snapshot node must not carry data.img.zst")

	// ── Block volume node ─────────────────────────────────────────────────────
	blockVolDir := filepath.Join(multiDiskDir, archive.SnapshotsDirName,
		archive.NodeDirName("VolumeSnapshot", blockShadow))
	assertE2ENodeComplete(t, blockVolDir)

	blockFile := filepath.Join(blockVolDir, archive.DataBlockName)
	_, err = os.Stat(blockFile)
	require.NoError(t, err, "block volume node data.img.zst must exist")
	require.Equal(t, rawBlock, e2eDecodeZstdFile(t, blockFile))

	// Block volume node must carry the captured PVC manifest.
	blockPVCManifest := filepath.Join(blockVolDir, archive.ManifestsDirName,
		fmt.Sprintf("persistentvolumeclaim_%s.yaml", e2eMultiBlockPVC))
	_, err = os.Stat(blockPVCManifest)
	require.NoError(t, err, "block volume node must have PVC manifest")

	// Block volume node snapshot.yaml must carry a Volume block.
	blockSY, err := archive.ReadSnapshotYAML(blockVolDir)
	require.NoError(t, err, "ReadSnapshotYAML for block volume node")
	require.NotNil(t, blockSY.Volume, "block volume node snapshot.yaml must carry Volume block")
	require.Equal(t, e2eMultiBlockPVC, blockSY.Volume.Target.Name,
		"block volume Volume.Target.Name must match block PVC")
	require.Equal(t, e2eMultiBlockVSC, blockSY.Volume.Artifact.Name,
		"block volume Volume.Artifact.Name must match block VSC")

	// ── Filesystem volume node ────────────────────────────────────────────────
	fsVolDir := filepath.Join(multiDiskDir, archive.SnapshotsDirName,
		archive.NodeDirName("VolumeSnapshot", fsShadow))
	assertE2ENodeComplete(t, fsVolDir)

	dataDir := filepath.Join(fsVolDir, archive.DataDirName)
	_, err = os.Stat(dataDir)
	require.NoError(t, err, "fs volume node data/ must exist")

	for _, f := range fsFiles {
		zstPath := filepath.Join(dataDir, f.rel+".zst")
		require.Equal(t, f.content, e2eDecodeZstdFile(t, zstPath),
			"fs volume file %s content mismatch", f.rel)
	}

	// FS volume node must carry the captured PVC manifest.
	fsPVCManifest := filepath.Join(fsVolDir, archive.ManifestsDirName,
		fmt.Sprintf("persistentvolumeclaim_%s.yaml", e2eMultiFSPVC))
	_, err = os.Stat(fsPVCManifest)
	require.NoError(t, err, "fs volume node must have PVC manifest")

	// FS volume node snapshot.yaml must carry a Volume block.
	fsSY, err := archive.ReadSnapshotYAML(fsVolDir)
	require.NoError(t, err, "ReadSnapshotYAML for fs volume node")
	require.NotNil(t, fsSY.Volume, "fs volume node snapshot.yaml must carry Volume block")
	require.Equal(t, e2eMultiFSPVC, fsSY.Volume.Target.Name,
		"fs volume Volume.Target.Name must match fs PVC")
	require.Equal(t, e2eMultiFSVSC, fsSY.Volume.Artifact.Name,
		"fs volume Volume.Artifact.Name must match fs VSC")

	// ── Resume: second run must be a no-op ────────────────────────────────────
	mtimes := e2eCollectMtimes(t, outputDir)
	time.Sleep(20 * time.Millisecond)

	require.NoError(t, pipeline.Run(context.Background(), cfg))

	for path, before := range mtimes {
		after := statMtime(t, path)
		require.Equal(t, before, after, "snapshot.yaml mtime changed on resume: %s", path)
	}
}

// buildMultiVolumeFakeClient constructs the fake kube client for TestPipeline_E2E_MultiVolume.
// The tree has a single snapshot node (multi-disk) with two dataRefs: one Block and one Filesystem.
func buildMultiVolumeFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := buildScheme(t)

	// Root snapshot (no children, no manifests).
	rootSnap := &snapshotapi.Snapshot{
		TypeMeta: metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "Snapshot"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      e2eMultiRootSnap,
			Namespace: e2eNS,
		},
		Status: snapshotapi.SnapshotStatus{
			BoundSnapshotContentName: "sc-multi-root",
			ChildrenSnapshotRefs: []snapshotapi.SnapshotChildRef{
				{APIVersion: e2eVMAPIVersion, Kind: e2eDiskKind, Name: e2eMultiDisk},
			},
		},
	}

	rootContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-multi-root"},
	}

	// multi-disk: VirtualDiskSnapshot with TWO dataRefs (block + fs).
	multiDiskSnap := makeUnstructuredE2ENode(e2eVMAPIVersion, e2eDiskKind, e2eNS, e2eMultiDisk, "sc-multi-disk", nil)

	// sc-multi-disk checkpoint has: configmap + block-pvc + fs-pvc.
	multiDiskPayload := fmt.Sprintf(
		`[`+
			`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"multi-cfg","namespace":%q}},`+
			`{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":%q,"namespace":%q,"uid":"uid-multi-block"},"spec":{"storageClassName":"csi-multi-block","volumeMode":"Block"}},`+
			`{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":%q,"namespace":%q,"uid":"uid-multi-fs"},"spec":{"storageClassName":"csi-multi-fs","volumeMode":"Filesystem"}}`+
			`]`,
		e2eNS,
		e2eMultiBlockPVC, e2eNS,
		e2eMultiFSPVC, e2eNS,
	)

	multiDiskMCP := &snapshotapi.ManifestCheckpoint{
		TypeMeta:   metav1.TypeMeta{APIVersion: snapshotterAPIVersion, Kind: "ManifestCheckpoint"},
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-multi-disk"},
		Spec:       snapshotapi.ManifestCheckpointSpec{SourceNamespace: e2eNS},
		Status: snapshotapi.ManifestCheckpointStatus{
			TotalObjects: 3,
			Chunks:       []snapshotapi.ChunkInfo{{Index: 0, Name: "chunk-multi-disk-0", ObjectsCount: 3}},
		},
	}

	multiDiskChunk := makeE2EManifestChunkWithPayload(t, "chunk-multi-disk-0", "mcp-multi-disk", 0, multiDiskPayload)

	multiDiskContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-multi-disk"},
		Status: snapshotapi.SnapshotContentStatus{
			ManifestCheckpointName: "mcp-multi-disk",
			DataRefs: []snapshotapi.SnapshotDataBinding{
				{
					TargetUID: "uid-multi-block",
					Target: snapshotapi.SnapshotSubjectRef{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Namespace:  e2eNS,
						Name:       e2eMultiBlockPVC,
						UID:        "uid-multi-block",
					},
					Artifact: snapshotapi.SnapshotDataArtifactRef{
						APIVersion: "snapshot.storage.k8s.io/v1",
						Kind:       "VolumeSnapshotContent",
						Name:       e2eMultiBlockVSC,
					},
				},
				{
					TargetUID: "uid-multi-fs",
					Target: snapshotapi.SnapshotSubjectRef{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Namespace:  e2eNS,
						Name:       e2eMultiFSPVC,
						UID:        "uid-multi-fs",
					},
					Artifact: snapshotapi.SnapshotDataArtifactRef{
						APIVersion: "snapshot.storage.k8s.io/v1",
						Kind:       "VolumeSnapshotContent",
						Name:       e2eMultiFSVSC,
					},
				},
			},
		},
	}

	// Real VSCs for EnsureShadowPair.
	blockHandle := "handle-multi-block"
	realBlockVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: e2eMultiBlockVSC},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "test.driver",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &blockHandle},
			VolumeSnapshotRef: corev1.ObjectReference{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       "placeholder-multi-block",
				Namespace:  "default",
			},
		},
	}

	fsHandle := "handle-multi-fs"
	realFSVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: e2eMultiFSVSC},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "test.driver",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &fsHandle},
			VolumeSnapshotRef: corev1.ObjectReference{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       "placeholder-multi-fs",
				Namespace:  "default",
			},
		},
	}

	// Live source PVCs for resolveShadowMeta (present in cluster).
	blockMode := corev1.PersistentVolumeBlock
	blockSC := "csi-multi-block"
	liveBlockPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: e2eMultiBlockPVC, Namespace: e2eNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &blockSC,
			VolumeMode:       &blockMode,
		},
	}

	fsMode := corev1.PersistentVolumeFilesystem
	fsSC := "csi-multi-fs"
	liveFSPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: e2eMultiFSPVC, Namespace: e2eNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &fsSC,
			VolumeMode:       &fsMode,
		},
	}

	typed := []client.Object{
		rootSnap, rootContent,
		multiDiskContent, multiDiskMCP, multiDiskChunk,
		realBlockVSC, realFSVSC,
		liveBlockPVC, liveFSPVC,
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(typed...).
		WithObjects(multiDiskSnap).
		Build()
}

// TestPipeline_E2E_DeletedPVC verifies that when the source PVC no longer exists in the
// cluster, the pipeline resolves shadow metadata from the captured PVC manifest that
// WriteVolumeManifest wrote into the volume node's manifests/ directory.
//
// This tests the full end-to-end path of the manifest fallback:
//  1. del-disk's SnapshotContent checkpoint contains the del-pvc manifest.
//  2. WriteVolumeManifest writes it to the volume node's manifests/ dir.
//  3. The live PVC GET returns NotFound (not in fake client).
//  4. resolveShadowMeta falls back to reading manifests/persistentvolumeclaim_del-pvc.yaml.
//  5. The download succeeds with the correct storageClass and volumeMode from the manifest.
//
// Tree:
//
//	e2e-del-root (Snapshot)
//	  └─ del-disk (VirtualDiskSnapshot, checkpoint contains del-pvc manifest)
//	       └─ volumesnapshot_<del-shadow>/ (volume node, block data via manifest fallback)
func TestPipeline_E2E_DeletedPVC(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("D"), e2eBlockSize)
	blockSrv := makeE2EBlockServer(t, rawBlock)

	delShadow := exporter.ShadowName(e2eDelVSC)

	var capturedMeta exporter.ShadowMeta

	c := buildDeletedPVCFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            e2eNS,
		RootSnapshot:         e2eDelRootSnap,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		WaitShadowVS:         noopWaitShadowVS,
		OpenExport: func(ctx context.Context, namespace, shadowVSName, _ string) (*exporter.Export, error) {
			if shadowVSName != delShadow {
				return nil, fmt.Errorf("e2e-del: unknown shadow VS %q", shadowVSName)
			}

			// Capture the shadow VS annotations set from the manifest fallback.
			var shadowVS snapv1.VolumeSnapshot
			if getErr := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: shadowVSName}, &shadowVS); getErr == nil {
				capturedMeta = exporter.ShadowMeta{
					StorageClass: shadowVS.Annotations[exporter.AnnotationStorageClassName],
					VolumeMode:   shadowVS.Annotations[exporter.AnnotationVolumeMode],
				}
			}

			return exporter.NewExport(namespace, "de-del", "Block", blockSrv.URL, exporter.NewFetcher(blockSrv.Client())), nil
		},
	}

	require.NoError(t, pipeline.Run(context.Background(), cfg))

	// del-disk snapshot node must be complete.
	delDiskDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eDelDisk))
	assertE2ENodeComplete(t, delDiskDir)

	// Volume node must be complete with the block data.
	delVolDir := filepath.Join(delDiskDir, archive.SnapshotsDirName,
		archive.NodeDirName("VolumeSnapshot", delShadow))
	assertE2ENodeComplete(t, delVolDir)

	blockFile := filepath.Join(delVolDir, archive.DataBlockName)
	_, err := os.Stat(blockFile)
	require.NoError(t, err, "del volume node data.img.zst must exist")
	require.Equal(t, rawBlock, e2eDecodeZstdFile(t, blockFile),
		"del volume data must match original rawBlock")

	// The captured PVC manifest must be in the volume node's manifests/.
	pvcManifestPath := filepath.Join(delVolDir, archive.ManifestsDirName,
		fmt.Sprintf("persistentvolumeclaim_%s.yaml", e2eDelPVC))
	_, err = os.Stat(pvcManifestPath)
	require.NoError(t, err, "del volume node must have the captured PVC manifest from checkpoint")

	// Shadow VS annotations must come from the manifest fallback (not live PVC).
	require.Equal(t, "csi-del-sc", capturedMeta.StorageClass,
		"storageClass must be resolved from captured PVC manifest")
	require.Equal(t, "Block", capturedMeta.VolumeMode,
		"volumeMode must be resolved from captured PVC manifest")
}

// buildDeletedPVCFakeClient constructs the fake kube client for TestPipeline_E2E_DeletedPVC.
// The del-pvc is intentionally NOT seeded in the fake client to simulate a deleted PVC.
func buildDeletedPVCFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := buildScheme(t)

	rootSnap := &snapshotapi.Snapshot{
		TypeMeta: metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "Snapshot"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      e2eDelRootSnap,
			Namespace: e2eNS,
		},
		Status: snapshotapi.SnapshotStatus{
			BoundSnapshotContentName: "sc-del-root",
			ChildrenSnapshotRefs: []snapshotapi.SnapshotChildRef{
				{APIVersion: e2eVMAPIVersion, Kind: e2eDiskKind, Name: e2eDelDisk},
			},
		},
	}

	rootContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-del-root"},
	}

	delDiskSnap := makeUnstructuredE2ENode(e2eVMAPIVersion, e2eDiskKind, e2eNS, e2eDelDisk, "sc-del-disk", nil)

	// The del-disk checkpoint contains ONLY the del-pvc manifest (the live PVC is gone).
	delPayload := fmt.Sprintf(
		`[{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":%q,"namespace":%q,"uid":"uid-del"},"spec":{"storageClassName":"csi-del-sc","volumeMode":"Block"}}]`,
		e2eDelPVC, e2eNS,
	)

	delMCP := &snapshotapi.ManifestCheckpoint{
		TypeMeta:   metav1.TypeMeta{APIVersion: snapshotterAPIVersion, Kind: "ManifestCheckpoint"},
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-del-disk"},
		Spec:       snapshotapi.ManifestCheckpointSpec{SourceNamespace: e2eNS},
		Status: snapshotapi.ManifestCheckpointStatus{
			TotalObjects: 1,
			Chunks:       []snapshotapi.ChunkInfo{{Index: 0, Name: "chunk-del-disk-0", ObjectsCount: 1}},
		},
	}

	delChunk := makeE2EManifestChunkWithPayload(t, "chunk-del-disk-0", "mcp-del-disk", 0, delPayload)

	delContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-del-disk"},
		Status: snapshotapi.SnapshotContentStatus{
			ManifestCheckpointName: "mcp-del-disk",
			DataRefs: []snapshotapi.SnapshotDataBinding{
				{
					TargetUID: "uid-del",
					Target: snapshotapi.SnapshotSubjectRef{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Namespace:  e2eNS,
						Name:       e2eDelPVC,
						UID:        "uid-del",
					},
					Artifact: snapshotapi.SnapshotDataArtifactRef{
						APIVersion: "snapshot.storage.k8s.io/v1",
						Kind:       "VolumeSnapshotContent",
						Name:       e2eDelVSC,
					},
				},
			},
		},
	}

	delHandle := "handle-del-block"
	realDelVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: e2eDelVSC},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "test.driver",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &delHandle},
			VolumeSnapshotRef: corev1.ObjectReference{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       "placeholder-del",
				Namespace:  "default",
			},
		},
	}

	// NOTE: del-pvc is intentionally NOT seeded here.
	typed := []client.Object{
		rootSnap, rootContent,
		delContent, delMCP, delChunk,
		realDelVSC,
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(typed...).
		WithObjects(delDiskSnap).
		Build()
}
