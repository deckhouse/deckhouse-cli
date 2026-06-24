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
	"compress/gzip"
	"context"
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
	lz4 "github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
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

	// Deleted-PVC test constants.
	e2eDelRootSnap = "e2e-del-root"
	e2eDelDisk     = "del-disk"
	e2eDelVSC      = "vsc-del-block"
	e2eDelPVC      = "del-pvc"

	// Orphan-leaf test constants.
	e2eAggRootSnap = "e2e-agg-root"
)

// TestPipeline_E2E_FullTree runs the full pipeline against a fake kube client and
// httptest servers. The tree is:
//
//	root (manifests: root-cm)
//	  └─ vm-snap (VirtualMachineSnapshot, manifests: vm-cm)
//	       ├─ disk-block (VirtualDiskSnapshot, non-aggregator, 1 OwnDataRef → block)
//	       │    data.bin.zst    (block data directly in the node dir)
//	       └─ disk-fs   (VirtualDiskSnapshot, non-aggregator, 1 OwnDataRef → fs)
//	            data.tar        (filesystem tar directly in the node dir)
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

	require.NoError(t, runPipeline(context.Background(), cfg))

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
	// disk-block is a non-aggregator: it owns one DataRef, so data.bin.zst lives
	// directly in the node directory (flat layout). No snapshots/ subdirectory.
	blockDir := filepath.Join(vmDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eBlockDisk))
	assertE2ENodeComplete(t, blockDir)

	blockFile := filepath.Join(blockDir, archive.DataBlockName(".zst"))
	_, err = os.Stat(blockFile)
	require.NoError(t, err, "disk-block data.bin.zst must exist directly in the node dir")

	// Non-aggregator nodes with no children must not have a snapshots/ subdir.
	_, noSnapshotsErr := os.Stat(filepath.Join(blockDir, archive.SnapshotsDirName))
	require.True(t, os.IsNotExist(noSnapshotsErr),
		"disk-block must not have a snapshots/ subdir (no children)")

	// Verify decoded block data equals the original.
	require.Equal(t, rawBlock, e2eDecodeZstdFile(t, blockFile))

	// ── disk-fs node assertions ───────────────────────────────────────────────
	// disk-fs is a non-aggregator: filesystem data lives directly in data/ inside
	// the node directory (flat layout). No snapshots/ subdirectory.
	fsDir := filepath.Join(vmDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eFSDisk))
	assertE2ENodeComplete(t, fsDir)

	fsTarPath := filepath.Join(fsDir, archive.FsTarName)
	_, err = os.Stat(fsTarPath)
	require.NoError(t, err, "disk-fs data.tar must exist directly in the node dir")

	_, noSnapshotsFSErr := os.Stat(filepath.Join(fsDir, archive.SnapshotsDirName))
	require.True(t, os.IsNotExist(noSnapshotsFSErr),
		"disk-fs must not have a snapshots/ subdir (no children)")

	for _, f := range fsFiles {
		// Per-file-compressed model: entries named <relPath>.zst (default zstd codec).
		entryName := f.rel + ".zst"
		compressed, tarErr := readTarEntry(t, fsTarPath, entryName)
		require.NoError(t, tarErr, "disk-fs tar must have entry %s", entryName)
		require.Equal(t, f.content, e2eDecodeZstdBytes(t, compressed), "disk-fs file %s content mismatch", f.rel)
	}

	// ── Resume: second run must be a no-op ────────────────────────────────────
	mtimes := e2eCollectMtimes(t, outputDir)
	time.Sleep(20 * time.Millisecond)

	require.NoError(t, runPipeline(context.Background(), cfg))

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

// e2eDecodeZstdBytes decompresses a zstd-encoded byte slice.
func e2eDecodeZstdBytes(t *testing.T, data []byte) []byte {
	t.Helper()

	dec, err := zstd.NewReader(bytes.NewReader(data))
	require.NoError(t, err, "zstd.NewReader")

	defer dec.Close()

	out, err := io.ReadAll(dec)
	require.NoError(t, err, "decode zstd bytes")

	return out
}

// readTarEntry opens a tar archive at tarPath and returns the contents of the
// entry with the given name. Returns an error if the entry is not found.
func readTarEntry(t *testing.T, tarPath, entryName string) ([]byte, error) {
	t.Helper()

	f, err := os.Open(tarPath)
	require.NoError(t, err, "open tar %s", tarPath)

	defer f.Close()

	tr := tar.NewReader(f)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("entry %q not found in %s", entryName, tarPath)
		}

		require.NoError(t, err, "read tar header in %s", tarPath)

		if hdr.Name == entryName {
			return io.ReadAll(tr)
		}
	}
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
					`{"name":"alpha.txt","type":"file","uri":"alpha.txt","attributes":{"permissions":"0644","modtime":"2024-03-01T12:00:00Z","uid":0,"gid":0,"size":11}},`+
					`{"name":"subdir","type":"dir","uri":"subdir/","attributes":{"permissions":"0755","modtime":"2024-03-01T12:00:00Z","uid":0,"gid":0}}`+
					`]}`)

		case "/api/v1/files/subdir/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w,
				`{"apiVersion":"v1","items":[`+
					`{"name":"beta.txt","type":"file","uri":"subdir/beta.txt","attributes":{"permissions":"0644","modtime":"2024-03-01T12:00:00Z","uid":0,"gid":0,"size":10}}`+
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

	// Root SnapshotContent: no volume (own manifests served by the stub ManifestSource).
	rootContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-e2e-root"},
	}

	// ── vm-snap unstructured ──────────────────────────────────────────────────
	vmSnap := makeUnstructuredE2ENode(e2eVMAPIVersion, e2eVMKind, e2eNS, e2eVMSnap, "sc-e2e-vm",
		[]map[string]interface{}{
			{"apiVersion": e2eVMAPIVersion, "kind": e2eDiskKind, "name": e2eBlockDisk},
			{"apiVersion": e2eVMAPIVersion, "kind": e2eDiskKind, "name": e2eFSDisk},
		})

	vmContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-e2e-vm"},
	}

	// ── disk-block unstructured (non-aggregator: no childrenSnapshotRefs) ─────
	// Its DataRef in blockContent becomes an OwnDataRef on the node;
	// block data is downloaded directly into the disk-block node directory.
	blockSnap := makeUnstructuredE2ENode(e2eVMAPIVersion, e2eDiskKind, e2eNS, e2eBlockDisk, "sc-e2e-block", nil)

	blockContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-e2e-block"},
		Status: snapshotapi.SnapshotContentStatus{
			DataRef: &snapshotapi.SnapshotDataBinding{
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
	}

	// ── disk-fs unstructured (non-aggregator: no childrenSnapshotRefs) ────────
	fsSnap := makeUnstructuredE2ENode(e2eVMAPIVersion, e2eDiskKind, e2eNS, e2eFSDisk, "sc-e2e-fs", nil)

	fsContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-e2e-fs"},
		Status: snapshotapi.SnapshotContentStatus{
			DataRef: &snapshotapi.SnapshotDataBinding{
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
		rootSnap, rootContent,
		vmContent,
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

// TestPipeline_E2E_DeletedPVC verifies that a non-aggregator OwnDataRef node with a
// genuinely deleted backing PVC still downloads successfully. The live PVC is absent
// from the cluster. Shadow metadata (storageClass, volumeMode) is resolved from the
// node's own-scope manifests via shadowMetaFromManifests, not from the absent live PVC
// and not from an on-disk manifest (excluded from disk by the OwnDataRef rule).
//
//  1. del-disk's own manifests (stub ManifestSource) contain the del-pvc manifest.
//  2. WriteNodeManifests excludes the PVC (OwnDataRef rule); del-disk/manifests/ has no PVC file.
//  3. The live PVC is absent (genuinely deleted); resolveShadowMeta falls back to those manifests.
//  4. shadowMetaFromManifests reads storageClass and volumeMode from the captured PVC object.
//  5. The download succeeds with the correct storageClass and volumeMode from the manifests.
//
// Tree:
//
//	e2e-del-root (Snapshot)
//	  └─ del-disk (VirtualDiskSnapshot, non-aggregator, 1 OwnDataRef → block)
//	       data.bin.zst   (block data directly in the node dir)
//	       (no persistentvolumeclaim_del-pvc.yaml — OwnDataRef PVC manifest excluded)
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

	require.NoError(t, runPipeline(context.Background(), cfg))

	// del-disk is a non-aggregator: block data lands directly in its node dir.
	delDiskDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eDelDisk))
	assertE2ENodeComplete(t, delDiskDir)

	// Block data must be in the node dir directly (flat layout).
	blockFile := filepath.Join(delDiskDir, archive.DataBlockName(".zst"))
	_, err := os.Stat(blockFile)
	require.NoError(t, err, "del-disk data.bin.zst must exist directly in the node dir")
	require.Equal(t, rawBlock, e2eDecodeZstdFile(t, blockFile),
		"del-disk volume data must match original rawBlock")

	// The OwnDataRef PVC manifest must NOT be in del-disk/manifests/; the new rule
	// excludes backing PVC manifests from data-owning domain nodes.
	pvcManifestPath := filepath.Join(delDiskDir, archive.ManifestsDirName,
		fmt.Sprintf("persistentvolumeclaim_%s.yaml", e2eDelPVC))
	_, pvcStatErr := os.Stat(pvcManifestPath)
	require.True(t, os.IsNotExist(pvcStatErr),
		"del-disk must NOT have the backing PVC manifest in its manifests/ (OwnDataRef PVC excluded)")

	// Shadow VS annotations come from the manifests fallback (live PVC is absent).
	require.Equal(t, "csi-del-sc", capturedMeta.StorageClass,
		"storageClass must be resolved from the manifests fallback")
	require.Equal(t, "Block", capturedMeta.VolumeMode,
		"volumeMode must be resolved from the manifests fallback")
}

// buildDeletedPVCFakeClient constructs the fake kube client for TestPipeline_E2E_DeletedPVC.
// The stub ManifestSource serves del-disk's own manifests, including the del-pvc manifest
// with storageClass and volumeMode. The live PVC is deliberately absent from the fake client
// so that resolveShadowMeta must fall back to shadowMetaFromManifests. WriteNodeManifests
// excludes the PVC from del-disk/manifests/ per the OwnDataRef rule; the on-disk file is also
// intentionally absent.
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

	// del-disk: non-aggregator (no childrenSnapshotRefs).
	// Its DataRef becomes an OwnDataRef. The captured del-pvc manifest (carrying the
	// storageClass/volumeMode that shadowMetaFromManifests must surface when the live
	// PVC is absent) is served by the stub ManifestSource keyed by the del-disk node ref.
	delDiskSnap := makeUnstructuredE2ENode(e2eVMAPIVersion, e2eDiskKind, e2eNS, e2eDelDisk, "sc-del-disk", nil)

	delContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-del-disk"},
		Status: snapshotapi.SnapshotContentStatus{
			DataRef: &snapshotapi.SnapshotDataBinding{
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

	// The live PVC is intentionally absent: resolveShadowMeta must fall back to
	// shadowMetaFromCheckpoint, reading storageClass/volumeMode from mcp-del-disk.

	typed := []client.Object{
		rootSnap, rootContent,
		delContent,
		realDelVSC,
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(typed...).
		WithObjects(delDiskSnap).
		Build()
}

// TestPipeline_E2E_OrphanPVCLeaf verifies the aggregator + orphan-leaf model:
// a snapshot node with a VolumeSnapshot visibility-leaf in its childrenSnapshotRefs
// is treated as an aggregator. Its DataRefs become orphan leaf volume nodes, each
// rooted at snapshots/volumesnapshot_<pvcName>/.
//
// Assertions:
//  1. The aggregator node has snapshots/ (for the orphan leaf) but no data payload.
//  2. The aggregator manifests/ includes only non-PVC manifests (ConfigMap);
//     the orphan leaf's PVC is excluded from the aggregator.
//  3. The orphan leaf node has data.bin.zst and its captured PVC manifest.
//  4. The directory name uses the PVC name (SourceName), not the shadow VS name.
//
// Tree:
//
//	e2e-agg-root (Snapshot)
//	  └─ agg-snap (VirtualDiskSnapshot, aggregator: visibility-leaf in childrenSnapshotRefs)
//	       manifests/configmap_agg-cm.yaml    (PVC excluded from aggregator manifests)
//	       snapshots/
//	         volumesnapshot_pvc-agg/          (orphan leaf named after PVC, not shadow VS)
//	           data.bin.zst
//	           manifests/persistentvolumeclaim_pvc-agg.yaml
func TestPipeline_E2E_OrphanPVCLeaf(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("A"), e2eBlockSize)
	blockSrv := makeE2EBlockServer(t, rawBlock)

	// Shadow VS name derived from the artifact VSC (same convention as all other tests).
	aggShadow := exporter.ShadowName("vsc-agg")

	c := buildOrphanLeafFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            e2eNS,
		RootSnapshot:         e2eAggRootSnap,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		WaitShadowVS:         noopWaitShadowVS,
		OpenExport: func(_ context.Context, namespace, shadowVSName, _ string) (*exporter.Export, error) {
			if shadowVSName != aggShadow {
				return nil, fmt.Errorf("e2e-agg: unknown shadow VS %q", shadowVSName)
			}

			return exporter.NewExport(namespace, "de-agg", "Block", blockSrv.URL, exporter.NewFetcher(blockSrv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	// agg-snap is the aggregator node.
	aggSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, "agg-snap"))
	assertE2ENodeComplete(t, aggSnapDir)

	// Aggregator must have a snapshots/ dir for the orphan leaf child.
	_, err := os.Stat(filepath.Join(aggSnapDir, archive.SnapshotsDirName))
	require.NoError(t, err, "aggregator snapshots/ must exist for the orphan leaf")

	// Aggregator must have NO data payload — data lives in the leaf, not here.
	_, noBlock := os.Stat(filepath.Join(aggSnapDir, archive.DataBlockName(".zst")))
	require.True(t, os.IsNotExist(noBlock), "aggregator must not carry data.bin.zst")

	_, noData := os.Stat(filepath.Join(aggSnapDir, archive.DataDirName))
	require.True(t, os.IsNotExist(noData), "aggregator must not carry data/ dir")

	// Aggregator manifests/ must include the ConfigMap but NOT the orphan leaf's PVC.
	manifestsDir := filepath.Join(aggSnapDir, archive.ManifestsDirName)
	_, err = os.Stat(filepath.Join(manifestsDir, "configmap_agg-cm.yaml"))
	require.NoError(t, err, "aggregator manifests/ must include ConfigMap")

	_, statErr := os.Stat(filepath.Join(manifestsDir, "persistentvolumeclaim_pvc-agg.yaml"))
	require.True(t, os.IsNotExist(statErr),
		"aggregator manifests/ must NOT include orphan leaf PVC (it belongs in the leaf dir)")

	// Orphan leaf node: directory name is volumesnapshot_<pvcName> (SourceName = PVC name).
	// The visibility-leaf VS name "nss-vs-agg-pvc" is not used for directory naming.
	leafDir := filepath.Join(aggSnapDir, archive.SnapshotsDirName,
		archive.NodeDirName("VolumeSnapshot", "pvc-agg"))
	assertE2ENodeComplete(t, leafDir)

	// Orphan leaf must carry the block data.
	blockFile := filepath.Join(leafDir, archive.DataBlockName(".zst"))
	_, err = os.Stat(blockFile)
	require.NoError(t, err, "orphan leaf data.bin.zst must exist")
	require.Equal(t, rawBlock, e2eDecodeZstdFile(t, blockFile), "orphan leaf block data mismatch")

	// Orphan leaf manifests/ must carry the captured PVC manifest.
	pvcManifestPath := filepath.Join(leafDir, archive.ManifestsDirName,
		"persistentvolumeclaim_pvc-agg.yaml")
	_, err = os.Stat(pvcManifestPath)
	require.NoError(t, err, "orphan leaf manifests/ must include the captured PVC manifest")

	// ── Resume: second run must be a no-op ────────────────────────────────────
	mtimes := e2eCollectMtimes(t, outputDir)
	time.Sleep(20 * time.Millisecond)

	require.NoError(t, runPipeline(context.Background(), cfg))

	for path, before := range mtimes {
		after := statMtime(t, path)
		require.Equal(t, before, after, "snapshot.yaml mtime changed on resume: %s", path)
	}
}

// buildOrphanLeafFakeClient constructs the fake kube client for TestPipeline_E2E_OrphanPVCLeaf.
// The agg-snap snapshot has a VolumeSnapshot visibility-leaf in its childrenSnapshotRefs,
// making the tree builder treat it as an aggregator: its DataRef becomes an orphan leaf child.
func buildOrphanLeafFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := buildScheme(t)

	rootSnap := &snapshotapi.Snapshot{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "Snapshot"},
		ObjectMeta: metav1.ObjectMeta{Name: e2eAggRootSnap, Namespace: e2eNS},
		Status: snapshotapi.SnapshotStatus{
			BoundSnapshotContentName: "sc-agg-root",
			ChildrenSnapshotRefs: []snapshotapi.SnapshotChildRef{
				{APIVersion: e2eVMAPIVersion, Kind: e2eDiskKind, Name: "agg-snap"},
			},
		},
	}

	rootContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-agg-root"},
	}

	// agg-snap has a VolumeSnapshot visibility-leaf in its childrenSnapshotRefs (Variant A).
	// The tree builder calls visitVisibilityLeaf("nss-vs-agg-pvc"): it fetches the VS,
	// reads status.boundSnapshotContentName → "sc-agg-child", fetches that child content,
	// and takes the orphan leaf Binding from sc-agg-child.status.dataRef.
	// The aggregator content (sc-agg) has no own dataRef.
	aggSnap := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": e2eVMAPIVersion,
			"kind":       e2eDiskKind,
			"metadata": map[string]interface{}{
				"name":      "agg-snap",
				"namespace": e2eNS,
			},
			"status": map[string]interface{}{
				"boundSnapshotContentName": "sc-agg",
				"childrenSnapshotRefs": []interface{}{
					map[string]interface{}{
						"apiVersion": "snapshot.storage.k8s.io/v1",
						"kind":       "VolumeSnapshot",
						"name":       "nss-vs-agg-pvc",
					},
				},
			},
		},
	}

	// Aggregator SnapshotContent (sc-agg): no own dataRef (Variant A).
	aggContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-agg"},
	}

	// VolumeSnapshot visibility-leaf: carries status.boundSnapshotContentName → sc-agg-child.
	// visitVisibilityLeaf fetches this object to locate the child SnapshotContent.
	aggVS := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "snapshot.storage.k8s.io/v1",
			"kind":       "VolumeSnapshot",
			"metadata": map[string]interface{}{
				"name":      "nss-vs-agg-pvc",
				"namespace": e2eNS,
			},
			"status": map[string]interface{}{
				"boundSnapshotContentName": "sc-agg-child",
			},
		},
	}

	// Child SnapshotContent (sc-agg-child): carries the single dataRef for pvc-agg.
	// This is the Binding source for the orphan leaf node (Variant A).
	aggChildContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "sc-agg-child"},
		Status: snapshotapi.SnapshotContentStatus{
			DataRef: &snapshotapi.SnapshotDataBinding{
				TargetUID: "uid-agg-pvc",
				Target: snapshotapi.SnapshotSubjectRef{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Namespace:  e2eNS,
					Name:       "pvc-agg",
					UID:        "uid-agg-pvc",
				},
				Artifact: snapshotapi.SnapshotDataArtifactRef{
					APIVersion: "snapshot.storage.k8s.io/v1",
					Kind:       "VolumeSnapshotContent",
					Name:       "vsc-agg",
				},
			},
		},
	}

	aggHandle := "handle-agg"
	realAggVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: "vsc-agg"},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "test.driver",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &aggHandle},
			VolumeSnapshotRef: corev1.ObjectReference{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       "placeholder-agg",
				Namespace:  "default",
			},
		},
	}

	// Live PVC for resolveShadowMeta (shadow meta comes from live PVC in this test).
	aggMode := corev1.PersistentVolumeBlock
	aggSC := "csi-agg-sc"
	liveAggPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-agg", Namespace: e2eNS},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &aggSC,
			VolumeMode:       &aggMode,
		},
	}

	typed := []client.Object{
		rootSnap, rootContent,
		aggContent,
		aggChildContent,
		realAggVSC, liveAggPVC,
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(typed...).
		WithObjects(aggSnap, aggVS).
		Build()
}

// TestPipeline_BlockCodecMatrix verifies that block-volume download+merge produces the
// correct data.bin[.<ext>] file for every supported codec (zstd, lz4, gzip, none) and
// that the merged file decodes back to the original raw bytes.
func TestPipeline_BlockCodecMatrix(t *testing.T) {
	t.Parallel()

	// rawBlock must span multiple chunks to exercise multi-chunk merge per codec.
	rawBlock := bytes.Repeat([]byte("C"), e2eBlockSize)

	cases := []struct {
		codec string
		ext   string
	}{
		{"zstd", ".zst"},
		{"lz4", ".lz4"},
		{"gzip", ".gz"},
		{"none", ""},
	}

	for _, tc := range cases {
		t.Run(tc.codec, func(t *testing.T) {
			t.Parallel()

			srv := makeBlockServer(t, rawBlock)
			defer srv.Close()

			c := buildFakeClient(t)
			outputDir := t.TempDir()

			codec, err := compress.New(tc.codec, 0)
			require.NoError(t, err)

			cfg := pipeline.Config{
				Namespace:            testNS,
				RootSnapshot:         rootSnapshot,
				OutputDir:            outputDir,
				Workers:              1,
				PerVolumeConcurrency: 1,
				KubeClient:           c,
				Compression:          codec,
				WaitShadowVS:         noopWaitShadowVS,
				OpenExport: func(_ context.Context, namespace, _ string, _ string) (*exporter.Export, error) {
					return exporter.NewExport(namespace, "de-"+tc.codec, "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
				},
			}

			require.NoError(t, runPipeline(context.Background(), cfg))

			diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
				archive.NodeDirName(childKind, diskSnapName))

			blockFile := filepath.Join(diskSnapDir, archive.DataBlockName(tc.ext))
			_, statErr := os.Stat(blockFile)
			require.NoError(t, statErr, "codec %s: expected %s to exist", tc.codec, archive.DataBlockName(tc.ext))

			foundPath, found, findErr := archive.FindBlockData(diskSnapDir)
			require.NoError(t, findErr)
			require.True(t, found, "codec %s: FindBlockData must find a file", tc.codec)
			require.Equal(t, blockFile, foundPath, "codec %s: only the expected file must exist", tc.codec)

			compressed, readErr := os.ReadFile(blockFile)
			require.NoError(t, readErr)
			decoded := decodeBlockFile(t, tc.codec, compressed)
			require.Equal(t, rawBlock, decoded, "codec %s: decoded bytes must match original", tc.codec)
		})
	}
}

// decodeBlockFile decodes a merged block-volume file using the matching decoder for codecName.
func decodeBlockFile(t *testing.T, codecName string, data []byte) []byte {
	t.Helper()

	switch codecName {
	case "zstd":
		return decodeZstdBlock(t, data)
	case "lz4":
		return decodeLZ4Block(t, data)
	case "gzip":
		return decodeGzipBlock(t, data)
	case "none":
		return data
	default:
		t.Fatalf("decodeBlockFile: unknown codec %q", codecName)
		return nil
	}
}

// decodeZstdBlock decodes a multi-frame zstd stream (concatenated per-chunk frames).
func decodeZstdBlock(t *testing.T, data []byte) []byte {
	t.Helper()

	dec, err := zstd.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer dec.Close()

	var buf bytes.Buffer

	_, err = buf.ReadFrom(dec)
	require.NoError(t, err)

	return buf.Bytes()
}

// decodeLZ4Block decodes a multi-frame LZ4 stream (concatenated per-chunk frames).
func decodeLZ4Block(t *testing.T, data []byte) []byte {
	t.Helper()

	var buf bytes.Buffer

	r := lz4.NewReader(bytes.NewReader(data))

	_, err := buf.ReadFrom(r)
	require.NoError(t, err)

	return buf.Bytes()
}

// decodeGzipBlock decodes a stream of concatenated gzip members (one per chunk).
func decodeGzipBlock(t *testing.T, data []byte) []byte {
	t.Helper()

	r := bytes.NewReader(data)
	var buf bytes.Buffer

	for r.Len() > 0 {
		gz, err := gzip.NewReader(r)
		require.NoError(t, err)

		_, err = io.Copy(&buf, gz)
		require.NoError(t, err)

		require.NoError(t, gz.Close())
	}

	return buf.Bytes()
}

// TestPipeline_E2E_FSNoneCodecEntries verifies that when codec=none is selected,
// data.tar file entries carry plain names (no extension suffix) and uncompressed bytes.
// This complements TestPipeline_E2E_FullTree (which uses the default zstd codec).
func TestPipeline_E2E_FSNoneCodecEntries(t *testing.T) {
	fsFiles := []fsE2EFile{
		{rel: "alpha.txt", content: []byte("hello-alpha")},
		{rel: "subdir/beta.txt", content: []byte("hello-beta")},
	}

	blockSrv := makeE2EBlockServer(t, bytes.Repeat([]byte("Z"), e2eBlockSize))
	fsSrv := makeE2EFSServer(t, fsFiles)

	c := buildE2EFakeClient(t)
	outputDir := t.TempDir()

	blockShadow := exporter.ShadowName(e2eBlockVSC)
	fsShadow := exporter.ShadowName(e2eFSVSC)

	noneCodec, err := compress.New("none", 0)
	require.NoError(t, err)

	cfg := pipeline.Config{
		Namespace:            e2eNS,
		RootSnapshot:         e2eRootSnap,
		OutputDir:            outputDir,
		Workers:              2,
		PerVolumeConcurrency: 2,
		KubeClient:           c,
		Compression:          noneCodec,
		WaitShadowVS:         noopWaitShadowVS,
		OpenExport: func(_ context.Context, namespace, shadowVSName, _ string) (*exporter.Export, error) {
			switch shadowVSName {
			case blockShadow:
				return exporter.NewExport(namespace, "de-block-none", "Block", blockSrv.URL, exporter.NewFetcher(blockSrv.Client())), nil
			case fsShadow:
				return exporter.NewExport(namespace, "de-fs-none", "Filesystem", fsSrv.URL, exporter.NewFetcher(fsSrv.Client())), nil
			default:
				return nil, fmt.Errorf("e2e-none: unknown shadow VS %q", shadowVSName)
			}
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	// Locate the FS node directory.
	vmDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eVMKind, e2eVMSnap))
	fsDir := filepath.Join(vmDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eFSDisk))

	fsTarPath := filepath.Join(fsDir, archive.FsTarName)
	_, err = os.Stat(fsTarPath)
	require.NoError(t, err, "disk-fs data.tar must exist with none codec")

	// codec=none: entries keep plain names (no extension suffix) and hold uncompressed bytes.
	for _, f := range fsFiles {
		got, tarErr := readTarEntry(t, fsTarPath, f.rel)
		require.NoError(t, tarErr, "none-codec tar must have plain entry %s", f.rel)
		require.Equal(t, f.content, got, "none-codec file %s content mismatch", f.rel)
	}
}
