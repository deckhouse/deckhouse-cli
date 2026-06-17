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
)

// TestPipeline_E2E_FullTree runs the full pipeline against a fake kube client and
// httptest servers. The tree is:
//
//	root (manifests: root-cm)
//	  └─ vm-snap (VirtualMachineSnapshot, manifests: vm-cm)
//	       ├─ disk-block (VirtualDiskSnapshot, block volume)
//	       └─ disk-fs   (VirtualDiskSnapshot, fs volume)
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
	blockDir := filepath.Join(vmDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eBlockDisk))
	assertE2ENodeComplete(t, blockDir)

	blockFile := filepath.Join(blockDir, archive.DataBlockName)
	_, err = os.Stat(blockFile)
	require.NoError(t, err, "disk-block data.img.zst must exist")

	// Verify decoded block data equals the original.
	require.Equal(t, rawBlock, e2eDecodeZstdFile(t, blockFile))

	// ── disk-fs node assertions ───────────────────────────────────────────────
	fsDir := filepath.Join(vmDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eFSDisk))
	assertE2ENodeComplete(t, fsDir)

	dataDir := filepath.Join(fsDir, archive.DataDirName)
	_, err = os.Stat(dataDir)
	require.NoError(t, err, "disk-fs data/ must exist")

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

	var srv *httptest.Server

	fileMap := make(map[string][]byte, len(files))
	for _, f := range files {
		fileMap[f.rel] = f.content
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/files/":
			w.Header().Set("Content-Type", "application/json")
			listing := fmt.Sprintf(
				`{"apiVersion":"v1","items":[`+
					`{"name":"alpha.txt","type":"file","uri":"%s/api/v1/files/alpha.txt","attributes":{}},`+
					`{"name":"subdir","type":"directory","uri":"%s/api/v1/files/subdir/","attributes":{}}`+
					`]}`,
				srv.URL, srv.URL,
			)
			_, _ = io.WriteString(w, listing)

		case "/api/v1/files/subdir/":
			w.Header().Set("Content-Type", "application/json")
			listing := fmt.Sprintf(
				`{"apiVersion":"v1","items":[`+
					`{"name":"beta.txt","type":"file","uri":"%s/api/v1/files/subdir/beta.txt","attributes":{}}`+
					`]}`,
				srv.URL,
			)
			_, _ = io.WriteString(w, listing)

		case "/api/v1/files/alpha.txt":
			_, _ = w.Write(fileMap["alpha.txt"])

		case "/api/v1/files/subdir/beta.txt":
			_, _ = w.Write(fileMap["subdir/beta.txt"])

		default:
			http.NotFound(w, r)
		}
	})

	srv = httptest.NewServer(mux)

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

	typed := []client.Object{
		rootSnap, rootContent, rootMCP, rootChunk,
		vmContent, vmMCP, vmChunk,
		blockContent,
		fsContent,
		realBlockVSC, realFSVSC,
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
