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

package restore_test

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/restore"
)

// objectSpec describes a single Kubernetes object to include in a test archive.
type objectSpec struct {
	nodeID     string
	apiVersion string
	kind       string
	name       string
	namespace  string
	spec       map[string]any
}

// makeArchive creates a minimal, properly structured archive in a temp directory.
// It correctly calls AppendProgress per node and Finalize at the end.
func makeArchive(
	t *testing.T,
	nodes []archive.NodeRecord,
	objects []objectSpec,
	vols []archive.VolumeProgressRecord,
) string {
	t.Helper()

	dir := t.TempDir()

	meta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		CreatedAt:     time.Now(),
		Source: archive.Source{
			Namespace: "source-ns",
			RootSnapshot: archive.SnapshotRef{
				Name: "test-snap",
			},
		},
	}

	w, err := archive.NewDirWriter(dir, meta)
	require.NoError(t, err)

	// Group objects by nodeID.
	byNode := make(map[string][]objectSpec)
	for _, obj := range objects {
		byNode[obj.nodeID] = append(byNode[obj.nodeID], obj)
	}

	// For each node, add all its objects and append a progress record.
	for _, node := range nodes {
		specs := byNode[node.ID]
		var recs []archive.ObjectRecord

		for _, s := range specs {
			body := map[string]any{
				"apiVersion": s.apiVersion,
				"kind":       s.kind,
				"metadata": map[string]any{
					"name":      s.name,
					"namespace": s.namespace,
				},
			}
			if s.spec != nil {
				body["spec"] = s.spec
			}

			raw, merr := json.Marshal(body)
			require.NoError(t, merr)

			rec, aerr := w.AddObject(node.ID, raw)
			require.NoError(t, aerr)

			recs = append(recs, rec)
		}

		if len(recs) > 0 {
			require.NoError(t, w.AppendProgress(archive.ProgressRecord{
				NodeID:  node.ID,
				Objects: recs,
			}))
		}
	}

	for _, vol := range vols {
		require.NoError(t, w.AppendVolumeProgress(vol))
	}

	idx := archive.Index{SchemaVersion: archive.SchemaVersion}
	_, err = w.Finalize(idx, nodes, true)
	require.NoError(t, err)

	return dir
}

// writeGzipFile writes a dummy gzip file at path with the given content.
func writeGzipFile(t *testing.T, path string, content []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	f, err := os.Create(path)
	require.NoError(t, err)
	gz := gzip.NewWriter(f)
	_, _ = gz.Write(content)
	require.NoError(t, gz.Close())
	require.NoError(t, f.Close())
}

// Tests for Build.

func TestBuildPlan_ManifestsOnly(t *testing.T) {
	nodes := []archive.NodeRecord{
		{ID: "Snapshot--root", Kind: "Snapshot", Name: "root", Namespace: "source-ns"},
	}
	objects := []objectSpec{
		{nodeID: "Snapshot--root", apiVersion: "apps/v1", kind: "Deployment", name: "my-app", namespace: "source-ns"},
		{nodeID: "Snapshot--root", apiVersion: "v1", kind: "Service", name: "my-svc", namespace: "source-ns"},
	}

	dir := makeArchive(t, nodes, objects, nil)

	plan, err := restore.Build(dir, restore.Options{
		TargetNamespace: "target-ns",
		Mode:            restore.ModeManifestsOnly,
	})

	require.NoError(t, err)
	assert.Len(t, plan.Manifests, 2)
	assert.Len(t, plan.Volumes, 0)

	for _, op := range plan.Manifests {
		var obj map[string]any
		require.NoError(t, json.Unmarshal(op.Data, &obj))
		assert.NotEmpty(t, obj["apiVersion"])
		assert.NotEmpty(t, obj["kind"])
	}
}

func TestBuildPlan_DataOnly_VolumeCorrelation(t *testing.T) {
	nodes := []archive.NodeRecord{
		{
			ID:      "Snapshot--root",
			Kind:    "Snapshot",
			Name:    "root",
			HasData: true,
			DataRefs: []archive.VolumeDataRef{
				{VSCName: "vsc-abc123", PVCName: "my-disk"},
			},
		},
	}
	objects := []objectSpec{
		{
			nodeID:     "Snapshot--root",
			apiVersion: "v1",
			kind:       "PersistentVolumeClaim",
			name:       "my-disk",
			namespace:  "source-ns",
			spec: map[string]any{
				"accessModes": []any{"ReadWriteOnce"},
				"volumeMode":  "Block",
				"resources":   map[string]any{"requests": map[string]any{"storage": "5Gi"}},
			},
		},
	}
	vols := []archive.VolumeProgressRecord{
		{
			NodeID:      "Snapshot--root",
			VSCName:     "vsc-abc123",
			PVCName:     "my-disk",
			VolumeMode:  "Block",
			Compression: "gzip",
			BytesTotal:  5 * 1024 * 1024 * 1024,
			Complete:    true,
		},
	}

	dir := makeArchive(t, nodes, objects, vols)

	// Write a dummy block data file so the path is non-empty.
	writeGzipFile(t, filepath.Join(dir, "data", "Snapshot--root", "vsc-abc123.img.gz"), []byte("dummy"))

	plan, err := restore.Build(dir, restore.Options{
		TargetNamespace: "target-ns",
		Mode:            restore.ModeDataOnly,
	})

	require.NoError(t, err)
	assert.Len(t, plan.Manifests, 0, "DataOnly mode should have no manifest ops")
	require.Len(t, plan.Volumes, 1)

	vol := plan.Volumes[0]
	assert.Equal(t, "vsc-abc123", vol.VSCName)
	assert.Equal(t, "my-disk", vol.PVCName)
	assert.Equal(t, "Block", vol.VolumeMode)
	assert.Equal(t, "gzip", vol.Compression)
	assert.Equal(t, int64(5*1024*1024*1024), vol.BytesTotal)

	require.NotNil(t, vol.PVCSpec)
	assert.Equal(t, "my-disk", vol.PVCSpec.Name)
	assert.Equal(t, "5Gi", vol.PVCSpec.StorageRequest)
}

func TestBuildPlan_AllMode_PVCWithDataExcludedFromManifests(t *testing.T) {
	nodes := []archive.NodeRecord{
		{
			ID:      "Snapshot--root",
			Kind:    "Snapshot",
			Name:    "root",
			HasData: true,
			DataRefs: []archive.VolumeDataRef{
				{VSCName: "vsc-001", PVCName: "my-pvc"},
			},
		},
	}
	objects := []objectSpec{
		// PVC with data → should become a VolumeOp.
		{nodeID: "Snapshot--root", apiVersion: "v1", kind: "PersistentVolumeClaim", name: "my-pvc", namespace: "source-ns"},
		// Plain deployment → should become a ManifestOp.
		{nodeID: "Snapshot--root", apiVersion: "apps/v1", kind: "Deployment", name: "my-app", namespace: "source-ns"},
	}
	vols := []archive.VolumeProgressRecord{
		{NodeID: "Snapshot--root", VSCName: "vsc-001", PVCName: "my-pvc", VolumeMode: "Filesystem", Complete: true},
	}

	dir := makeArchive(t, nodes, objects, vols)

	// Create filesystem data dir.
	fsDir := filepath.Join(dir, "data", "Snapshot--root", "my-pvc")
	require.NoError(t, os.MkdirAll(fsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(fsDir, "file.txt"), []byte("hello"), 0o644))

	plan, err := restore.Build(dir, restore.Options{
		TargetNamespace: "target-ns",
		Mode:            restore.ModeAll,
	})

	require.NoError(t, err)
	require.Len(t, plan.Manifests, 1, "only the Deployment should be in manifests")
	assert.Equal(t, "Deployment", plan.Manifests[0].Kind)

	require.Len(t, plan.Volumes, 1, "PVC should be in volumes")
	assert.Equal(t, "my-pvc", plan.Volumes[0].PVCName)
	assert.Equal(t, "Filesystem", plan.Volumes[0].VolumeMode)
}

func TestBuildPlan_ManifestsOnlyMode_PVCWithDataIncludedInManifests(t *testing.T) {
	nodes := []archive.NodeRecord{
		{
			ID:      "Snapshot--root",
			Kind:    "Snapshot",
			Name:    "root",
			HasData: true,
			DataRefs: []archive.VolumeDataRef{
				{VSCName: "vsc-001", PVCName: "my-pvc"},
			},
		},
	}
	objects := []objectSpec{
		{nodeID: "Snapshot--root", apiVersion: "v1", kind: "PersistentVolumeClaim", name: "my-pvc"},
		{nodeID: "Snapshot--root", apiVersion: "apps/v1", kind: "Deployment", name: "my-app"},
	}
	vols := []archive.VolumeProgressRecord{
		{NodeID: "Snapshot--root", VSCName: "vsc-001", PVCName: "my-pvc", VolumeMode: "Block", Complete: true},
	}

	dir := makeArchive(t, nodes, objects, vols)

	plan, err := restore.Build(dir, restore.Options{Mode: restore.ModeManifestsOnly})

	require.NoError(t, err)
	// In manifests-only mode, PVCs are applied like any other object.
	assert.Len(t, plan.Manifests, 2)
	assert.Len(t, plan.Volumes, 0)
}

func TestBuildPlan_NodeFilter(t *testing.T) {
	nodes := []archive.NodeRecord{
		{
			ID:       "Snapshot--root",
			Kind:     "Snapshot",
			Name:     "root",
			Children: []string{"VDSnapshot--disk-a"},
		},
		{
			ID:       "VDSnapshot--disk-a",
			Kind:     "VirtualDiskSnapshot",
			Name:     "disk-a",
			ParentID: "Snapshot--root",
		},
	}
	objects := []objectSpec{
		{nodeID: "Snapshot--root", apiVersion: "v1", kind: "ConfigMap", name: "root-cm"},
		{nodeID: "VDSnapshot--disk-a", apiVersion: "v1", kind: "ConfigMap", name: "disk-cm"},
	}

	dir := makeArchive(t, nodes, objects, nil)

	plan, err := restore.Build(dir, restore.Options{
		NodeFilter: "VDSnapshot--disk-a",
		Mode:       restore.ModeManifestsOnly,
	})

	require.NoError(t, err)
	require.Len(t, plan.Manifests, 1)
	assert.Equal(t, "disk-cm", plan.Manifests[0].Name)
}

func TestBuildPlan_NodeFilterNotFound(t *testing.T) {
	nodes := []archive.NodeRecord{
		{ID: "Snapshot--root", Kind: "Snapshot", Name: "root"},
	}
	dir := makeArchive(t, nodes, nil, nil)

	_, err := restore.Build(dir, restore.Options{
		NodeFilter: "NonExistent--node",
		Mode:       restore.ModeManifestsOnly,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "NonExistent--node")
}

func TestBuildPlan_ObjectFilter(t *testing.T) {
	nodes := []archive.NodeRecord{
		{ID: "Snapshot--root", Kind: "Snapshot", Name: "root"},
	}
	objects := []objectSpec{
		{nodeID: "Snapshot--root", apiVersion: "apps/v1", kind: "Deployment", name: "app-a"},
		{nodeID: "Snapshot--root", apiVersion: "apps/v1", kind: "Deployment", name: "app-b"},
	}

	dir := makeArchive(t, nodes, objects, nil)

	plan, err := restore.Build(dir, restore.Options{
		ObjectFilter: "apps/v1/Deployment/app-a",
		Mode:         restore.ModeManifestsOnly,
	})

	require.NoError(t, err)
	require.Len(t, plan.Manifests, 1)
	assert.Equal(t, "app-a", plan.Manifests[0].Name)
	assert.Equal(t, "Deployment", plan.Manifests[0].Kind)
}

func TestBuildPlan_ObjectFilter_BadFormat(t *testing.T) {
	nodes := []archive.NodeRecord{
		{ID: "Snapshot--root", Kind: "Snapshot", Name: "root"},
	}
	dir := makeArchive(t, nodes, nil, nil)

	_, err := restore.Build(dir, restore.Options{
		ObjectFilter: "wrong-format",
		Mode:         restore.ModeManifestsOnly,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "--object")
}

func TestBuildPlan_IncompleteArchive(t *testing.T) {
	dir := t.TempDir()

	meta := archive.Meta{Magic: archive.Magic, SchemaVersion: archive.SchemaVersion}
	w, err := archive.NewDirWriter(dir, meta)
	require.NoError(t, err)

	_, err = w.Finalize(archive.Index{}, nil, false) // complete=false
	require.NoError(t, err)

	_, err = restore.Build(dir, restore.Options{AllowIncomplete: false})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete")

	// With AllowIncomplete=true it should succeed.
	_, err = restore.Build(dir, restore.Options{AllowIncomplete: true})
	require.NoError(t, err)
}

func TestManifestSortOrder(t *testing.T) {
	nodes := []archive.NodeRecord{
		{ID: "Snapshot--root", Kind: "Snapshot", Name: "root"},
	}
	objects := []objectSpec{
		{nodeID: "Snapshot--root", apiVersion: "apps/v1", kind: "Deployment", name: "dep"},
		{nodeID: "Snapshot--root", apiVersion: "v1", kind: "Namespace", name: "ns"},
		{nodeID: "Snapshot--root", apiVersion: "v1", kind: "PersistentVolumeClaim", name: "pvc"},
		{nodeID: "Snapshot--root", apiVersion: "apiextensions.k8s.io/v1", kind: "CustomResourceDefinition", name: "crd"},
	}

	dir := makeArchive(t, nodes, objects, nil)

	plan, err := restore.Build(dir, restore.Options{Mode: restore.ModeManifestsOnly})
	require.NoError(t, err)
	require.Len(t, plan.Manifests, 4)

	// Expected order: Namespace(0) < CRD(1) < PVC(10) < Deployment(40).
	assert.Equal(t, "Namespace", plan.Manifests[0].Kind)
	assert.Equal(t, "CustomResourceDefinition", plan.Manifests[1].Kind)
	assert.Equal(t, "PersistentVolumeClaim", plan.Manifests[2].Kind)
	assert.Equal(t, "Deployment", plan.Manifests[3].Kind)
}

// Tests for BytesToStorage.

func TestBytesToStorage(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "10Gi"},
		{-1, "10Gi"},
		{1, "1Gi"},
		{1024 * 1024 * 1024, "1Gi"},
		{1024*1024*1024 + 1, "2Gi"},
		{10 * 1024 * 1024 * 1024, "10Gi"},
		{10*1024*1024*1024 + 1, "11Gi"},
	}

	for _, tc := range tests {
		got := restore.BytesToStorage(tc.bytes)
		assert.Equal(t, tc.want, got, "BytesToStorage(%d)", tc.bytes)
	}
}

// Tests for PVC spec extraction.

func TestExtractPVCSpec_FromManifest(t *testing.T) {
	nodes := []archive.NodeRecord{
		{
			ID:      "Snapshot--root",
			Kind:    "Snapshot",
			Name:    "root",
			HasData: true,
			DataRefs: []archive.VolumeDataRef{
				{VSCName: "vsc-001", PVCName: "my-pvc"},
			},
		},
	}
	objects := []objectSpec{
		{
			nodeID:     "Snapshot--root",
			apiVersion: "v1",
			kind:       "PersistentVolumeClaim",
			name:       "my-pvc",
			namespace:  "ns",
			spec: map[string]any{
				"accessModes":      []any{"ReadWriteOnce", "ReadOnlyMany"},
				"storageClassName": "ceph-rbd",
				"volumeMode":       "Block",
				"resources":        map[string]any{"requests": map[string]any{"storage": "20Gi"}},
			},
		},
	}
	vols := []archive.VolumeProgressRecord{
		{
			NodeID:     "Snapshot--root",
			VSCName:    "vsc-001",
			PVCName:    "my-pvc",
			VolumeMode: "Block",
			Complete:   true,
			BytesTotal: 20 * 1024 * 1024 * 1024,
		},
	}

	dir := makeArchive(t, nodes, objects, vols)

	// Create dummy block data file.
	writeGzipFile(t, filepath.Join(dir, "data", "Snapshot--root", "vsc-001.img.gz"), []byte("x"))

	plan, err := restore.Build(dir, restore.Options{Mode: restore.ModeDataOnly})
	require.NoError(t, err)
	require.Len(t, plan.Volumes, 1)

	spec := plan.Volumes[0].PVCSpec
	require.NotNil(t, spec)
	assert.Equal(t, "my-pvc", spec.Name)
	assert.Equal(t, []string{"ReadWriteOnce", "ReadOnlyMany"}, spec.AccessModes)
	assert.Equal(t, "ceph-rbd", spec.StorageClassName)
	assert.Equal(t, "Block", spec.VolumeMode)
	assert.Equal(t, "20Gi", spec.StorageRequest)
}

// Tests for stripPVCBindingFields.

func TestStripPVCBindingFields(t *testing.T) {
	nodes := []archive.NodeRecord{
		{
			ID:      "Snapshot--root",
			Kind:    "Snapshot",
			Name:    "root",
			HasData: true,
			DataRefs: []archive.VolumeDataRef{
				// ONLY this PVC has data → goes to volumeOps.
				{VSCName: "vsc-bound", PVCName: "bound-pvc"},
			},
		},
	}

	objects := []objectSpec{
		// bound-pvc has data → goes to DataImport, not manifests.
		{
			nodeID:     "Snapshot--root",
			apiVersion: "v1",
			kind:       "PersistentVolumeClaim",
			name:       "bound-pvc",
			namespace:  "ns",
			spec: map[string]any{
				"volumeName":       "pvc-old-volume-id",
				"storageClassName": "fast",
				"accessModes":      []any{"ReadWriteOnce"},
				"resources":        map[string]any{"requests": map[string]any{"storage": "1Gi"}},
			},
		},
		// unbound-pvc has NO data → goes to manifests WITH binding stripped.
		{
			nodeID:     "Snapshot--root",
			apiVersion: "v1",
			kind:       "PersistentVolumeClaim",
			name:       "unbound-pvc",
			namespace:  "ns",
			spec: map[string]any{
				"volumeName":       "pvc-other-old-id",
				"storageClassName": "fast",
				"accessModes":      []any{"ReadWriteOnce"},
				"resources":        map[string]any{"requests": map[string]any{"storage": "2Gi"}},
			},
		},
	}

	vols := []archive.VolumeProgressRecord{
		{NodeID: "Snapshot--root", VSCName: "vsc-bound", PVCName: "bound-pvc", VolumeMode: "Block", Complete: true},
	}

	dir := makeArchive(t, nodes, objects, vols)
	writeGzipFile(t, filepath.Join(dir, "data", "Snapshot--root", "vsc-bound.img.gz"), []byte("x"))

	plan, err := restore.Build(dir, restore.Options{Mode: restore.ModeAll})
	require.NoError(t, err)

	// bound-pvc → volumeOps (DataImport)
	require.Len(t, plan.Volumes, 1)
	assert.Equal(t, "bound-pvc", plan.Volumes[0].PVCName)

	// unbound-pvc → manifestOps with volumeName stripped
	require.Len(t, plan.Manifests, 1)
	assert.Equal(t, "unbound-pvc", plan.Manifests[0].Name)

	var spec map[string]any
	require.NoError(t, json.Unmarshal(plan.Manifests[0].Data, &spec))

	pvcSpec, _ := spec["spec"].(map[string]any)
	require.NotNil(t, pvcSpec)
	assert.Empty(t, pvcSpec["volumeName"], "volumeName must be stripped from unbound-pvc manifest")
}

func TestExtractPVCSpec_FallbackWhenNoPVCManifest(t *testing.T) {
	// Node has volume data but no PVC manifest was captured.
	nodes := []archive.NodeRecord{
		{
			ID:      "Snapshot--root",
			Kind:    "Snapshot",
			Name:    "root",
			HasData: true,
			DataRefs: []archive.VolumeDataRef{
				{VSCName: "vsc-002"},
			},
		},
	}
	vols := []archive.VolumeProgressRecord{
		{
			NodeID:     "Snapshot--root",
			VSCName:    "vsc-002",
			VolumeMode: "Block",
			Complete:   true,
			BytesTotal: 2 * 1024 * 1024 * 1024,
		},
	}

	dir := makeArchive(t, nodes, nil, vols)
	writeGzipFile(t, filepath.Join(dir, "data", "Snapshot--root", "vsc-002.img.gz"), []byte("x"))

	plan, err := restore.Build(dir, restore.Options{Mode: restore.ModeDataOnly})
	require.NoError(t, err)
	require.Len(t, plan.Volumes, 1)

	spec := plan.Volumes[0].PVCSpec
	require.NotNil(t, spec)
	// Fallback name: "restore-<vscName>"
	assert.Equal(t, "restore-vsc-002", spec.Name)
	assert.Equal(t, []string{"ReadWriteOnce"}, spec.AccessModes)
	assert.Equal(t, "Block", spec.VolumeMode)
	assert.Equal(t, "2Gi", spec.StorageRequest)
}

// Tests for ReadObjectBlob roundtrip.

func TestReadObjectBlob_Roundtrip(t *testing.T) {
	nodes := []archive.NodeRecord{
		{ID: "Snapshot--root", Kind: "Snapshot", Name: "root"},
	}
	raw := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test-cm","namespace":"default"}}`)
	objects := []objectSpec{
		{nodeID: "Snapshot--root", apiVersion: "v1", kind: "ConfigMap", name: "test-cm", namespace: "default"},
	}

	dir := makeArchive(t, nodes, objects, nil)

	r, err := archive.OpenDir(dir)
	require.NoError(t, err)

	nodes2, err := r.Nodes()
	require.NoError(t, err)
	require.Len(t, nodes2, 1)

	var blob []byte
	err = r.ForEachObject(func(rec archive.ObjectRecord) error {
		b, e := r.ReadObjectBlob(rec)
		if e != nil {
			return e
		}
		blob = b
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, blob, "expected at least one blob")

	// The stored blob is the canonical re-marshalled form, not the exact original.
	var stored, original map[string]any
	require.NoError(t, json.Unmarshal(blob, &stored))
	require.NoError(t, json.Unmarshal(raw, &original))
	assert.Equal(t, original["kind"], stored["kind"])
	assert.Equal(t, original["apiVersion"], stored["apiVersion"])
}
