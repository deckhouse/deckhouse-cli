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

package archive_test

import (
	"testing"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

func buildTestArchive(t *testing.T) (dir string, w *archive.DirWriter) {
	t.Helper()

	dir = t.TempDir()

	meta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "a-reader-test",
		CreatedAt:     time.Now().UTC(),
		CreatedBy:     archive.Creator{Tool: "d8", Version: "test"},
		Source: archive.Source{
			Cluster:   archive.Cluster{Server: "https://test.example.com"},
			Namespace: "demo",
			RootSnapshot: archive.SnapshotRef{
				APIVersion: "storage.deckhouse.io/v1alpha1",
				Kind:       "Snapshot",
				Resource:   "snapshots",
				Name:       "my-snap",
			},
			RootSnapshotContent: archive.SnapshotContentRef{
				APIVersion: "storage.deckhouse.io/v1alpha1",
				Kind:       "SnapshotContent",
				Name:       "snap-content-root",
			},
		},
		Selection: archive.Selection{
			Mode:            archive.SelectionFull,
			RootNodeID:      "Snapshot--my-snap",
			SelectedNodeIDs: []string{"Snapshot--my-snap"},
		},
	}

	w, err := archive.NewDirWriter(dir, meta)
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	return dir, w
}

func TestOpenDir_Missing(t *testing.T) {
	_, err := archive.OpenDir("/tmp/definitely-does-not-exist-xyz")
	if err == nil {
		t.Fatal("expected error for missing directory, got nil")
	}
}

func TestOpenDir_NotAnArchive(t *testing.T) {
	dir := t.TempDir()

	_, err := archive.OpenDir(dir)
	if err == nil {
		t.Fatal("expected error for directory without archive.json, got nil")
	}
}

func TestDirReader_Meta(t *testing.T) {
	dir, w := buildTestArchive(t)

	idx := archive.Index{SchemaVersion: archive.SchemaVersion}

	if _, err := w.Finalize(idx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	r, err := archive.OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}

	meta, err := r.Meta()
	if err != nil {
		t.Fatalf("Meta: %v", err)
	}

	if meta.Magic != archive.Magic {
		t.Fatalf("meta.Magic = %q, want %q", meta.Magic, archive.Magic)
	}

	if meta.ArchiveID != "a-reader-test" {
		t.Fatalf("meta.ArchiveID = %q, want %q", meta.ArchiveID, "a-reader-test")
	}

	if meta.Selection.RootNodeID != "Snapshot--my-snap" {
		t.Fatalf("meta.Selection.RootNodeID = %q", meta.Selection.RootNodeID)
	}
}

func TestDirReader_Index(t *testing.T) {
	dir, w := buildTestArchive(t)

	if err := w.AppendNode(archive.NodeRecord{
		ID: "Snapshot--my-snap", Kind: "Snapshot", Name: "my-snap", Children: []string{},
	}); err != nil {
		t.Fatalf("AppendNode: %v", err)
	}

	idx := archive.Index{
		SchemaVersion: archive.SchemaVersion,
		Capabilities:  archive.IndexCapabilities{Manifests: true},
	}

	if _, err := w.Finalize(idx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	r, err := archive.OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}

	index, err := r.Index()
	if err != nil {
		t.Fatalf("Index: %v", err)
	}

	if !index.Capabilities.Manifests {
		t.Fatal("index.Capabilities.Manifests should be true")
	}

	if index.Summary.Nodes != 1 {
		t.Fatalf("index.Summary.Nodes = %d, want 1", index.Summary.Nodes)
	}
}

func TestDirReader_Nodes(t *testing.T) {
	dir, w := buildTestArchive(t)

	nodes := []archive.NodeRecord{
		{ID: "Snapshot--my-snap", Kind: "Snapshot", Name: "my-snap", Children: []string{"Snapshot--child"}},
		{ID: "Snapshot--child", Kind: "Snapshot", Name: "child", ParentID: "Snapshot--my-snap", Children: []string{}},
	}

	for _, nr := range nodes {
		if err := w.AppendNode(nr); err != nil {
			t.Fatalf("AppendNode %s: %v", nr.ID, err)
		}
	}

	if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	r, err := archive.OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}

	got, err := r.Nodes()
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("Nodes: got %d records, want 2", len(got))
	}

	if got[0].ID != "Snapshot--my-snap" {
		t.Fatalf("Nodes[0].ID = %q, want %q", got[0].ID, "Snapshot--my-snap")
	}

	if got[1].ID != "Snapshot--child" {
		t.Fatalf("Nodes[1].ID = %q, want %q", got[1].ID, "Snapshot--child")
	}
}

func TestDirReader_ForEachObject(t *testing.T) {
	dir, w := buildTestArchive(t)

	raws := [][]byte{
		[]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"demo"}}`),
		[]byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"app","namespace":"demo"}}`),
	}

	for _, raw := range raws {
		rec, err := w.AddObject("Snapshot--my-snap", raw)
		if err != nil {
			t.Fatalf("AddObject: %v", err)
		}

		if err := w.AppendObject(rec); err != nil {
			t.Fatalf("AppendObject: %v", err)
		}
	}

	if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	r, err := archive.OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir: %v", err)
	}

	var count int

	if err := r.ForEachObject(func(_ archive.ObjectRecord) error {
		count++

		return nil
	}); err != nil {
		t.Fatalf("ForEachObject: %v", err)
	}

	if count != 2 {
		t.Fatalf("ForEachObject: got %d objects, want 2", count)
	}
}
