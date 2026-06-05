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
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// Layout tests.

func TestBlobPath(t *testing.T) {
	tests := []struct {
		name    string
		digest  string
		want    string
		wantErr bool
	}{
		{
			name:   "standard digest",
			digest: "a8e9abcdef1234567890abcdef1234567890abcdef1234567890abcdef123456",
			want:   filepath.Join("manifests", "objects", "a8", "e9", "o-a8e9abcdef1234567890abcdef1234567890abcdef1234567890abcdef123456.json.gz"),
		},
		{
			name:   "all zeros",
			digest: "0000111122223333444455556666777788889999aaaabbbbccccddddeeeeffff",
			want:   filepath.Join("manifests", "objects", "00", "00", "o-0000111122223333444455556666777788889999aaaabbbbccccddddeeeeffff.json.gz"),
		},
		{
			name:    "too short",
			digest:  "abc",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := archive.BlobPath(tc.digest)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tc.want {
				t.Fatalf("BlobPath(%q) = %q, want %q", tc.digest, got, tc.want)
			}
		})
	}
}

func TestNodeID(t *testing.T) {
	got := archive.NodeID("Snapshot", "ns-snap")
	if got != "Snapshot--ns-snap" {
		t.Fatalf("NodeID = %q, want %q", got, "Snapshot--ns-snap")
	}
}

func TestAggregatedPath(t *testing.T) {
	got := archive.AggregatedPath("demo", "snapshots", "my-snap")
	want := "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/demo/snapshots/my-snap/manifests"

	if got != want {
		t.Fatalf("AggregatedPath = %q, want %q", got, want)
	}
}

// Writer roundtrip test.

func TestDirWriterRoundtrip(t *testing.T) {
	dir := t.TempDir()

	meta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "a-test-001",
		CreatedAt:     time.Now().UTC(),
		CreatedBy:     archive.Creator{Tool: "d8", Version: "test"},
		Source: archive.Source{
			Cluster:   archive.Cluster{Server: "https://test.example.com"},
			Namespace: "demo",
			RootSnapshot: archive.SnapshotRef{
				APIVersion: "storage.deckhouse.io/v1alpha1",
				Kind:       "Snapshot",
				Resource:   "snapshots",
				Name:       "ns-snap",
			},
			RootSnapshotContent: archive.SnapshotContentRef{
				APIVersion: "storage.deckhouse.io/v1alpha1",
				Kind:       "SnapshotContent",
				Name:       "snapcontent-root",
			},
		},
		Selection: archive.Selection{
			Mode:            archive.SelectionFull,
			RootNodeID:      "Snapshot--ns-snap",
			SelectedNodeIDs: []string{"Snapshot--ns-snap"},
		},
	}

	w, err := archive.NewDirWriter(dir, meta)
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	nodeRec := archive.NodeRecord{
		ID:         "Snapshot--ns-snap",
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "ns-snap",
		Namespace:  "demo",
		Children:   []string{},
		HasData:    false,
	}

	rawCM := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"demo"}}`)
	rawDeploy := []byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"my-deploy","namespace":"demo"}}`)

	obj1, err := w.AddObject("Snapshot--ns-snap", rawCM)
	if err != nil {
		t.Fatalf("AddObject(cm): %v", err)
	}

	obj2, err := w.AddObject("Snapshot--ns-snap", rawDeploy)
	if err != nil {
		t.Fatalf("AddObject(deploy): %v", err)
	}

	// Dedup: same object again must return the same digest without rewriting the blob.
	obj1b, err := w.AddObject("Snapshot--ns-snap", rawCM)
	if err != nil {
		t.Fatalf("AddObject(cm dup): %v", err)
	}

	if obj1b.Digest != obj1.Digest {
		t.Fatalf("dedup: expected same digest, got %s != %s", obj1b.Digest, obj1.Digest)
	}

	if obj1b.Size == 0 {
		t.Fatalf("dedup: expected non-zero Size on deduplicated record")
	}

	// Record progress for the node (only 2 unique objects despite 3 AddObject calls).
	prec := archive.ProgressRecord{
		NodeID:  "Snapshot--ns-snap",
		Objects: []archive.ObjectRecord{obj1, obj2},
	}

	if err := w.AppendProgress(prec); err != nil {
		t.Fatalf("AppendProgress: %v", err)
	}

	idx := archive.Index{
		SchemaVersion: archive.SchemaVersion,
		Capabilities: archive.IndexCapabilities{
			Manifests: true,
		},
		ManifestModel: archive.IndexManifestModel{
			Format:      "json",
			Compression: "gzip-per-object",
			SourceKind:  "aggregated-subtree",
		},
		Catalogs: archive.IndexCatalogs{
			Nodes:   "indexes/nodes.jsonl",
			Objects: "indexes/objects.jsonl",
		},
		Paths: archive.IndexPaths{
			ManifestsRoot: "manifests/objects",
			DataRoot:      "data",
		},
	}

	if _, err := w.Finalize(idx, []archive.NodeRecord{nodeRec}, true); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// COMPLETE sentinel must exist.
	if !archive.IsComplete(dir) {
		t.Fatal("expected COMPLETE sentinel, not found")
	}

	// archive.json must be valid JSON with magic field.
	checkJSONFile(t, filepath.Join(dir, "archive.json"), func(m map[string]any) {
		if m["magic"] != archive.Magic {
			t.Fatalf("archive.json magic = %v, want %q", m["magic"], archive.Magic)
		}
	})

	// index.json must have summary with correct counts.
	checkJSONFile(t, filepath.Join(dir, "index.json"), func(m map[string]any) {
		summary, ok := m["summary"].(map[string]any)
		if !ok {
			t.Fatalf("index.json: missing summary")
		}

		if summary["nodes"].(float64) != 1 {
			t.Fatalf("index.json summary.nodes = %v, want 1", summary["nodes"])
		}

		if summary["objects"].(float64) != 2 {
			t.Fatalf("index.json summary.objects = %v, want 2", summary["objects"])
		}

		if summary["complete"] != true {
			t.Fatalf("index.json summary.complete = %v, want true", summary["complete"])
		}
	})

	// nodes.jsonl must have exactly 1 line.
	checkJSONLCount(t, filepath.Join(dir, "indexes", "nodes.jsonl"), 1)

	// objects.jsonl must have 2 lines (dedup: only 2 unique objects were appended).
	checkJSONLCount(t, filepath.Join(dir, "indexes", "objects.jsonl"), 2)

	// Blob for CM must exist and be valid gzip JSON.
	blobPath, _ := archive.BlobPath(obj1.Digest)
	checkGzipBlob(t, filepath.Join(dir, blobPath), rawCM)

	// NodeID on the returned record must equal the node ID passed in.
	if obj1.NodeID != "Snapshot--ns-snap" {
		t.Fatalf("obj1.NodeID = %q, want %q", obj1.NodeID, "Snapshot--ns-snap")
	}
}

// Identity and resume tests.

func TestArchiveIdentity_Equal(t *testing.T) {
	base := archive.ArchiveIdentity{
		Namespace:       "demo",
		Snapshot:        "my-snap",
		Mode:            archive.SelectionFull,
		RootNodeID:      "Snapshot--my-snap",
		SelectedNodeIDs: []string{"Snapshot--my-snap"},
	}

	t.Run("equal_identical", func(t *testing.T) {
		if !base.Equal(base) {
			t.Fatal("identical identities should be equal")
		}
	})

	t.Run("equal_sorted_nodes", func(t *testing.T) {
		other := base
		other.SelectedNodeIDs = []string{"Snapshot--my-snap"}

		if !base.Equal(other) {
			t.Fatal("same sorted node IDs should be equal")
		}
	})

	t.Run("mismatch_namespace", func(t *testing.T) {
		other := base
		other.Namespace = "other-ns"

		if base.Equal(other) {
			t.Fatal("different namespace should not be equal")
		}
	})

	t.Run("mismatch_snapshot", func(t *testing.T) {
		other := base
		other.Snapshot = "other-snap"

		if base.Equal(other) {
			t.Fatal("different snapshot should not be equal")
		}
	})

	t.Run("mismatch_mode", func(t *testing.T) {
		other := base
		other.Mode = archive.SelectionSubtree

		if base.Equal(other) {
			t.Fatal("different mode should not be equal")
		}
	})

	t.Run("mismatch_object_filter", func(t *testing.T) {
		other := base
		other.ObjectFilter = "v1/ConfigMap/cm1"

		if base.Equal(other) {
			t.Fatal("different objectFilter should not be equal")
		}
	})

	t.Run("mismatch_selected_nodes", func(t *testing.T) {
		other := base
		other.SelectedNodeIDs = []string{"Snapshot--my-snap", "Snapshot--child"}

		if base.Equal(other) {
			t.Fatal("different selectedNodeIDs should not be equal")
		}
	})
}

func TestIdentityOf_SortsNodeIDs(t *testing.T) {
	meta := archive.Meta{
		Source: archive.Source{
			Namespace:    "demo",
			RootSnapshot: archive.SnapshotRef{Name: "snap"},
		},
		Selection: archive.Selection{
			Mode:            archive.SelectionFull,
			RootNodeID:      "Snapshot--snap",
			SelectedNodeIDs: []string{"Snapshot--z", "Snapshot--a", "Snapshot--m"},
		},
	}

	id := archive.IdentityOf(meta)

	want := []string{"Snapshot--a", "Snapshot--m", "Snapshot--z"}

	for i, got := range id.SelectedNodeIDs {
		if got != want[i] {
			t.Fatalf("SelectedNodeIDs[%d] = %q, want %q", i, got, want[i])
		}
	}
}

func TestOpenForResume(t *testing.T) {
	dir := t.TempDir()

	rawCM := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"demo"}}`)

	meta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "resume-test-001",
		CreatedAt:     time.Now().UTC(),
		CreatedBy:     archive.Creator{Tool: "d8", Version: "test"},
		Source: archive.Source{
			Cluster:             archive.Cluster{Server: "https://test.example.com"},
			Namespace:           "demo",
			RootSnapshot:        archive.SnapshotRef{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "Snapshot", Resource: "snapshots", Name: "ns-snap"},
			RootSnapshotContent: archive.SnapshotContentRef{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "SnapshotContent", Name: "sc-root"},
		},
		Selection: archive.Selection{
			Mode:            archive.SelectionFull,
			RootNodeID:      "Snapshot--ns-snap",
			SelectedNodeIDs: []string{"Snapshot--ns-snap"},
		},
	}

	// First run: write one node then simulate a crash (no Finalize, no COMPLETE).
	w1, err := archive.NewDirWriter(dir, meta)
	if err != nil {
		t.Fatalf("first NewDirWriter: %v", err)
	}

	obj1, err := w1.AddObject("Snapshot--ns-snap", rawCM)
	if err != nil {
		t.Fatalf("first AddObject: %v", err)
	}

	prec1 := archive.ProgressRecord{NodeID: "Snapshot--ns-snap", ContentRef: "sc-root", Objects: []archive.ObjectRecord{obj1}}

	if err := w1.AppendProgress(prec1); err != nil {
		t.Fatalf("first AppendProgress: %v", err)
	}

	// Simulate crash: do NOT call Finalize. Just close the progress file.
	w1.Close()

	if archive.IsComplete(dir) {
		t.Fatal("COMPLETE should not exist after simulated crash")
	}

	// Second run: open for resume.
	w2, existing, _, err := archive.OpenForResume(dir)
	if err != nil {
		t.Fatalf("OpenForResume: %v", err)
	}

	if len(existing) != 1 {
		t.Fatalf("expected 1 existing progress record, got %d", len(existing))
	}

	rec, ok := existing["Snapshot--ns-snap"]
	if !ok {
		t.Fatal("missing progress record for Snapshot--ns-snap")
	}

	if rec.ContentRef != "sc-root" {
		t.Fatalf("ContentRef = %q, want sc-root", rec.ContentRef)
	}

	// Finalize without downloading anything new (resume carries forward).
	nodeRec := archive.NodeRecord{ID: "Snapshot--ns-snap", Kind: "Snapshot", Name: "ns-snap", Children: []string{}}

	summary, err := w2.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, []archive.NodeRecord{nodeRec}, true)
	if err != nil {
		t.Fatalf("resume Finalize: %v", err)
	}

	if !archive.IsComplete(dir) {
		t.Fatal("COMPLETE should exist after successful resume")
	}

	if summary.Objects != 1 {
		t.Fatalf("summary.Objects = %d, want 1", summary.Objects)
	}
}

func TestFinalizeIncompleteMissingComplete(t *testing.T) {
	dir := t.TempDir()

	meta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "incomplete-test",
		CreatedAt:     time.Now().UTC(),
		CreatedBy:     archive.Creator{Tool: "d8", Version: "test"},
		Source: archive.Source{
			Namespace:    "demo",
			RootSnapshot: archive.SnapshotRef{Name: "ns-snap"},
		},
		Selection: archive.Selection{Mode: archive.SelectionFull, RootNodeID: "Snapshot--ns-snap"},
	}

	w, err := archive.NewDirWriter(dir, meta)
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	// Finalize with complete=false → COMPLETE must NOT exist.
	if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, nil, false); err != nil {
		t.Fatalf("Finalize incomplete: %v", err)
	}

	if archive.IsComplete(dir) {
		t.Fatal("COMPLETE must not exist for incomplete archive")
	}
}

func TestProgressFile_TruncatedLineTolerance(t *testing.T) {
	dir := t.TempDir()

	meta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "trunc-test",
		CreatedAt:     time.Now().UTC(),
		CreatedBy:     archive.Creator{Tool: "d8", Version: "test"},
		Source: archive.Source{
			Namespace:    "demo",
			RootSnapshot: archive.SnapshotRef{Name: "snap"},
		},
		Selection: archive.Selection{Mode: archive.SelectionFull, RootNodeID: "Snapshot--snap"},
	}

	w, err := archive.NewDirWriter(dir, meta)
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	raw := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"demo"}}`)

	obj, err := w.AddObject("Snapshot--snap", raw)
	if err != nil {
		t.Fatalf("AddObject: %v", err)
	}

	prec := archive.ProgressRecord{NodeID: "Snapshot--snap", ContentRef: "sc-1", Objects: []archive.ObjectRecord{obj}}

	if err := w.AppendProgress(prec); err != nil {
		t.Fatalf("AppendProgress: %v", err)
	}

	w.Close()

	// Append a truncated line to simulate a crash mid-write.
	progressPath := filepath.Join(dir, "indexes", "progress.jsonl")

	f, err := os.OpenFile(progressPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open progress for truncation test: %v", err)
	}

	_, _ = f.WriteString(`{"nodeId":"Snapshot--snap","contentRef":"sc-2","objects":[`) // truncated!
	f.Close()

	// OpenForResume should tolerate the truncated line.
	w2, existing, _, err := archive.OpenForResume(dir)
	if err != nil {
		t.Fatalf("OpenForResume with truncated progress: %v", err)
	}

	defer w2.Close()

	if len(existing) != 1 {
		t.Fatalf("expected 1 valid progress record (truncated line skipped), got %d", len(existing))
	}

	if existing["Snapshot--snap"].ContentRef != "sc-1" {
		t.Fatalf("expected ContentRef=sc-1 from first valid record, got %q", existing["Snapshot--snap"].ContentRef)
	}
}

func TestWipeDir(t *testing.T) {
	dir := t.TempDir()

	meta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "wipe-test",
		CreatedAt:     time.Now().UTC(),
		Source: archive.Source{
			Namespace:    "demo",
			RootSnapshot: archive.SnapshotRef{Name: "snap"},
		},
		Selection: archive.Selection{Mode: archive.SelectionFull, RootNodeID: "Snapshot--snap"},
	}

	w, err := archive.NewDirWriter(dir, meta)
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	raw := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"demo"}}`)

	obj, err := w.AddObject("Snapshot--snap", raw)
	if err != nil {
		t.Fatalf("AddObject: %v", err)
	}

	prec := archive.ProgressRecord{NodeID: "Snapshot--snap", Objects: []archive.ObjectRecord{obj}}

	if err := w.AppendProgress(prec); err != nil {
		t.Fatalf("AppendProgress: %v", err)
	}

	nodeRec := archive.NodeRecord{ID: "Snapshot--snap", Children: []string{}}

	if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, []archive.NodeRecord{nodeRec}, true); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	if !archive.IsComplete(dir) {
		t.Fatal("expected COMPLETE before wipe")
	}

	if err := archive.WipeDir(dir); err != nil {
		t.Fatalf("WipeDir: %v", err)
	}

	if archive.IsComplete(dir) {
		t.Fatal("COMPLETE should be removed after wipe")
	}

	// archive.json should be gone.
	if _, err := os.Stat(filepath.Join(dir, "archive.json")); !os.IsNotExist(err) {
		t.Fatal("archive.json should be removed after wipe")
	}

	// Dir itself should still exist.
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("directory should still exist after wipe: %v", err)
	}
}

func TestOrphanBlobGC(t *testing.T) {
	dir := t.TempDir()

	meta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "gc-test",
		CreatedAt:     time.Now().UTC(),
		Source: archive.Source{
			Namespace:    "demo",
			RootSnapshot: archive.SnapshotRef{Name: "snap"},
		},
		Selection: archive.Selection{Mode: archive.SelectionFull, RootNodeID: "Snapshot--snap"},
	}

	w, err := archive.NewDirWriter(dir, meta)
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	cm1 := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"demo"}}`)
	cm2 := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm2","namespace":"demo"}}`)

	obj1, err := w.AddObject("Snapshot--snap", cm1)
	if err != nil {
		t.Fatalf("AddObject cm1: %v", err)
	}

	obj2, err := w.AddObject("Snapshot--snap", cm2)
	if err != nil {
		t.Fatalf("AddObject cm2: %v", err)
	}

	// Only record obj1 in progress; obj2's blob should be GC'd.
	prec := archive.ProgressRecord{NodeID: "Snapshot--snap", Objects: []archive.ObjectRecord{obj1}}

	if err := w.AppendProgress(prec); err != nil {
		t.Fatalf("AppendProgress: %v", err)
	}

	nodeRec := archive.NodeRecord{ID: "Snapshot--snap", Children: []string{}}

	if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, []archive.NodeRecord{nodeRec}, true); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// obj1 blob must still exist.
	if _, err := os.Stat(filepath.Join(dir, obj1.Blob)); err != nil {
		t.Fatalf("obj1 blob must survive GC: %v", err)
	}

	// obj2 blob must have been GC'd.
	if _, err := os.Stat(filepath.Join(dir, obj2.Blob)); err == nil {
		t.Fatal("obj2 blob should have been removed by GC")
	}
}

// checkJSONFile reads a JSON file and invokes fn with the decoded map.
func checkJSONFile(t *testing.T, path string, fn func(map[string]any)) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var m map[string]any

	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	fn(m)
}

// checkJSONLCount counts non-empty lines in a JSONL file.
func checkJSONLCount(t *testing.T, path string, want int) {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}

	defer f.Close()

	count := 0
	sc := bufio.NewScanner(f)

	for sc.Scan() {
		if line := sc.Text(); line != "" {
			count++
		}
	}

	if sc.Err() != nil {
		t.Fatalf("scan %s: %v", path, sc.Err())
	}

	if count != want {
		t.Fatalf("%s: got %d lines, want %d", path, count, want)
	}
}

// checkGzipBlob reads a gzip blob and asserts its content matches wantRaw (after JSON canonicalisation).
func checkGzipBlob(t *testing.T, path string, wantRaw []byte) {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open blob %s: %v", path, err)
	}

	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip.NewReader(%s): %v", path, err)
	}

	defer gz.Close()

	got, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read gzip body %s: %v", path, err)
	}

	var gotV, wantV any

	if err := json.Unmarshal(got, &gotV); err != nil {
		t.Fatalf("parse blob JSON: %v", err)
	}

	if err := json.Unmarshal(wantRaw, &wantV); err != nil {
		t.Fatalf("parse want JSON: %v", err)
	}

	gotB, _ := json.Marshal(gotV)
	wantB, _ := json.Marshal(wantV)

	if string(gotB) != string(wantB) {
		t.Fatalf("blob content mismatch:\ngot:  %s\nwant: %s", gotB, wantB)
	}
}
