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

package listing_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/listing"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

func testLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// buildTestArchiveDir writes a two-node archive and returns the directory.
func buildTestArchiveDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()

	meta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "a-list-test",
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
				Name:       "sc-root",
			},
		},
		Selection: archive.Selection{
			Mode:            archive.SelectionFull,
			RootNodeID:      "Snapshot--my-snap",
			SelectedNodeIDs: []string{"Snapshot--my-snap", "Snapshot--child"},
		},
	}

	w, err := archive.NewDirWriter(dir, meta)
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	nodes := []archive.NodeRecord{
		{ID: "Snapshot--my-snap", Kind: "Snapshot", Name: "my-snap", Namespace: "demo", Children: []string{"Snapshot--child"}},
		{ID: "Snapshot--child", Kind: "Snapshot", Name: "child", Namespace: "demo", ParentID: "Snapshot--my-snap", Children: []string{}},
	}

	type rawItem struct {
		nodeID string
		raw    []byte
	}

	raws := []rawItem{
		{"Snapshot--my-snap", []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"demo"}}`)},
		{"Snapshot--my-snap", []byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"app","namespace":"demo"}}`)},
		{"Snapshot--child", []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm2","namespace":"demo"}}`)},
	}

	// Group objects by node and write progress records.
	nodeObjs := make(map[string][]archive.ObjectRecord)

	for _, item := range raws {
		rec, err := w.AddObject(item.nodeID, item.raw)
		if err != nil {
			t.Fatalf("AddObject: %v", err)
		}

		nodeObjs[item.nodeID] = append(nodeObjs[item.nodeID], rec)
	}

	for _, nr := range nodes {
		prec := archive.ProgressRecord{NodeID: nr.ID, Objects: nodeObjs[nr.ID]}

		if err := w.AppendProgress(prec); err != nil {
			t.Fatalf("AppendProgress %s: %v", nr.ID, err)
		}
	}

	if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, nodes, true); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	return dir
}

func TestBuildFromArchive_Tree(t *testing.T) {
	dir := buildTestArchiveDir(t)

	tree, err := listing.BuildFromArchive(listing.Options{ArchiveDir: dir}, testLog())
	if err != nil {
		t.Fatalf("BuildFromArchive: %v", err)
	}

	if tree.Source.Kind != "archive" {
		t.Fatalf("tree.Source.Kind = %q, want archive", tree.Source.Kind)
	}

	if tree.Root == nil {
		t.Fatal("tree.Root is nil")
	}

	if tree.Root.ID != "Snapshot--my-snap" {
		t.Fatalf("tree.Root.ID = %q, want Snapshot--my-snap", tree.Root.ID)
	}

	if len(tree.Root.Children) != 1 {
		t.Fatalf("root has %d children, want 1", len(tree.Root.Children))
	}

	if tree.Root.Children[0].ID != "Snapshot--child" {
		t.Fatalf("child.ID = %q, want Snapshot--child", tree.Root.Children[0].ID)
	}

	if tree.Root.ObjectCount != 2 {
		t.Fatalf("root.ObjectCount = %d, want 2", tree.Root.ObjectCount)
	}

	if tree.Root.Children[0].ObjectCount != 1 {
		t.Fatalf("child.ObjectCount = %d, want 1", tree.Root.Children[0].ObjectCount)
	}

	// Objects not populated unless WithObjects is set.
	if len(tree.Root.Objects) != 0 {
		t.Fatalf("expected no Objects without WithObjects, got %d", len(tree.Root.Objects))
	}
}

func TestBuildFromArchive_WithObjects(t *testing.T) {
	dir := buildTestArchiveDir(t)

	tree, err := listing.BuildFromArchive(listing.Options{ArchiveDir: dir, WithObjects: true}, testLog())
	if err != nil {
		t.Fatalf("BuildFromArchive: %v", err)
	}

	if len(tree.Root.Objects) != 2 {
		t.Fatalf("root.Objects count = %d, want 2", len(tree.Root.Objects))
	}

	if len(tree.Root.Children[0].Objects) != 1 {
		t.Fatalf("child.Objects count = %d, want 1", len(tree.Root.Children[0].Objects))
	}
}

func TestBuildFromArchive_NodeFilter(t *testing.T) {
	dir := buildTestArchiveDir(t)

	tree, err := listing.BuildFromArchive(listing.Options{ArchiveDir: dir, NodeFilter: "Snapshot--child"}, testLog())
	if err != nil {
		t.Fatalf("BuildFromArchive with node filter: %v", err)
	}

	if tree.Root.ID != "Snapshot--child" {
		t.Fatalf("tree.Root.ID = %q, want Snapshot--child", tree.Root.ID)
	}

	if len(tree.Root.Children) != 0 {
		t.Fatalf("child node has %d children, want 0", len(tree.Root.Children))
	}
}

func TestBuildFromArchive_NodeFilter_NotFound(t *testing.T) {
	dir := buildTestArchiveDir(t)

	_, err := listing.BuildFromArchive(listing.Options{ArchiveDir: dir, NodeFilter: "Snapshot--nonexistent"}, testLog())
	if err == nil {
		t.Fatal("expected error for missing node filter, got nil")
	}
}

func TestBuildFromArchive_MissingDir(t *testing.T) {
	_, err := listing.BuildFromArchive(listing.Options{ArchiveDir: "/no/such/dir"}, testLog())
	if err == nil {
		t.Fatal("expected error for missing archive dir, got nil")
	}
}

// Cluster tests use stubbed seams.

var stubRootNode = &source.Node{
	ID:                       "Snapshot--my-snap",
	APIVersion:               "storage.deckhouse.io/v1alpha1",
	Kind:                     "Snapshot",
	Resource:                 "snapshots",
	Name:                     "my-snap",
	Namespace:                "demo",
	BoundSnapshotContentName: "sc-root",
	Children: []*source.Node{
		{
			ID:         "Snapshot--child",
			APIVersion: "storage.deckhouse.io/v1alpha1",
			Kind:       "Snapshot",
			Resource:   "snapshots",
			Name:       "child",
			Namespace:  "demo",
			ParentID:   "Snapshot--my-snap",
		},
	},
}

func stubBuildTree(_ context.Context, _ ctrlrtclient.Client, _, _ string) (*source.Node, error) {
	return stubRootNode, nil
}

func stubFetchManifests(_ context.Context, _ *safeClient.SafeClient, n *source.Node) ([][]byte, error) {
	switch n.ID {
	case "Snapshot--child":
		return [][]byte{
			[]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm2","namespace":"demo"}}`),
		}, nil
	default:
		return [][]byte{
			[]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm1","namespace":"demo"}}`),
		}, nil
	}
}

func TestBuildFromCluster_Tree(t *testing.T) {
	listing.SetBuildTreeFunc(stubBuildTree)
	listing.SetFetchManifestsFunc(stubFetchManifests)

	t.Cleanup(func() {
		listing.ResetFuncs()
	})

	tree, err := listing.BuildFromCluster(context.Background(), nil, nil, listing.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
	}, testLog())
	if err != nil {
		t.Fatalf("BuildFromCluster: %v", err)
	}

	if tree.Root == nil {
		t.Fatal("tree.Root is nil")
	}

	if tree.Root.ID != "Snapshot--my-snap" {
		t.Fatalf("tree.Root.ID = %q", tree.Root.ID)
	}

	if len(tree.Root.Children) != 1 {
		t.Fatalf("root has %d children, want 1", len(tree.Root.Children))
	}

	// Without WithObjects, ObjectCount should be -1.
	if tree.Root.ObjectCount != -1 {
		t.Fatalf("root.ObjectCount = %d, want -1 (unknown)", tree.Root.ObjectCount)
	}
}

func TestBuildFromCluster_WithObjects(t *testing.T) {
	listing.SetBuildTreeFunc(stubBuildTree)
	listing.SetFetchManifestsFunc(stubFetchManifests)

	t.Cleanup(func() {
		listing.ResetFuncs()
	})

	tree, err := listing.BuildFromCluster(context.Background(), nil, nil, listing.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		WithObjects:  true,
	}, testLog())
	if err != nil {
		t.Fatalf("BuildFromCluster: %v", err)
	}

	// root gets cm1 (exclusive); child gets cm2 (exclusive) — no overlap, no dedup effect.
	if tree.Root.ObjectCount != 1 {
		t.Fatalf("root.ObjectCount = %d, want 1", tree.Root.ObjectCount)
	}

	if len(tree.Root.Objects) != 1 {
		t.Fatalf("root.Objects count = %d, want 1", len(tree.Root.Objects))
	}

	if tree.Root.Objects[0].Kind != "ConfigMap" {
		t.Fatalf("root.Objects[0].Kind = %q, want ConfigMap", tree.Root.Objects[0].Kind)
	}
}

func TestBuildFromCluster_Dedup(t *testing.T) {
	listing.SetBuildTreeFunc(stubBuildTree)

	// Both root and child return the same "shared" object plus a unique one.
	listing.SetFetchManifestsFunc(func(_ context.Context, _ *safeClient.SafeClient, n *source.Node) ([][]byte, error) {
		shared := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"shared","namespace":"demo"}}`)

		if n.ID == "Snapshot--child" {
			return [][]byte{shared}, nil
		}

		unique := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"root-only","namespace":"demo"}}`)

		return [][]byte{unique, shared}, nil
	})

	t.Cleanup(func() {
		listing.ResetFuncs()
	})

	tree, err := listing.BuildFromCluster(context.Background(), nil, nil, listing.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		WithObjects:  true,
	}, testLog())
	if err != nil {
		t.Fatalf("BuildFromCluster dedup: %v", err)
	}

	// "shared" is captured by child → removed from root; root keeps only "root-only".
	if tree.Root.ObjectCount != 1 {
		t.Fatalf("root.ObjectCount = %d, want 1 after dedup", tree.Root.ObjectCount)
	}

	if tree.Root.Objects[0].Name != "root-only" {
		t.Fatalf("root.Objects[0].Name = %q, want root-only", tree.Root.Objects[0].Name)
	}

	child := tree.Root.Children[0]

	if child.ObjectCount != 1 {
		t.Fatalf("child.ObjectCount = %d, want 1", child.ObjectCount)
	}

	if child.Objects[0].Name != "shared" {
		t.Fatalf("child.Objects[0].Name = %q, want shared", child.Objects[0].Name)
	}
}

func TestBuildFromArchive_Dedup(t *testing.T) {
	dir := t.TempDir()

	shared := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"shared","namespace":"demo"}}`)
	rootOnly := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"root-only","namespace":"demo"}}`)

	meta := archive.Meta{
		Magic: archive.Magic, SchemaVersion: archive.SchemaVersion, ArchiveID: "dedup-test",
		CreatedAt: time.Now().UTC(),
		CreatedBy: archive.Creator{Tool: "d8", Version: "test"},
		Source: archive.Source{
			Cluster:             archive.Cluster{Server: "https://test.example.com"},
			Namespace:           "demo",
			RootSnapshot:        archive.SnapshotRef{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "Snapshot", Resource: "snapshots", Name: "root"},
			RootSnapshotContent: archive.SnapshotContentRef{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "SnapshotContent", Name: "sc-root"},
		},
		Selection: archive.Selection{
			Mode:            archive.SelectionFull,
			RootNodeID:      "Snapshot--root",
			SelectedNodeIDs: []string{"Snapshot--root", "Snapshot--child"},
		},
	}

	nodeRecs := []archive.NodeRecord{
		{ID: "Snapshot--root", Kind: "Snapshot", Name: "root", Namespace: "demo", Children: []string{"Snapshot--child"}},
		{ID: "Snapshot--child", Kind: "Snapshot", Name: "child", Namespace: "demo", ParentID: "Snapshot--root", Children: []string{}},
	}

	w, err := archive.NewDirWriter(dir, meta)
	if err != nil {
		t.Fatalf("NewDirWriter: %v", err)
	}

	// root has [rootOnly, shared]; child has [shared].
	rootOnlyRec, err := w.AddObject("Snapshot--root", rootOnly)
	if err != nil {
		t.Fatalf("AddObject rootOnly: %v", err)
	}

	sharedAtRoot, err := w.AddObject("Snapshot--root", shared)
	if err != nil {
		t.Fatalf("AddObject shared@root: %v", err)
	}

	sharedAtChild, err := w.AddObject("Snapshot--child", shared)
	if err != nil {
		t.Fatalf("AddObject shared@child: %v", err)
	}

	for _, prec := range []archive.ProgressRecord{
		{NodeID: "Snapshot--root", Objects: []archive.ObjectRecord{rootOnlyRec, sharedAtRoot}},
		{NodeID: "Snapshot--child", Objects: []archive.ObjectRecord{sharedAtChild}},
	} {
		if err := w.AppendProgress(prec); err != nil {
			t.Fatalf("AppendProgress: %v", err)
		}
	}

	if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, nodeRecs, true); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	tree, err := listing.BuildFromArchive(listing.Options{ArchiveDir: dir, WithObjects: true}, testLog())
	if err != nil {
		t.Fatalf("BuildFromArchive dedup: %v", err)
	}

	// root had [root-only, shared]; shared moves to child → root keeps only root-only.
	if tree.Root.ObjectCount != 1 {
		t.Fatalf("root.ObjectCount = %d, want 1 after dedup", tree.Root.ObjectCount)
	}

	if tree.Root.Objects[0].Name != "root-only" {
		t.Fatalf("root.Objects[0].Name = %q, want root-only", tree.Root.Objects[0].Name)
	}

	child := tree.Root.Children[0]

	if child.ObjectCount != 1 {
		t.Fatalf("child.ObjectCount = %d, want 1", child.ObjectCount)
	}

	if child.Objects[0].Name != "shared" {
		t.Fatalf("child.Objects[0].Name = %q, want shared", child.Objects[0].Name)
	}
}

func TestBuildFromArchive_ObjectModePrune(t *testing.T) {
	makeObjectArchive := func(t *testing.T, objectNodeID string) string {
		t.Helper()

		dir := t.TempDir()

		raw := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"selected","namespace":"demo"}}`)

		meta := archive.Meta{
			Magic: archive.Magic, SchemaVersion: archive.SchemaVersion, ArchiveID: "obj-prune-test",
			CreatedAt: time.Now().UTC(),
			CreatedBy: archive.Creator{Tool: "d8", Version: "test"},
			Source: archive.Source{
				Cluster:             archive.Cluster{Server: "https://test.example.com"},
				Namespace:           "demo",
				RootSnapshot:        archive.SnapshotRef{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "Snapshot", Resource: "snapshots", Name: "root"},
				RootSnapshotContent: archive.SnapshotContentRef{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "SnapshotContent", Name: "sc-root"},
			},
			Selection: archive.Selection{
				Mode:            archive.SelectionObject,
				RootNodeID:      "Snapshot--root",
				SelectedNodeIDs: []string{"Snapshot--root", "Snapshot--child-a", "Snapshot--child-b"},
			},
		}

		nodeRecs := []archive.NodeRecord{
			{ID: "Snapshot--root", Kind: "Snapshot", Name: "root", Namespace: "demo", Children: []string{"Snapshot--child-a", "Snapshot--child-b"}},
			{ID: "Snapshot--child-a", Kind: "Snapshot", Name: "child-a", Namespace: "demo", ParentID: "Snapshot--root", Children: []string{}},
			{ID: "Snapshot--child-b", Kind: "Snapshot", Name: "child-b", Namespace: "demo", ParentID: "Snapshot--root", Children: []string{}},
		}

		w, err := archive.NewDirWriter(dir, meta)
		if err != nil {
			t.Fatalf("NewDirWriter: %v", err)
		}

		objRec, err := w.AddObject(objectNodeID, raw)
		if err != nil {
			t.Fatalf("AddObject: %v", err)
		}

		prec := archive.ProgressRecord{NodeID: objectNodeID, Objects: []archive.ObjectRecord{objRec}}

		if err := w.AppendProgress(prec); err != nil {
			t.Fatalf("AppendProgress: %v", err)
		}

		if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, nodeRecs, true); err != nil {
			t.Fatalf("Finalize: %v", err)
		}

		return dir
	}

	t.Run("object_at_root_children_pruned", func(t *testing.T) {
		dir := makeObjectArchive(t, "Snapshot--root")

		tree, err := listing.BuildFromArchive(listing.Options{ArchiveDir: dir, WithObjects: true}, testLog())
		if err != nil {
			t.Fatalf("BuildFromArchive: %v", err)
		}

		if tree.Root.ObjectCount != 1 {
			t.Fatalf("root.ObjectCount = %d, want 1", tree.Root.ObjectCount)
		}

		// Both empty children must be pruned.
		if len(tree.Root.Children) != 0 {
			t.Fatalf("expected 0 children after pruning, got %d", len(tree.Root.Children))
		}
	})

	t.Run("object_at_child_a_child_b_pruned", func(t *testing.T) {
		dir := makeObjectArchive(t, "Snapshot--child-a")

		tree, err := listing.BuildFromArchive(listing.Options{ArchiveDir: dir, WithObjects: true}, testLog())
		if err != nil {
			t.Fatalf("BuildFromArchive: %v", err)
		}

		// child-b has no objects; child-a has one → only child-a kept.
		if len(tree.Root.Children) != 1 {
			t.Fatalf("expected 1 child after pruning, got %d", len(tree.Root.Children))
		}

		if tree.Root.Children[0].ID != "Snapshot--child-a" {
			t.Fatalf("surviving child = %q, want Snapshot--child-a", tree.Root.Children[0].ID)
		}
	})
}
