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
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

var stubNode = &source.Node{
	ID:                       "Snapshot--my-snap",
	APIVersion:               "storage.deckhouse.io/v1alpha1",
	Kind:                     "Snapshot",
	Resource:                 "snapshots",
	Name:                     "my-snap",
	Namespace:                "demo",
	BoundSnapshotContentName: "snapcontent-root",
}

var (
	rawCM     = []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cfg"}}`)
	rawDeploy = []byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"app"}}`)
)

func stubBuildTree(_ context.Context, _ ctrlrtclient.Client, _, _ string) (*source.Node, error) {
	return stubNode, nil
}

func stubFetchManifests(_ context.Context, _ *safeClient.SafeClient, _ *source.Node) ([][]byte, error) {
	return [][]byte{rawCM, rawDeploy}, nil
}

func testLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// setupSeams swaps the build and fetch seams, registers cleanup, and returns a
// restore func (for overriding only one seam in a test).
func setupSeams(t *testing.T, buildFn func(context.Context, ctrlrtclient.Client, string, string) (*source.Node, error), fetchFn func(context.Context, *safeClient.SafeClient, *source.Node) ([][]byte, error)) {
	t.Helper()

	origTree := pipeline.BuildTreeFunc
	origFetch := pipeline.FetchManifestsFunc

	t.Cleanup(func() {
		pipeline.BuildTreeFunc = origTree
		pipeline.FetchManifestsFunc = origFetch
	})

	if buildFn != nil {
		pipeline.BuildTreeFunc = buildFn
	}

	if fetchFn != nil {
		pipeline.FetchManifestsFunc = fetchFn
	}
}

// --- Basic full download ---

func TestRun_Full(t *testing.T) {
	setupSeams(t, stubBuildTree, stubFetchManifests)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		OutputDir:    dir,
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !archive.IsComplete(dir) {
		t.Fatal("expected COMPLETE sentinel, not found")
	}
}

func TestRun_ObjectFilter(t *testing.T) {
	setupSeams(t, stubBuildTree, stubFetchManifests)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		OutputDir:    dir,
		ObjectFilter: "v1/ConfigMap/cfg",
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run with filter: %v", err)
	}

	if !archive.IsComplete(dir) {
		t.Fatal("expected COMPLETE sentinel")
	}
}

func TestRun_NodeFilter_NotFound(t *testing.T) {
	setupSeams(t, stubBuildTree, stubFetchManifests)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		OutputDir:    dir,
		NodeFilter:   "Snapshot--nonexistent",
	}

	err := pipeline.Run(context.Background(), nil, nil, opts, testLog())
	if err == nil {
		t.Fatal("expected error for missing node, got nil")
	}
}

// --- Noop (already up to date) ---

func TestRun_Noop(t *testing.T) {
	var fetchCalls atomic.Int32

	setupSeams(t, stubBuildTree, func(_ context.Context, _ *safeClient.SafeClient, _ *source.Node) ([][]byte, error) {
		fetchCalls.Add(1)

		return [][]byte{rawCM}, nil
	})

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		OutputDir:    dir,
	}

	// First run: downloads and creates COMPLETE.
	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	callsAfterFirst := fetchCalls.Load()

	// Second run: should be noop - fetch seam must not be called.
	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("noop Run: %v", err)
	}

	if fetchCalls.Load() != callsAfterFirst {
		t.Fatalf("fetch called on noop run: calls before=%d, after=%d", callsAfterFirst, fetchCalls.Load())
	}

	if !archive.IsComplete(dir) {
		t.Fatal("COMPLETE must still be present after noop")
	}
}

// --- Resume: first run partially fails, second run completes ---

func TestRun_Resume_PartialThenComplete(t *testing.T) {
	// The tree has two nodes: root + child.
	root := &source.Node{
		ID:                       "Snapshot--root",
		APIVersion:               "storage.deckhouse.io/v1alpha1",
		Kind:                     "Snapshot",
		Resource:                 "snapshots",
		Name:                     "root",
		Namespace:                "demo",
		BoundSnapshotContentName: "sc-root",
		Children: []*source.Node{
			{
				ID:                       "Snapshot--child",
				APIVersion:               "storage.deckhouse.io/v1alpha1",
				Kind:                     "Snapshot",
				Resource:                 "snapshots",
				Name:                     "child",
				Namespace:                "demo",
				BoundSnapshotContentName: "sc-child",
				ParentID:                 "Snapshot--root",
			},
		},
	}

	buildFn := func(_ context.Context, _ ctrlrtclient.Client, _, _ string) (*source.Node, error) {
		return root, nil
	}

	// First run: child node fetch fails.
	var runCount atomic.Int32

	firstFetchFn := func(_ context.Context, _ *safeClient.SafeClient, n *source.Node) ([][]byte, error) {
		if n.ID == "Snapshot--child" && runCount.Load() == 0 {
			return nil, errors.New("transient network error")
		}

		return [][]byte{rawCM}, nil
	}

	setupSeams(t, buildFn, firstFetchFn)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:    "demo",
		SnapshotName: "root",
		OutputDir:    dir,
		Retries:      1,
		RetryDelay:   time.Millisecond,
	}

	err := pipeline.Run(context.Background(), nil, nil, opts, testLog())
	if err == nil {
		t.Fatal("expected error on partial download, got nil")
	}

	if archive.IsComplete(dir) {
		t.Fatal("COMPLETE must not exist after partial download")
	}

	// Second run: all nodes succeed.
	runCount.Add(1)

	var childFetched atomic.Bool

	pipeline.FetchManifestsFunc = func(_ context.Context, _ *safeClient.SafeClient, n *source.Node) ([][]byte, error) {
		if n.ID == "Snapshot--child" {
			childFetched.Store(true)
		}

		return [][]byte{rawCM}, nil
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("resume run failed: %v", err)
	}

	if !archive.IsComplete(dir) {
		t.Fatal("COMPLETE must exist after successful resume")
	}

	if !childFetched.Load() {
		t.Fatal("child node must be re-fetched on resume")
	}
}

// --- Update: contentRef changed → re-fetches changed node ---

func TestRun_Update_ChangedContentRef(t *testing.T) {
	// Mutable node: we'll change BoundSnapshotContentName between runs.
	node := &source.Node{
		ID:                       "Snapshot--my-snap",
		APIVersion:               "storage.deckhouse.io/v1alpha1",
		Kind:                     "Snapshot",
		Resource:                 "snapshots",
		Name:                     "my-snap",
		Namespace:                "demo",
		BoundSnapshotContentName: "sc-v1",
	}

	buildFn := func(_ context.Context, _ ctrlrtclient.Client, _, _ string) (*source.Node, error) {
		return node, nil
	}

	var fetchCalls atomic.Int32

	fetchFn := func(_ context.Context, _ *safeClient.SafeClient, _ *source.Node) ([][]byte, error) {
		fetchCalls.Add(1)

		return [][]byte{rawCM}, nil
	}

	setupSeams(t, buildFn, fetchFn)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		OutputDir:    dir,
	}

	// First run.
	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	if fetchCalls.Load() != 1 {
		t.Fatalf("expected 1 fetch call after first run, got %d", fetchCalls.Load())
	}

	callsBeforeUpdate := fetchCalls.Load()

	// Simulate snapshot content changing (new SnapshotContent bound).
	node.BoundSnapshotContentName = "sc-v2"

	// Second run: node is no longer satisfied (contentRef changed) → re-fetch.
	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("update Run: %v", err)
	}

	if fetchCalls.Load() != callsBeforeUpdate+1 {
		t.Fatalf("expected 1 additional fetch for updated node, got %d additional", fetchCalls.Load()-callsBeforeUpdate)
	}

	if !archive.IsComplete(dir) {
		t.Fatal("COMPLETE must exist after successful update")
	}
}

// --- Retry exhaustion: all retries fail → failedNodes, no COMPLETE ---

func TestRun_RetryExhaustion(t *testing.T) {
	var fetchCalls atomic.Int32

	setupSeams(t, stubBuildTree, func(_ context.Context, _ *safeClient.SafeClient, _ *source.Node) ([][]byte, error) {
		fetchCalls.Add(1)

		return nil, errors.New("persistent fetch error")
	})

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		OutputDir:    dir,
		Retries:      3,
		RetryDelay:   time.Millisecond,
	}

	err := pipeline.Run(context.Background(), nil, nil, opts, testLog())
	if err == nil {
		t.Fatal("expected error after retry exhaustion, got nil")
	}

	if archive.IsComplete(dir) {
		t.Fatal("COMPLETE must not exist after retry exhaustion")
	}

	if fetchCalls.Load() != 3 {
		t.Fatalf("expected 3 fetch attempts, got %d", fetchCalls.Load())
	}
}

// --- Overwrite: different identity with Fresh=true ---

func TestRun_Fresh_OverwritesDifferentIdentity(t *testing.T) {
	setupSeams(t, stubBuildTree, stubFetchManifests)

	dir := t.TempDir()

	// Pre-populate directory with an archive for a different snapshot.
	differentMeta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "different-001",
		CreatedAt:     time.Now().UTC(),
		Source: archive.Source{
			Namespace:    "demo",
			RootSnapshot: archive.SnapshotRef{Name: "other-snap"},
		},
		Selection: archive.Selection{
			Mode:            archive.SelectionFull,
			RootNodeID:      "Snapshot--other-snap",
			SelectedNodeIDs: []string{"Snapshot--other-snap"},
		},
	}

	w, err := archive.NewDirWriter(dir, differentMeta)
	if err != nil {
		t.Fatalf("pre-populate NewDirWriter: %v", err)
	}

	if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, nil, true); err != nil {
		t.Fatalf("pre-populate Finalize: %v", err)
	}

	opts := pipeline.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		OutputDir:    dir,
		Fresh:        true,
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run with --fresh: %v", err)
	}

	if !archive.IsComplete(dir) {
		t.Fatal("COMPLETE must exist after successful --fresh overwrite")
	}

	// The archive.json must now contain the new snapshot name.
	r, err := archive.OpenDir(dir)
	if err != nil {
		t.Fatalf("OpenDir after fresh: %v", err)
	}

	meta, err := r.Meta()
	if err != nil {
		t.Fatalf("Meta after fresh: %v", err)
	}

	if meta.Source.RootSnapshot.Name != "my-snap" {
		t.Fatalf("archive.Source.RootSnapshot.Name = %q, want my-snap", meta.Source.RootSnapshot.Name)
	}
}

// --- Overwrite: different identity without Fresh and without TTY → error ---

func TestRun_Overwrite_NoFresh_NoPrompt_Errors(t *testing.T) {
	setupSeams(t, stubBuildTree, stubFetchManifests)

	dir := t.TempDir()

	// Pre-populate with a different archive.
	differentMeta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "different-002",
		CreatedAt:     time.Now().UTC(),
		Source: archive.Source{
			Namespace:    "demo",
			RootSnapshot: archive.SnapshotRef{Name: "other-snap"},
		},
		Selection: archive.Selection{
			Mode:            archive.SelectionFull,
			RootNodeID:      "Snapshot--other-snap",
			SelectedNodeIDs: []string{"Snapshot--other-snap"},
		},
	}

	w, err := archive.NewDirWriter(dir, differentMeta)
	if err != nil {
		t.Fatalf("pre-populate NewDirWriter: %v", err)
	}

	if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, nil, true); err != nil {
		t.Fatalf("pre-populate Finalize: %v", err)
	}

	// OverwritePromptFn=nil simulates non-TTY stdin without --fresh.
	opts := pipeline.Options{
		Namespace:         "demo",
		SnapshotName:      "my-snap",
		OutputDir:         dir,
		Fresh:             false,
		OverwritePromptFn: nil,
	}

	err = pipeline.Run(context.Background(), nil, nil, opts, testLog())
	if err == nil {
		t.Fatal("expected error when no TTY and no --fresh, got nil")
	}
}

// --- Overwrite: prompt returns false → error ---

func TestRun_Overwrite_PromptDeclined_Errors(t *testing.T) {
	setupSeams(t, stubBuildTree, stubFetchManifests)

	dir := t.TempDir()

	differentMeta := archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     "different-003",
		CreatedAt:     time.Now().UTC(),
		Source: archive.Source{
			Namespace:    "demo",
			RootSnapshot: archive.SnapshotRef{Name: "other-snap"},
		},
		Selection: archive.Selection{
			Mode:            archive.SelectionFull,
			RootNodeID:      "Snapshot--other-snap",
			SelectedNodeIDs: []string{"Snapshot--other-snap"},
		},
	}

	w, err := archive.NewDirWriter(dir, differentMeta)
	if err != nil {
		t.Fatalf("pre-populate NewDirWriter: %v", err)
	}

	if _, err := w.Finalize(archive.Index{SchemaVersion: archive.SchemaVersion}, nil, true); err != nil {
		t.Fatalf("pre-populate Finalize: %v", err)
	}

	opts := pipeline.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		OutputDir:    dir,
		OverwritePromptFn: func(_ string) bool {
			return false // user declined
		},
	}

	err = pipeline.Run(context.Background(), nil, nil, opts, testLog())
	if err == nil {
		t.Fatal("expected error when prompt declined, got nil")
	}
}

// --- Non-empty non-archive directory → error without --fresh ---

func TestRun_NonArchiveDir_Errors(t *testing.T) {
	setupSeams(t, stubBuildTree, stubFetchManifests)

	dir := t.TempDir()

	// Create a non-archive file in the directory.
	if err := os.WriteFile(dir+"/somefile.txt", []byte("hello"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	opts := pipeline.Options{
		Namespace:         "demo",
		SnapshotName:      "my-snap",
		OutputDir:         dir,
		Fresh:             false,
		OverwritePromptFn: nil,
	}

	err := pipeline.Run(context.Background(), nil, nil, opts, testLog())
	if err == nil {
		t.Fatal("expected error for non-archive directory, got nil")
	}
}

// --- Context cancellation stops retries immediately ---

func TestRun_ContextCancellation(t *testing.T) {
	var fetchCalls atomic.Int32

	setupSeams(t, stubBuildTree, func(_ context.Context, _ *safeClient.SafeClient, _ *source.Node) ([][]byte, error) {
		fetchCalls.Add(1)

		return nil, context.Canceled
	})

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:    "demo",
		SnapshotName: "my-snap",
		OutputDir:    dir,
		Retries:      5,
		RetryDelay:   time.Millisecond,
	}

	err := pipeline.Run(context.Background(), nil, nil, opts, testLog())
	if err == nil {
		t.Fatal("expected error after context cancellation, got nil")
	}

	// context.Canceled is non-retryable; exactly 1 attempt expected.
	if fetchCalls.Load() != 1 {
		t.Fatalf("expected 1 fetch attempt for non-retryable error, got %d", fetchCalls.Load())
	}
}
