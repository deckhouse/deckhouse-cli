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

package pipeline

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

const (
	dedupeNS        = "dedupe-ns"
	dedupeChildKind = "VirtualDiskSnapshot"
	dedupeChildAPI  = "demo.deckhouse.io/v1alpha1"
	dedupeSharedSrc = "shared-src"
)

// dedupeTestLogger returns a logger that discards output so the per-redirect
// WARN lines do not pollute test output.
func dedupeTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// dedupeChild builds a child snapshot node with the given CR name and source
// object name. Two children sharing sourceName (and kind) map to the same
// on-disk directory — the collision this task guards against.
func dedupeChild(name, sourceName string) *source.Node {
	return &source.Node{
		APIVersion: dedupeChildAPI,
		Kind:       dedupeChildKind,
		Name:       name,
		Namespace:  dedupeNS,
		SourceName: sourceName,
		OwnDataRefs: []snapshotapi.SnapshotDataBinding{
			{Target: snapshotapi.SnapshotSubjectRef{Name: sourceName}},
		},
	}
}

// dedupeTreeWithChildren builds a root Snapshot node with the given children.
func dedupeTreeWithChildren(children ...*source.Node) *source.Node {
	return &source.Node{
		APIVersion: storageAPIVersion,
		Kind:       "Snapshot",
		Name:       "root",
		Namespace:  dedupeNS,
		Children:   children,
	}
}

// collectAndDedupe runs the real collection + dedupe path on an on-disk output
// directory, returning the deduped task list.
func collectAndDedupe(t *testing.T, root *source.Node, outputDir string) []nodeTask {
	t.Helper()

	tasks, err := collectNodeTasks(root, outputDir)
	require.NoError(t, err)

	deduped, err := dedupeSiblingTargetDirs(tasks, dedupeTestLogger())
	require.NoError(t, err)

	return deduped
}

// taskFor returns the deduped task whose node is exactly node (pointer identity),
// so assertions do not depend on list ordering.
func taskFor(t *testing.T, tasks []nodeTask, node *source.Node) nodeTask {
	t.Helper()

	for _, tk := range tasks {
		if tk.node == node {
			return tk
		}
	}

	t.Fatalf("no task found for node %s/%s", node.Kind, node.Name)

	return nodeTask{}
}

const storageAPIVersion = "state-snapshotter.deckhouse.io/v1alpha1"

// childSnapshotsDir is the snapshots/ directory holding child node dirs directly
// under the root output directory.
func childSnapshotsDir(outputDir string) string {
	return filepath.Join(outputDir, archive.SnapshotsDirName)
}

// TestDedupeSiblingTargetDirs_TwoSiblings verifies the core guard: two siblings
// sharing Kind+source-name resolve to one primary directory, so the FIRST keeps
// the primary path and the SECOND is redirected to a deterministic collision
// path derived from its OWN CR identity. Running the collection twice on the
// same input yields the identical suffix (deterministic, not random).
func TestDedupeSiblingTargetDirs_TwoSiblings(t *testing.T) {
	t.Parallel()

	childA := dedupeChild("cr-a", dedupeSharedSrc)
	childB := dedupeChild("cr-b", dedupeSharedSrc)
	root := dedupeTreeWithChildren(childA, childB)

	outputDir := t.TempDir()
	tasks := collectAndDedupe(t, root, outputDir)

	primary := filepath.Join(childSnapshotsDir(outputDir), archive.NodeDirName(dedupeChildKind, dedupeSharedSrc))
	wantB := archive.CollisionNodeDir(childSnapshotsDir(outputDir), dedupeChildKind, dedupeSharedSrc, nodeCollisionShort(childB))

	require.Equal(t, primary, taskFor(t, tasks, childA).nodeDir, "first sibling keeps the primary path")
	require.Equal(t, wantB, taskFor(t, tasks, childB).nodeDir, "second sibling is redirected to its own-identity collision path")
	require.NotEqual(t, primary, wantB, "the two siblings never share one target dir")

	// Determinism: a second independent invocation recomputes the exact same
	// collision path (a resumed run must find its own data).
	tasks2 := collectAndDedupe(t, root, t.TempDir())
	require.Equal(t, filepath.Base(wantB), filepath.Base(taskFor(t, tasks2, childB).nodeDir),
		"collision suffix is deterministic across invocations")
}

// TestDedupeSiblingTargetDirs_ThreeWay verifies that a three-way collision
// yields three distinct target directories: the first keeps the primary and the
// other two get distinct own-identity collision paths.
func TestDedupeSiblingTargetDirs_ThreeWay(t *testing.T) {
	t.Parallel()

	childA := dedupeChild("cr-a", dedupeSharedSrc)
	childB := dedupeChild("cr-b", dedupeSharedSrc)
	childC := dedupeChild("cr-c", dedupeSharedSrc)
	root := dedupeTreeWithChildren(childA, childB, childC)

	outputDir := t.TempDir()
	tasks := collectAndDedupe(t, root, outputDir)

	dirA := taskFor(t, tasks, childA).nodeDir
	dirB := taskFor(t, tasks, childB).nodeDir
	dirC := taskFor(t, tasks, childC).nodeDir

	primary := filepath.Join(childSnapshotsDir(outputDir), archive.NodeDirName(dedupeChildKind, dedupeSharedSrc))
	require.Equal(t, primary, dirA, "first sibling keeps the primary path")

	dirs := map[string]struct{}{dirA: {}, dirB: {}, dirC: {}}
	require.Len(t, dirs, 3, "all three siblings resolve to distinct target dirs")
}

// TestDedupeSiblingTargetDirs_NoDuplicates verifies zero behavior change when no
// two siblings collide: the deduped list is identical to the raw collected list.
func TestDedupeSiblingTargetDirs_NoDuplicates(t *testing.T) {
	t.Parallel()

	childA := dedupeChild("cr-a", "src-a")
	childB := dedupeChild("cr-b", "src-b")
	root := dedupeTreeWithChildren(childA, childB)

	outputDir := t.TempDir()

	raw, err := collectNodeTasks(root, outputDir)
	require.NoError(t, err)

	deduped, err := dedupeSiblingTargetDirs(raw, dedupeTestLogger())
	require.NoError(t, err)

	require.Len(t, deduped, len(raw))

	for i := range raw {
		require.Equal(t, raw[i].node, deduped[i].node, "node order unchanged")
		require.Equal(t, raw[i].nodeDir, deduped[i].nodeDir, "target dir unchanged")
		require.Equal(t, raw[i].done, deduped[i].done, "resume decision unchanged")
		require.Equal(t, raw[i].observed, deduped[i].observed, "observed label unchanged")
	}
}

// TestDedupeSiblingTargetDirs_ResumesPartialCollisionDir verifies that when a
// redirected sibling's own collision directory already holds partial data from a
// prior run, re-running the collection recomputes the same path and the redirected
// task's resume state reflects the on-disk scan (inv. #10b / acceptance #2, #4).
func TestDedupeSiblingTargetDirs_ResumesPartialCollisionDir(t *testing.T) {
	t.Parallel()

	childA := dedupeChild("cr-a", dedupeSharedSrc)
	childB := dedupeChild("cr-b", dedupeSharedSrc)
	root := dedupeTreeWithChildren(childA, childB)

	outputDir := t.TempDir()

	// Seed childB's deterministic collision dir with an in-progress block chunk
	// staging dir plus childB's identity marker, mimicking a prior interrupted
	// run that already redirected childB here and started downloading.
	collisionDir := archive.CollisionNodeDir(childSnapshotsDir(outputDir), dedupeChildKind, dedupeSharedSrc, nodeCollisionShort(childB))
	require.NoError(t, os.MkdirAll(filepath.Join(collisionDir, archive.BlockChunksDirName), 0o755))
	require.NoError(t, archive.WriteNodeIdentityMarker(collisionDir, nodeIdentity(childB)))

	tasks := collectAndDedupe(t, root, outputDir)

	taskB := taskFor(t, tasks, childB)
	require.Equal(t, collisionDir, taskB.nodeDir, "redirected sibling recomputes its own collision path")
	require.False(t, taskB.done, "the collision-redirect plan must be honestly not done")
	require.Equal(t, archive.ObservedBlockPartial, taskB.observed,
		"the redirect re-scans the collision dir's real contents and observes its in-progress block staging")
}

// TestDownloadOwnDataRefs_MultiRef_RejectedByGuard pins the NO-DEAD-STATE guard
// added for the Variant-A contract (decision #9, .agent/implementer-prompt.md:125-140):
// a SnapshotContent carries at most one dataRef in every real payload, so
// len(node.OwnDataRefs) > 1 can only happen if an unexpected producer payload
// violates the contract. downloadOwnDataRefs must reject that with a loud,
// descriptive error instead of silently guessing at a per-pvc multi-volume
// layout — and must do so before touching the filesystem or opening any
// DataExport.
//
// This is an internal (package pipeline) test because downloadOwnDataRefs is
// unexported; pipeline_test.go's external package cannot reach it directly.
func TestDownloadOwnDataRefs_MultiRef_RejectedByGuard(t *testing.T) {
	t.Parallel()

	node := &source.Node{
		Kind: "Snapshot",
		Name: "multi",
		OwnDataRefs: []snapshotapi.SnapshotDataBinding{
			{Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-a"}},
			{Target: snapshotapi.SnapshotSubjectRef{Name: "pvc-b"}},
		},
	}

	cfg := applyDefaults(Config{})
	nodeDir := t.TempDir()

	err := downloadOwnDataRefs(context.Background(), cfg, node, nodeDir, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "2 dataRefs", "error must name the offending dataRef count")
	require.Contains(t, err.Error(), "Snapshot/multi", "error must identify the offending node")

	entries, readErr := os.ReadDir(nodeDir)
	require.NoError(t, readErr)
	require.Empty(t, entries, "guard must return before any download attempt touches nodeDir")
}

// TestDownloadOwnDataRefs_ZeroRefs_IsNoop verifies the len(refs) == 0 branch
// (unreachable from processNode today, which only calls downloadOwnDataRefs
// when len(node.OwnDataRefs) > 0, but downloadOwnDataRefs must still behave
// defensively if called directly) returns cleanly with no error and no
// filesystem side effects.
func TestDownloadOwnDataRefs_ZeroRefs_IsNoop(t *testing.T) {
	t.Parallel()

	node := &source.Node{Kind: "Snapshot", Name: "empty"}
	cfg := applyDefaults(Config{})
	nodeDir := t.TempDir()

	err := downloadOwnDataRefs(context.Background(), cfg, node, nodeDir, nil)
	require.NoError(t, err)

	entries, readErr := os.ReadDir(nodeDir)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}
