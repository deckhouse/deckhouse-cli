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
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

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
