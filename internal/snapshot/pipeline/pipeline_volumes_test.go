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
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"

	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// stubNodeWithData builds a Node that has one DataRef.
func stubNodeWithData() *source.Node {
	return &source.Node{
		ID:                       "Snapshot--vol-snap",
		APIVersion:               "storage.deckhouse.io/v1alpha1",
		Kind:                     "Snapshot",
		Resource:                 "snapshots",
		Name:                     "vol-snap",
		Namespace:                "demo",
		BoundSnapshotContentName: "sc-vol",
		HasData:                  true,
		DataRefs: []source.DataRef{
			{VSCName: "vsc-disk-1", PVCName: "my-pvc", PVCNamespace: "demo"},
		},
	}
}

// noopDownloadVolumes is a DownloadNodeVolumesFunc stub that succeeds immediately.
func noopDownloadVolumes(
	_ context.Context,
	_ ctrlrtclient.Client,
	_ *safeClient.SafeClient,
	w *archive.DirWriter,
	n *source.Node,
	_ map[string]archive.VolumeProgressRecord,
	_ pipeline.Options,
	_ *slog.Logger,
) error {
	for _, dr := range n.DataRefs {
		if err := w.AppendVolumeProgress(archive.VolumeProgressRecord{
			NodeID:     n.ID,
			VSCName:    dr.VSCName,
			PVCName:    dr.PVCName,
			VolumeMode: "Block",
			BytesDone:  1024,
			BytesTotal: 1024,
			Complete:   true,
		}); err != nil {
			return err
		}
	}

	return nil
}

// failDownloadVolumes is a DownloadNodeVolumesFunc stub that always returns an error.
func failDownloadVolumes(
	_ context.Context,
	_ ctrlrtclient.Client,
	_ *safeClient.SafeClient,
	_ *archive.DirWriter,
	_ *source.Node,
	_ map[string]archive.VolumeProgressRecord,
	_ pipeline.Options,
	_ *slog.Logger,
) error {
	return fmt.Errorf("simulated volume download failure")
}

// setupVolumeSeams swaps all three seams (tree, manifests, volumes) and registers cleanup.
func setupVolumeSeams(
	t *testing.T,
	buildFn func(context.Context, ctrlrtclient.Client, string, string) (*source.Node, error),
	fetchFn func(context.Context, *safeClient.SafeClient, *source.Node) ([][]byte, error),
	volFn func(context.Context, ctrlrtclient.Client, *safeClient.SafeClient, *archive.DirWriter, *source.Node, map[string]archive.VolumeProgressRecord, pipeline.Options, *slog.Logger) error,
) {
	t.Helper()

	origTree := pipeline.BuildTreeFunc
	origFetch := pipeline.FetchManifestsFunc
	origVol := pipeline.DownloadNodeVolumesFunc

	t.Cleanup(func() {
		pipeline.BuildTreeFunc = origTree
		pipeline.FetchManifestsFunc = origFetch
		pipeline.DownloadNodeVolumesFunc = origVol
	})

	if buildFn != nil {
		pipeline.BuildTreeFunc = buildFn
	}

	if fetchFn != nil {
		pipeline.FetchManifestsFunc = fetchFn
	}

	if volFn != nil {
		pipeline.DownloadNodeVolumesFunc = volFn
	}
}

// stubBuildTreeWithData returns a tree stub using stubNodeWithData.
func stubBuildTreeWithData(_ context.Context, _ ctrlrtclient.Client, _, _ string) (*source.Node, error) {
	return stubNodeWithData(), nil
}

// TestRun_IncludeVolumes_Noop verifies that when IncludeVolumes=false the
// DownloadNodeVolumesFunc is never called even for nodes with HasData=true.
func TestRun_IncludeVolumes_Noop(t *testing.T) {
	var volCalls atomic.Int32

	countingVol := func(
		_ context.Context,
		_ ctrlrtclient.Client,
		_ *safeClient.SafeClient,
		_ *archive.DirWriter,
		_ *source.Node,
		_ map[string]archive.VolumeProgressRecord,
		_ pipeline.Options,
		_ *slog.Logger,
	) error {
		volCalls.Add(1)
		return nil
	}

	setupVolumeSeams(t, stubBuildTreeWithData, stubFetchManifests, countingVol)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:        "demo",
		SnapshotName:     "vol-snap",
		OutputDir:        dir,
		IncludeManifests: true,
		IncludeVolumes:   false,
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if volCalls.Load() != 0 {
		t.Errorf("DownloadNodeVolumesFunc called %d times, want 0 when IncludeVolumes=false", volCalls.Load())
	}
}

// TestRun_IncludeVolumes_Success verifies that DownloadNodeVolumesFunc is
// called when the node has data and IncludeVolumes=true.
func TestRun_IncludeVolumes_Success(t *testing.T) {
	var volCalls atomic.Int32

	countingVol := func(
		ctx context.Context,
		rc ctrlrtclient.Client,
		sc *safeClient.SafeClient,
		w *archive.DirWriter,
		n *source.Node,
		evp map[string]archive.VolumeProgressRecord,
		opts pipeline.Options,
		log *slog.Logger,
	) error {
		volCalls.Add(1)
		return noopDownloadVolumes(ctx, rc, sc, w, n, evp, opts, log)
	}

	setupVolumeSeams(t, stubBuildTreeWithData, stubFetchManifests, countingVol)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:        "demo",
		SnapshotName:     "vol-snap",
		OutputDir:        dir,
		IncludeManifests: true,
		IncludeVolumes:   true,
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run with volumes: %v", err)
	}

	if volCalls.Load() != 1 {
		t.Errorf("DownloadNodeVolumesFunc called %d times, want 1", volCalls.Load())
	}

	if !archive.IsComplete(dir) {
		t.Fatal("COMPLETE must exist after successful run with volumes")
	}
}

// TestRun_VolumeFail_BestEffort verifies that a volume download failure is
// propagated as a node failure and the archive is NOT marked COMPLETE.
func TestRun_VolumeFail_BestEffort(t *testing.T) {
	setupVolumeSeams(t, stubBuildTreeWithData, stubFetchManifests, failDownloadVolumes)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:        "demo",
		SnapshotName:     "vol-snap",
		OutputDir:        dir,
		IncludeManifests: true,
		IncludeVolumes:   true,
		Retries:          1,
	}

	err := pipeline.Run(context.Background(), nil, nil, opts, testLog())
	if err == nil {
		t.Fatal("expected error from volume download failure, got nil")
	}

	if archive.IsComplete(dir) {
		t.Fatal("COMPLETE must NOT exist when a volume download fails")
	}
}

// TestRun_Volumes_Resume verifies that after a partial run where volumes were
// already recorded as complete, a second run does NOT call DownloadNodeVolumesFunc
// again for those volumes.
func TestRun_Volumes_Resume(t *testing.T) {
	var volCalls atomic.Int32

	// First run: record volume progress.
	countingVol := func(
		ctx context.Context,
		rc ctrlrtclient.Client,
		sc *safeClient.SafeClient,
		w *archive.DirWriter,
		n *source.Node,
		evp map[string]archive.VolumeProgressRecord,
		opts pipeline.Options,
		log *slog.Logger,
	) error {
		volCalls.Add(1)
		return noopDownloadVolumes(ctx, rc, sc, w, n, evp, opts, log)
	}

	setupVolumeSeams(t, stubBuildTreeWithData, stubFetchManifests, countingVol)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:        "demo",
		SnapshotName:     "vol-snap",
		OutputDir:        dir,
		IncludeManifests: true,
		IncludeVolumes:   true,
	}

	// First run: should call volume download once.
	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	if volCalls.Load() != 1 {
		t.Fatalf("expected 1 vol call after first run, got %d", volCalls.Load())
	}

	// Second run: archive is already COMPLETE → pipeline should short-circuit (noop).
	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("second Run: %v", err)
	}

	// Vol calls should still be 1 — the second run was a noop.
	if volCalls.Load() != 1 {
		t.Errorf("expected no additional vol calls on noop resume, got %d", volCalls.Load())
	}
}

// TestRun_DefaultBothEnabled verifies that omitting IncludeManifests/IncludeVolumes
// (zero values) enables both by default (backward compatibility for tests and direct callers).
func TestRun_DefaultBothEnabled(t *testing.T) {
	var volCalls atomic.Int32

	countingVol := func(
		ctx context.Context,
		rc ctrlrtclient.Client,
		sc *safeClient.SafeClient,
		w *archive.DirWriter,
		n *source.Node,
		evp map[string]archive.VolumeProgressRecord,
		opts pipeline.Options,
		log *slog.Logger,
	) error {
		volCalls.Add(1)
		return noopDownloadVolumes(ctx, rc, sc, w, n, evp, opts, log)
	}

	setupVolumeSeams(t, stubBuildTreeWithData, stubFetchManifests, countingVol)

	dir := t.TempDir()
	opts := pipeline.Options{
		// Neither IncludeManifests nor IncludeVolumes set → both should default to true.
		Namespace:    "demo",
		SnapshotName: "vol-snap",
		OutputDir:    dir,
	}

	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if volCalls.Load() != 1 {
		t.Errorf("expected 1 vol call when both defaults enabled, got %d", volCalls.Load())
	}
}

// TestRun_NodeWithoutData_NoVolumeCall verifies that DownloadNodeVolumesFunc
// is NOT called for nodes that have HasData = false.
func TestRun_NodeWithoutData_NoVolumeCall(t *testing.T) {
	var volCalls atomic.Int32

	countingVol := func(
		_ context.Context,
		_ ctrlrtclient.Client,
		_ *safeClient.SafeClient,
		_ *archive.DirWriter,
		_ *source.Node,
		_ map[string]archive.VolumeProgressRecord,
		_ pipeline.Options,
		_ *slog.Logger,
	) error {
		volCalls.Add(1)
		return nil
	}

	setupVolumeSeams(t, stubBuildTree, stubFetchManifests, countingVol)

	dir := t.TempDir()
	opts := pipeline.Options{
		Namespace:        "demo",
		SnapshotName:     "my-snap",
		OutputDir:        dir,
		IncludeManifests: true,
		IncludeVolumes:   true,
	}

	// stubBuildTree returns stubNode which has HasData = false.
	if err := pipeline.Run(context.Background(), nil, nil, opts, testLog()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if volCalls.Load() != 0 {
		t.Errorf("DownloadNodeVolumesFunc called %d times for node without data, want 0", volCalls.Load())
	}
}
