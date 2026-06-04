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
	"log/slog"
	"os"
	"testing"

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

func TestRun_Full(t *testing.T) {
	origTree := pipeline.BuildTreeFunc
	origFetch := pipeline.FetchManifestsFunc

	t.Cleanup(func() {
		pipeline.BuildTreeFunc = origTree
		pipeline.FetchManifestsFunc = origFetch
	})

	pipeline.BuildTreeFunc = stubBuildTree
	pipeline.FetchManifestsFunc = stubFetchManifests

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

	data, err := os.ReadFile(dir + "/index.json")
	if err != nil {
		t.Fatalf("read index.json: %v", err)
	}

	t.Logf("index.json: %s", data)
}

func TestRun_ObjectFilter(t *testing.T) {
	origTree := pipeline.BuildTreeFunc
	origFetch := pipeline.FetchManifestsFunc

	t.Cleanup(func() {
		pipeline.BuildTreeFunc = origTree
		pipeline.FetchManifestsFunc = origFetch
	})

	pipeline.BuildTreeFunc = stubBuildTree
	pipeline.FetchManifestsFunc = stubFetchManifests

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
	origTree := pipeline.BuildTreeFunc
	origFetch := pipeline.FetchManifestsFunc

	t.Cleanup(func() {
		pipeline.BuildTreeFunc = origTree
		pipeline.FetchManifestsFunc = origFetch
	})

	pipeline.BuildTreeFunc = stubBuildTree
	pipeline.FetchManifestsFunc = stubFetchManifests

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
