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
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
)

const (
	capTestNS       = "cap-ns"
	capTestRootSnap = "cap-root"
	capTestKind     = "VirtualDiskSnapshot"
	capTestAPIVer   = "demo.deckhouse.io/v1alpha1"
)

// TestPipeline_MaxParallelDownloadsCap verifies that the --max-parallel-downloads
// semaphore limits the number of concurrently active whole-volume-stream downloads
// across all nodes, independently of Workers (node errgroup) and PerVolumeConcurrency.
//
// The tree has nVolumes leaf nodes. Workers and nVolumes both exceed the cap so
// all nodes are eligible to start simultaneously. The OpenExport closure blocks
// until released, letting the test observe exactly how many goroutines are
// concurrently inside the semaphore-protected section.
func TestPipeline_MaxParallelDownloadsCap(t *testing.T) {
	const (
		cap      = 2
		nVolumes = 5 // more than cap; all nodes start immediately with Workers >= nVolumes
	)

	c := buildCapTestClient(t, nVolumes)
	outputDir := t.TempDir()

	blockSrv := makeBlockServer(t, bytes.Repeat([]byte("X"), 300))

	defer blockSrv.Close()

	var (
		mu      sync.Mutex
		active  int
		maxSeen int
	)

	// release is closed once the test has verified the cap; after that all
	// OpenExport calls complete immediately.
	release := make(chan struct{})

	// arrived is buffered so that no OpenExport call blocks on sending.
	arrived := make(chan struct{}, nVolumes)

	cfg := pipeline.Config{
		Namespace:            capTestNS,
		RootSnapshot:         capTestRootSnap,
		OutputDir:            outputDir,
		Workers:              nVolumes,
		PerVolumeConcurrency: 1,
		MaxParallelDownloads: cap,
		KubeClient:           c,
		ManifestSource:       newManifestStub(),
		OpenExport: func(_ context.Context, ns string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			mu.Lock()
			active++

			if active > maxSeen {
				maxSeen = active
			}

			mu.Unlock()

			arrived <- struct{}{}

			<-release

			mu.Lock()
			active--
			mu.Unlock()

			return exporter.NewExport(ns, "de-cap", "Block", blockSrv.URL, exporter.NewFetcher(blockSrv.Client())), nil
		},
	}

	done := make(chan error, 1)

	go func() {
		done <- pipeline.Run(context.Background(), cfg)
	}()

	// Wait for exactly cap goroutines to be blocked inside OpenExport.
	for i := 0; i < cap; i++ {
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			close(release)
			t.Fatal("timeout: fewer goroutines than cap reached OpenExport")
		}
	}

	// At this point: cap goroutines are inside OpenExport (blocked on <-release).
	// The remaining nVolumes-cap goroutines are blocked on semaphore.Acquire.
	mu.Lock()
	currentActive := active
	mu.Unlock()

	require.Equal(t, cap, currentActive,
		"exactly cap=%d goroutines must be concurrently inside OpenExport", cap)

	// Unblock all current and future OpenExport calls.
	close(release)

	select {
	case err := <-done:
		require.NoError(t, err, "pipeline must complete successfully")
	case <-time.After(30 * time.Second):
		t.Fatal("pipeline did not complete within 30s after release")
	}

	// The global cap must never have been exceeded.
	require.LessOrEqual(t, maxSeen, cap,
		"max concurrent stream downloads must not exceed MaxParallelDownloads=%d", cap)
}

// TestPipeline_MaxParallelDownloads_ZeroDefault is a table-driven test verifying that
// MaxParallelDownloads defaults to 5 when set to zero (and that the pipeline completes
// successfully with both zero and positive values).
func TestPipeline_MaxParallelDownloads_ZeroDefault(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		maxPar   int
		wantWork bool
	}{
		{"zero uses default (5)", 0, true},
		{"positive value kept", 3, true},
	}

	rawBlock := bytes.Repeat([]byte("D"), 300)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := makeBlockServer(t, rawBlock)
			defer srv.Close()

			c := buildFakeClient(t)
			outputDir := t.TempDir()

			cfg := pipeline.Config{
				Namespace:            testNS,
				RootSnapshot:         rootSnapshot,
				OutputDir:            outputDir,
				Workers:              1,
				PerVolumeConcurrency: 1,
				MaxParallelDownloads: tc.maxPar,
				KubeClient:           c,
				OpenExport: func(_ context.Context, ns string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
					return exporter.NewExport(ns, "de-default", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
				},
			}

			require.NoError(t, runPipeline(context.Background(), cfg))

			diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
				archive.NodeDirName(childKind, diskSnapName))
			assertNodeComplete(t, diskSnapDir)
		})
	}
}

// buildCapTestClient constructs a fake kube client with a root Snapshot and
// nVolumes VirtualDiskSnapshot leaf nodes, each owning one block DataRef.
// The ManifestSource for the cap test is set to an empty stub (no manifests
// needed for the concurrency assertion).
func buildCapTestClient(t *testing.T, nVolumes int) client.Client {
	t.Helper()

	scheme := buildScheme(t)

	children := make([]snapshotapi.SnapshotChildRef, 0, nVolumes)

	for i := 0; i < nVolumes; i++ {
		children = append(children, snapshotapi.SnapshotChildRef{
			APIVersion: capTestAPIVer,
			Kind:       capTestKind,
			Name:       fmt.Sprintf("cap-disk-%d", i),
		})
	}

	rootSnap := &snapshotapi.Snapshot{
		TypeMeta: metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "Snapshot"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      capTestRootSnap,
			Namespace: capTestNS,
		},
		Status: snapshotapi.SnapshotStatus{
			BoundSnapshotContentName: "cap-sc-root",
			ChildrenSnapshotRefs:     children,
		},
	}

	rootContent := &snapshotapi.SnapshotContent{
		TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
		ObjectMeta: metav1.ObjectMeta{Name: "cap-sc-root"},
	}

	typed := []client.Object{rootSnap, rootContent}
	unstructuredObjs := make([]client.Object, 0, nVolumes*2)

	for i := 0; i < nVolumes; i++ {
		diskName := fmt.Sprintf("cap-disk-%d", i)
		contentName := fmt.Sprintf("cap-sc-%d", i)

		leafSnap := makeUnstructuredSnap(capTestAPIVer, capTestKind, capTestNS, diskName, contentName)

		leafContent := &snapshotapi.SnapshotContent{
			TypeMeta:   metav1.TypeMeta{APIVersion: storageAPIVersion, Kind: "SnapshotContent"},
			ObjectMeta: metav1.ObjectMeta{Name: contentName},
			Status: snapshotapi.SnapshotContentStatus{
				DataRef: &snapshotapi.SnapshotDataBinding{
					TargetUID: fmt.Sprintf("uid-cap-%d", i),
					Target: snapshotapi.SnapshotSubjectRef{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Namespace:  capTestNS,
						Name:       fmt.Sprintf("pvc-cap-%d", i),
					},
					Artifact: snapshotapi.SnapshotDataArtifactRef{
						APIVersion: "snapshot.storage.k8s.io/v1",
						Kind:       "VolumeSnapshotContent",
						Name:       fmt.Sprintf("vsc-cap-%d", i),
					},
				},
			},
		}

		typed = append(typed, leafContent)
		unstructuredObjs = append(unstructuredObjs, leafSnap)
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(typed...).
		WithObjects(unstructuredObjs...).
		Build()
}
