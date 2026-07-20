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
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
)

// isSnapshotContentGVK reports whether gvk addresses the cluster-scoped core
// SnapshotContent object (or its list). Under the unified contract the download pipeline
// resolves the whole tree from each node's own namespaced status.sourceRef/status.data and
// must never read cluster-scoped SnapshotContent at runtime.
func isSnapshotContentGVK(gvk schema.GroupVersionKind) bool {
	return gvk.Kind == "SnapshotContent" || gvk.Kind == "SnapshotContentList"
}

// failOnSnapshotContentReads wraps inner with an interceptor that fails the test on any
// runtime Get/List/Watch addressing a cluster-scoped SnapshotContent. It is the behavioral
// guard for the "no SnapshotContent reads" invariant: it turns a silent regression (some
// path re-introducing a SnapshotContent read) into a hard test failure.
func failOnSnapshotContentReads(t *testing.T, inner client.WithWatch) client.WithWatch {
	t.Helper()

	guard := func(op string, obj runtime.Object) {
		gvk := obj.GetObjectKind().GroupVersionKind()
		if isSnapshotContentGVK(gvk) {
			t.Errorf("runtime %s must not touch cluster-scoped SnapshotContent (got %s)", op, gvk.String())
		}
	}

	return interceptor.NewClient(inner, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			guard("Get", obj)

			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			guard("List", list)

			return c.List(ctx, list, opts...)
		},
		Watch: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
			guard("Watch", list)

			return c.Watch(ctx, list, opts...)
		},
	})
}

// TestPipeline_Download_NeverReadsSnapshotContent is the behavioral guard for the unified
// contract invariant "runtime Get/List/watch SnapshotContent must disappear". It runs the
// full download pipeline over the aggregator + orphan-leaf tree — the scenario that, before
// the rework, resolved leaf data via VolumeSnapshot.status.boundSnapshotContentName → child
// SnapshotContent → dataRef. The kube client is wrapped so any Get/List/Watch of a
// cluster-scoped SnapshotContent fails the test; a clean run proves the tree and leaf data
// are resolved purely from namespaced status.
func TestPipeline_Download_NeverReadsSnapshotContent(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("G"), e2eBlockSize)
	blockSrv := makeE2EBlockServer(t, rawBlock)

	inner, ok := buildOrphanLeafFakeClient(t).(client.WithWatch)
	require.True(t, ok, "fake client must implement client.WithWatch")

	guarded := failOnSnapshotContentReads(t, inner)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            e2eNS,
		RootSnapshot:         e2eAggRootSnap,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           guarded,
		OpenExport: func(_ context.Context, namespace string, leafRef aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-guard", "Block", blockSrv.URL, exporter.NewFetcher(blockSrv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))
}
