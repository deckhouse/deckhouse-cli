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
	"crypto/md5" //nolint:gosec // test fixture digest, matches the exporter's hash.md5 contract
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vbauerster/mpb/v8/decor"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/progress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/localscan"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/pipeline"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/snapimport"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/transport"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

const (
	testNS        = "test-ns"
	rootSnapshot  = "my-snap"
	diskSnapName  = "disk-snap"
	diskSnapUID   = "uid-disk-snap"
	sourcePVCName = "pvc-disk-source"

	storageAPIVersion = "state-snapshotter.deckhouse.io/v1alpha1"
	childAPIVersion   = "demo.deckhouse.io/v1alpha1"
	childKind         = "VirtualDiskSnapshot"

	runOwnerAnnotationKey  = "snapshot.deckhouse.io/download-run-id"
	targetUIDAnnotationKey = "snapshot.deckhouse.io/target-uid"
)

func diskSnapshotDataExportName() string {
	return exporter.DataExportName(
		testNS,
		"demo.deckhouse.io",
		"virtualdisksnapshots",
		childKind,
		diskSnapName,
		types.UID(diskSnapUID),
	)
}

type dataExportTargetMapping struct {
	apiVersion string
	kind       string
	resource   string
}

type repeatedByteReaderAt struct {
	value byte
}

func (r repeatedByteReaderAt) ReadAt(data []byte, _ int64) (int, error) {
	for i := range data {
		data[i] = r.value
	}

	return len(data), nil
}

func dataExportAggClient(t *testing.T, mappings ...dataExportTargetMapping) *aggapi.Client {
	t.Helper()

	groupVersions := make([]schema.GroupVersion, 0, len(mappings))
	for _, mapping := range mappings {
		gv, err := schema.ParseGroupVersion(mapping.apiVersion)
		require.NoError(t, err)

		groupVersions = append(groupVersions, gv)
	}

	mapper := meta.NewDefaultRESTMapper(groupVersions)
	for _, mapping := range mappings {
		gv, err := schema.ParseGroupVersion(mapping.apiVersion)
		require.NoError(t, err)

		gvk := gv.WithKind(mapping.kind)
		plural := gv.WithResource(mapping.resource)
		singular := gv.WithResource(strings.TrimSuffix(mapping.resource, "s"))
		mapper.AddSpecific(gvk, plural, singular, meta.RESTScopeNamespace)
	}

	return aggapi.NewClient(nil, mapper)
}

func authenticatedTestTransportClient() *transport.Client {
	return transport.NewClientForConfig(&rest.Config{BearerToken: "test-token"})
}

// snapObj is a builder for an unstructured snapshot-tree object described purely by its
// namespaced status fragments (status.sourceRef / status.data / status.childrenSnapshotRefs) —
// the only inputs BuildTree/ParseNodeStatus read. No cluster-scoped SnapshotContent is involved:
// a node's captured volume lives in its own status.data (Variant A, cardinality ≤1) and its
// readable directory base comes from status.sourceRef.name (when set) or the CR name.
type snapObj struct {
	apiVersion string
	kind       string
	namespace  string
	name       string
	uid        string
	sourceRef  map[string]interface{}   // status.sourceRef (optional)
	data       map[string]interface{}   // status.data (optional)
	children   []map[string]interface{} // status.childrenSnapshotRefs (optional)
}

func (s snapObj) build() *unstructured.Unstructured {
	status := map[string]interface{}{}

	if s.sourceRef != nil {
		status["sourceRef"] = s.sourceRef
	}

	if s.data != nil {
		status["data"] = s.data
	}

	if len(s.children) > 0 {
		raw := make([]interface{}, len(s.children))
		for i, c := range s.children {
			raw[i] = c
		}

		status["childrenSnapshotRefs"] = raw
	}

	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": s.apiVersion,
		"kind":       s.kind,
		"metadata": map[string]interface{}{
			"name":      s.name,
			"namespace": s.namespace,
			"uid":       s.uid,
		},
		"status": status,
	}}
}

// pvcData builds a status.data map for a PVC-backed captured volume (Variant A, ≤1 per node).
func pvcData(namespace, pvcName, pvcUID, vscName string) map[string]interface{} {
	return map[string]interface{}{
		"sourceRef": map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "PersistentVolumeClaim",
			"namespace":  namespace,
			"name":       pvcName,
			"uid":        pvcUID,
		},
		"artifactRef": map[string]interface{}{
			"apiVersion": "snapshot.storage.k8s.io/v1",
			"kind":       "VolumeSnapshotContent",
			"name":       vscName,
		},
	}
}

// pvcSourceRefMap builds a status.sourceRef map for a captured PVC source object.
func pvcSourceRefMap(namespace, pvcName, pvcUID string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"namespace":  namespace,
		"name":       pvcName,
		"uid":        pvcUID,
	}
}

// namespaceSourceRefMap builds the root capture-Snapshot's status.sourceRef: the cluster-scoped
// v1/Namespace source, which legitimately carries no namespace field.
func namespaceSourceRefMap(name, uid string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"name":       name,
		"uid":        uid,
	}
}

// childRefMap builds one status.childrenSnapshotRefs element.
func childRefMap(apiVersion, kind, name string) map[string]interface{} {
	return map[string]interface{}{"apiVersion": apiVersion, "kind": kind, "name": name}
}

// seedResumeIdentityMarker stamps the identity marker the pipeline itself writes
// on a node's first touch (ensureNodeSubdirs -> archive.WriteNodeIdentityMarker).
// Tests that HAND-CRAFT a partial node directory without first running the
// pipeline must seed it: after partial-node-dir-identity-marker, a marker-less
// non-empty partial dir is treated as foreign and collision-redirected rather
// than resumed, so a realistic same-snapshot crash fixture must carry the marker
// its interrupted run would already have written.
//
// Tests that build the partial state by running the full pipeline once and then
// deleting snapshot.yaml must ALSO re-seed the marker (via
// reseedResumeMarkerFromSnapshotYAML): finalize-removes-identity-marker deletes
// the marker once snapshot.yaml is durable, so after a completed run the marker
// is gone. Restoring it is the honest crash residue — a real crash happens
// BEFORE the snapshot.yaml write, leaving the marker in place.
func seedResumeIdentityMarker(t *testing.T, nodeDir string, id archive.NodeIdentity) {
	t.Helper()

	require.NoError(t, archive.WriteNodeIdentityMarker(nodeDir, id))
}

// reseedResumeMarkerFromSnapshotYAML restores the resume identity marker on a
// finalized node using the identity recorded in its snapshot.yaml. It must be
// called while snapshot.yaml still exists (before a fixture deletes it to fake a
// crash window). Because FinalizeNode removes the marker once snapshot.yaml is
// written, a fixture that completes a full pipeline run and then drops
// snapshot.yaml to simulate a crash-after-commit must re-stamp the marker its
// interrupted run would still carry.
func reseedResumeMarkerFromSnapshotYAML(t *testing.T, nodeDir string) {
	t.Helper()

	sy, err := archive.ReadSnapshotYAML(nodeDir)
	require.NoError(t, err)

	seedResumeIdentityMarker(t, nodeDir, archive.NodeIdentity{
		APIVersion: sy.APIVersion,
		Kind:       sy.Kind,
		Name:       sy.Name,
		Namespace:  sy.Namespace,
		UID:        sy.UID,
	})
}

// reseedResumeMarkerAndDropEnvelopesUpTo rolls nodeDir AND every ancestor node
// directory up to outputDir (inclusive) back to the pre-publication state a real
// interrupted run leaves behind: identity marker present, snapshot.yaml absent.
//
// Ancestors must be rolled back together with the descendant because publication
// is strictly bottom-up and a parent's ChildrenChecksum authenticates its direct
// children's envelopes. A run interrupted before nodeDir's envelope became
// durable therefore cannot have written any ancestor envelope either, and once a
// publication transaction has been completed and cleaned no crash can leave a
// finalized ancestor committing to a descendant envelope that is gone. Dropping
// only the descendant's envelope would synthesize a hybrid tree — a stale
// parent commitment with no run-start publication transaction authorizing it —
// which the pipeline must reject before touching anything (AC-1), not resume
// into.
func reseedResumeMarkerAndDropEnvelopesUpTo(t *testing.T, outputDir, nodeDir string) {
	t.Helper()

	outputDir = filepath.Clean(outputDir)
	dir := filepath.Clean(nodeDir)

	require.True(t, dir == outputDir || strings.HasPrefix(dir, outputDir+string(filepath.Separator)),
		"node dir %s must live under output dir %s", dir, outputDir)

	for {
		reseedResumeMarkerFromSnapshotYAML(t, dir)
		require.NoError(t, os.Remove(filepath.Join(dir, archive.SnapshotYAMLName)))

		if dir == outputDir {
			return
		}

		// A child node dir is always <parent>/snapshots/<kind>_<name>.
		dir = filepath.Dir(filepath.Dir(dir))
	}
}

// assertNoIdentityMarkers walks the whole output tree rooted at root and fails
// if any identity.json (archive.NodeIdentityMarkerName) remains. After a fully
// successful run every finalized node must drop its resume marker, so the tree
// holds only snapshot.yaml + manifests/ + optional snapshots/ + at most one
// volume payload per node.
func assertNoIdentityMarkers(t *testing.T, root string) {
	t.Helper()

	require.NoError(t, filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && d.Name() == archive.NodeIdentityMarkerName {
			t.Errorf("stray identity marker must not survive finalization: %s", path)
		}

		return nil
	}))
}

// diskSnapMarkerIdentity is the identity the pipeline computes (nodeIdentity) for
// the buildFakeClient tree's disk-snap leaf, used to seed hand-crafted
// partial-dir resume fixtures so their marker matches the scan-time identity.
func diskSnapMarkerIdentity() archive.NodeIdentity {
	return archive.NodeIdentity{
		APIVersion: childAPIVersion,
		Kind:       childKind,
		Name:       diskSnapName,
		DirName:    diskSnapName,
		Namespace:  testNS,
		UID:        diskSnapUID,
	}
}

// TestPipeline_HappyPath verifies the full download pipeline against a fake
// Kubernetes client and an httptest block-volume server.
//
// disk-snap has one OwnDataRef (non-aggregator), so it downloads its volume data
// directly into its own node directory using the flat layout.
//
// Layout after the run:
//
//	outputDir/ (root Snapshot node)
//	  manifests/configmap_test-cfg.yaml
//	  snapshots/
//	    virtualdisksnapshot_disk-snap/ (non-aggregator; 1 OwnDataRef → flat layout)
//	      manifests/
//	      data.bin.zst
//	      snapshot.yaml
//	  snapshot.yaml
func TestPipeline_HappyPath(t *testing.T) {
	// Raw block data for the child disk snapshot.
	rawBlock := bytes.Repeat([]byte("B"), 600)

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
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	// Root node must be complete.
	assertNodeComplete(t, outputDir)

	// Root must have a manifests/ dir with one ConfigMap file.
	manifestsDir := filepath.Join(outputDir, archive.ManifestsDirName)
	entries, err := os.ReadDir(manifestsDir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "expected one manifest file under root/manifests/")

	// Root must have a snapshots/ dir (because it has a child).
	_, err = os.Stat(filepath.Join(outputDir, archive.SnapshotsDirName))
	require.NoError(t, err, "root snapshots/ directory must exist")

	// disk-snap is a non-aggregator: complete, with data.bin.zst in its own dir.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)

	_, err = os.Stat(filepath.Join(diskSnapDir, archive.DataBlockName(".zst")))
	require.NoError(t, err, "non-aggregator node must have data.bin.zst directly")

	// disk-snap has no children, so no snapshots/ subdir.
	_, noSnapErr := os.Stat(filepath.Join(diskSnapDir, archive.SnapshotsDirName))
	require.True(t, os.IsNotExist(noSnapErr),
		"non-aggregator node must not have a snapshots/ subdir")

	// A fully successful run must leave no resume identity markers anywhere in
	// the output tree (finalize-removes-identity-marker).
	assertNoIdentityMarkers(t, outputDir)

	// Second run must be a no-op: snapshot.yaml mtime must not change.
	rootYAML := filepath.Join(outputDir, archive.SnapshotYAMLName)
	diskSnapYAML := filepath.Join(diskSnapDir, archive.SnapshotYAMLName)

	rootMod := statMtime(t, rootYAML)
	diskSnapMod := statMtime(t, diskSnapYAML)

	// Sleep briefly so that any writes would produce a different mtime.
	time.Sleep(20 * time.Millisecond)

	err = runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	require.Equal(t, rootMod, statMtime(t, rootYAML),
		"root snapshot.yaml must not be rewritten on second run")
	require.Equal(t, diskSnapMod, statMtime(t, diskSnapYAML),
		"disk-snap snapshot.yaml must not be rewritten on second run")
}

func TestPipeline_UnsupportedFilesystemEntryDoesNotFinalizeNode(t *testing.T) {
	t.Parallel()

	var fileGets atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"regular.txt","type":"file","uri":"regular.txt","attributes":{"size":4}},`+
				`{"name":"execq","type":"other","uri":"execq","attributes":{"permissions":"0660","modtime":"2026-07-23T12:00:00Z","uid":0,"gid":999}}`+
				`]}`)
		case "/api/v1/files/regular.txt":
			fileGets.Add(1)
			_, _ = io.WriteString(w, "data")
		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	outputDir := t.TempDir()
	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           buildFakeClient(t),
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-unsupported-fs", "Filesystem", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.Error(t, err)
	require.ErrorContains(t, err, "execq")
	require.ErrorContains(t, err, "other")

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))

	_, statErr := os.Stat(filepath.Join(diskSnapDir, archive.FsTarName))
	require.True(t, os.IsNotExist(statErr), "unsupported listing item must not publish data.tar")

	_, statErr = os.Stat(filepath.Join(diskSnapDir, archive.SnapshotYAMLName))
	require.True(t, os.IsNotExist(statErr),
		"unsupported listing item must not reach node finalization or publish snapshot.yaml")
	require.Zero(t, fileGets.Load(), "listing validation must finish before any regular file is staged")
}

func TestPipeline_DirectoryCreationFailureDoesNotFinalizeNode(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()
	scaffoldPath := filepath.Join(outputDir, archive.SnapshotsDirName)
	require.NoError(t, os.WriteFile(scaffoldPath, []byte("not a directory"), 0o644))

	cfg := pipeline.Config{
		Namespace:        testNS,
		RootSnapshot:     rootSnapshot,
		OutputDir:        outputDir,
		Workers:          1,
		KubeClient:       buildFakeClient(t),
		SelectedNodeKind: childKind,
		SelectedNodeName: diskSnapName,
		OpenExport: func(context.Context, string, aggapi.NodeRef, string) (*exporter.Export, error) {
			t.Fatal("OpenExport must not run after scaffold creation fails")

			return nil, nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.Error(t, err)
	require.ErrorContains(t, err, "scaffold")

	_, statErr := os.Stat(filepath.Join(outputDir, archive.SnapshotYAMLName))
	require.ErrorIs(t, statErr, os.ErrNotExist,
		"directory creation failure must stop before node finalization")
}

func TestPipeline_ProductionExportReusesAndClosesHTTPConnections(t *testing.T) {
	t.Parallel()

	const (
		fileCount = 200
		workers   = 4
	)

	var (
		newConnections    atomic.Int64
		closedConnections atomic.Int64
		hashRequests      atomic.Int64
		fileRequests      atomic.Int64
		nonHTTP2Requests  atomic.Int64
	)

	files := make(map[string][]byte, fileCount)
	items := make([]string, 0, fileCount)

	for i := range fileCount {
		name := fmt.Sprintf("small-%03d.txt", i)
		content := []byte(fmt.Sprintf("content-%03d", i))
		files[name] = content
		items = append(items, fmt.Sprintf(
			`{"name":%q,"type":"file","uri":%q,"attributes":{"permissions":"0644","modtime":"2026-07-23T12:00:00Z","uid":0,"gid":0,"size":%d}}`,
			name, name, len(content),
		))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			nonHTTP2Requests.Add(1)
		}

		if r.URL.Path == "/api/v1/files/" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+strings.Join(items, ",")+`]}`)

			return
		}

		name := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")
		content, ok := files[name]
		if !ok {
			http.NotFound(w, r)

			return
		}

		if r.Method == http.MethodHead {
			hashRequests.Add(1)

			sum := md5.Sum(content)
			w.Header().Set("X-Attribute-Hash-Md5", fmt.Sprintf("%x", sum))
		} else {
			fileRequests.Add(1)
		}

		http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(content))
	})

	srv := httptest.NewUnstartedServer(mux)
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			newConnections.Add(1)
		case http.StateClosed:
			closedConnections.Add(1)
		}
	}
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	c := buildFakeClient(t)
	readyExport := readyFilesystemDataExport(t, srv)
	require.NoError(t, c.Create(context.Background(), readyExport))

	sc, err := transport.NewClient()
	require.NoError(t, err)

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            t.TempDir(),
		Workers:              1,
		PerVolumeConcurrency: workers,
		KubeClient:           c,
		OpenExport: func(ctx context.Context, namespace string, leafRef aggapi.NodeRef, ttl string) (*exporter.Export, error) {
			return exporter.OpenExport(
				ctx,
				slog.Default(),
				c,
				namespace,
				"demo.deckhouse.io",
				"virtualdisksnapshots",
				childKind,
				leafRef.Name,
				ttl,
				sc,
				exporter.WithTargetUID(types.UID(diskSnapUID)),
			)
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))
	require.Equal(t, int64(fileCount), hashRequests.Load())
	require.Equal(t, int64(fileCount), fileRequests.Load())
	require.Zero(t, nonHTTP2Requests.Load(), "production export requests must use HTTP/2")

	connectionCount := newConnections.Load()
	require.LessOrEqual(t, connectionCount, int64(2*workers+2),
		"two persistent transports must bound connections by in-flight workers")
	require.Less(t, connectionCount*10, int64(fileCount*2),
		"HEAD and GET request count must greatly exceed connection creation")

	requireEventually(t, time.Second, func() bool {
		return closedConnections.Load() == newConnections.Load()
	})
}

func TestPipeline_ProductionMixedGVKUsesDistinctOwnedDataExports(t *testing.T) {
	t.Parallel()

	const (
		leafName = "shared-snapshot"
		runID    = "run-mixed-gvk"
	)

	targets := []struct {
		apiVersion    string
		group         string
		resource      string
		kind          string
		snapshotUID   types.UID
		dataExportUID types.UID
		payload       []byte
	}{
		{
			apiVersion:    "alpha.example.io/v1",
			group:         "alpha.example.io",
			resource:      "alphasnapshots",
			kind:          "AlphaSnapshot",
			snapshotUID:   "uid-alpha-snapshot",
			dataExportUID: "uid-alpha-export",
			payload:       bytes.Repeat([]byte("A"), 300),
		},
		{
			apiVersion:    "beta.example.io/v1",
			group:         "beta.example.io",
			resource:      "betasnapshots",
			kind:          "BetaSnapshot",
			snapshotUID:   "uid-beta-snapshot",
			dataExportUID: "uid-beta-export",
			payload:       bytes.Repeat([]byte("B"), 300),
		},
	}

	var (
		started   atomic.Int64
		startOnce sync.Once
	)

	bothStarted := make(chan struct{})
	servers := make(map[string]*httptest.Server, len(targets))

	for _, target := range targets {
		payload := target.payload
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodHead {
				if started.Add(1) == int64(len(targets)) {
					startOnce.Do(func() { close(bothStarted) })
				}

				select {
				case <-bothStarted:
				case <-r.Context().Done():
					return
				}
			}

			http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(payload))
		}))
		t.Cleanup(server.Close)

		servers[target.kind] = server
	}

	root := snapObj{
		apiVersion: storageAPIVersion,
		kind:       "Snapshot",
		namespace:  testNS,
		name:       rootSnapshot,
		uid:        "uid-mixed-root",
		sourceRef:  namespaceSourceRefMap(testNS, "uid-ns"),
		children: []map[string]interface{}{
			childRefMap(targets[0].apiVersion, targets[0].kind, leafName),
			childRefMap(targets[1].apiVersion, targets[1].kind, leafName),
		},
	}.build()

	objects := []client.Object{root}
	for i, target := range targets {
		objects = append(objects, snapObj{
			apiVersion: target.apiVersion,
			kind:       target.kind,
			namespace:  testNS,
			name:       leafName,
			uid:        string(target.snapshotUID),
			data:       pvcData(testNS, fmt.Sprintf("pvc-%d", i), fmt.Sprintf("uid-pvc-%d", i), fmt.Sprintf("vsc-%d", i)),
		}.build())
	}

	type deleteRecord struct {
		name            string
		uid             types.UID
		preconditionUID types.UID
	}

	var (
		evidenceMu sync.Mutex
		created    = make(map[string]*deapi.DataExport, len(targets))
		deleted    = make(map[string]deleteRecord, len(targets))
	)

	c := fake.NewClientBuilder().
		WithScheme(buildScheme(t)).
		WithObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				opts ...client.CreateOption,
			) error {
				de, ok := obj.(*deapi.DataExport)
				if !ok {
					return cl.Create(ctx, obj, opts...)
				}

				var dataExportUID types.UID
				for _, target := range targets {
					if de.Spec.TargetRef.Kind == target.kind {
						dataExportUID = target.dataExportUID
					}
				}

				server := servers[de.Spec.TargetRef.Kind]
				if server == nil || dataExportUID == "" {
					return fmt.Errorf("unexpected DataExport target: %+v", de.Spec.TargetRef)
				}

				certificate := server.Certificate()
				caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})

				de.UID = dataExportUID
				de.Status = deapi.DataExportStatus{
					URL:        server.URL,
					CA:         base64.StdEncoding.EncodeToString(caPEM),
					VolumeMode: "Block",
					Conditions: []metav1.Condition{{
						Type:   "Ready",
						Status: metav1.ConditionTrue,
						Reason: "PodReady",
					}},
				}

				if err := cl.Create(ctx, de, opts...); err != nil {
					return err
				}

				evidenceMu.Lock()
				created[de.Name] = de.DeepCopy()
				evidenceMu.Unlock()

				return nil
			},
			Delete: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				opts ...client.DeleteOption,
			) error {
				de, ok := obj.(*deapi.DataExport)
				if !ok {
					return cl.Delete(ctx, obj, opts...)
				}

				deleteOptions := client.DeleteOptions{}
				deleteOptions.ApplyOptions(opts)

				record := deleteRecord{name: de.Name, uid: de.UID}
				if deleteOptions.Preconditions != nil && deleteOptions.Preconditions.UID != nil {
					record.preconditionUID = *deleteOptions.Preconditions.UID
				}

				evidenceMu.Lock()
				deleted[de.Name] = record
				evidenceMu.Unlock()

				return cl.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	mappings := make([]dataExportTargetMapping, 0, len(targets))
	for _, target := range targets {
		mappings = append(mappings, dataExportTargetMapping{
			apiVersion: target.apiVersion,
			kind:       target.kind,
			resource:   target.resource,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            t.TempDir(),
		Workers:              2,
		PerVolumeConcurrency: 1,
		MaxParallelDownloads: 2,
		KubeClient:           c,
		AggClient:            dataExportAggClient(t, mappings...),
		ManifestSource:       testManifestSource(),
		TransportClient:      authenticatedTestTransportClient(),
		RunID:                runID,
		ReadinessTimeout:     5 * time.Second,
	}

	require.NoError(t, runPipeline(ctx, cfg))
	require.Equal(t, int64(len(targets)), started.Load(),
		"both different-GVK leaves must enter transfer concurrently")

	evidenceMu.Lock()
	createdCopy := make(map[string]*deapi.DataExport, len(created))
	for name, de := range created {
		createdCopy[name] = de.DeepCopy()
	}
	deletedCopy := make(map[string]deleteRecord, len(deleted))
	for name, record := range deleted {
		deletedCopy[name] = record
	}
	evidenceMu.Unlock()

	require.Len(t, createdCopy, len(targets))
	require.Len(t, deletedCopy, len(targets))

	names := make([]string, 0, len(targets))
	for _, target := range targets {
		name := exporter.DataExportName(
			testNS,
			target.group,
			target.resource,
			target.kind,
			leafName,
			target.snapshotUID,
		)
		names = append(names, name)

		de := createdCopy[name]
		require.NotNil(t, de, "production path must create the expected canonical DataExport")
		require.Equal(t, target.dataExportUID, de.UID)
		require.Equal(t, runID, de.Annotations[runOwnerAnnotationKey])
		require.Equal(t, string(target.snapshotUID), de.Annotations[targetUIDAnnotationKey])
		require.Equal(t, deapi.TargetRefSpec{
			Group:    target.group,
			Resource: target.resource,
			Kind:     target.kind,
			Name:     leafName,
		}, de.Spec.TargetRef)

		record, ok := deletedCopy[name]
		require.True(t, ok, "each acquired DataExport must be independently cleaned up")
		require.Equal(t, target.dataExportUID, record.uid)
		require.Equal(t, target.dataExportUID, record.preconditionUID,
			"cleanup must use the exact acquired UID as its delete precondition")

		remaining := new(deapi.DataExport)
		getErr := c.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, remaining)
		require.True(t, apierrors.IsNotFound(getErr), "owned DataExport %q must be removed", name)
	}

	require.NotEqual(t, names[0], names[1], "same-name leaves with different GVK must never alias")
}

func TestPipeline_ProductionSnapshotUIDRecreationIsolated(t *testing.T) {
	t.Parallel()

	const (
		uidA   = types.UID("uid-snapshot-a")
		uidB   = types.UID("uid-snapshot-b")
		runA   = "run-snapshot-a"
		runB   = "run-snapshot-b"
		deUIDA = types.UID("uid-dataexport-a")
		deUIDB = types.UID("uid-dataexport-b")
	)

	type serverEvidence struct {
		server   *httptest.Server
		requests atomic.Int64
	}

	servers := map[types.UID]*serverEvidence{}
	for _, targetUID := range []types.UID{uidA, uidB} {
		evidence := new(serverEvidence)
		evidence.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			evidence.requests.Add(1)
			http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(bytes.Repeat([]byte(targetUID), 40)))
		}))
		t.Cleanup(evidence.server.Close)

		servers[targetUID] = evidence
	}

	root := snapObj{
		apiVersion: storageAPIVersion,
		kind:       "Snapshot",
		namespace:  testNS,
		name:       rootSnapshot,
		uid:        "uid-recreation-root",
		sourceRef:  namespaceSourceRefMap(testNS, "uid-ns"),
		children:   []map[string]interface{}{childRefMap(childAPIVersion, childKind, diskSnapName)},
	}.build()
	childA := snapObj{
		apiVersion: childAPIVersion,
		kind:       childKind,
		namespace:  testNS,
		name:       diskSnapName,
		uid:        string(uidA),
		data:       pvcData(testNS, sourcePVCName, "uid-pvc-a", "vsc-a"),
	}.build()

	type deleteEvidence struct {
		uid          types.UID
		precondition types.UID
	}

	var (
		evidenceMu         sync.Mutex
		lifecycleUIDs      = make([]types.UID, 0, 2)
		createdByTargetUID = make(map[types.UID]*deapi.DataExport, 2)
		deleted            = make(map[string]deleteEvidence, 1)
	)

	c := fake.NewClientBuilder().
		WithScheme(buildScheme(t)).
		WithObjects(root, childA).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				opts ...client.CreateOption,
			) error {
				de, ok := obj.(*deapi.DataExport)
				if !ok {
					return cl.Create(ctx, obj, opts...)
				}

				targetUID := types.UID(de.Annotations[targetUIDAnnotationKey])
				serverEvidence := servers[targetUID]
				if serverEvidence == nil {
					return fmt.Errorf("unexpected target UID annotation %q", targetUID)
				}

				switch targetUID {
				case uidA:
					de.UID = deUIDA
				case uidB:
					de.UID = deUIDB
				}

				certificate := serverEvidence.server.Certificate()
				caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
				de.Status = deapi.DataExportStatus{
					URL:        serverEvidence.server.URL,
					CA:         base64.StdEncoding.EncodeToString(caPEM),
					VolumeMode: "Block",
					Conditions: []metav1.Condition{{
						Type:   "Ready",
						Status: metav1.ConditionTrue,
						Reason: "PodReady",
					}},
				}

				if err := cl.Create(ctx, de, opts...); err != nil {
					return err
				}

				evidenceMu.Lock()
				lifecycleUIDs = append(lifecycleUIDs, targetUID)
				createdByTargetUID[targetUID] = de.DeepCopy()
				evidenceMu.Unlock()

				return nil
			},
			Delete: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				opts ...client.DeleteOption,
			) error {
				de, ok := obj.(*deapi.DataExport)
				if !ok {
					return cl.Delete(ctx, obj, opts...)
				}

				deleteOptions := client.DeleteOptions{}
				deleteOptions.ApplyOptions(opts)
				record := deleteEvidence{uid: de.UID}
				if deleteOptions.Preconditions != nil && deleteOptions.Preconditions.UID != nil {
					record.precondition = *deleteOptions.Preconditions.UID
				}

				evidenceMu.Lock()
				deleted[de.Name] = record
				evidenceMu.Unlock()

				return cl.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	baseConfig := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		AggClient: dataExportAggClient(t, dataExportTargetMapping{
			apiVersion: childAPIVersion,
			kind:       childKind,
			resource:   "virtualdisksnapshots",
		}),
		ManifestSource:   testManifestSource(),
		TransportClient:  authenticatedTestTransportClient(),
		ReadinessTimeout: 5 * time.Second,
		ReleaseTimeout:   time.Second,
	}

	cfgA := baseConfig
	cfgA.OutputDir = t.TempDir()
	cfgA.RunID = runA
	cfgA.KeepExports = true
	require.NoError(t, runPipeline(context.Background(), cfgA))

	evidenceMu.Lock()
	createdA := createdByTargetUID[uidA]
	lifecycleUIDsAfterA := append([]types.UID(nil), lifecycleUIDs...)
	evidenceMu.Unlock()

	require.Equal(t, []types.UID{uidA}, lifecycleUIDsAfterA,
		"the first Snapshot UID must reach the production DataExport lifecycle")
	require.NotNil(t, createdA)
	createdA = createdA.DeepCopy()

	nameA := createdA.Name
	staleA := new(deapi.DataExport)
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: nameA}, staleA))
	staleABefore := staleA.DeepCopy()
	requestsAAfterFirstRun := servers[uidA].requests.Load()
	require.Positive(t, requestsAAfterFirstRun)

	require.NoError(t, c.Delete(context.Background(), childA))
	childB := snapObj{
		apiVersion: childAPIVersion,
		kind:       childKind,
		namespace:  testNS,
		name:       diskSnapName,
		uid:        string(uidB),
		data:       pvcData(testNS, sourcePVCName, "uid-pvc-b", "vsc-b"),
	}.build()
	require.NoError(t, c.Create(context.Background(), childB))

	cfgB := baseConfig
	cfgB.OutputDir = t.TempDir()
	cfgB.RunID = runB
	require.NoError(t, runPipeline(context.Background(), cfgB))

	evidenceMu.Lock()
	createdB := createdByTargetUID[uidB]
	lifecycleUIDsAfterB := append([]types.UID(nil), lifecycleUIDs...)
	deletedCopy := make(map[string]deleteEvidence, len(deleted))
	for name, record := range deleted {
		deletedCopy[name] = record
	}
	evidenceMu.Unlock()

	require.Equal(t, []types.UID{uidA, uidB}, lifecycleUIDsAfterB,
		"both Snapshot incarnations must reach the production DataExport lifecycle with their exact UIDs")
	require.NotNil(t, createdB)
	createdB = createdB.DeepCopy()

	nameB := createdB.Name
	require.NotEqual(t, nameA, nameB)
	require.LessOrEqual(t, len(nameA), 63)
	require.LessOrEqual(t, len(nameB), 63)
	require.Empty(t, validation.IsDNS1123Label(nameA))
	require.Empty(t, validation.IsDNS1123Label(nameB))
	require.Equal(t, requestsAAfterFirstRun, servers[uidA].requests.Load(),
		"UID B must never stream from stale UID A")
	require.Positive(t, servers[uidB].requests.Load())

	staleAAfter := new(deapi.DataExport)
	require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: nameA}, staleAAfter))
	require.Equal(t, staleABefore, staleAAfter, "UID B must not mutate or delete stale UID A")

	require.Equal(t, deUIDA, createdA.UID)
	require.Equal(t, string(uidA), createdA.Annotations[targetUIDAnnotationKey])
	require.Equal(t, runA, createdA.Annotations[runOwnerAnnotationKey])
	require.Equal(t, deUIDB, createdB.UID)
	require.Equal(t, string(uidB), createdB.Annotations[targetUIDAnnotationKey])
	require.Equal(t, runB, createdB.Annotations[runOwnerAnnotationKey])
	require.Equal(t, createdA.Spec.TargetRef, createdB.Spec.TargetRef)

	deletedB, ok := deletedCopy[nameB]
	require.True(t, ok)
	require.Equal(t, deUIDB, deletedB.uid)
	require.Equal(t, deUIDB, deletedB.precondition)
	require.NotContains(t, deletedCopy, nameA)

	remainingB := new(deapi.DataExport)
	getErr := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: nameB}, remainingB)
	require.True(t, apierrors.IsNotFound(getErr))
}

func TestPipeline_ProductionCreateGetCancellationCleansExactAcquisition(t *testing.T) {
	t.Parallel()

	const (
		runID       = "run-create-get-cancel"
		acquiredUID = types.UID("uid-created-before-get-cancel")
	)

	deName := diskSnapshotDataExportName()

	var (
		dataExportGets atomic.Int32
		evidenceMu     sync.Mutex
		created        *deapi.DataExport
		deletedUID     types.UID
		precondition   types.UID
	)

	c := buildFakeClientBuilder(t).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(
				ctx context.Context,
				cl client.WithWatch,
				key client.ObjectKey,
				obj client.Object,
				opts ...client.GetOption,
			) error {
				if _, ok := obj.(*deapi.DataExport); ok && key.Name == deName {
					if dataExportGets.Add(1) == 2 {
						return fmt.Errorf("deterministic post-create get cancellation: %w", context.Canceled)
					}
				}

				return cl.Get(ctx, key, obj, opts...)
			},
			Create: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				opts ...client.CreateOption,
			) error {
				de, ok := obj.(*deapi.DataExport)
				if !ok {
					return cl.Create(ctx, obj, opts...)
				}

				de.UID = acquiredUID
				if err := cl.Create(ctx, de, opts...); err != nil {
					return err
				}

				evidenceMu.Lock()
				created = de.DeepCopy()
				evidenceMu.Unlock()

				return nil
			},
			Delete: func(
				ctx context.Context,
				cl client.WithWatch,
				obj client.Object,
				opts ...client.DeleteOption,
			) error {
				de, ok := obj.(*deapi.DataExport)
				if !ok {
					return cl.Delete(ctx, obj, opts...)
				}

				deleteOptions := client.DeleteOptions{}
				deleteOptions.ApplyOptions(opts)

				evidenceMu.Lock()
				deletedUID = de.UID
				if deleteOptions.Preconditions != nil && deleteOptions.Preconditions.UID != nil {
					precondition = *deleteOptions.Preconditions.UID
				}
				evidenceMu.Unlock()

				return cl.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    t.TempDir(),
		Workers:      1,
		KubeClient:   c,
		AggClient: dataExportAggClient(t, dataExportTargetMapping{
			apiVersion: childAPIVersion,
			kind:       childKind,
			resource:   "virtualdisksnapshots",
		}),
		ManifestSource:   testManifestSource(),
		TransportClient:  authenticatedTestTransportClient(),
		RunID:            runID,
		ReadinessTimeout: time.Second,
		ReleaseTimeout:   time.Second,
	}

	err := runPipeline(context.Background(), cfg)
	require.ErrorIs(t, err, context.Canceled)

	evidenceMu.Lock()
	createdCopy := created
	deletedUIDCopy := deletedUID
	preconditionCopy := precondition
	evidenceMu.Unlock()

	require.NotNil(t, createdCopy)
	require.Equal(t, acquiredUID, createdCopy.UID)
	require.Equal(t, diskSnapUID, createdCopy.Annotations[targetUIDAnnotationKey])
	require.Equal(t, runID, createdCopy.Annotations[runOwnerAnnotationKey])
	require.Equal(t, acquiredUID, deletedUIDCopy)
	require.Equal(t, acquiredUID, preconditionCopy)

	remaining := new(deapi.DataExport)
	getErr := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: deName}, remaining)
	require.True(t, apierrors.IsNotFound(getErr))
}

func TestPipeline_ClosesExportHTTPClientsOnEveryTransferExit(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		first := &pipelineIdleCloser{}
		second := &pipelineIdleCloser{}
		srv := makeBlockServer(t, []byte("block-content"))
		defer srv.Close()

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            t.TempDir(),
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           buildFakeClient(t),
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				return exporter.NewExport(
					namespace,
					"de-success",
					"Block",
					srv.URL,
					exporter.NewFetcher(srv.Client()),
					first,
					second,
				), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))
		require.Equal(t, int64(1), first.calls.Load())
		require.Equal(t, int64(1), second.calls.Load())
	})

	t.Run("error", func(t *testing.T) {
		first := &pipelineIdleCloser{}
		second := &pipelineIdleCloser{}

		cfg := pipeline.Config{
			Namespace:    testNS,
			RootSnapshot: rootSnapshot,
			OutputDir:    t.TempDir(),
			Workers:      1,
			KubeClient:   buildFakeClient(t),
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				return exporter.NewExport(
					namespace,
					"de-error",
					"Unsupported",
					"https://exporter.invalid",
					nil,
					first,
					second,
				), nil
			},
		}

		require.Error(t, runPipeline(context.Background(), cfg))
		require.Equal(t, int64(1), first.calls.Load())
		require.Equal(t, int64(1), second.calls.Load())
	})

	t.Run("cancellation", func(t *testing.T) {
		first := &pipelineIdleCloser{}
		second := &pipelineIdleCloser{}
		requestStarted := make(chan struct{})
		var signalOnce sync.Once

		srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			signalOnce.Do(func() { close(requestStarted) })
			<-r.Context().Done()
		}))
		defer srv.Close()

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            t.TempDir(),
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           buildFakeClient(t),
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				return exporter.NewExport(
					namespace,
					"de-cancel",
					"Block",
					srv.URL,
					exporter.NewFetcher(srv.Client()),
					first,
					second,
				), nil
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Second, cancel)

		go func() {
			<-requestStarted
			cancel()
		}()

		err := runPipeline(ctx, cfg)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, int64(1), first.calls.Load())
		require.Equal(t, int64(1), second.calls.Load())
	})
}

type pipelineIdleCloser struct {
	calls atomic.Int64
}

func (c *pipelineIdleCloser) CloseIdleConnections() {
	c.calls.Add(1)
}

func readyFilesystemDataExport(t *testing.T, srv *httptest.Server) *deapi.DataExport {
	t.Helper()

	certificate := srv.Certificate()
	require.NotNil(t, certificate)

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})

	return &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:        diskSnapshotDataExportName(),
			Namespace:   testNS,
			Annotations: map[string]string{targetUIDAnnotationKey: diskSnapUID},
		},
		Spec: deapi.DataexportSpec{
			TTL: "2h",
			TargetRef: deapi.TargetRefSpec{
				Group:    "demo.deckhouse.io",
				Resource: "virtualdisksnapshots",
				Kind:     childKind,
				Name:     diskSnapName,
			},
		},
		Status: deapi.DataExportStatus{
			URL:        srv.URL,
			CA:         base64.StdEncoding.EncodeToString(caPEM),
			VolumeMode: "Filesystem",
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
					Reason: "PodReady",
				},
			},
		},
	}
}

func requireEventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition was not satisfied before timeout")
}

// TestPipeline_ChecksumMismatchAfterFinalize_SurfacesNotReblessed is the
// end-to-end regression for resume-checksum-mismatch: a node is fully
// downloaded and finalized, then its data.bin is corrupted AFTER finalize. On
// the next Run into the same output dir the run must FAIL with a wrapped
// ErrChecksumMismatch and MUST NOT re-stamp snapshot.yaml with the corrupt
// data's digest (the silent re-bless the fix closes).
func TestPipeline_ChecksumMismatchAfterFinalize_SurfacesNotReblessed(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("B"), 600)

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
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)

	// Capture the finalized (correct) checksum and snapshot.yaml mtime.
	syBefore, err := archive.ReadSnapshotYAML(diskSnapDir)
	require.NoError(t, err)

	diskSnapYAML := filepath.Join(diskSnapDir, archive.SnapshotYAMLName)
	yamlModBefore := statMtime(t, diskSnapYAML)

	// Corrupt the finalized volume payload (bit rot / tamper after finalize).
	blockPath := filepath.Join(diskSnapDir, archive.DataBlockName(".zst"))
	orig, err := os.ReadFile(blockPath)
	require.NoError(t, err)

	corrupt := append([]byte(nil), orig...)
	corrupt[0] ^= 0xFF
	require.NoError(t, os.WriteFile(blockPath, corrupt, 0o644))

	time.Sleep(20 * time.Millisecond)

	// The next run must surface the mismatch, not skip-and-re-bless it.
	err = runPipeline(context.Background(), cfg)
	require.Error(t, err, "a post-finalize checksum mismatch must fail the run")
	require.ErrorIs(t, err, archive.ErrChecksumMismatch)

	// snapshot.yaml must NOT be rewritten to the corrupt data's digest.
	require.Equal(t, yamlModBefore, statMtime(t, diskSnapYAML),
		"snapshot.yaml must not be re-stamped over corrupt data")

	syAfter, err := archive.ReadSnapshotYAML(diskSnapDir)
	require.NoError(t, err)
	require.Equal(t, syBefore.Checksum.Hex, syAfter.Checksum.Hex,
		"recorded checksum must not be re-blessed to the corrupt digest")
}

// TestPipeline_CrashWindowDeleteSnapshotYAML_ReFinalizes pins the crash-window
// regression that must keep working: data committed but snapshot.yaml never
// written (here: deleted after a full run) re-finalizes on the next run rather
// than being surfaced as a mismatch.
func TestPipeline_CrashWindowDeleteSnapshotYAML_ReFinalizes(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("B"), 600)

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
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)

	// Simulate a crash after the block volume committed but before snapshot.yaml
	// was written: re-stamp the identity marker (finalize removed it on the first
	// run) and delete snapshot.yaml here and in every ancestor, which bottom-up
	// publication could not have written yet. The merged data.bin.zst stays in
	// place.
	reseedResumeMarkerAndDropEnvelopesUpTo(t, outputDir, diskSnapDir)

	// OpenExport must not run: the merged data is detected and only FinalizeNode
	// re-runs.
	cfg.OpenExport = func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
		t.Error("OpenExport must not be called: data.bin.zst already merged")

		return nil, errors.New("unexpected OpenExport call")
	}

	require.NoError(t, runPipeline(context.Background(), cfg))
	assertNodeComplete(t, diskSnapDir)
	assertNoIdentityMarkers(t, outputDir)
}

// publicationTransactionPath is where the download pipeline keeps its active
// publication transaction record, and publicationReceiptPath its receipt: both
// live directly in the archive root, beside the root node's snapshot.yaml.
//
// Neither may live under archive.SnapshotsDirName. That directory carries child
// node directories and nothing else — snapimport.childNodeNames opens every entry
// it finds there as a directory, so one regular file there aborts upload planning
// (see TestPipeline_PublicationStateKeepsUploadAndLocalScanWorking). Both helpers
// mirror pipeline.publicationStatePath, which the internal
// publication_transaction_test.go exercises directly.
func publicationTransactionPath(outputDir string) string {
	return filepath.Join(outputDir, ".d8-snapshot-publication-v1.json")
}

func publicationReceiptPath(outputDir string) string {
	return filepath.Join(outputDir, ".d8-snapshot-publication-receipt-v1.json")
}

// crashOnEnvelopeDurability returns a directory-sync hook that fails nodeDir's
// durability confirmation in the window where that node's own snapshot.yaml has
// just become visible while a publication transaction record is already durable —
// the "crashed while publishing this node's envelope" fixture state.
//
// The snapshot.yaml precondition is load-bearing, not decoration. The transaction
// record lives directly in the archive root, so publishing the record confirms
// that same directory; keying on the directory and the record's existence alone
// would fire on the record's OWN commit instead, which happens before any
// envelope exists and would leave the fixture never reaching envelope
// publication at all.
func crashOnEnvelopeDurability(nodeDir, transactionPath string, crashErr error) archive.DirectorySyncHook {
	return func(path string, next func() error) error {
		if filepath.Clean(path) != filepath.Clean(nodeDir) {
			return next()
		}

		if _, err := os.Stat(transactionPath); err != nil {
			return next()
		}

		if _, err := os.Stat(filepath.Join(nodeDir, archive.SnapshotYAMLName)); err != nil {
			return next()
		}

		return crashErr
	}
}

func TestPipeline_PublicationTransactionRecoversBottomUpAfterParentCrash(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("P"), 600)

	srv := makeBlockServer(t, rawBlock)
	defer srv.Close()

	outputDir := t.TempDir()
	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           buildFakeClient(t),
		OpenExport: func(
			_ context.Context,
			namespace string,
			_ aggapi.NodeRef,
			_ string,
		) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	transactionPath := publicationTransactionPath(outputDir)
	crashErr := errors.New("injected parent publication crash")
	ctx := archive.WithDirectorySyncHook(
		context.Background(),
		crashOnEnvelopeDurability(outputDir, transactionPath, crashErr),
	)

	err := runPipeline(ctx, cfg)
	require.ErrorIs(t, err, crashErr)
	require.FileExists(t, transactionPath)
	transactionData, err := os.ReadFile(transactionPath)
	require.NoError(t, err)

	childDir := filepath.Join(
		outputDir,
		archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName),
	)
	assertNodeComplete(t, childDir)

	var openExportCalls atomic.Int64
	cfg.OpenExport = func(
		_ context.Context,
		_ string,
		_ aggapi.NodeRef,
		_ string,
	) (*exporter.Export, error) {
		openExportCalls.Add(1)
		t.Error("OpenExport must not run while replaying durable publication")

		return nil, errors.New("unexpected OpenExport call")
	}

	receiptPath := publicationReceiptPath(outputDir)
	cleanupErr := errors.New("injected transaction cleanup crash")
	// The cleanup window is confirmed in the archive root now that both records
	// live there: it is the confirmation of the cleaned receipt, taken once the
	// active transaction has already been removed.
	cleanupCtx := archive.WithDirectorySyncHook(
		context.Background(),
		func(path string, next func() error) error {
			if filepath.Clean(path) == filepath.Clean(outputDir) {
				_, transactionErr := os.Stat(transactionPath)
				_, receiptErr := os.Stat(receiptPath)
				if errors.Is(transactionErr, os.ErrNotExist) && receiptErr == nil {
					return cleanupErr
				}
			}

			return next()
		},
	)
	require.ErrorIs(t, runPipeline(cleanupCtx, cfg), cleanupErr)
	require.NoFileExists(t, transactionPath)
	require.FileExists(t, receiptPath)

	require.NoError(t, runPipeline(context.Background(), cfg))
	assertNodeComplete(t, outputDir)
	require.NoFileExists(t, transactionPath)

	// Cleanup replay is convergent: after receipt publication and active-record
	// removal, an additional run is a no-op success.
	require.NoError(t, runPipeline(context.Background(), cfg))

	// A transaction that reappears after its authenticated cleaned receipt is
	// fail-closed rather than authorizing a second publication.
	require.NoError(t, os.WriteFile(transactionPath, transactionData, 0o600))
	err = runPipeline(context.Background(), cfg)
	require.ErrorContains(t, err, "replayed after durable cleanup")
	require.Zero(t, openExportCalls.Load())

	require.NoError(t, os.WriteFile(transactionPath, []byte("{"), 0o600))
	err = runPipeline(context.Background(), cfg)
	require.ErrorContains(t, err, "decode publication transaction")
	require.Zero(t, openExportCalls.Load())

	var foreignTransaction map[string]interface{}
	require.NoError(t, json.Unmarshal(transactionData, &foreignTransaction))
	foreignTransaction["archiveRoot"] = outputDir + "-foreign"
	foreignData, err := json.Marshal(foreignTransaction)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(transactionPath, foreignData, 0o600))
	err = runPipeline(context.Background(), cfg)
	require.ErrorContains(t, err, "does not match this archive and source tree")
	require.Zero(t, openExportCalls.Load())

	require.NoError(t, os.Remove(transactionPath))
	require.NoError(t, os.WriteFile(transactionPath+".tmp", []byte("partial"), 0o600))
	require.NoError(t, runPipeline(context.Background(), cfg))
	require.Zero(t, openExportCalls.Load())
}

func TestPipeline_PublicationTransactionRecoversSuccessiveAncestorCrashes(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("T"), 600)

	srv := makeBlockServer(t, rawBlock)
	defer srv.Close()

	outputDir := t.TempDir()
	aggDir := filepath.Join(
		outputDir,
		archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, "agg-snap"),
	)
	leafDir := filepath.Join(
		aggDir,
		archive.SnapshotsDirName,
		archive.NodeDirName("VolumeSnapshot", "pvc-agg"),
	)
	transactionPath := publicationTransactionPath(outputDir)
	cfg := pipeline.Config{
		Namespace:            e2eNS,
		RootSnapshot:         e2eAggRootSnap,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           buildOrphanLeafFakeClient(t),
		OpenExport: func(
			_ context.Context,
			namespace string,
			_ aggapi.NodeRef,
			_ string,
		) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-tree", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	recoveryReads := make(map[string]int64)
	runWithCrash := func(nodeDir string, crashErr error) error {
		t.Helper()

		countingCtx := pipeline.WithPayloadChecksumObserver(
			context.Background(),
			func(nodeDir string, payloadBytes int64) {
				recoveryReads[nodeDir] += payloadBytes
			},
		)
		ctx := archive.WithDirectorySyncHook(
			countingCtx,
			crashOnEnvelopeDurability(nodeDir, transactionPath, crashErr),
		)

		return runPipeline(ctx, cfg)
	}

	firstCrash := errors.New("injected aggregator publication crash")
	require.ErrorIs(t, runWithCrash(aggDir, firstCrash), firstCrash)
	require.FileExists(t, transactionPath)
	assertNodeComplete(t, leafDir)
	require.NoFileExists(t, filepath.Join(outputDir, archive.SnapshotYAMLName))
	clear(recoveryReads)

	var openExportCalls atomic.Int64
	cfg.OpenExport = func(
		_ context.Context,
		_ string,
		_ aggapi.NodeRef,
		_ string,
	) (*exporter.Export, error) {
		openExportCalls.Add(1)

		return nil, errors.New("unexpected OpenExport call")
	}

	secondCrash := errors.New("injected root publication crash")
	require.ErrorIs(t, runWithCrash(outputDir, secondCrash), secondCrash)
	require.FileExists(t, transactionPath)
	require.Zero(t, openExportCalls.Load())

	finalRecoveryCtx := pipeline.WithPayloadChecksumObserver(
		context.Background(),
		func(nodeDir string, payloadBytes int64) {
			recoveryReads[nodeDir] += payloadBytes
		},
	)
	require.NoError(t, runPipeline(finalRecoveryCtx, cfg))
	assertNodeComplete(t, aggDir)
	assertNodeComplete(t, outputDir)
	require.NoFileExists(t, transactionPath)
	require.Zero(t, openExportCalls.Load())

	leafInfo, err := os.Stat(filepath.Join(leafDir, archive.DataBlockName(".zst")))
	require.NoError(t, err)
	require.LessOrEqual(t, recoveryReads[leafDir], leafInfo.Size())
	t.Logf("two-level recovery payload bytes %s=%d (payload=%d)",
		leafDir, recoveryReads[leafDir], leafInfo.Size())
}

func TestPipeline_StaleParentWithoutPublicationTransactionIsRejected(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("S"), 600)

	srv := makeBlockServer(t, rawBlock)
	defer srv.Close()

	outputDir := t.TempDir()
	cfg := pipeline.Config{
		Namespace:            e2eNS,
		RootSnapshot:         e2eAggRootSnap,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           buildOrphanLeafFakeClient(t),
		OpenExport: func(
			_ context.Context,
			namespace string,
			_ aggapi.NodeRef,
			_ string,
		) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-stale", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}
	require.NoError(t, runPipeline(context.Background(), cfg))

	leafDir := filepath.Join(
		outputDir,
		archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, "agg-snap"),
		archive.SnapshotsDirName,
		archive.NodeDirName("VolumeSnapshot", "pvc-agg"),
	)
	require.NoError(t, os.WriteFile(
		filepath.Join(leafDir, archive.ManifestsDirName, "republished.yaml"),
		[]byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: republished\n"),
		0o600,
	))

	metadata, err := archive.ReadSnapshotYAML(leafDir)
	require.NoError(t, err)
	metadata.Checksum, err = archive.ComputeNodeChecksum(leafDir)
	require.NoError(t, err)
	require.NoError(t, archive.WriteSnapshotYAML(leafDir, metadata))

	var openExportCalls atomic.Int64
	cfg.OpenExport = func(
		_ context.Context,
		_ string,
		_ aggapi.NodeRef,
		_ string,
	) (*exporter.Export, error) {
		openExportCalls.Add(1)

		return nil, errors.New("unexpected OpenExport call")
	}

	err = runPipeline(context.Background(), cfg)
	require.ErrorIs(t, err, archive.ErrChildrenChecksumMismatch)
	require.Zero(t, openExportCalls.Load())
}

func TestPipeline_StaleParentWithIncompleteSiblingIsRejectedAtRunStart(t *testing.T) {
	const payloadSize = int64(600)

	srv := makeSizedBlockServer(t, payloadSize, 'A')
	outputDir := t.TempDir()
	cfg := twoDataChildPipelineConfig(t, outputDir, srv)
	require.NoError(t, runPipeline(context.Background(), cfg))

	childADir := twoDataChildDir(outputDir, "disk-a")
	childBDir := twoDataChildDir(outputDir, "disk-b")
	require.NoError(t, os.WriteFile(
		filepath.Join(childADir, archive.ManifestsDirName, "republished.yaml"),
		[]byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: republished\n"),
		0o600,
	))

	metadata, err := archive.ReadSnapshotYAML(childADir)
	require.NoError(t, err)
	metadata.Checksum, err = archive.ComputeNodeChecksum(childADir)
	require.NoError(t, err)
	require.NoError(t, archive.WriteSnapshotYAML(childADir, metadata))

	reseedResumeMarkerFromSnapshotYAML(t, childBDir)
	require.NoError(t, os.Remove(filepath.Join(childBDir, archive.SnapshotYAMLName)))

	transactionPath := publicationTransactionPath(outputDir)
	receiptPath := publicationReceiptPath(outputDir)
	require.NoFileExists(t, transactionPath)
	receiptBefore, err := os.ReadFile(receiptPath)
	require.NoError(t, err)

	envelopesBefore := map[string][]byte{}
	for _, nodeDir := range []string{outputDir, childADir} {
		envelope, readErr := os.ReadFile(filepath.Join(nodeDir, archive.SnapshotYAMLName))
		require.NoError(t, readErr)
		envelopesBefore[nodeDir] = envelope
	}

	var openExportCalls atomic.Int64
	cfg.OpenExport = func(
		_ context.Context,
		_ string,
		_ aggapi.NodeRef,
		_ string,
	) (*exporter.Export, error) {
		openExportCalls.Add(1)

		return nil, errors.New("unexpected payload redownload")
	}

	err = runPipeline(context.Background(), cfg)
	require.ErrorIs(t, err, archive.ErrChildrenChecksumMismatch)
	require.Zero(t, openExportCalls.Load())
	require.NoFileExists(t, transactionPath)
	require.NoFileExists(t, filepath.Join(childBDir, archive.SnapshotYAMLName))
	receiptAfter, readErr := os.ReadFile(receiptPath)
	require.NoError(t, readErr)
	require.Equal(t, receiptBefore, receiptAfter)

	for nodeDir, before := range envelopesBefore {
		after, readErr := os.ReadFile(filepath.Join(nodeDir, archive.SnapshotYAMLName))
		require.NoError(t, readErr)
		require.Equal(t, before, after, "snapshot envelope changed at %s", nodeDir)
	}
}

// assertChildDirectoriesOnly fails if any archive.SnapshotsDirName directory in
// the tree rooted at root holds an entry that is not a directory.
//
// That is not a stylistic preference: every consumer of a downloaded archive
// treats snapshots/ as "child node directories and nothing else".
// snapimport.childNodeNames lists the directory and opens EVERY entry it returns
// as a directory, so one regular file there fails upload planning outright with
// archive.ErrNonRegularArchiveArtifact. localscan.scanDir and
// archive.computeNodeChildrenChecksum read the same directory under the same
// convention and happen to skip non-directories, so they tolerate a stray entry
// rather than sanction one. Download machinery sidecars therefore belong beside
// snapshot.yaml in the node directory root, never inside snapshots/.
func assertChildDirectoriesOnly(t *testing.T, root string) {
	t.Helper()

	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if !entry.IsDir() || entry.Name() != archive.SnapshotsDirName {
			return nil
		}

		children, err := os.ReadDir(path)
		if err != nil {
			return err
		}

		for _, child := range children {
			if !child.IsDir() {
				t.Errorf("%s must hold child node directories only, found non-directory %q",
					path, child.Name())
			}
		}

		return nil
	}))
}

// uploadLayoutPVCData extends pvcData with the captured-volume metadata that
// snapshot.yaml's strict envelope validation demands of a data node
// (storageClassName, a positive size, and a volumeMode agreeing with the on-disk
// payload). A real state-snapshotter status.data always carries them; the plain
// pvcData fixture omits them, which is enough for checksum-only assertions but
// not for archive.ValidateNodeMetadata, the pass localscan.ScanVerified and the
// upload preflight both run.
func uploadLayoutPVCData(pvcName, pvcUID, vscName, volumeMode string) map[string]interface{} {
	data := pvcData(e2eNS, pvcName, pvcUID, vscName)
	data["volumeMode"] = volumeMode
	data["storageClassName"] = "csi-upload-layout-sc"
	data["size"] = "1Gi"

	return data
}

// buildUploadLayoutFakeClient mirrors the three-level e2e tree (core Snapshot root
// -> domain aggregator -> one block and one filesystem data leaf) with fully
// populated status.data on both leaves.
func buildUploadLayoutFakeClient(t *testing.T) client.Client {
	t.Helper()

	root := snapObj{
		apiVersion: storageAPIVersion, kind: "Snapshot",
		namespace: e2eNS, name: e2eRootSnap, uid: "uid-upload-root",
		sourceRef: namespaceSourceRefMap(e2eNS, "uid-upload-ns"),
		children:  []map[string]interface{}{childRefMap(e2eVMAPIVersion, e2eVMKind, e2eVMSnap)},
	}.build()

	vmSnap := snapObj{
		apiVersion: e2eVMAPIVersion, kind: e2eVMKind,
		namespace: e2eNS, name: e2eVMSnap, uid: "uid-upload-vm",
		children: []map[string]interface{}{
			childRefMap(e2eVMAPIVersion, e2eDiskKind, e2eBlockDisk),
			childRefMap(e2eVMAPIVersion, e2eDiskKind, e2eFSDisk),
		},
	}.build()

	blockSnap := snapObj{
		apiVersion: e2eVMAPIVersion, kind: e2eDiskKind,
		namespace: e2eNS, name: e2eBlockDisk, uid: "uid-upload-block",
		data: uploadLayoutPVCData("pvc-block-source", "uid-block", e2eBlockVSC, archive.VolumeModeBlock),
	}.build()

	fsSnap := snapObj{
		apiVersion: e2eVMAPIVersion, kind: e2eDiskKind,
		namespace: e2eNS, name: e2eFSDisk, uid: "uid-upload-fs",
		data: uploadLayoutPVCData("pvc-fs-source", "uid-fs", e2eFSVSC, archive.VolumeModeFilesystem),
	}.build()

	return fake.NewClientBuilder().
		WithScheme(buildScheme(t)).
		WithObjects(root, vmSnap, blockSnap, fsSnap).
		Build()
}

// TestPipeline_PublicationStateKeepsUploadAndLocalScanWorking is the regression
// test for the publication records' on-disk location. A completed download leaves
// its publication receipt behind permanently, so wherever that record lands is
// part of the archive layout every downstream consumer must tolerate.
//
// Placing it inside archive.SnapshotsDirName broke two independent consumers of
// EVERY archive the pipeline produced: snapimport.BuildPlan (d8 snapshot upload's
// planner) opened the record as a child node directory and failed the whole plan,
// and localscan (d8 snapshot local/describe) walks the same directory under the
// same assumption. This test drives the real production pipeline over a
// three-level tree, then exercises both consumers plus the selected-node upload
// preflight against the resulting directory.
func TestPipeline_PublicationStateKeepsUploadAndLocalScanWorking(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("U"), e2eBlockSize)
	fsFiles := []fsE2EFile{
		{rel: "alpha.txt", content: []byte("upload-alpha")},
		{rel: "subdir/beta.txt", content: []byte("upload-beta")},
	}

	blockSrv := makeE2EBlockServer(t, rawBlock)
	fsSrv := makeE2EFSServer(t, fsFiles)

	outputDir := t.TempDir()
	cfg := pipeline.Config{
		Namespace:            e2eNS,
		RootSnapshot:         e2eRootSnap,
		OutputDir:            outputDir,
		Workers:              2,
		PerVolumeConcurrency: 2,
		KubeClient:           buildUploadLayoutFakeClient(t),
		OpenExport: func(_ context.Context, namespace string, leafRef aggapi.NodeRef, _ string) (*exporter.Export, error) {
			switch leafRef.Name {
			case e2eBlockDisk:
				return exporter.NewExport(namespace, "de-upload-block", "Block", blockSrv.URL,
					exporter.NewFetcher(blockSrv.Client())), nil
			case e2eFSDisk:
				return exporter.NewExport(namespace, "de-upload-fs", "Filesystem", fsSrv.URL,
					exporter.NewFetcher(fsSrv.Client())), nil
			default:
				return nil, fmt.Errorf("upload-layout: unknown leaf %q", leafRef.Name)
			}
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	vmDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eVMKind, e2eVMSnap))
	blockDir := filepath.Join(vmDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eBlockDisk))
	fsDir := filepath.Join(vmDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, e2eFSDisk))

	for _, nodeDir := range []string{outputDir, vmDir, blockDir, fsDir} {
		assertNodeComplete(t, nodeDir)
	}

	// The receipt survives a completed run and lives beside the root envelope,
	// exactly like the identity sidecar convention, never inside snapshots/.
	require.FileExists(t, publicationReceiptPath(outputDir))
	require.NoFileExists(t, publicationTransactionPath(outputDir))
	require.NoFileExists(t, filepath.Join(outputDir, archive.SnapshotsDirName,
		filepath.Base(publicationReceiptPath(outputDir))))
	assertChildDirectoriesOnly(t, outputDir)

	wantPlan := []string{
		e2eDiskKind + "/" + e2eBlockDisk,
		e2eDiskKind + "/" + e2eFSDisk,
		e2eVMKind + "/" + e2eVMSnap,
		"Snapshot/" + e2eRootSnap,
	}

	planLabels := func(t *testing.T, rootDir string) []string {
		t.Helper()

		plan, err := snapimport.BuildPlan(rootDir)
		require.NoError(t, err, "upload plan must build from %s", rootDir)

		labels := make([]string, 0, len(plan))
		for _, node := range plan {
			labels = append(labels, node.Kind+"/"+node.Name)
			require.NotEmpty(t, node.NodeChecksum, "planned node %s carries no checksum", node.Name)
		}

		return labels
	}

	// Full-tree upload semantics: post-order, leaves first, archive root last.
	require.Equal(t, wantPlan, planLabels(t, outputDir))

	// Selected-node upload semantics: every subtree root must plan standalone, and
	// the ancestors --node excludes must still authenticate through the pinned
	// archive (snapimport.verifySelectedNodeExternalAncestors' contract).
	require.Equal(t, wantPlan[:3], planLabels(t, vmDir))
	require.Equal(t, wantPlan[:1], planLabels(t, blockDir))

	view, err := archive.OpenVerifiedArchive(outputDir)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, view.Close()) })

	for _, ancestorDir := range []string{vmDir, outputDir} {
		require.NoError(t, view.VerifyNodeChildrenChecksum(context.Background(), ancestorDir),
			"excluded ancestor %s must authenticate", ancestorDir)
	}

	verified, err := view.VerifyNode(context.Background(), blockDir)
	require.NoError(t, err)
	require.NotEmpty(t, verified.Checksum().Hex)

	// d8 snapshot local / describe walk the same snapshots/ directories.
	assertLocalScan := func(t *testing.T, scan func(string) (*localscan.Node, error), name string) {
		t.Helper()

		root, err := scan(outputDir)
		require.NoError(t, err, "%s must succeed on a downloaded archive", name)
		require.Equal(t, e2eRootSnap, root.Name)
		require.Len(t, root.Children, 1)
		require.Equal(t, e2eVMSnap, root.Children[0].Name)
		require.Len(t, root.Children[0].Children, 2)
		require.Equal(t, 2, root.VolumeCount())
	}

	assertLocalScan(t, localscan.Scan, "localscan.Scan")
	assertLocalScan(t, localscan.ScanVerified, "localscan.ScanVerified")

	// A resume run over the completed tree republishes nothing and must leave both
	// consumers working against the same directory.
	require.NoError(t, runPipeline(context.Background(), cfg))
	assertChildDirectoriesOnly(t, outputDir)
	require.Equal(t, wantPlan, planLabels(t, outputDir))
	assertLocalScan(t, localscan.ScanVerified, "localscan.ScanVerified after resume")
}

// TestPipeline_PayloadChecksumReadsAreBounded bounds the payload bytes the
// PUBLICATION path hashes, counted through the production Run call path over
// multi-chunk data-bearing nodes.
//
// Scope matters for reading the numbers: the observer sees only publication
// checksum passes (transaction construction and the recovery content
// re-verification). A fresh node is hashed exactly once there and that one
// digest is then carried through the transaction entry into finalization and
// envelope-only post-publication verification. A completed rerun publishes
// nothing, hence zero publication-path bytes — but it is NOT a zero-read run:
// the resume scan still performs its own single full-content verification per
// already-sealed node, which is the only thing that can catch a same-size
// payload rewrite after finalization (see
// TestPipeline_ChecksumMismatchAfterFinalize_SurfacesNotReblessed). That gate is
// deliberately not optimized away; eliminating it would trade tamper detection
// for I/O.
func TestPipeline_PayloadChecksumReadsAreBounded(t *testing.T) {
	const payloadSize = int64(volume.DefaultChunkSize) + 1

	srv := makeSizedBlockServer(t, payloadSize, 'B')
	outputDir := t.TempDir()
	cfg := twoDataChildPipelineConfig(t, outputDir, srv)

	freshReads := make(map[string]int64)
	freshCtx := pipeline.WithPayloadChecksumObserver(
		context.Background(),
		func(nodeDir string, payloadBytes int64) {
			freshReads[nodeDir] += payloadBytes
		},
	)
	require.NoError(t, runPipeline(freshCtx, cfg))

	dataNodeDirs := []string{
		twoDataChildDir(outputDir, "disk-a"),
		twoDataChildDir(outputDir, "disk-b"),
	}
	for _, nodeDir := range dataNodeDirs {
		info, err := os.Stat(filepath.Join(nodeDir, archive.DataBlockName(".zst")))
		require.NoError(t, err)
		require.Equal(t, info.Size(), freshReads[nodeDir], "fresh checksum payload bytes at %s", nodeDir)
		require.Greater(t, info.Size(), int64(0))
		t.Logf("fresh payload bytes %s=%d", nodeDir, freshReads[nodeDir])
	}

	completedReads := make(map[string]int64)
	completedCtx := pipeline.WithPayloadChecksumObserver(
		context.Background(),
		func(nodeDir string, payloadBytes int64) {
			completedReads[nodeDir] += payloadBytes
		},
	)
	require.NoError(t, runPipeline(completedCtx, cfg))
	for _, nodeDir := range dataNodeDirs {
		require.Zero(t, completedReads[nodeDir], "completed rerun payload bytes at %s", nodeDir)
		t.Logf("completed rerun payload bytes %s=%d", nodeDir, completedReads[nodeDir])
	}

	transactionPath := publicationTransactionPath(outputDir)
	reseedResumeMarkerFromSnapshotYAML(t, outputDir)
	require.NoError(t, os.Remove(filepath.Join(outputDir, archive.SnapshotYAMLName)))

	crashErr := errors.New("injected root publication crash")
	crashCtx := archive.WithDirectorySyncHook(
		context.Background(),
		crashOnEnvelopeDurability(outputDir, transactionPath, crashErr),
	)
	require.ErrorIs(t, runPipeline(crashCtx, cfg), crashErr)
	require.FileExists(t, transactionPath)

	recoveryReads := make(map[string]int64)
	recoveryCtx := pipeline.WithPayloadChecksumObserver(
		context.Background(),
		func(nodeDir string, payloadBytes int64) {
			recoveryReads[nodeDir] += payloadBytes
		},
	)
	require.NoError(t, runPipeline(recoveryCtx, cfg))
	for _, nodeDir := range dataNodeDirs {
		info, err := os.Stat(filepath.Join(nodeDir, archive.DataBlockName(".zst")))
		require.NoError(t, err)
		require.LessOrEqual(t, recoveryReads[nodeDir], info.Size(), "recovery checksum payload bytes at %s", nodeDir)
		t.Logf("one-level recovery payload bytes %s=%d (payload=%d)",
			nodeDir, recoveryReads[nodeDir], info.Size())
	}
}

func TestPipeline_ProductionTargetMismatchNeverCleansUpCollision(t *testing.T) {
	t.Parallel()

	const runID = "run-colliding-leaf"

	cases := []struct {
		name        string
		annotations map[string]string
	}{
		{name: "unstamped collision"},
		{
			name:        "same-run collision",
			annotations: map[string]string{"snapshot.deckhouse.io/download-run-id": runID},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			deName := diskSnapshotDataExportName()
			foreignRef := deapi.TargetRefSpec{
				Group:    "foreign.example.io",
				Resource: "foreignsnapshots",
				Kind:     "ForeignSnapshot",
				Name:     diskSnapName,
			}
			collision := &deapi.DataExport{
				ObjectMeta: metav1.ObjectMeta{
					Name:        deName,
					Namespace:   testNS,
					UID:         "uid-collision",
					Annotations: tc.annotations,
				},
				Spec: deapi.DataexportSpec{TTL: "2h", TargetRef: foreignRef},
			}

			var deleteCalls atomic.Int64

			c := buildFakeClientBuilder(t).
				WithObjects(collision).
				WithInterceptorFuncs(interceptor.Funcs{
					Delete: func(
						ctx context.Context,
						cl client.WithWatch,
						obj client.Object,
						opts ...client.DeleteOption,
					) error {
						if _, ok := obj.(*deapi.DataExport); ok {
							deleteCalls.Add(1)
						}

						return cl.Delete(ctx, obj, opts...)
					},
				}).
				Build()

			cfg := pipeline.Config{
				Namespace:    testNS,
				RootSnapshot: rootSnapshot,
				OutputDir:    t.TempDir(),
				Workers:      1,
				KubeClient:   c,
				AggClient: dataExportAggClient(t, dataExportTargetMapping{
					apiVersion: childAPIVersion,
					kind:       childKind,
					resource:   "virtualdisksnapshots",
				}),
				ManifestSource:   testManifestSource(),
				TransportClient:  authenticatedTestTransportClient(),
				RunID:            runID,
				ReadinessTimeout: time.Second,
			}

			err := runPipeline(context.Background(), cfg)
			require.ErrorIs(t, err, exporter.ErrTargetRefMismatch)
			require.Zero(t, deleteCalls.Load(),
				"a target mismatch must not gain cleanup authority from name or run annotation")

			preserved := new(deapi.DataExport)
			require.NoError(t, c.Get(
				context.Background(),
				types.NamespacedName{Namespace: testNS, Name: deName},
				preserved,
			))
			require.Equal(t, types.UID("uid-collision"), preserved.UID)
			require.Equal(t, tc.annotations, preserved.Annotations)
			require.Equal(t, foreignRef, preserved.Spec.TargetRef)
		})
	}
}

func TestPipeline_ProductionWaitFailureCleansExactAcquisition(t *testing.T) {
	const runID = "run-wait-failure"

	cases := []struct {
		name        string
		cancelRun   bool
		wantContext error
	}{
		{
			name:        "readiness error",
			wantContext: context.DeadlineExceeded,
		},
		{
			name:        "cancellation",
			cancelRun:   true,
			wantContext: context.Canceled,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const acquiredUID = types.UID("uid-wait-acquired")

			created := make(chan struct{})
			var createOnce sync.Once

			var (
				evidenceMu      sync.Mutex
				createdEvidence *deapi.DataExport
				deletedUID      types.UID
				preconditionUID types.UID
			)

			c := buildFakeClientBuilder(t).
				WithInterceptorFuncs(interceptor.Funcs{
					Create: func(
						ctx context.Context,
						cl client.WithWatch,
						obj client.Object,
						opts ...client.CreateOption,
					) error {
						de, ok := obj.(*deapi.DataExport)
						if !ok {
							return cl.Create(ctx, obj, opts...)
						}

						de.UID = acquiredUID
						if err := cl.Create(ctx, de, opts...); err != nil {
							return err
						}

						evidenceMu.Lock()
						createdEvidence = de.DeepCopy()
						evidenceMu.Unlock()
						createOnce.Do(func() { close(created) })

						return nil
					},
					Delete: func(
						ctx context.Context,
						cl client.WithWatch,
						obj client.Object,
						opts ...client.DeleteOption,
					) error {
						de, ok := obj.(*deapi.DataExport)
						if !ok {
							return cl.Delete(ctx, obj, opts...)
						}

						deleteOptions := client.DeleteOptions{}
						deleteOptions.ApplyOptions(opts)

						evidenceMu.Lock()
						deletedUID = de.UID
						if deleteOptions.Preconditions != nil && deleteOptions.Preconditions.UID != nil {
							preconditionUID = *deleteOptions.Preconditions.UID
						}
						evidenceMu.Unlock()

						return cl.Delete(ctx, obj, opts...)
					},
				}).
				Build()

			readinessTimeout := 50 * time.Millisecond
			if tc.cancelRun {
				readinessTimeout = 5 * time.Second
			}

			cfg := pipeline.Config{
				Namespace:    testNS,
				RootSnapshot: rootSnapshot,
				OutputDir:    t.TempDir(),
				Workers:      1,
				KubeClient:   c,
				AggClient: dataExportAggClient(t, dataExportTargetMapping{
					apiVersion: childAPIVersion,
					kind:       childKind,
					resource:   "virtualdisksnapshots",
				}),
				ManifestSource:   testManifestSource(),
				TransportClient:  authenticatedTestTransportClient(),
				RunID:            runID,
				ReadinessTimeout: readinessTimeout,
				ReleaseTimeout:   time.Second,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tc.cancelRun {
				go func() {
					<-created
					cancel()
				}()
			}

			err := runPipeline(ctx, cfg)
			require.ErrorIs(t, err, tc.wantContext)

			evidenceMu.Lock()
			var createdCopy *deapi.DataExport
			if createdEvidence != nil {
				createdCopy = createdEvidence.DeepCopy()
			}

			deletedUIDCopy := deletedUID
			preconditionUIDCopy := preconditionUID
			evidenceMu.Unlock()

			require.NotNil(t, createdCopy)
			require.Equal(t, acquiredUID, createdCopy.UID)
			require.Equal(t, runID, createdCopy.Annotations["snapshot.deckhouse.io/download-run-id"])
			require.Equal(t, acquiredUID, deletedUIDCopy)
			require.Equal(t, acquiredUID, preconditionUIDCopy,
				"wait failure cleanup must retain exact UID acquisition evidence")

			remaining := new(deapi.DataExport)
			getErr := c.Get(context.Background(), types.NamespacedName{
				Namespace: testNS,
				Name:      diskSnapshotDataExportName(),
			}, remaining)
			require.True(t, apierrors.IsNotFound(getErr),
				"the exact DataExport acquired before WaitReady failed must be removed")
		})
	}
}

// TestPipeline_OpenExportErrorReleasesCleanly is a regression guard for a
// live-reproduced leak: OpenExport's production implementation creates the
// DataExport CR (EnsureDataExport) BEFORE waiting for it to become Ready
// (WaitReady), so a cancellation/error during that wait can leave a DataExport
// behind even though OpenExport itself returns an error and no *exporter.Export
// value. The fake OpenExport below simulates exactly that: the DataExport is
// pre-seeded in the fake client (as if EnsureDataExport already created it),
// then OpenExport still fails (as if WaitReady errored). The pipeline must
// release the pre-seeded DataExport by its deterministic name even though it
// never received an *exporter.Export to call Release through.
func TestPipeline_OpenExportErrorReleasesCleanly(t *testing.T) {
	t.Parallel()

	const runID = "run-open-error"

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	deName := diskSnapshotDataExportName()

	de := &deapi.DataExport{
		TypeMeta: metav1.TypeMeta{APIVersion: "storage-foundation.deckhouse.io/v1alpha1", Kind: "DataExport"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: testNS,
			UID:       "uid-open-error",
			Annotations: map[string]string{
				runOwnerAnnotationKey:  runID,
				targetUIDAnnotationKey: diskSnapUID,
			},
		},
		Spec: deapi.DataexportSpec{
			TTL: "2h",
			TargetRef: deapi.TargetRefSpec{
				Group:    "demo.deckhouse.io",
				Resource: "virtualdisksnapshots",
				Kind:     childKind,
				Name:     diskSnapName,
			},
		},
	}
	require.NoError(t, c.Create(context.Background(), de))

	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    outputDir,
		Workers:      1,
		KubeClient:   c,
		RunID:        runID,
		OpenExportWithAcquisition: func(
			ctx context.Context,
			namespace string,
			leafRef aggapi.NodeRef,
			ttl string,
		) (*exporter.Export, *exporter.DataExportAcquisition, error) {
			var acquisition *exporter.DataExportAcquisition

			_, ensureErr := exporter.EnsureDataExport(
				ctx,
				c,
				namespace,
				"demo.deckhouse.io",
				"virtualdisksnapshots",
				childKind,
				leafRef.Name,
				ttl,
				exporter.WithTargetUID(types.UID(diskSnapUID)),
				exporter.WithRunOwner(runID, slog.Default()),
				exporter.WithAcquisition(&acquisition),
			)
			if ensureErr != nil {
				return nil, nil, ensureErr
			}

			return nil, acquisition, errors.New("simulated WaitReady cancellation after EnsureDataExport acquired the CR")
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.Error(t, err, "expected pipeline to fail when OpenExport errors")

	got := &deapi.DataExport{}
	getErr := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: deName}, got)
	require.Truef(t, apierrors.IsNotFound(getErr),
		"pre-seeded DataExport %q must be released even though OpenExport failed before returning an *exporter.Export, got err=%v", deName, getErr)
}

// ctxDeadlineClient wraps a client.Client and, on Get, returns ctx.Err() wrapped
// as a rate-limiter-style failure whenever ctx is already done, before ever
// delegating to the underlying client. This reproduces what client-go's rate
// limiter Wait(ctx) does against an already-expired context in production (the
// live incident WARN read "client rate limiter Wait returned an error: context
// deadline exceeded") — behavior the in-memory fake client does not exhibit on
// its own, since it never inspects ctx.
type ctxDeadlineClient struct {
	client.Client
}

// Get returns ctx's own error, wrapped, if ctx is already done; otherwise it
// delegates to the wrapped client unchanged.
func (c ctxDeadlineClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("client rate limiter Wait returned an error: %w", err)
	}

	return c.Client.Get(ctx, key, obj, opts...)
}

// TestPipeline_ReleaseGetsFreshTimeoutAfterSlowOpenExport is a regression guard
// for a live-reproduced leak distinct from TestPipeline_OpenExportErrorReleasesCleanly
// above: a FULLY SUCCESSFUL download whose OpenExport (EnsureDataExport +
// WaitReady) plus volume transfer together take longer than the release
// timeout used to leak its DataExport. The prior fix computed that timeout
// ONCE, before calling OpenExport, but the release defer only actually runs at
// function return — by which point the clock had often already run out on any
// real-sized volume. This test pins that the timeout budget is instead derived
// FRESH at the moment the release defer executes, so it is unaffected by how
// long the preceding work took.
//
// cfg.ReleaseTimeout is set to a short duration and OpenExport is stubbed to
// sleep past it before returning success — no real 30-second wait is needed;
// only the relative ordering (OpenExport's delay exceeds ReleaseTimeout)
// matters. ctxDeadlineClient supplies the "already-expired context fails the
// very next call" behavior that a real rate-limited client exhibits, which is
// what actually distinguishes the fixed and pre-fix implementations here.
func TestPipeline_ReleaseGetsFreshTimeoutAfterSlowOpenExport(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("B"), 600)

	srv := makeBlockServer(t, rawBlock)
	defer srv.Close()

	c := ctxDeadlineClient{buildFakeClient(t)}
	outputDir := t.TempDir()

	const releaseTimeout = 20 * time.Millisecond
	const runID = "run-slow-open"

	deName := diskSnapshotDataExportName()

	de := &deapi.DataExport{
		TypeMeta: metav1.TypeMeta{APIVersion: "storage-foundation.deckhouse.io/v1alpha1", Kind: "DataExport"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: testNS,
			UID:       "uid-slow-open",
			Annotations: map[string]string{
				runOwnerAnnotationKey:  runID,
				targetUIDAnnotationKey: diskSnapUID,
			},
		},
		Spec: deapi.DataexportSpec{
			TTL: "2h",
			TargetRef: deapi.TargetRefSpec{
				Group:    "demo.deckhouse.io",
				Resource: "virtualdisksnapshots",
				Kind:     childKind,
				Name:     diskSnapName,
			},
		},
	}
	require.NoError(t, c.Create(context.Background(), de))

	cfg := pipeline.Config{
		Namespace:      testNS,
		RootSnapshot:   rootSnapshot,
		OutputDir:      outputDir,
		Workers:        1,
		KubeClient:     c,
		ReleaseTimeout: releaseTimeout,
		RunID:          runID,
		OpenExportWithAcquisition: func(
			ctx context.Context,
			namespace string,
			leafRef aggapi.NodeRef,
			ttl string,
		) (*exporter.Export, *exporter.DataExportAcquisition, error) {
			// Simulate WaitReady taking longer than ReleaseTimeout, mirroring the
			// live repro where WaitReady alone took ~30s against a fixed 30s
			// budget. A pre-fix cleanupCtx created before this sleep would
			// already be expired by the time release runs.
			time.Sleep(3 * releaseTimeout)

			var acquisition *exporter.DataExportAcquisition

			_, ensureErr := exporter.EnsureDataExport(
				ctx,
				c,
				namespace,
				"demo.deckhouse.io",
				"virtualdisksnapshots",
				childKind,
				leafRef.Name,
				ttl,
				exporter.WithTargetUID(types.UID(diskSnapUID)),
				exporter.WithRunOwner(runID, slog.Default()),
				exporter.WithAcquisition(&acquisition),
			)
			if ensureErr != nil {
				return nil, nil, ensureErr
			}

			return exporter.NewExport(
				namespace,
				deName,
				"Block",
				srv.URL,
				exporter.NewFetcher(srv.Client()),
			), acquisition, nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err, "expected the download to succeed despite the slow OpenExport")

	got := &deapi.DataExport{}
	getErr := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: deName}, got)
	require.Truef(t, apierrors.IsNotFound(getErr),
		"DataExport %q must be released on a fully successful download even though OpenExport+transfer took longer than ReleaseTimeout, got err=%v", deName, getErr)
}

// TestPipeline_BlockResumeAfterMerge verifies that when data.bin.zst already exists
// in a node directory (crash-after-merge-before-snapshot.yaml window), the pipeline
// skips DataExport creation entirely and only calls FinalizeNode.
func TestPipeline_BlockResumeAfterMerge(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	// Pre-create disk-snap's directory with data.bin.zst but no snapshot.yaml,
	// simulating a crash after block chunks were merged but before FinalizeNode ran.
	// disk-snap is a non-aggregator: it downloads its OwnDataRef flat into its own dir.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))

	require.NoError(t, os.MkdirAll(filepath.Join(diskSnapDir, archive.ManifestsDirName), 0o755))
	seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())
	require.NoError(t, os.WriteFile(
		filepath.Join(diskSnapDir, archive.DataBlockName(".zst")),
		[]byte("pre-merged-block-data"),
		0o644,
	))

	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    outputDir,
		Workers:      1,
		KubeClient:   c,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when data.bin.zst already exists")
			return nil, errors.New("unexpected OpenExport call")
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	// FinalizeNode must have been called: disk-snap directory must now be complete.
	assertNodeComplete(t, diskSnapDir)
}

// TestPipeline_FSResumeAfterTar verifies that when data.tar already exists in a
// node directory (crash-after-tar-assembly-before-snapshot.yaml window), the
// pipeline skips DataExport creation entirely and only calls FinalizeNode.
func TestPipeline_FSResumeAfterTar(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	// Pre-create disk-snap's directory with data.tar but no snapshot.yaml,
	// simulating a crash after the FS tar was assembled but before FinalizeNode ran.
	// disk-snap is a non-aggregator: it downloads its OwnDataRef flat into its own dir.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))

	require.NoError(t, os.MkdirAll(filepath.Join(diskSnapDir, archive.ManifestsDirName), 0o755))
	seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())
	require.NoError(t, os.WriteFile(
		filepath.Join(diskSnapDir, archive.FsTarName),
		[]byte("pre-assembled-fs-tar"),
		0o644,
	))

	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    outputDir,
		Workers:      1,
		KubeClient:   c,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when data.tar already exists")

			return nil, errors.New("unexpected OpenExport call")
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	// FinalizeNode must have been called: disk-snap directory must now be complete.
	assertNodeComplete(t, diskSnapDir)
}

func TestPipeline_FSResumeAfterTarConfirmsDurabilityBeforeCompletion(t *testing.T) {
	tests := []struct {
		name         string
		newClient    func(*testing.T) client.Client
		namespace    string
		rootSnapshot string
		nodeDir      func(string) string
	}{
		{
			name:         "OwnData",
			newClient:    buildFakeClient,
			namespace:    testNS,
			rootSnapshot: rootSnapshot,
			nodeDir: func(outputDir string) string {
				return filepath.Join(outputDir, archive.SnapshotsDirName,
					archive.NodeDirName(childKind, diskSnapName))
			},
		},
		{
			name:         "VolumeLeaf",
			newClient:    buildOrphanLeafFakeClient,
			namespace:    e2eNS,
			rootSnapshot: e2eAggRootSnap,
			nodeDir: func(outputDir string) string {
				return filepath.Join(outputDir, archive.SnapshotsDirName,
					archive.NodeDirName(e2eDiskKind, "agg-snap"),
					archive.SnapshotsDirName,
					archive.NodeDirName("VolumeSnapshot", "pvc-agg"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsSrv := makeE2EFSServer(t, []fsE2EFile{
				{rel: "alpha.txt", content: []byte("hello-alpha")},
				{rel: "subdir/beta.txt", content: []byte("hello-beta")},
			})
			defer fsSrv.Close()

			c := tt.newClient(t)
			outputDir := t.TempDir()
			firstCfg := pipeline.Config{
				Namespace:    tt.namespace,
				RootSnapshot: tt.rootSnapshot,
				OutputDir:    outputDir,
				Workers:      1,
				KubeClient:   c,
				OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
					return exporter.NewExport(
						namespace,
						"de-fs-durability",
						"Filesystem",
						fsSrv.URL,
						exporter.NewFetcher(fsSrv.Client()),
					), nil
				},
			}

			require.NoError(t, runPipeline(context.Background(), firstCfg))

			nodeDir := tt.nodeDir(outputDir)
			tarPath := filepath.Join(nodeDir, archive.FsTarName)
			snapshotPath := filepath.Join(nodeDir, archive.SnapshotYAMLName)
			stagingDir := filepath.Join(nodeDir, archive.FsTarStagingDirName)
			stagingPath := filepath.Join(stagingDir, "alpha.txt.zst")
			identityPath := filepath.Join(nodeDir, archive.NodeIdentityMarkerName)

			tarBytes, err := os.ReadFile(tarPath)
			require.NoError(t, err)

			stagedBytes, err := readTarEntry(t, tarPath, "alpha.txt.zst")
			require.NoError(t, err)

			reseedResumeMarkerAndDropEnvelopesUpTo(t, outputDir, nodeDir)
			require.NoError(t, os.MkdirAll(stagingDir, 0o755))
			require.NoError(t, os.WriteFile(stagingPath, stagedBytes, 0o644))

			confirmationErr := errors.New("tar durability confirmation sentinel")
			var confirmationCalls atomic.Int64
			ctx := archive.WithDirectorySyncHook(
				context.Background(),
				func(path string, next func() error) error {
					if filepath.Clean(path) != filepath.Clean(nodeDir) {
						return next()
					}

					call := confirmationCalls.Add(1)
					if call <= 2 {
						if _, err := os.Stat(stagingPath); err != nil {
							return fmt.Errorf("staging missing before durability confirmation: %w", err)
						}

						if _, err := os.Stat(snapshotPath); !errors.Is(err, os.ErrNotExist) {
							return fmt.Errorf("snapshot finalized before durability confirmation: %w", err)
						}
					}

					if call == 1 {
						return confirmationErr
					}

					return next()
				},
			)

			var openExportCalls atomic.Int64
			failureProgress := &recordingSink{}
			retryCfg := firstCfg
			retryCfg.Progress = failureProgress
			retryCfg.OpenExport = func(context.Context, string, aggapi.NodeRef, string) (*exporter.Export, error) {
				openExportCalls.Add(1)

				return nil, errors.New("unexpected OpenExport call")
			}

			err = runPipeline(ctx, retryCfg)
			require.ErrorIs(t, err, confirmationErr)
			require.FileExists(t, tarPath)
			require.FileExists(t, stagingPath)
			require.FileExists(t, identityPath)
			require.NoFileExists(t, snapshotPath)
			require.Zero(t, openExportCalls.Load())

			gotTarBytes, err := os.ReadFile(tarPath)
			require.NoError(t, err)
			require.Equal(t, tarBytes, gotTarBytes)

			failedStreams := failureProgress.snapshot()
			require.Len(t, failedStreams, 1)
			require.Zero(t, failedStreams[0].doneCnt)

			successProgress := &recordingSink{}
			retryCfg.Progress = successProgress
			require.NoError(t, runPipeline(ctx, retryCfg))

			require.GreaterOrEqual(t, confirmationCalls.Load(), int64(2))
			require.NoDirExists(t, stagingDir)
			require.FileExists(t, snapshotPath)
			require.NoFileExists(t, identityPath)
			require.Zero(t, openExportCalls.Load())

			gotTarBytes, err = os.ReadFile(tarPath)
			require.NoError(t, err)
			require.Equal(t, tarBytes, gotTarBytes)

			successStreams := successProgress.snapshot()
			require.Len(t, successStreams, 1)
			require.Equal(t, 1, successStreams[0].doneCnt)
			require.Zero(t, successStreams[0].failCnt)
		})
	}
}

func TestPipeline_BlockResumeAfterMergeConfirmsDurabilityBeforeCompletion(t *testing.T) {
	tests := []struct {
		name         string
		newClient    func(*testing.T) client.Client
		namespace    string
		rootSnapshot string
		nodeDir      func(string) string
	}{
		{
			name:         "OwnData",
			newClient:    buildFakeClient,
			namespace:    testNS,
			rootSnapshot: rootSnapshot,
			nodeDir: func(outputDir string) string {
				return filepath.Join(outputDir, archive.SnapshotsDirName,
					archive.NodeDirName(childKind, diskSnapName))
			},
		},
		{
			name:         "VolumeLeaf",
			newClient:    buildOrphanLeafFakeClient,
			namespace:    e2eNS,
			rootSnapshot: e2eAggRootSnap,
			nodeDir: func(outputDir string) string {
				return filepath.Join(outputDir, archive.SnapshotsDirName,
					archive.NodeDirName(e2eDiskKind, "agg-snap"),
					archive.SnapshotsDirName,
					archive.NodeDirName("VolumeSnapshot", "pvc-agg"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawBlock := bytes.Repeat([]byte("durable-block-payload-"), 40)
			blockSrv := makeBlockServer(t, rawBlock)
			defer blockSrv.Close()

			c := tt.newClient(t)
			outputDir := t.TempDir()
			firstCfg := pipeline.Config{
				Namespace:            tt.namespace,
				RootSnapshot:         tt.rootSnapshot,
				OutputDir:            outputDir,
				Workers:              1,
				PerVolumeConcurrency: 1,
				KubeClient:           c,
				OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
					return exporter.NewExport(
						namespace,
						"de-block-durability",
						"Block",
						blockSrv.URL,
						exporter.NewFetcher(blockSrv.Client()),
					), nil
				},
			}

			require.NoError(t, runPipeline(context.Background(), firstCfg))

			nodeDir := tt.nodeDir(outputDir)
			blockPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))
			snapshotPath := filepath.Join(nodeDir, archive.SnapshotYAMLName)
			identityPath := filepath.Join(nodeDir, archive.NodeIdentityMarkerName)

			blockBytes, err := os.ReadFile(blockPath)
			require.NoError(t, err)

			reseedResumeMarkerAndDropEnvelopesUpTo(t, outputDir, nodeDir)
			chunkDir := seedLeftoverBlockChunkDir(t, nodeDir)
			chunkPath := filepath.Join(chunkDir, archive.ChunkFileName(0, ".zst"))

			confirmationErr := errors.New("block durability confirmation sentinel")
			var confirmationCalls atomic.Int64
			ctx := archive.WithDirectorySyncHook(
				context.Background(),
				func(path string, next func() error) error {
					if filepath.Clean(path) != filepath.Clean(nodeDir) {
						return next()
					}

					call := confirmationCalls.Add(1)
					if call <= 2 {
						if _, err := os.Stat(chunkPath); err != nil {
							return fmt.Errorf("chunks missing before durability confirmation: %w", err)
						}

						if _, err := os.Stat(snapshotPath); !errors.Is(err, os.ErrNotExist) {
							return fmt.Errorf("snapshot finalized before durability confirmation: %w", err)
						}
					}

					if call == 1 {
						return confirmationErr
					}

					return next()
				},
			)

			var openExportCalls atomic.Int64
			failureProgress := &recordingSink{}
			retryCfg := firstCfg
			retryCfg.Progress = failureProgress
			retryCfg.OpenExport = func(context.Context, string, aggapi.NodeRef, string) (*exporter.Export, error) {
				openExportCalls.Add(1)

				return nil, errors.New("unexpected OpenExport call")
			}

			err = runPipeline(ctx, retryCfg)
			require.ErrorIs(t, err, confirmationErr)
			require.FileExists(t, blockPath)
			require.FileExists(t, chunkPath)
			require.FileExists(t, identityPath)
			require.NoFileExists(t, snapshotPath)
			require.Zero(t, openExportCalls.Load())

			gotBlockBytes, err := os.ReadFile(blockPath)
			require.NoError(t, err)
			require.Equal(t, blockBytes, gotBlockBytes)

			failedStreams := failureProgress.snapshot()
			require.Len(t, failedStreams, 1)
			require.Zero(t, failedStreams[0].doneCnt)

			successProgress := &recordingSink{}
			retryCfg.Progress = successProgress
			require.NoError(t, runPipeline(ctx, retryCfg))

			require.GreaterOrEqual(t, confirmationCalls.Load(), int64(2))
			require.NoDirExists(t, chunkDir)
			require.FileExists(t, snapshotPath)
			require.NoFileExists(t, identityPath)
			require.Zero(t, openExportCalls.Load())

			gotBlockBytes, err = os.ReadFile(blockPath)
			require.NoError(t, err)
			require.Equal(t, blockBytes, gotBlockBytes)

			successStreams := successProgress.snapshot()
			require.Len(t, successStreams, 1)
			require.Equal(t, 1, successStreams[0].doneCnt)
			require.Zero(t, successStreams[0].failCnt)
		})
	}
}

func TestPipeline_RootedDirectoryAncestryRetryBlocksPublication(t *testing.T) {
	rawBlock := bytes.Repeat([]byte("rooted-directory-ancestry-"), 64)
	blockServer := makeBlockServer(t, rawBlock)
	defer blockServer.Close()

	outputDir := t.TempDir()
	nodeDir := filepath.Join(
		outputDir,
		archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName),
	)
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	confirmationPath := filepath.Dir(nodeDir)
	snapshotPath := filepath.Join(nodeDir, archive.SnapshotYAMLName)
	identityPath := filepath.Join(nodeDir, archive.NodeIdentityMarkerName)
	blockPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

	lock, err := archive.AcquireWriteLock(outputDir)
	require.NoError(t, err)
	defer func() { require.NoError(t, lock.Unlock()) }()

	var openExportCalls atomic.Int64
	baseConfig := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		SelectedNodeKind:     childKind,
		SelectedNodeName:     diskSnapName,
		KubeClient:           buildFakeClient(t),
		ManifestSource:       testManifestSource(),
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			openExportCalls.Add(1)

			return exporter.NewExport(
				namespace,
				"de-rooted-directory-ancestry",
				"Block",
				blockServer.URL,
				exporter.NewFetcher(blockServer.Client()),
			), nil
		},
	}

	var confirmations atomic.Int64

	runAttempt := func(
		ctx context.Context,
		cancel context.CancelFunc,
		cause error,
		sink *recordingSink,
	) error {
		t.Helper()

		destination, destinationErr := archive.NewLockedRootedDestination(lock, nil)
		require.NoError(t, destinationErr)
		defer func() { require.NoError(t, destination.Close()) }()

		destination.SetDirectorySyncHook(func(path string, next func() error) error {
			if filepath.Clean(path) != filepath.Clean(confirmationPath) {
				return next()
			}

			if _, statErr := os.Stat(chunkDir); statErr != nil {
				if errors.Is(statErr, os.ErrNotExist) {
					return next()
				}

				return statErr
			}

			confirmations.Add(1)
			if cause == nil {
				return next()
			}

			if cancel != nil {
				cancel()

				return ctx.Err()
			}

			return cause
		})

		config := baseConfig
		config.Progress = sink

		return pipeline.RunRooted(ctx, config, destination)
	}

	firstFailure := errors.New("first rooted ancestry sync failure")
	firstProgress := &recordingSink{}
	err = runAttempt(context.Background(), nil, firstFailure, firstProgress)
	require.ErrorIs(t, err, firstFailure)
	require.DirExists(t, chunkDir)
	require.FileExists(t, identityPath)
	require.NoFileExists(t, snapshotPath)
	require.NoFileExists(t, blockPath)
	require.Equal(t, int64(1), openExportCalls.Load())

	identity, err := os.ReadFile(identityPath)
	require.NoError(t, err)

	assertFailedProgress := func(sink *recordingSink, wantStreams int) {
		t.Helper()

		streams := sink.snapshot()
		require.Len(t, streams, wantStreams)
		if len(streams) == 0 {
			return
		}

		require.Zero(t, streams[0].Current())
		require.Zero(t, streams[0].doneCnt)
		require.Equal(t, 1, streams[0].failCnt)
	}
	assertFailedProgress(firstProgress, 1)

	secondFailure := errors.New("second rooted ancestry sync failure")
	secondProgress := &recordingSink{}
	err = runAttempt(context.Background(), nil, secondFailure, secondProgress)
	require.ErrorIs(t, err, secondFailure)
	require.Equal(t, int64(1), openExportCalls.Load(), "retry must fail before reopening the export")
	require.DirExists(t, chunkDir)
	require.FileExists(t, identityPath)
	require.NoFileExists(t, snapshotPath)
	require.NoFileExists(t, blockPath)
	assertFailedProgress(secondProgress, 0)

	retainedIdentity, err := os.ReadFile(identityPath)
	require.NoError(t, err)
	require.Equal(t, identity, retainedIdentity)

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelProgress := &recordingSink{}
	err = runAttempt(cancelCtx, cancel, context.Canceled, cancelProgress)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(1), openExportCalls.Load(), "cancelled retry must fail before reopening the export")
	require.DirExists(t, chunkDir)
	require.FileExists(t, identityPath)
	require.NoFileExists(t, snapshotPath)
	require.NoFileExists(t, blockPath)
	assertFailedProgress(cancelProgress, 0)

	retainedIdentity, err = os.ReadFile(identityPath)
	require.NoError(t, err)
	require.Equal(t, identity, retainedIdentity)

	successProgress := &recordingSink{}
	require.NoError(t, runAttempt(context.Background(), nil, nil, successProgress))
	require.GreaterOrEqual(t, confirmations.Load(), int64(4))
	require.Equal(t, int64(2), openExportCalls.Load())
	require.NoDirExists(t, chunkDir)
	require.NoFileExists(t, identityPath)
	require.FileExists(t, snapshotPath)
	require.FileExists(t, blockPath)

	successStreams := successProgress.snapshot()
	require.Len(t, successStreams, 1)
	require.Equal(t, successStreams[0].Total(), successStreams[0].Current())
	require.Equal(t, 1, successStreams[0].doneCnt)
	require.Zero(t, successStreams[0].failCnt)
}

func TestPipeline_SnapshotYAMLRecoversDurabilityBeforeDone(t *testing.T) {
	tests := []struct {
		name            string
		newClient       func(*testing.T) client.Client
		namespace       string
		rootSnapshot    string
		selectedKind    string
		selectedName    string
		nodeDir         func(string) string
		initialFailCall int64
	}{
		{
			name:         "Root",
			newClient:    buildFakeClient,
			namespace:    testNS,
			rootSnapshot: rootSnapshot,
			nodeDir: func(outputDir string) string {
				return outputDir
			},
			initialFailCall: 1,
		},
		{
			name:         "OrdinaryChildWithData",
			newClient:    buildFakeClient,
			namespace:    testNS,
			rootSnapshot: rootSnapshot,
			selectedKind: childKind,
			selectedName: diskSnapName,
			nodeDir: func(outputDir string) string {
				return filepath.Join(outputDir, archive.SnapshotsDirName,
					archive.NodeDirName(childKind, diskSnapName))
			},
			initialFailCall: 2,
		},
		{
			name:         "VolumeLeaf",
			newClient:    buildOrphanLeafFakeClient,
			namespace:    e2eNS,
			rootSnapshot: e2eAggRootSnap,
			nodeDir: func(outputDir string) string {
				return filepath.Join(outputDir, archive.SnapshotsDirName,
					archive.NodeDirName(e2eDiskKind, "agg-snap"),
					archive.SnapshotsDirName,
					archive.NodeDirName("VolumeSnapshot", "pvc-agg"))
			},
			initialFailCall: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawBlock := bytes.Repeat([]byte("snapshot-durability-payload-"), 40)
			blockSrv := makeBlockServer(t, rawBlock)
			defer blockSrv.Close()

			outputDir := t.TempDir()
			nodeDir := tt.nodeDir(outputDir)
			initialSyncErr := errors.New("initial snapshot sync sentinel")
			retrySyncErr := errors.New("retry snapshot sync sentinel")
			var syncCalls atomic.Int64

			ctx := archive.WithDirectorySyncHook(context.Background(), func(path string, next func() error) error {
				if filepath.Clean(path) != filepath.Clean(nodeDir) {
					return next()
				}

				// The publication transaction/receipt records live directly in the
				// archive root, so publishing one confirms the archive root itself.
				// In the Root case that is the very directory this fixture counts
				// confirmations for, yet it is a different operation: it happens
				// while the root envelope does not exist, so it must not consume a
				// call number. Child cases are unaffected — no record is ever
				// published into a child node directory.
				if filepath.Clean(path) == filepath.Clean(outputDir) {
					if _, err := os.Stat(filepath.Join(outputDir, archive.SnapshotYAMLName)); err != nil {
						return next()
					}
				}

				call := syncCalls.Add(1)
				if call == tt.initialFailCall {
					if _, err := os.Stat(filepath.Join(nodeDir, archive.SnapshotYAMLName)); err != nil {
						return fmt.Errorf("snapshot.yaml missing after publication: %w", err)
					}

					if _, err := os.Stat(filepath.Join(nodeDir, archive.NodeIdentityMarkerName)); err != nil {
						return fmt.Errorf("identity marker missing after publication: %w", err)
					}

					return initialSyncErr
				}

				if call == tt.initialFailCall+1 {
					if _, err := os.Stat(filepath.Join(nodeDir, archive.SnapshotYAMLName)); err != nil {
						return fmt.Errorf("snapshot.yaml missing before durability retry: %w", err)
					}

					if _, err := os.Stat(filepath.Join(nodeDir, archive.NodeIdentityMarkerName)); err != nil {
						return fmt.Errorf("identity marker missing before durability retry: %w", err)
					}

					return retrySyncErr
				}

				return next()
			})

			cfg := pipeline.Config{
				Namespace:            tt.namespace,
				RootSnapshot:         tt.rootSnapshot,
				OutputDir:            outputDir,
				Workers:              1,
				PerVolumeConcurrency: 1,
				SelectedNodeKind:     tt.selectedKind,
				SelectedNodeName:     tt.selectedName,
				KubeClient:           tt.newClient(t),
				OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
					return exporter.NewExport(
						namespace,
						"de-snapshot-durability",
						"Block",
						blockSrv.URL,
						exporter.NewFetcher(blockSrv.Client()),
					), nil
				},
			}

			err := runPipeline(ctx, cfg)
			require.ErrorIs(t, err, initialSyncErr)
			require.Equal(t, archive.PublicationPublished, archive.CommitPublicationState(err))

			snapshotPath := filepath.Join(nodeDir, archive.SnapshotYAMLName)
			identityPath := filepath.Join(nodeDir, archive.NodeIdentityMarkerName)
			require.FileExists(t, snapshotPath)
			require.FileExists(t, identityPath)
			require.NoFileExists(t, snapshotPath+".tmp")

			published, err := os.ReadFile(snapshotPath)
			require.NoError(t, err)

			var openExportCalls atomic.Int64
			retryCfg := cfg
			retryCfg.OpenExport = func(context.Context, string, aggapi.NodeRef, string) (*exporter.Export, error) {
				openExportCalls.Add(1)

				return nil, errors.New("unexpected OpenExport call")
			}

			err = runPipeline(ctx, retryCfg)
			require.ErrorIs(t, err, retrySyncErr)
			require.Equal(t, archive.PublicationPublished, archive.CommitPublicationState(err))
			require.FileExists(t, identityPath)
			require.Zero(t, openExportCalls.Load())

			afterFailedRetry, err := os.ReadFile(snapshotPath)
			require.NoError(t, err)
			require.Equal(t, published, afterFailedRetry)

			require.NoError(t, runPipeline(ctx, retryCfg))
			require.NoFileExists(t, identityPath)
			require.Zero(t, openExportCalls.Load())

			recovered, err := os.ReadFile(snapshotPath)
			require.NoError(t, err)
			require.Equal(t, published, recovered)
		})
	}
}

// TestPipeline_ForeignMergedBlock_NotLaunderedByResume is the scenario-B
// regression test for partial-node-dir-identity-marker: a node's PRIMARY dir
// already holds a merged data.bin* left by a DIFFERENT snapshot (a mismatched
// identity marker, and — like every partial dir — no snapshot.yaml). Before this
// fix ScanNode classified it BlockPartial/ManifestsOnly by directory probes
// alone, processVolumeNode/downloadOwnDataRefs's "already merged" skip fired, and
// FinalizeNode stamped a fresh valid snapshot.yaml + checksum over the FOREIGN
// bytes — permanently laundering them. Now the mismatched marker collision-
// redirects the node to a fresh sibling path, so the foreign dir is never
// skipped-into or finalized, and the real volume downloads correctly beside it.
func TestPipeline_ForeignMergedBlock_NotLaunderedByResume(t *testing.T) {
	t.Parallel()

	correctBlock := bytes.Repeat([]byte("C"), 600)
	foreignBytes := []byte("foreign-merged-block-from-another-snapshot")

	srv := makeBlockServer(t, correctBlock)
	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	// Pre-create disk-snap's PRIMARY dir with a merged data.bin.zst and a marker
	// for a DIFFERENT snapshot, no snapshot.yaml — exactly the foreign
	// crash-after-merge state scenario B abuses.
	primaryDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	require.NoError(t, os.MkdirAll(filepath.Join(primaryDir, archive.ManifestsDirName), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(primaryDir, archive.DataBlockName(".zst")),
		foreignBytes,
		0o644,
	))
	seedResumeIdentityMarker(t, primaryDir, archive.NodeIdentity{
		APIVersion: childAPIVersion,
		Kind:       childKind,
		Name:       "some-other-snapshot",
		Namespace:  testNS,
		UID:        "foreign-uid",
	})

	var openExportCalled atomic.Bool

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			openExportCalled.Store(true)

			return exporter.NewExport(namespace, "de-foreign", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	// The foreign primary dir must be left untouched: no snapshot.yaml was ever
	// stamped over it, and its data.bin.zst still holds the foreign bytes.
	_, statErr := os.Stat(filepath.Join(primaryDir, archive.SnapshotYAMLName))
	require.True(t, os.IsNotExist(statErr),
		"a foreign merged dir must NOT be finalized (its bytes must not be laundered)")

	gotForeign, err := os.ReadFile(filepath.Join(primaryDir, archive.DataBlockName(".zst")))
	require.NoError(t, err)
	require.Equal(t, foreignBytes, gotForeign, "foreign bytes must be left exactly as-is")

	// The real volume must have been downloaded (not skipped into the foreign dir)
	// and it must land in a single collision-redirected sibling dir that decodes
	// to the CORRECT bytes.
	require.True(t, openExportCalled.Load(),
		"the real volume must be downloaded, not skipped into the foreign dir")

	matches, err := filepath.Glob(filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName)+"__*"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "exactly one collision-redirected dir must be created")

	collisionDir := matches[0]
	assertNodeComplete(t, collisionDir)

	compressed, err := os.ReadFile(filepath.Join(collisionDir, archive.DataBlockName(".zst")))
	require.NoError(t, err)
	require.Equal(t, correctBlock, decodeZstdBlock(t, compressed),
		"collision dir must hold the correctly-downloaded bytes")
}

// assertNodeComplete checks that snapshot.yaml exists in dir and VerifyNode passes.
func assertNodeComplete(t *testing.T, dir string) {
	t.Helper()

	yamlPath := filepath.Join(dir, archive.SnapshotYAMLName)
	_, err := os.Stat(yamlPath)
	require.NoError(t, err, "snapshot.yaml must exist in %s", dir)

	require.NoError(t, archive.VerifyNode(dir), "VerifyNode must pass for %s", dir)
}

// statMtime returns the modification time of path.
func statMtime(t *testing.T, path string) time.Time {
	t.Helper()

	fi, err := os.Stat(path)
	require.NoError(t, err)

	return fi.ModTime()
}

// makeBlockServer creates an httptest.Server that serves rawData at /api/v1/block.
// It supports HEAD (Content-Length) and Range GET requests.
func makeBlockServer(t *testing.T, rawData []byte) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(rawData))
	})

	return httptest.NewServer(mux)
}

func makeSizedBlockServer(t *testing.T, size int64, value byte) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, request *http.Request) {
		reader := io.NewSectionReader(repeatedByteReaderAt{value: value}, 0, size)
		http.ServeContent(w, request, "data", time.Time{}, reader)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return server
}

func twoDataChildPipelineConfig(t *testing.T, outputDir string, server *httptest.Server) pipeline.Config {
	t.Helper()

	return pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           buildTwoDataChildFakeClient(t),
		OpenExport: func(
			_ context.Context,
			namespace string,
			_ aggapi.NodeRef,
			_ string,
		) (*exporter.Export, error) {
			return exporter.NewExport(
				namespace,
				"de-two-data-children",
				"Block",
				server.URL,
				exporter.NewFetcher(server.Client()),
			), nil
		},
	}
}

func twoDataChildDir(outputDir, name string) string {
	return filepath.Join(outputDir, archive.SnapshotsDirName, archive.NodeDirName(childKind, name))
}

func buildTwoDataChildFakeClient(t *testing.T) client.Client {
	t.Helper()

	root := snapObj{
		apiVersion: storageAPIVersion,
		kind:       "Snapshot",
		namespace:  testNS,
		name:       rootSnapshot,
		uid:        "uid-root",
		sourceRef:  namespaceSourceRefMap(testNS, "uid-ns"),
		children: []map[string]interface{}{
			childRefMap(childAPIVersion, childKind, "disk-a"),
			childRefMap(childAPIVersion, childKind, "disk-b"),
		},
	}.build()

	childA := snapObj{
		apiVersion: childAPIVersion,
		kind:       childKind,
		namespace:  testNS,
		name:       "disk-a",
		uid:        "uid-disk-a",
		data:       pvcData(testNS, "pvc-a", "uid-pvc-a", "vsc-a"),
	}.build()
	childB := snapObj{
		apiVersion: childAPIVersion,
		kind:       childKind,
		namespace:  testNS,
		name:       "disk-b",
		uid:        "uid-disk-b",
		data:       pvcData(testNS, "pvc-b", "uid-pvc-b", "vsc-b"),
	}.build()

	return fake.NewClientBuilder().
		WithScheme(buildScheme(t)).
		WithObjects(root, childA, childB).
		Build()
}

// buildFakeClient constructs a controller-runtime fake client pre-populated with
// all objects needed for the pipeline test.
func buildFakeClient(t *testing.T) client.Client {
	t.Helper()

	return buildFakeClientBuilder(t).Build()
}

func buildFakeClientBuilder(t *testing.T) *fake.ClientBuilder {
	t.Helper()

	scheme := buildScheme(t)

	// Root Snapshot: cluster-scoped Namespace source (no sourceRef.namespace) with one child ref.
	root := snapObj{
		apiVersion: storageAPIVersion, kind: "Snapshot",
		namespace: testNS, name: rootSnapshot, uid: "uid-root",
		sourceRef: namespaceSourceRefMap(testNS, "uid-ns"),
		children:  []map[string]interface{}{childRefMap(childAPIVersion, childKind, diskSnapName)},
	}.build()

	// disk-snap: non-aggregator domain node that captured its own volume (status.data).
	// It has no status.sourceRef, so its readable directory base falls back to the CR name
	// (diskSnapName) — matching the archive.NodeDirName(childKind, diskSnapName) assertions.
	diskSnap := snapObj{
		apiVersion: childAPIVersion, kind: childKind,
		namespace: testNS, name: diskSnapName, uid: diskSnapUID,
		data: pvcData(testNS, sourcePVCName, "uid-disk", "vsc-disk"),
	}.build()

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(root, diskSnap)
}

// buildScheme registers all types needed by the pipeline test.
func buildScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, snapshotapi.AddToScheme(scheme))
	require.NoError(t, deapi.AddToScheme(scheme))

	return scheme
}

func snapshotReplacementTree(t *testing.T, root string) []string {
	t.Helper()

	var snapshot []string

	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		require.NoError(t, walkErr)

		relative, err := filepath.Rel(root, path)
		require.NoError(t, err)

		info, err := entry.Info()
		require.NoError(t, err)

		line := fmt.Sprintf("%s|%s|%d", relative, info.Mode(), info.Size())
		switch {
		case info.Mode().IsRegular():
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			line += "|" + string(data)
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			require.NoError(t, err)
			line += "|" + target
		}

		snapshot = append(snapshot, line)

		return nil
	}))

	return snapshot
}

func TestPipeline_RootedMutationsFailClosedAfterNamespaceReplacement(t *testing.T) {
	type boundary struct {
		name   string
		phase  archive.MutationPhase
		target func(string) bool
	}

	boundaries := []boundary{
		{
			name:  "directory-ancestry-confirmation",
			phase: archive.MutationSync,
			target: func(path string) bool {
				return filepath.Base(path) == archive.ManifestsDirName
			},
		},
		{
			name:  "active-manifest-write",
			phase: archive.MutationCreate,
			target: func(path string) bool {
				return strings.Contains(path, string(filepath.Separator)+archive.ManifestsDirName+string(filepath.Separator))
			},
		},
		{
			name:  "chunk-write",
			phase: archive.MutationCreate,
			target: func(path string) bool {
				return strings.HasSuffix(path, ".part")
			},
		},
		{
			name:  "atomic-rename",
			phase: archive.MutationRename,
			target: func(path string) bool {
				return filepath.Base(path) == archive.SnapshotYAMLName &&
					strings.Contains(path, archive.NodeDirName(childKind, diskSnapName))
			},
		},
		{
			name:  "staging-cleanup",
			phase: archive.MutationRemove,
			target: func(path string) bool {
				return filepath.Base(path) == archive.BlockChunksDirName
			},
		},
	}

	for _, replaceAncestor := range []bool{false, true} {
		replacementName := "root"
		if replaceAncestor {
			replacementName = "ancestor"
		}

		for _, boundary := range boundaries {
			t.Run(replacementName+"/"+boundary.name, func(t *testing.T) {
				base := t.TempDir()
				ancestor := filepath.Join(base, "archive-parent")
				outputDir := filepath.Join(ancestor, "archive")
				outsideDir := filepath.Join(base, "outside")
				require.NoError(t, os.MkdirAll(outputDir, 0o755))
				require.NoError(t, os.MkdirAll(outsideDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(outsideDir, "sentinel"), []byte("outside"), 0o644))

				rawBlock := bytes.Repeat([]byte("rooted-boundary"), 128)
				server := makeBlockServer(t, rawBlock)
				defer server.Close()

				lock, err := archive.AcquireWriteLock(outputDir)
				require.NoError(t, err)

				var (
					fired            atomic.Bool
					pinnedRoot       string
					pinnedAtBoundary []string
					replacementTree  []string
					outsideTree      []string
				)

				hook := func(phase archive.MutationPhase, path string) {
					if phase != boundary.phase || !boundary.target(path) || !fired.CompareAndSwap(false, true) {
						return
					}

					if replaceAncestor {
						pinnedAncestor := ancestor + ".pinned"
						require.NoError(t, os.Rename(ancestor, pinnedAncestor))
						pinnedRoot = filepath.Join(pinnedAncestor, filepath.Base(outputDir))
						require.NoError(t, os.MkdirAll(outputDir, 0o755))
					} else {
						pinnedRoot = outputDir + ".pinned"
						require.NoError(t, os.Rename(outputDir, pinnedRoot))
						require.NoError(t, os.MkdirAll(outputDir, 0o755))
					}

					require.NoError(t, os.WriteFile(filepath.Join(outputDir, "sentinel"), []byte("replacement"), 0o644))
					require.NoError(t, os.Symlink(outsideDir, filepath.Join(outputDir, archive.ManifestsDirName)))
					require.NoError(t, os.Symlink(outsideDir, filepath.Join(outputDir, archive.SnapshotsDirName)))

					pinnedAtBoundary = snapshotReplacementTree(t, pinnedRoot)
					replacementTree = snapshotReplacementTree(t, outputDir)
					outsideTree = snapshotReplacementTree(t, outsideDir)

				}

				destination, err := archive.NewLockedRootedDestination(lock, hook)
				require.NoError(t, err)

				cfg := pipeline.Config{
					Namespace:            testNS,
					RootSnapshot:         rootSnapshot,
					OutputDir:            outputDir,
					Workers:              2,
					PerVolumeConcurrency: 2,
					KubeClient:           buildFakeClient(t),
					ManifestSource:       testManifestSource(),
					OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
						return exporter.NewExport(
							namespace,
							"de-rooted-boundary",
							"Block",
							server.URL,
							exporter.NewFetcher(server.Client()),
						), nil
					},
				}

				runErr := pipeline.RunRooted(context.Background(), cfg, destination)
				require.Error(t, runErr)
				require.ErrorIs(t, runErr, archive.ErrNonRegularArchiveArtifact)
				require.True(t, fired.Load(), "target mutation boundary was not reached")
				require.Equal(t, pinnedAtBoundary, snapshotReplacementTree(t, pinnedRoot),
					"no mutation may start in the pinned tree after binding loss")
				require.Equal(t, replacementTree, snapshotReplacementTree(t, outputDir),
					"replacement namespace must stay byte-for-byte untouched")
				require.Equal(t, outsideTree, snapshotReplacementTree(t, outsideDir),
					"outside symlink target must stay byte-for-byte untouched")

				require.NoError(t, destination.Close())
				require.NoError(t, lock.Unlock())
			})
		}
	}
}

func TestPipeline_LockedSiblingDestinationsRemainConcurrent(t *testing.T) {
	base := t.TempDir()
	firstDir := filepath.Join(base, "first")
	secondDir := filepath.Join(base, "second")
	require.NoError(t, os.Mkdir(firstDir, 0o755))
	require.NoError(t, os.Mkdir(secondDir, 0o755))

	firstLock, err := archive.AcquireWriteLock(firstDir)
	require.NoError(t, err)
	secondLock, err := archive.AcquireWriteLock(secondDir)
	require.NoError(t, err)

	reached := make(chan struct{})
	release := make(chan struct{})
	var (
		blockOnce   sync.Once
		releaseOnce sync.Once
	)
	defer releaseOnce.Do(func() { close(release) })

	firstDestination, err := archive.NewLockedRootedDestination(
		firstLock,
		func(phase archive.MutationPhase, path string) {
			if phase != archive.MutationCreate ||
				!strings.Contains(path, string(filepath.Separator)+archive.ManifestsDirName+string(filepath.Separator)) {
				return
			}

			blockOnce.Do(func() {
				close(reached)
				<-release
			})
		},
	)
	require.NoError(t, err)

	secondDestination, err := archive.NewLockedRootedDestination(secondLock, nil)
	require.NoError(t, err)

	rootOnlyClient := func() client.Client {
		root := snapObj{
			apiVersion: storageAPIVersion,
			kind:       "Snapshot",
			namespace:  testNS,
			name:       rootSnapshot,
			uid:        "uid-root",
			sourceRef:  namespaceSourceRefMap(testNS, "uid-ns"),
		}.build()

		return fake.NewClientBuilder().
			WithScheme(buildScheme(t)).
			WithObjects(root).
			Build()
	}

	config := func(output string) pipeline.Config {
		return pipeline.Config{
			Namespace:      testNS,
			RootSnapshot:   rootSnapshot,
			OutputDir:      output,
			Workers:        1,
			KubeClient:     rootOnlyClient(),
			ManifestSource: testManifestSource(),
			OpenExport: func(context.Context, string, aggapi.NodeRef, string) (*exporter.Export, error) {
				return nil, errors.New("root-only pipeline must not open an export")
			},
		}
	}

	firstResult := make(chan error, 1)
	go func() {
		firstResult <- pipeline.RunRooted(context.Background(), config(firstDir), firstDestination)
	}()

	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("first destination did not reach the blocked manifest boundary")
	}

	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSecond()
	require.NoError(t, pipeline.RunRooted(secondCtx, config(secondDir), secondDestination),
		"an independent sibling destination must complete while the first is blocked")

	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-firstResult)

	require.NoError(t, firstDestination.Close())
	require.NoError(t, secondDestination.Close())
	require.NoError(t, firstLock.Unlock())
	require.NoError(t, secondLock.Unlock())
}

// TestPipeline_LeafTargetRef verifies that OpenExport receives the correct snapshot
// leaf NodeRef (not a shadow VS name) when a domain snapshot node downloads its
// OwnDataRef volume.
func TestPipeline_LeafTargetRef(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("B"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	var capturedRef aggapi.NodeRef

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, leafRef aggapi.NodeRef, _ string) (*exporter.Export, error) {
			capturedRef = leafRef
			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	// OpenExport must receive the disk-snap domain snapshot ref, not a shadow VS.
	require.Equal(t, childAPIVersion, capturedRef.APIVersion,
		"OpenExport must receive the domain snapshot APIVersion")
	require.Equal(t, childKind, capturedRef.Kind,
		"OpenExport must receive the domain snapshot Kind")
	require.Equal(t, diskSnapName, capturedRef.Name,
		"OpenExport must receive the domain snapshot Name")
	require.Equal(t, testNS, capturedRef.Namespace,
		"OpenExport must receive the correct Namespace")
}

// TestPipeline_SubtreeSelection verifies that when SelectedNodeKind/SelectedNodeName
// identify a direct child of the root, only that node (and its descendants) is
// downloaded. The root directory gets content-free scaffold directories (snapshots/)
// but no snapshot.yaml or manifests/.
//
// Tree used by buildFakeClient:
//
//	outputDir/                         ← root Snapshot (scaffold only)
//	  snapshots/
//	    virtualdisksnapshot_disk-snap/ ← selected node (fully downloaded)
//	      manifests/
//	      data.bin.zst
//	      snapshot.yaml
func TestPipeline_SubtreeSelection(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("S"), 600)
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
		KubeClient:           c,
		SelectedNodeKind:     childKind,
		SelectedNodeName:     diskSnapName,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-subtree", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	// Root dir must NOT have a snapshot.yaml — it was not processed, only scaffolded.
	_, err := os.Stat(filepath.Join(outputDir, archive.SnapshotYAMLName))
	require.True(t, os.IsNotExist(err),
		"root snapshot.yaml must not exist when only a subtree was selected")

	// Root dir must NOT have a manifests/ directory.
	_, err = os.Stat(filepath.Join(outputDir, archive.ManifestsDirName))
	require.True(t, os.IsNotExist(err),
		"root manifests/ must not exist when only a subtree was selected")

	// The selected node must be fully complete at its real path.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)

	// The selected node must have its block-volume data.
	_, err = os.Stat(filepath.Join(diskSnapDir, archive.DataBlockName(".zst")))
	require.NoError(t, err, "selected node must have data.bin.zst")

	// Resume: a second run must not overwrite the completed node.
	diskYAML := filepath.Join(diskSnapDir, archive.SnapshotYAMLName)
	diskMod := statMtime(t, diskYAML)

	time.Sleep(20 * time.Millisecond)

	require.NoError(t, runPipeline(context.Background(), cfg))
	require.Equal(t, diskMod, statMtime(t, diskYAML),
		"disk-snap snapshot.yaml must not be rewritten on second run")
}

// TestPipeline_SubtreeRootSelection verifies that selecting the root node by kind
// and name produces the same result as a full-tree download (both root and child
// nodes are fully processed).
func TestPipeline_SubtreeRootSelection(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("R"), 600)
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
		KubeClient:           c,
		SelectedNodeKind:     "Snapshot",
		SelectedNodeName:     rootSnapshot,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-root-sel", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	// Root node must be complete (same as full-tree download).
	assertNodeComplete(t, outputDir)

	// Child node must also be complete.
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)
}

// TestPipeline_NoneCompression verifies that when Compression is set to the
// "none" codec the pipeline produces data.bin (no extension) for block volumes.
func TestPipeline_NoneCompression(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("N"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	noneCodec, err := compress.New("none", 0)
	require.NoError(t, err, "compress.New(none, 0)")

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Compression:          noneCodec,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-none", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)

	// none codec → no extension: data.bin (not data.bin.zst)
	noneBlockPath := filepath.Join(diskSnapDir, archive.DataBlockName(""))
	_, statErr := os.Stat(noneBlockPath)
	require.NoError(t, statErr, "none-compressed block must produce data.bin (no extension)")

	// The compressed file with .zst extension must NOT exist.
	_, statZstErr := os.Stat(filepath.Join(diskSnapDir, archive.DataBlockName(".zst")))
	require.True(t, os.IsNotExist(statZstErr),
		"none-compression must not produce data.bin.zst")

	got, readErr := os.ReadFile(noneBlockPath)
	require.NoError(t, readErr)
	require.Equal(t, rawBlock, got, "none-compressed block data must match original")
}

// TestPipeline_Progress_NonTTYFallback verifies that injecting a non-TTY progress.Sink
// into pipeline.Config causes the aggregate "downloaded X / total Y" line to be emitted
// to the configured io.Writer after the run completes, using a known block-volume total.
//
// The Sink is constructed with a very long tick interval so only sink.Wait() emits output,
// making the assertion fully deterministic.
func TestPipeline_Progress_NonTTYFallback(t *testing.T) {
	t.Parallel()

	// 600-byte block payload gives a known per-volume total for the assertion.
	rawBlock := bytes.Repeat([]byte("P"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	var buf bytes.Buffer

	// Long interval ensures no periodic tick fires during the test; only Wait() emits.
	sink := progress.New(&buf, false, progress.WithInterval(time.Hour))

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Progress:             sink,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-progress", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	sink.Wait()

	got := buf.String()

	// The non-TTY sink emits "downloaded X / total Y (N/M volumes)" using
	// decor.SizeB1024 with the "% .1f" verb — replicate the same format to pin the
	// exact expected line. This run has exactly one volume stream (the root's
	// single block leaf), so N/M settles at 1/1.
	total := int64(len(rawBlock))
	want := fmt.Sprintf("downloaded % .1f / total % .1f (1/1 volumes)\n",
		decor.SizeB1024(total), decor.SizeB1024(total))

	require.True(t, strings.Contains(got, want),
		"non-TTY Sink must emit the aggregate line after pipeline completes\ngot:  %q\nwant (contained): %q",
		got, want)
}

// TestPipeline_Progress_NilSinkIsNoop verifies that nil Progress in Config does not
// change pipeline behavior: the download completes normally and no progress output is
// produced.
func TestPipeline_Progress_NilSinkIsNoop(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("Q"), 300)
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
		KubeClient:           c,
		// Progress deliberately left nil to test the no-op path.
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-nil-progress", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.NoError(t, err)

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	assertNodeComplete(t, diskSnapDir)
}

// TestPipeline_PartialChunkResume verifies the block_partial resume path: when
// a node's data.bin.d/ chunk directory already holds a durable partial prefix
// of its (single, volume.DefaultChunkSize-geometry — see
// block-chunk-size-hardcode-only) chunk and there is no snapshot.yaml, the
// pipeline fetches only the missing suffix, merges the chunk, removes
// data.bin.d/, and finalizes the node.
func TestPipeline_PartialChunkResume(t *testing.T) {
	t.Parallel()

	const (
		testTotalSize int64 = 300
		partialBytes  int64 = 100 // durable prefix already on disk before the crash
	)

	rawBlock := bytes.Repeat([]byte("Z"), int(testTotalSize))

	// Track which Range GET headers the server receives.
	var (
		mu            sync.Mutex
		fetchedRanges []string
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			mu.Lock()
			fetchedRanges = append(fetchedRanges, r.Header.Get("Range"))
			mu.Unlock()
		}

		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(rawBlock))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	codec, err := compress.New("zstd", 0)
	require.NoError(t, err)

	// Pre-seed the sole chunk's durable ".part" prefix, simulating a crash
	// mid-download of the volume's only chunk (300 bytes is well under
	// volume.DefaultChunkSize, so this volume is always exactly one chunk).
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	chunkDir := filepath.Join(diskSnapDir, archive.BlockChunksDirName)
	require.NoError(t, os.MkdirAll(chunkDir, 0o755))
	seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())

	require.NoError(t, os.WriteFile(
		filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part"),
		rawBlock[:partialBytes],
		0o644,
	))
	// A durable ".part.offset" sidecar must accompany the ".part" file so
	// partialChunkSize trusts this partial prefix instead of truncating it to
	// zero (see download-resume-part-trusted-prefix).
	require.NoError(t, os.WriteFile(
		filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part.offset"),
		[]byte(strconv.FormatInt(partialBytes, 10)),
		0o644,
	))

	// A real interrupted run always has a chunks.meta recording the geometry
	// (written before the first chunk is even fetched — see the
	// chunk-size-mismatch-resume-corruption-guard fix), so seed one matching
	// this run's now-fixed geometry; otherwise the geometry guard cannot
	// distinguish this partial dir from a foreign one and would (correctly)
	// purge and re-fetch from byte zero.
	require.NoError(t, archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: volume.DefaultChunkSize, TotalSize: testTotalSize}))

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Compression:          codec,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-partial-resume", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	// (a) The durable prefix must not have been re-fetched from byte zero;
	// only the missing suffix must have been fetched.
	mu.Lock()
	gotRanges := append([]string(nil), fetchedRanges...)
	mu.Unlock()

	for _, hdr := range gotRanges {
		require.NotEqual(t, fmt.Sprintf("bytes=0-%d", testTotalSize-1), hdr,
			"the durable partial prefix was pre-seeded and must not be re-fetched from byte zero")
	}

	require.Contains(t, gotRanges, fmt.Sprintf("bytes=%d-%d", partialBytes, testTotalSize-1),
		"only the missing suffix must be fetched")

	// (b) Merged data.bin.zst must decode to the original rawBlock.
	blockFile := filepath.Join(diskSnapDir, archive.DataBlockName(codec.Ext()))
	compressed, readErr := os.ReadFile(blockFile)
	require.NoError(t, readErr)
	require.Equal(t, rawBlock, decodeZstdBlock(t, compressed),
		"merged block must decode to original bytes")

	// (c) The node must be fully finalized.
	assertNodeComplete(t, diskSnapDir)

	// (d) The chunk directory must have been removed after merge.
	_, statErr := os.Stat(chunkDir)
	require.True(t, os.IsNotExist(statErr), "data.bin.d/ must be removed after merge")
}

// ── recording progress helpers ────────────────────────────────────────────────

// recordedStream is a progress.Stream stub that counts Activate, Done, and Fail
// calls, and tracks the current/total byte counters exactly like the real
// sinks (IncrBy adds, SetTotal/SetCurrent set absolute values) so tests can
// assert on the observable progress numbers, e.g. the download-progress-seed-
// committed-bytes tests below. It mirrors the real progress.Stream contract's
// "first terminal call wins" semantics (see ttyStream.finalize /
// plainStream.finalize in internal/progress/multibar.go): once Done or Fail
// has been called once, a later call to either is a no-op on the counters.
// This matters for pipeline.Run's post-g.Wait() defensive sweep, which calls
// Fail() on every pre-created stream unconditionally — against the real sinks
// that is a safe no-op for already-Done streams, and this stub must behave
// the same way for tests exercising the sweep to assert anything meaningful.
// All methods are safe for concurrent use.
//
// history records every value the current counter took on, in call order, as
// set by either IncrBy or SetCurrent — used by the
// progress-no-regression-on-activate tests to assert the displayed value
// never visibly drops after a positive seed (see History).
type recordedStream struct {
	name        string
	mu          sync.Mutex
	activateCnt int
	doneCnt     int
	failCnt     int
	settled     bool
	current     int64
	total       int64
	history     []int64
	samples     []streamSample
}

// streamSample is a point-in-time snapshot of a stream's (current, total) pair,
// recorded after every counter-mutating call (IncrBy, SetCurrent, SetTotal).
// The clamp-resume-seed-to-fresh-total tests walk these to assert the displayed
// current never exceeds the total at ANY step — in particular in the window
// right after SetTotal lowers the total, which the plain current-only history
// cannot observe (see Samples).
type streamSample struct {
	current int64
	total   int64
}

func (s *recordedStream) IncrBy(n int) {
	s.mu.Lock()
	s.current += int64(n)
	s.history = append(s.history, s.current)
	s.samples = append(s.samples, streamSample{current: s.current, total: s.total})
	s.mu.Unlock()
}

func (s *recordedStream) SetTotal(total int64) {
	s.mu.Lock()
	s.total = total
	s.samples = append(s.samples, streamSample{current: s.current, total: s.total})
	s.mu.Unlock()
}

func (s *recordedStream) SetCurrent(current int64) {
	s.mu.Lock()
	s.current = current
	s.history = append(s.history, s.current)
	s.samples = append(s.samples, streamSample{current: s.current, total: s.total})
	s.mu.Unlock()
}

// Current returns the stream's current byte counter as last set by IncrBy/SetCurrent.
func (s *recordedStream) Current() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.current
}

// Total returns the stream's expected total as last set by SetTotal.
func (s *recordedStream) Total() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.total
}

// History returns a copy of every value the current counter took on, in call
// order (see the history field doc comment).
func (s *recordedStream) History() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]int64, len(s.history))
	copy(out, s.history)

	return out
}

// Samples returns a copy of the (current, total) pair recorded after each
// counter-mutating call, in call order (see the samples field doc comment).
func (s *recordedStream) Samples() []streamSample {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]streamSample, len(s.samples))
	copy(out, s.samples)

	return out
}

func (s *recordedStream) Activate() {
	s.mu.Lock()
	s.activateCnt++
	s.mu.Unlock()
}

func (s *recordedStream) Done() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.settled {
		return
	}

	s.settled = true
	s.doneCnt++
}

func (s *recordedStream) Fail() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.settled {
		return
	}

	s.settled = true
	s.failCnt++
}

// recordingSink is a progress.Sink stub that captures NewStream calls in creation
// order. All methods are safe for concurrent use.
type recordingSink struct {
	mu          sync.Mutex
	seen        []*recordedStream
	onNewStream func()
}

func (s *recordingSink) NewStream(name string, _ int64) progress.Stream {
	rs := &recordedStream{name: name}
	s.mu.Lock()
	s.seen = append(s.seen, rs)
	s.mu.Unlock()

	if s.onNewStream != nil {
		s.onNewStream()
	}

	return rs
}

func (s *recordingSink) SetVolumeTotal(int)   {}
func (s *recordingSink) Wait()                {}
func (s *recordingSink) LogWriter() io.Writer { return io.Discard }

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.seen)
}

func (s *recordingSink) snapshot() []*recordedStream {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*recordedStream, len(s.seen))
	copy(out, s.seen)

	return out
}

// TestPipeline_Progress_PrecreateStreams verifies that the pipeline pre-creates
// exactly one progress.Stream per volume leaf BEFORE any download starts, and
// creates no stream for aggregator/manifest-only nodes.
//
// Two leaf shapes are exercised:
//   - single-OwnDataRef (non-aggregator snapshot node)
//   - Binding (orphan VolumeSnapshot leaf)
func TestPipeline_Progress_PrecreateStreams(t *testing.T) {
	t.Parallel()

	t.Run("SingleOwnDataRef", func(t *testing.T) {
		t.Parallel()

		rawBlock := bytes.Repeat([]byte("X"), 300)
		srv := makeBlockServer(t, rawBlock)

		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()
		rec := &recordingSink{}

		var (
			once               sync.Once
			streamsAtFirstCall int
		)

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				once.Do(func() { streamsAtFirstCall = rec.count() })

				return exporter.NewExport(namespace, "de-precreate", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		// Exactly one stream for the single-OwnDataRef leaf; none for the root.
		require.Equal(t, 1, rec.count(), "exactly 1 stream for 1 volume leaf")
		require.Equal(t, 1, streamsAtFirstCall,
			"all streams must be pre-created before the first OpenExport call")

		streams := rec.snapshot()
		// disk-snap has no status.sourceRef (see buildFakeClient), so DisplayLabel falls
		// back to the snapshot CR's own Kind/Name.
		require.Equal(t, childKind+"/"+diskSnapName, streams[0].name, "stream name = node.DisplayLabel()")
		require.Equal(t, 1, streams[0].activateCnt, "leaf stream must be Activated exactly once")
		require.Equal(t, 1, streams[0].doneCnt, "leaf stream must be Done exactly once")
		require.Equal(t, 0, streams[0].failCnt, "a successful download must never call Fail")
	})

	t.Run("BindingLeaf", func(t *testing.T) {
		t.Parallel()

		rawBlock := bytes.Repeat([]byte("Y"), 300)
		srv := makeBlockServer(t, rawBlock)

		defer srv.Close()

		c := buildOrphanLeafFakeClient(t)
		outputDir := t.TempDir()
		rec := &recordingSink{}

		var (
			once               sync.Once
			streamsAtFirstCall int
		)

		cfg := pipeline.Config{
			Namespace:            e2eNS,
			RootSnapshot:         e2eAggRootSnap,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, leafRef aggapi.NodeRef, _ string) (*exporter.Export, error) {
				once.Do(func() { streamsAtFirstCall = rec.count() })

				if leafRef.Name != "nss-vs-agg-pvc" {
					return nil, fmt.Errorf("unexpected leaf %q", leafRef.Name)
				}

				return exporter.NewExport(namespace, "de-agg-leaf", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		// One stream for the binding leaf; none for root or aggregator nodes.
		require.Equal(t, 1, rec.count(),
			"exactly 1 stream for the binding leaf; aggregator/manifest-only nodes must not create streams")
		require.Equal(t, 1, streamsAtFirstCall,
			"all streams must be pre-created before the first OpenExport call")

		streams := rec.snapshot()
		// aggVS's status.sourceRef points at the captured PVC (see buildOrphanLeafFakeClient,
		// pvcSourceRefMap), so DisplayLabel prefers that original identity over the VS CR name.
		require.Equal(t, "PersistentVolumeClaim/pvc-agg", streams[0].name,
			"binding stream name = node.DisplayLabel() (original captured PVC identity)")
		require.Equal(t, 1, streams[0].activateCnt, "binding stream must be Activated exactly once")
		require.Equal(t, 1, streams[0].doneCnt, "binding stream must be Done exactly once")
		require.Equal(t, 0, streams[0].failCnt, "a successful download must never call Fail")
	})
}

// TestPipeline_Progress_ResumeSkip_NeverActivated verifies that when a leaf node is
// already complete (the resume plan is done), its pre-created stream is Done
// immediately in precreateStreams and is never Activated (OpenExport is not called).
func TestPipeline_Progress_ResumeSkip_NeverActivated(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("W"), 300)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	// First run: complete the pipeline so disk-snap becomes a done node.
	firstCfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-resume-first", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}
	require.NoError(t, runPipeline(context.Background(), firstCfg))

	// Second run: disk-snap is a done node; its stream must be Done immediately
	// (in precreateStreams) and must never be Activated.
	rec := &recordingSink{}

	secondCfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Progress:             rec,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when all nodes are already complete")
			return nil, errors.New("unexpected OpenExport call")
		},
	}
	require.NoError(t, runPipeline(context.Background(), secondCfg))

	require.Equal(t, 1, rec.count(), "one stream pre-created for the complete leaf node")

	streams := rec.snapshot()
	require.Equal(t, 0, streams[0].activateCnt,
		"resume-skipped stream must never be Activated")
	require.Equal(t, 1, streams[0].doneCnt,
		"resume-skipped stream must be Done exactly once (in precreateStreams)")
	require.Equal(t, 0, streams[0].failCnt, "a resume skip must never call Fail")
}

// TestPipeline_Progress_SeedsCommittedBytesBeforeTransfer verifies the
// download-progress-seed-committed-bytes fix: a resumed volume's progress
// stream must already reflect its on-disk committed bytes BEFORE the
// DataExport becomes ready / before any network call — captured here at the
// moment OpenExport is invoked, which is strictly before Activate, HEAD, and
// the listing call. It also proves the startup seed and the real per-chunk/
// per-file resume-skip crediting inside the download path never double
// count: the stream's final current must equal the volume's exact total size
// once the run completes.
func TestPipeline_Progress_SeedsCommittedBytesBeforeTransfer(t *testing.T) {
	t.Parallel()

	t.Run("Block", func(t *testing.T) {
		t.Parallel()

		const (
			testTotalSize int64 = 300
			partialBytes  int64 = 137 // durable prefix of the volume's single chunk
		)

		rawBlock := bytes.Repeat([]byte("Z"), int(testTotalSize))
		srv := makeBlockServer(t, rawBlock)

		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()

		codec, err := compress.New("zstd", 0)
		require.NoError(t, err)

		// Pre-seed the sole chunk's durable ".part" prefix, simulating a crash
		// mid-download (same technique as TestPipeline_PartialChunkResume; 300
		// bytes is well under volume.DefaultChunkSize, so this volume is always
		// exactly one chunk).
		diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
			archive.NodeDirName(childKind, diskSnapName))
		chunkDir := filepath.Join(diskSnapDir, archive.BlockChunksDirName)
		require.NoError(t, os.MkdirAll(chunkDir, 0o755))
		seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())

		require.NoError(t, os.WriteFile(
			filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part"),
			rawBlock[:partialBytes],
			0o644,
		))
		// A durable ".part.offset" sidecar must accompany the ".part" file so
		// partialChunkSize trusts this partial prefix instead of truncating it
		// to zero (see download-resume-part-trusted-prefix).
		require.NoError(t, os.WriteFile(
			filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part.offset"),
			[]byte(strconv.FormatInt(partialBytes, 10)),
			0o644,
		))

		require.NoError(t, archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: volume.DefaultChunkSize, TotalSize: testTotalSize}))

		rec := &recordingSink{}

		var (
			once          sync.Once
			seededCurrent int64
			seededTotal   int64
		)

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Compression:          codec,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				once.Do(func() {
					streams := rec.snapshot()
					if len(streams) == 1 {
						seededCurrent = streams[0].Current()
						seededTotal = streams[0].Total()
					}
				})

				return exporter.NewExport(namespace, "de-seed-block", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		require.Equal(t, partialBytes, seededCurrent,
			"stream must be seeded with the chunk's durable partial length before OpenExport ever runs")
		require.Equal(t, testTotalSize, seededTotal,
			"stream's total must be seeded from chunks.meta before OpenExport ever runs")

		streams := rec.snapshot()
		require.Equal(t, testTotalSize, streams[0].Current(),
			"final credited total must equal the exact volume size (no double count between the seed and the real resume-skip crediting)")
	})

	t.Run("Filesystem", func(t *testing.T) {
		t.Parallel()

		const (
			testTotalSize int64 = 250
			partialBytes  int64 = 142 // durable prefix of the file's single chunk
		)

		content := bytes.Repeat([]byte("F"), int(testTotalSize))

		mux := http.NewServeMux()
		mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/v1/files/":
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
					`{"name":"big.bin","type":"file","uri":"big.bin","attributes":{"size":`+strconv.FormatInt(testTotalSize, 10)+`}}`+
					`]}`)

			case "/api/v1/files/big.bin":
				http.ServeContent(w, r, "big.bin", time.Time{}, bytes.NewReader(content))

			default:
				http.NotFound(w, r)
			}
		})

		srv := httptest.NewServer(mux)
		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()

		codec, err := compress.New("zstd", 0)
		require.NoError(t, err)

		// Pre-seed big.bin's per-file chunk dir with its sole chunk's durable
		// ".part" prefix, simulating a crash mid-transfer (the realistic FS
		// analogue of the block sub-test above; 250 bytes is well under
		// volume.DefaultChunkSize, so this file is always exactly one chunk).
		diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
			archive.NodeDirName(childKind, diskSnapName))
		stagingDir := filepath.Join(diskSnapDir, archive.FsTarStagingDirName)
		fileChunkDir := filepath.Join(stagingDir, archive.FsFileChunksDirName("big.bin", codec.Ext()))
		require.NoError(t, os.MkdirAll(fileChunkDir, 0o755))
		seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())

		require.NoError(t, os.WriteFile(
			filepath.Join(fileChunkDir, archive.ChunkFileName(0, codec.Ext())+".part"),
			content[:partialBytes],
			0o644,
		))
		// A durable ".part.offset" sidecar must accompany the ".part" file so
		// partialChunkSize trusts this partial prefix instead of truncating it
		// to zero (see download-resume-part-trusted-prefix).
		require.NoError(t, os.WriteFile(
			filepath.Join(fileChunkDir, archive.ChunkFileName(0, codec.Ext())+".part.offset"),
			[]byte(strconv.FormatInt(partialBytes, 10)),
			0o644,
		))

		require.NoError(t, archive.WriteChunkMeta(fileChunkDir, archive.ChunkMeta{ChunkSize: volume.DefaultChunkSize, TotalSize: testTotalSize}))

		rec := &recordingSink{}

		var (
			once          sync.Once
			seededCurrent int64
		)

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Compression:          codec,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				once.Do(func() {
					streams := rec.snapshot()
					if len(streams) == 1 {
						seededCurrent = streams[0].Current()
					}
				})

				return exporter.NewExport(namespace, "de-seed-fs", "Filesystem", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		require.Equal(t, partialBytes, seededCurrent,
			"stream must be seeded with the in-progress per-file chunk dir's committed bytes before OpenExport ever runs")

		streams := rec.snapshot()
		require.Equal(t, testTotalSize, streams[0].Current(),
			"final credited total must equal the exact file size (no double count between the seed and the real resume-skip crediting)")
	})

	t.Run("FromScratchVolumeUnchanged", func(t *testing.T) {
		t.Parallel()

		rawBlock := bytes.Repeat([]byte("Q"), 300)
		srv := makeBlockServer(t, rawBlock)

		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()
		rec := &recordingSink{}

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				return exporter.NewExport(namespace, "de-seed-fromscratch", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		streams := rec.snapshot()
		require.Equal(t, int64(len(rawBlock)), streams[0].Current(),
			"a from-scratch volume (no seed applicable) must still reach exactly its full size")
	})
}

// TestPipeline_Progress_MonotonicAcrossActivate verifies the
// progress-no-regression-on-activate fix: once a resumed volume's stream has
// been seeded with a positive current value (seedStreamFromDisk, run before
// OpenExport/Activate), the recorded sequence of current values must never
// regress across the waiting->active transition — in particular it must
// never revisit 0, which is exactly what the previous
// stream.SetCurrent(0) reset (called right after Activate, before handing
// crediting to the real per-chunk/per-file resume-skip logic) produced as a
// visible dip. The final value must still land exactly on the volume's total
// size: pipeline.skipSeededBytes must discard precisely the resume-skip
// logic's re-derived credit for the already-seeded bytes, not more or less.
// A from-scratch (unseeded) stream is confirmed unaffected: its current
// value is still 0 at the moment OpenExport is invoked (same as before this
// fix), and its history — built entirely from real transfer bytes — is
// still trivially non-decreasing and reaches the exact total.
func TestPipeline_Progress_MonotonicAcrossActivate(t *testing.T) {
	t.Parallel()

	assertNonDecreasing := func(t *testing.T, history []int64) {
		t.Helper()

		for i := 1; i < len(history); i++ {
			require.GreaterOrEqualf(t, history[i], history[i-1],
				"current value regressed at history index %d: history=%v", i, history)
		}
	}

	t.Run("Block", func(t *testing.T) {
		t.Parallel()

		const (
			testTotalSize int64 = 300
			partialBytes  int64 = 137 // durable prefix of the volume's single chunk
		)

		rawBlock := bytes.Repeat([]byte("M"), int(testTotalSize))
		srv := makeBlockServer(t, rawBlock)

		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()

		codec, err := compress.New("zstd", 0)
		require.NoError(t, err)

		// Pre-seed the sole chunk's durable ".part" prefix, simulating a crash
		// mid-download (same technique as
		// TestPipeline_Progress_SeedsCommittedBytesBeforeTransfer; 300 bytes is
		// well under volume.DefaultChunkSize, so this volume is always exactly
		// one chunk).
		diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
			archive.NodeDirName(childKind, diskSnapName))
		chunkDir := filepath.Join(diskSnapDir, archive.BlockChunksDirName)
		require.NoError(t, os.MkdirAll(chunkDir, 0o755))
		seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())

		require.NoError(t, os.WriteFile(
			filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part"),
			rawBlock[:partialBytes],
			0o644,
		))
		// A durable ".part.offset" sidecar must accompany the ".part" file so
		// partialChunkSize trusts this partial prefix instead of truncating it
		// to zero (see download-resume-part-trusted-prefix).
		require.NoError(t, os.WriteFile(
			filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part.offset"),
			[]byte(strconv.FormatInt(partialBytes, 10)),
			0o644,
		))

		require.NoError(t, archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: volume.DefaultChunkSize, TotalSize: testTotalSize}))

		rec := &recordingSink{}

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Compression:          codec,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				return exporter.NewExport(namespace, "de-monotonic-block", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		streams := rec.snapshot()
		require.Len(t, streams, 1)

		history := streams[0].History()
		require.NotEmpty(t, history, "seeding must have recorded at least the initial seed value")
		require.Equal(t, partialBytes, history[0],
			"the very first recorded value must be the seed itself, before any SetCurrent(0)-style reset")
		require.NotContains(t, history[1:], int64(0),
			"current must never revisit 0 after a positive seed")
		assertNonDecreasing(t, history)
		require.Equal(t, testTotalSize, streams[0].Current(),
			"final credited total must equal the exact volume size (no double count)")
	})

	t.Run("Filesystem", func(t *testing.T) {
		t.Parallel()

		const (
			testTotalSize int64 = 250
			partialBytes  int64 = 142 // durable prefix of the file's single chunk
		)

		content := bytes.Repeat([]byte("N"), int(testTotalSize))

		mux := http.NewServeMux()
		mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/v1/files/":
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
					`{"name":"big.bin","type":"file","uri":"big.bin","attributes":{"size":`+strconv.FormatInt(testTotalSize, 10)+`}}`+
					`]}`)

			case "/api/v1/files/big.bin":
				http.ServeContent(w, r, "big.bin", time.Time{}, bytes.NewReader(content))

			default:
				http.NotFound(w, r)
			}
		})

		srv := httptest.NewServer(mux)
		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()

		codec, err := compress.New("zstd", 0)
		require.NoError(t, err)

		// Pre-seed big.bin's per-file chunk dir with its sole chunk's durable
		// ".part" prefix, simulating a crash mid-transfer (the realistic FS
		// analogue of the block sub-test above; 250 bytes is well under
		// volume.DefaultChunkSize, so this file is always exactly one chunk).
		diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
			archive.NodeDirName(childKind, diskSnapName))
		stagingDir := filepath.Join(diskSnapDir, archive.FsTarStagingDirName)
		fileChunkDir := filepath.Join(stagingDir, archive.FsFileChunksDirName("big.bin", codec.Ext()))
		require.NoError(t, os.MkdirAll(fileChunkDir, 0o755))
		seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())

		require.NoError(t, os.WriteFile(
			filepath.Join(fileChunkDir, archive.ChunkFileName(0, codec.Ext())+".part"),
			content[:partialBytes],
			0o644,
		))
		require.NoError(t, os.WriteFile(
			filepath.Join(fileChunkDir, archive.ChunkFileName(0, codec.Ext())+".part.offset"),
			[]byte(strconv.FormatInt(partialBytes, 10)),
			0o644,
		))

		require.NoError(t, archive.WriteChunkMeta(fileChunkDir, archive.ChunkMeta{ChunkSize: volume.DefaultChunkSize, TotalSize: testTotalSize}))

		rec := &recordingSink{}

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Compression:          codec,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				return exporter.NewExport(namespace, "de-monotonic-fs", "Filesystem", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		streams := rec.snapshot()
		require.Len(t, streams, 1)

		history := streams[0].History()
		require.NotEmpty(t, history, "seeding must have recorded at least the initial seed value")
		require.Equal(t, partialBytes, history[0],
			"the very first recorded value must be the seed itself, before any SetCurrent(0)-style reset")
		require.NotContains(t, history[1:], int64(0),
			"current must never revisit 0 after a positive seed")
		assertNonDecreasing(t, history)
		require.Equal(t, testTotalSize, streams[0].Current(),
			"final credited total must equal the exact file size (no double count)")
	})

	t.Run("FromScratch", func(t *testing.T) {
		t.Parallel()

		rawBlock := bytes.Repeat([]byte("P"), 300)
		srv := makeBlockServer(t, rawBlock)

		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()
		rec := &recordingSink{}

		var currentAtOpenExport int64 = -1

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				streams := rec.snapshot()
				if len(streams) == 1 {
					currentAtOpenExport = streams[0].Current()
				}

				return exporter.NewExport(namespace, "de-monotonic-fromscratch", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		require.Equal(t, int64(0), currentAtOpenExport,
			"a from-scratch stream (no seed applicable) is still 0 right up to OpenExport, unchanged by this fix")

		streams := rec.snapshot()
		require.Len(t, streams, 1)
		assertNonDecreasing(t, streams[0].History())
		require.Equal(t, int64(len(rawBlock)), streams[0].Current(),
			"a from-scratch volume must still reach exactly its full size")
	})
}

// TestPipeline_Progress_FSSizesSidecar_SeedsTotalAndCreditsStagedFile is the
// pipeline-level regression test for fs-resume-progress-sizes-sidecar: unlike
// the "Filesystem" sub-test above (which seeds a STILL-OPEN per-file chunk
// dir, the case ScanFSStagingProgress already handled), this seeds a sizes
// sidecar recording that one file has ALREADY been fully staged as a flat
// blob — its chunk dir merged away, so chunks.meta (the only other on-disk
// record of its raw size) is gone — plus a second file that has not been
// touched at all. Before this fix neither the flat blob's bytes nor the
// stream's total were seeded: the bar showed a "???" denominator and 0%
// until the DataExport became Ready and the listing was re-fetched over the
// network. Both must now be seeded from the sidecar alone, before OpenExport
// is ever called.
func TestPipeline_Progress_FSSizesSidecar_SeedsTotalAndCreditsStagedFile(t *testing.T) {
	t.Parallel()

	const (
		stagedFileSize  int64 = 90
		pendingFileSize int64 = 60
		testTotalSize   int64 = stagedFileSize + pendingFileSize
	)

	stagedContent := bytes.Repeat([]byte("A"), int(stagedFileSize))
	pendingContent := bytes.Repeat([]byte("B"), int(pendingFileSize))

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/files/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
				`{"name":"staged.bin","type":"file","uri":"staged.bin","attributes":{"size":`+strconv.FormatInt(stagedFileSize, 10)+`}},`+
				`{"name":"pending.bin","type":"file","uri":"pending.bin","attributes":{"size":`+strconv.FormatInt(pendingFileSize, 10)+`}}`+
				`]}`)

		case "/api/v1/files/staged.bin":
			http.ServeContent(w, r, "staged.bin", time.Time{}, bytes.NewReader(stagedContent))

		case "/api/v1/files/pending.bin":
			http.ServeContent(w, r, "pending.bin", time.Time{}, bytes.NewReader(pendingContent))

		default:
			http.NotFound(w, r)
		}
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	codec, err := compress.New("none", 0)
	require.NoError(t, err)

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	stagingDir := filepath.Join(diskSnapDir, archive.FsTarStagingDirName)
	require.NoError(t, os.MkdirAll(stagingDir, 0o755))
	seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())

	// staged.bin is already a fully-staged flat blob, as if a prior run had
	// merged it before crashing; pending.bin has not been touched at all.
	require.NoError(t, os.WriteFile(filepath.Join(stagingDir, "staged.bin"+codec.Ext()), stagedContent, 0o644))

	// Seed the sizes sidecar exactly as volume.DownloadFilesystemVolume would
	// have written it on the prior (interrupted) run's listing fetch: under the
	// reserved metadata namespace (stagingDir/.d8-meta/sizes.json), never the
	// staging root where a user file could shadow it.
	sizesJSON, err := json.Marshal(volume.FSSizesSidecar{
		Files: map[string]int64{"staged.bin": stagedFileSize, "pending.bin": pendingFileSize},
		Total: testTotalSize,
	})
	require.NoError(t, err)
	metaDir := filepath.Join(stagingDir, volume.FSMetaDirName)
	require.NoError(t, os.MkdirAll(metaDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(metaDir, volume.FSSizesSidecarName), sizesJSON, 0o644))

	rec := &recordingSink{}

	var (
		once          sync.Once
		seededCurrent int64
		seededTotal   int64
	)

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Compression:          codec,
		Progress:             rec,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			once.Do(func() {
				streams := rec.snapshot()
				if len(streams) == 1 {
					seededCurrent = streams[0].Current()
					seededTotal = streams[0].Total()
				}
			})

			return exporter.NewExport(namespace, "de-seed-fs-sizes", "Filesystem", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	require.Equal(t, stagedFileSize, seededCurrent,
		"stream must be seeded with the already-staged flat blob's persisted declared size before OpenExport ever runs")
	require.Equal(t, testTotalSize, seededTotal,
		"stream's total must be seeded from the sizes sidecar before OpenExport ever runs (no ??? denominator)")

	streams := rec.snapshot()
	require.Equal(t, testTotalSize, streams[0].Current(),
		"final credited total must equal the exact combined file size (no double count between the sidecar seed and the real resume-skip crediting)")
}

func TestPipeline_Progress_ResumeScanCancellationStopsBeforeExport(t *testing.T) {
	c := buildFakeClient(t)
	outputDir := t.TempDir()

	codec, err := compress.New("none", 0)
	require.NoError(t, err)

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	chunksRoot := filepath.Join(
		diskSnapDir,
		archive.FsTarStagingDirName,
		volume.FSMetaDirName,
		archive.FSChunksDirName,
	)
	require.NoError(t, os.MkdirAll(filepath.Join(chunksRoot, "wide-parent"), 0o755))
	seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())

	ctx, cancel := context.WithCancel(context.Background())
	rec := &recordingSink{onNewStream: cancel}

	var openExportCalls atomic.Int64

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Compression:          codec,
		Progress:             rec,
		OpenExport: func(context.Context, string, aggapi.NodeRef, string) (*exporter.Export, error) {
			openExportCalls.Add(1)

			return nil, errors.New("OpenExport must not run after resume scan cancellation")
		},
	}

	err = runPipeline(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, openExportCalls.Load())

	streams := rec.snapshot()
	require.Len(t, streams, 1)
	require.Equal(t, 1, streams[0].failCnt, "the partially pre-created stream must be settled on cancellation")
}

type armedCancellationContext struct {
	context.Context
	armed    atomic.Bool
	checks   atomic.Int64
	cancelAt int64
}

func (c *armedCancellationContext) Arm() {
	c.armed.Store(true)
}

func (c *armedCancellationContext) Err() error {
	if !c.armed.Load() {
		return nil
	}

	if c.checks.Add(1) >= c.cancelAt {
		return context.Canceled
	}

	return nil
}

func TestPipeline_Progress_BlockResumeScanCancellationStopsBeforeExport(t *testing.T) {
	const (
		cancelAt  int64 = 12
		chunkSize int64 = 1
		totalSize int64 = 10_000
	)

	c := buildFakeClient(t)
	outputDir := t.TempDir()
	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	chunkDir := filepath.Join(diskSnapDir, archive.BlockChunksDirName)
	require.NoError(t, os.MkdirAll(chunkDir, 0o755))
	seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())
	require.NoError(t, archive.WriteChunkMeta(
		chunkDir,
		archive.ChunkMeta{ChunkSize: chunkSize, TotalSize: totalSize},
	))

	for index := range 4 {
		require.NoError(t, os.WriteFile(
			filepath.Join(chunkDir, archive.ChunkFileName(index, "")),
			[]byte{byte(index)},
			0o644,
		))
	}

	codec, err := compress.New("none", 0)
	require.NoError(t, err)

	ctx := &armedCancellationContext{Context: context.Background(), cancelAt: cancelAt}
	rec := &recordingSink{onNewStream: ctx.Arm}

	var (
		openExportCalls atomic.Int64
		networkCalls    atomic.Int64
	)

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		networkCalls.Add(1)
	}))
	defer server.Close()

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Compression:          codec,
		Progress:             rec,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			openExportCalls.Add(1)

			return exporter.NewExport(namespace, "de-block-scan-cancel", "Block", server.URL, exporter.NewFetcher(server.Client())), nil
		},
	}

	err = runPipeline(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "scan chunk",
		"cancellation must be observed inside the production block chunk-index scan")
	require.Equal(t, cancelAt, ctx.checks.Load(),
		"no filesystem scan or later pipeline work may check the context after block cancellation")
	require.Zero(t, openExportCalls.Load(), "DataExport activation must not start")
	require.Zero(t, networkCalls.Load(), "network access must not start")

	streams := rec.snapshot()
	require.Len(t, streams, 1)
	require.Empty(t, streams[0].Samples(), "a cancelled scan must not mutate current or total counters")
	require.Zero(t, streams[0].Current())
	require.Zero(t, streams[0].Total())
	require.Zero(t, streams[0].activateCnt)
	require.Equal(t, 1, streams[0].failCnt, "the pre-created stream must be settled before return")

	samplesAfterReturn := streams[0].Samples()
	checksAfterReturn := ctx.checks.Load()
	require.Equal(t, samplesAfterReturn, streams[0].Samples(), "stream counters must remain quiescent after return")
	require.Equal(t, checksAfterReturn, ctx.checks.Load(), "filesystem scanning must remain quiescent after return")
	require.Zero(t, openExportCalls.Load())
	require.Zero(t, networkCalls.Load())
}

// TestPipeline_Progress_ClampStaleSeedToFreshTotal is the regression test for
// clamp-resume-seed-to-fresh-total: when seedStreamFromDisk credits committed
// bytes from an OLD on-disk geometry (chunks.meta or a sizes sidecar) that the
// current run's fresh HEAD/listing total contradicts (a changed --chunk-size or
// a shrunk volume between runs), the stream's displayed current must never
// exceed its total at any point — the ">100% for one frame" rendering artifact
// this task removes — and must still land exactly on the fresh total. A VALID
// seed (seeded <= fresh total) must be left untouched: no dip, monotonic
// forward progress preserved (the progress-no-regression-on-activate contract).
//
// assertNeverExceedsTotal walks the (current, total) samples the recordedStream
// records after every counter-mutating call; the dangerous sample is the one
// right after SetTotal lowers the total while current still holds the stale
// seed — which is exactly what the reconcile (SetCurrent(0) BEFORE SetTotal)
// prevents.
func TestPipeline_Progress_ClampStaleSeedToFreshTotal(t *testing.T) {
	t.Parallel()

	assertNeverExceedsTotal := func(t *testing.T, samples []streamSample) {
		t.Helper()

		for i, s := range samples {
			if s.total <= 0 {
				// An unknown total (0) renders a "???" denominator, not a
				// percentage, so it can never show above 100%.
				continue
			}

			require.LessOrEqualf(t, s.current, s.total,
				"displayed current %d exceeded total %d at sample %d: %+v", s.current, s.total, i, samples)
		}
	}

	t.Run("BlockStaleGeometryShrinksTotal", func(t *testing.T) {
		t.Parallel()

		const (
			oldTotalSize int64 = 300 // stale seed -> seedStreamFromDisk credits 300
			freshTotal   int64 = 150 // fresh HEAD reports a SMALLER volume
		)

		rawBlock := bytes.Repeat([]byte("Z"), int(freshTotal))
		srv := makeBlockServer(t, rawBlock)

		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()

		codec, err := compress.New("zstd", 0)
		require.NoError(t, err)

		// Seed an OLD geometry: chunks.meta claims 300 bytes, so
		// seedStreamFromDisk credits 300. ensureChunkGeometry will purge this
		// whole dir on the fresh run (meta 300 != fresh 150), so the chunk
		// file's content is irrelevant — it is re-fetched from byte zero and
		// the resume-skip crediting re-derives 0.
		diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
			archive.NodeDirName(childKind, diskSnapName))
		chunkDir := filepath.Join(diskSnapDir, archive.BlockChunksDirName)
		require.NoError(t, os.MkdirAll(chunkDir, 0o755))
		seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())

		require.NoError(t, os.WriteFile(
			filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())),
			[]byte("stale"), 0o644))

		require.NoError(t, archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: volume.DefaultChunkSize, TotalSize: oldTotalSize}))

		rec := &recordingSink{}

		var (
			once          sync.Once
			seededCurrent int64
		)

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Compression:          codec,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				once.Do(func() {
					streams := rec.snapshot()
					if len(streams) == 1 {
						seededCurrent = streams[0].Current()
					}
				})

				return exporter.NewExport(namespace, "de-clamp-block", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		require.Equal(t, oldTotalSize, seededCurrent,
			"the stale seed (300) must still be credited at OpenExport time — the clamp happens later, inside downloadBlock after the fresh HEAD")

		streams := rec.snapshot()
		require.Len(t, streams, 1)
		assertNeverExceedsTotal(t, streams[0].Samples())
		require.Equal(t, freshTotal, streams[0].Total(),
			"final total must be the fresh HEAD size")
		require.Equal(t, freshTotal, streams[0].Current(),
			"final current must land exactly on the fresh total after the stale seed is clamped")
	})

	t.Run("FilesystemStaleSizesSidecarShrinksTotal", func(t *testing.T) {
		t.Parallel()

		const (
			staleSize  int64 = 250 // sidecar + already-staged flat blob from a prior run
			freshTotal int64 = 150 // fresh listing reports a SMALLER file
		)

		content := bytes.Repeat([]byte("F"), int(freshTotal))

		mux := http.NewServeMux()
		mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/v1/files/":
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+
					`{"name":"a.bin","type":"file","uri":"a.bin","attributes":{"size":`+strconv.FormatInt(freshTotal, 10)+`}}`+
					`]}`)

			case "/api/v1/files/a.bin":
				http.ServeContent(w, r, "a.bin", time.Time{}, bytes.NewReader(content))

			default:
				http.NotFound(w, r)
			}
		})

		srv := httptest.NewServer(mux)
		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()

		codec, err := compress.New("none", 0)
		require.NoError(t, err)

		diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
			archive.NodeDirName(childKind, diskSnapName))
		stagingDir := filepath.Join(diskSnapDir, archive.FsTarStagingDirName)
		require.NoError(t, os.MkdirAll(stagingDir, 0o755))
		seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())

		// a.bin was fully staged as a flat blob under the OLD (larger) size, and
		// the sizes sidecar records that stale size, so seedStreamFromDisk seeds
		// both total (250) and current (250) — above the fresh listing total.
		require.NoError(t, os.WriteFile(filepath.Join(stagingDir, "a.bin"+codec.Ext()), bytes.Repeat([]byte("A"), int(staleSize)), 0o644))

		sizesJSON, err := json.Marshal(volume.FSSizesSidecar{
			Files: map[string]int64{"a.bin": staleSize},
			Total: staleSize,
		})
		require.NoError(t, err)
		metaDir := filepath.Join(stagingDir, volume.FSMetaDirName)
		require.NoError(t, os.MkdirAll(metaDir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(metaDir, volume.FSSizesSidecarName), sizesJSON, 0o644))

		rec := &recordingSink{}

		var (
			once          sync.Once
			seededCurrent int64
			seededTotal   int64
		)

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Compression:          codec,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				once.Do(func() {
					streams := rec.snapshot()
					if len(streams) == 1 {
						seededCurrent = streams[0].Current()
						seededTotal = streams[0].Total()
					}
				})

				return exporter.NewExport(namespace, "de-clamp-fs", "Filesystem", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		require.Equal(t, staleSize, seededCurrent,
			"the stale sidecar seed (250) must still be credited at OpenExport time — the clamp happens later, inside setTotal after the fresh listing")
		require.Equal(t, staleSize, seededTotal,
			"the stale sidecar total (250) is seeded before the fresh listing lowers it")

		streams := rec.snapshot()
		require.Len(t, streams, 1)
		assertNeverExceedsTotal(t, streams[0].Samples())
		require.Equal(t, freshTotal, streams[0].Total(),
			"final total must be the fresh listing size")
		require.Equal(t, freshTotal, streams[0].Current(),
			"final current must land exactly on the fresh total after the stale seed is clamped")
	})

	t.Run("ValidSeedIsNotClamped", func(t *testing.T) {
		t.Parallel()

		const (
			testTotalSize int64 = 300 // fresh HEAD == on-disk geometry: a same-geometry resume
			seedBytes     int64 = 100 // durable partial prefix already on disk
		)

		rawBlock := bytes.Repeat([]byte("V"), int(testTotalSize))
		srv := makeBlockServer(t, rawBlock)

		defer srv.Close()

		c := buildFakeClient(t)
		outputDir := t.TempDir()

		codec, err := compress.New("zstd", 0)
		require.NoError(t, err)

		// A VALID seed: the sole chunk's durable ".part" prefix under a geometry
		// that matches the fresh run exactly (volume.DefaultChunkSize, total
		// 300), so nothing is purged and the seed (100) stays strictly below
		// the fresh total (300). The clamp must NOT fire — no SetCurrent(0), no
		// dip.
		diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
			archive.NodeDirName(childKind, diskSnapName))
		chunkDir := filepath.Join(diskSnapDir, archive.BlockChunksDirName)
		require.NoError(t, os.MkdirAll(chunkDir, 0o755))
		seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())

		require.NoError(t, os.WriteFile(
			filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part"),
			rawBlock[:seedBytes],
			0o644,
		))
		// A durable ".part.offset" sidecar must accompany the ".part" file so
		// partialChunkSize trusts this partial prefix instead of truncating it
		// to zero (see download-resume-part-trusted-prefix).
		require.NoError(t, os.WriteFile(
			filepath.Join(chunkDir, archive.ChunkFileName(0, codec.Ext())+".part.offset"),
			[]byte(strconv.FormatInt(seedBytes, 10)),
			0o644,
		))

		require.NoError(t, archive.WriteChunkMeta(chunkDir, archive.ChunkMeta{ChunkSize: volume.DefaultChunkSize, TotalSize: testTotalSize}))

		rec := &recordingSink{}

		cfg := pipeline.Config{
			Namespace:            testNS,
			RootSnapshot:         rootSnapshot,
			OutputDir:            outputDir,
			Workers:              1,
			PerVolumeConcurrency: 1,
			KubeClient:           c,
			Compression:          codec,
			Progress:             rec,
			OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
				return exporter.NewExport(namespace, "de-clamp-valid", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
			},
		}

		require.NoError(t, runPipeline(context.Background(), cfg))

		streams := rec.snapshot()
		require.Len(t, streams, 1)

		history := streams[0].History()
		require.NotEmpty(t, history)
		require.Equal(t, seedBytes, history[0],
			"a valid seed's first recorded value must be the seed itself")
		require.NotContains(t, history[1:], int64(0),
			"a valid seed must never be reset to 0 (no SetCurrent(0)-style dip)")
		assertNeverExceedsTotal(t, streams[0].Samples())
		require.Equal(t, testTotalSize, streams[0].Current(),
			"a valid-seed resume must still land exactly on the total")
	})
}

// TestPipeline_Progress_DownloadFailure_CallsFailNotDone verifies that when a
// volume download fails AFTER its DataExport opened and its stream was
// Activated (e.g. the block server errors mid-transfer or the connection is
// cut), downloadVolumeBinding calls stream.Fail() exactly once and
// stream.Done() zero times — the interrupted volume must never be counted
// toward "N/M volumes downloaded". Before the fix this test observed doneCnt
// == 1 (the unconditional `defer stream.Done()`), which is the exact live bug
// reported on a cluster: an interrupted download's own deferred Done() call
// incremented the completed-volume counter.
func TestPipeline_Progress_DownloadFailure_CallsFailNotDone(t *testing.T) {
	t.Parallel()

	// A block server whose HEAD response always errors, so downloadBlock fails
	// inside downloadVolumeBinding right after stream.Activate() has run.
	failingSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "simulated block volume failure", http.StatusInternalServerError)
	}))
	defer failingSrv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()
	rec := &recordingSink{}

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Progress:             rec,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-fail", "Block", failingSrv.URL, exporter.NewFetcher(failingSrv.Client())), nil
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.Error(t, err, "expected pipeline to fail when the block volume HEAD request errors")

	streams := rec.snapshot()
	require.Len(t, streams, 1, "exactly 1 stream for the single volume leaf")
	require.Equal(t, 1, streams[0].activateCnt, "stream must still be Activated before the failure")
	require.Equal(t, 0, streams[0].doneCnt, "a failed download must never call Done")
	require.Equal(t, 1, streams[0].failCnt, "a failed download must call Fail exactly once")
}

// TestPipeline_KeepExports verifies the --cleanup / Config.KeepExports gate on
// downloadVolumeBinding's DataExport release: with KeepExports false (default,
// today's behavior) the DataExport CR is deleted after the volume completes;
// with KeepExports true it is left in the cluster for debugging.
func TestPipeline_KeepExports(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		keepExports   bool
		wantRemaining bool
	}{
		{name: "default deletes DataExport", keepExports: false, wantRemaining: false},
		{name: "KeepExports leaves DataExport", keepExports: true, wantRemaining: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const runID = "run-keep-exports"

			rawBlock := bytes.Repeat([]byte("K"), 300)
			srv := makeBlockServer(t, rawBlock)

			defer srv.Close()

			c := buildFakeClient(t)
			outputDir := t.TempDir()

			// The pipeline releases by the deterministic name derived from the leaf's
			// own node-ref name (exporter.DataExportName), not from whatever name the
			// OpenExport stub happens to hand back — release must find this object.
			deName := diskSnapshotDataExportName()

			de := &deapi.DataExport{
				TypeMeta: metav1.TypeMeta{APIVersion: "storage-foundation.deckhouse.io/v1alpha1", Kind: "DataExport"},
				ObjectMeta: metav1.ObjectMeta{
					Name:      deName,
					Namespace: testNS,
					UID:       "uid-keep-exports",
					Annotations: map[string]string{
						runOwnerAnnotationKey:  runID,
						targetUIDAnnotationKey: diskSnapUID,
					},
				},
				Spec: deapi.DataexportSpec{
					TTL: "2h",
					TargetRef: deapi.TargetRefSpec{
						Group:    "demo.deckhouse.io",
						Resource: "virtualdisksnapshots",
						Kind:     childKind,
						Name:     diskSnapName,
					},
				},
			}
			require.NoError(t, c.Create(context.Background(), de))

			cfg := pipeline.Config{
				Namespace:            testNS,
				RootSnapshot:         rootSnapshot,
				OutputDir:            outputDir,
				Workers:              1,
				PerVolumeConcurrency: 1,
				KubeClient:           c,
				KeepExports:          tc.keepExports,
				RunID:                runID,
				OpenExportWithAcquisition: func(
					ctx context.Context,
					namespace string,
					leafRef aggapi.NodeRef,
					ttl string,
				) (*exporter.Export, *exporter.DataExportAcquisition, error) {
					var acquisition *exporter.DataExportAcquisition

					_, ensureErr := exporter.EnsureDataExport(
						ctx,
						c,
						namespace,
						"demo.deckhouse.io",
						"virtualdisksnapshots",
						childKind,
						leafRef.Name,
						ttl,
						exporter.WithTargetUID(types.UID(diskSnapUID)),
						exporter.WithRunOwner(runID, slog.Default()),
						exporter.WithAcquisition(&acquisition),
					)
					if ensureErr != nil {
						return nil, nil, ensureErr
					}

					return exporter.NewExport(
						namespace,
						deName,
						"Block",
						srv.URL,
						exporter.NewFetcher(srv.Client()),
					), acquisition, nil
				},
			}

			require.NoError(t, runPipeline(context.Background(), cfg))

			got := &deapi.DataExport{}
			err := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: deName}, got)

			if tc.wantRemaining {
				require.NoError(t, err, "DataExport must remain in the cluster when KeepExports is true")
			} else {
				require.Truef(t, apierrors.IsNotFound(err),
					"DataExport must be deleted when KeepExports is false, got err=%v", err)
			}
		})
	}
}

// TestPipeline_Progress_OpenExportFailure_CallsFailNotDone verifies that when
// cfg.OpenExport itself returns an error (e.g. ctx cancelled while polling
// WaitReady, or the DataExport never becomes Ready), downloadVolumeBinding's
// stream.Fail()/Done() defer — now registered right after the stream semaphore
// is acquired, BEFORE cfg.OpenExport is even called — still settles the stream
// as Fail exactly once and Done zero times. Before the
// progress-finalize-streams-on-early-error-paths fix, the terminal defer was
// registered only after cfg.OpenExport returned successfully, so this exact
// path left the pre-created stream dangling (failCnt==0, doneCnt==0) and a real
// TTY sink's Wait() would block forever on it.
func TestPipeline_Progress_OpenExportFailure_CallsFailNotDone(t *testing.T) {
	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()
	rec := &recordingSink{}

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		Progress:             rec,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return nil, errors.New("simulated OpenExport failure (e.g. ctx cancelled mid-WaitReady)")
		},
	}

	err := runPipeline(context.Background(), cfg)
	require.Error(t, err, "expected pipeline to fail when OpenExport itself errors")

	streams := rec.snapshot()
	require.Len(t, streams, 1, "exactly 1 stream for the single volume leaf")
	require.Equal(t, 0, streams[0].activateCnt, "a stream must never be Activated before OpenExport succeeds")
	require.Equal(t, 0, streams[0].doneCnt, "an OpenExport failure must never call Done")
	require.Equal(t, 1, streams[0].failCnt, "an OpenExport failure must call Fail exactly once")
}

// TestPipeline_Progress_CancelDuringWait_DoesNotDeadlock is the end-to-end
// regression test for the live "had to press Ctrl-C twice" report: it drives a
// REAL progress.New(..., true) ttySink (not the recordingSink stub used
// elsewhere in this file) through a cancelled run and asserts sink.Wait() —
// the exact call cmd/download/download.go makes after pipeline.Run returns —
// completes promptly instead of blocking forever.
//
// The tree has two volume leaves and MaxParallelDownloads=1, so once the first
// leaf's goroutine is blocked inside OpenExport (holding the one stream-
// semaphore slot), the second leaf's goroutine is necessarily blocked on
// cfg.streamSem.Acquire. Cancelling ctx at that moment exercises BOTH early-
// return paths named in the task at once: the semaphore-acquire failure (only
// caught by Run's post-g.Wait() sweep) and the OpenExport failure (caught by
// downloadVolumeBinding's own relocated defer).
func TestPipeline_Progress_CancelDuringWait_DoesNotDeadlock(t *testing.T) {
	t.Parallel()

	const nVolumes = 2

	c := buildCapTestClient(t, nVolumes)
	outputDir := t.TempDir()
	sink := progress.New(&bytes.Buffer{}, true)

	arrived := make(chan struct{})

	var arrivedOnce sync.Once

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := pipeline.Config{
		Namespace:            capTestNS,
		RootSnapshot:         capTestRootSnap,
		OutputDir:            outputDir,
		Workers:              nVolumes,
		PerVolumeConcurrency: 1,
		MaxParallelDownloads: 1,
		KubeClient:           c,
		Progress:             sink,
		OpenExport: func(exportCtx context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			arrivedOnce.Do(func() { close(arrived) })

			<-exportCtx.Done()

			return nil, exportCtx.Err()
		},
	}

	runDone := make(chan error, 1)

	go func() {
		runDone <- runPipeline(ctx, cfg)
	}()

	// Wait until exactly one leaf is blocked inside OpenExport (holding the one
	// MaxParallelDownloads=1 slot); the other leaf is necessarily blocked on
	// cfg.streamSem.Acquire at this point.
	select {
	case <-arrived:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("timeout: no leaf reached OpenExport")
	}

	// Simulate a SIGINT: cancel the context both leaves are waiting on.
	cancel()

	var runErr error

	select {
	case runErr = <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("pipeline.Run did not return after ctx cancellation")
	}

	require.ErrorIs(t, runErr, context.Canceled,
		"a cancelled run must return ctx.Err(), not the per-node best-effort aggregate")

	// The critical regression assertion: sink.Wait() must return promptly. Before
	// the fix, the leaf blocked on streamSem.Acquire left its pre-created stream
	// permanently unsettled, and this call would hang forever.
	waitDone := make(chan struct{})

	go func() {
		sink.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("sink.Wait() deadlocked after a cancelled run — every pre-created stream must be terminally settled")
	}
}

// alwaysCanceledContext wraps context.Background() and overrides only Err(),
// never Done(). This deterministically reproduces the live-reproduced race
// (SIGINT arriving in the narrow window between the last node finishing and
// Run returning) without a timing-dependent goroutine dance: errgroup.WithContext
// propagates cancellation into its derived gctx by watching the PARENT's Done()
// channel (see context.propagateCancel), not by polling Err(), so a nil Done()
// here means gctx is never actually cancelled and every node genuinely runs to
// completion — while Run's own final check reads ctx.Err() directly and always
// observes a cancellation, exactly matching "ctx was cancelled but nodeErrs is
// empty because everything already succeeded."
type alwaysCanceledContext struct {
	context.Context
}

// Err always reports context.Canceled, regardless of Done().
func (alwaysCanceledContext) Err() error { return context.Canceled }

// TestPipeline_CancelAfterAllNodesSucceed_ReturnsNil is the regression test for
// the live-reproduced misreport: a fully successful download whose ctx happens
// to be cancelled right as the last node finishes must not be reported as a
// failure. See alwaysCanceledContext for how the race is made deterministic.
func TestPipeline_CancelAfterAllNodesSucceed_ReturnsNil(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("B"), 600)

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
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-mock", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := runPipeline(alwaysCanceledContext{context.Background()}, cfg)
	require.NoError(t, err,
		"a ctx observed as cancelled only after every node already succeeded must not turn the run into a reported failure")

	assertNodeComplete(t, outputDir)
}

// TestPipeline_BestEffort_OneNodeFailureDoesNotCancelSiblings is the regression
// test for the best-effort per-node download design: one node's permanent
// download failure must not cancel sibling nodes that are still downloading.
//
// Three independent leaf nodes (buildCapTestClient) start downloading
// concurrently (Workers == nVolumes, so every leaf's goroutine runs
// immediately). The failing leaf's OpenExport returns an error right away; the
// healthy leaves' OpenExport instead waits briefly while watching ctx — long
// enough that, under the OLD errgroup.WithContext(ctx) behavior (the first
// non-nil g.Go return cancels the shared derived context), a healthy leaf
// would observe ctx.Done() during that wait and fail too. Under the fixed
// best-effort behavior a per-node error never cancels the shared context, so
// the healthy leaves complete normally despite the sibling failure.
func TestPipeline_BestEffort_OneNodeFailureDoesNotCancelSiblings(t *testing.T) {
	t.Parallel()

	const (
		nVolumes = 3
		failIdx  = 1
	)

	failName := fmt.Sprintf("cap-disk-%d", failIdx)
	errPermanentFailure := errors.New("simulated permanent volume failure")

	c := buildCapTestClient(t, nVolumes)
	outputDir := t.TempDir()

	rawBlock := bytes.Repeat([]byte("Z"), 300)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	cfg := pipeline.Config{
		Namespace:            capTestNS,
		RootSnapshot:         capTestRootSnap,
		OutputDir:            outputDir,
		Workers:              nVolumes,
		PerVolumeConcurrency: 1,
		MaxParallelDownloads: nVolumes,
		KubeClient:           c,
		ManifestSource:       newManifestStub(),
		OpenExport: func(exportCtx context.Context, ns string, ref aggapi.NodeRef, _ string) (*exporter.Export, error) {
			if ref.Name == failName {
				return nil, fmt.Errorf("node %s: %w", ref.Name, errPermanentFailure)
			}

			// Give the failing leaf's goroutine time to return and, under the old
			// first-error-cancels-all behavior, cancel the shared context while
			// this healthy leaf is still "in flight".
			select {
			case <-time.After(150 * time.Millisecond):
			case <-exportCtx.Done():
				return nil, exportCtx.Err()
			}

			return exporter.NewExport(ns, "de-"+ref.Name, "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	err := pipeline.Run(context.Background(), cfg)
	require.Error(t, err, "expected an aggregated error naming the permanently failed node")
	require.ErrorIs(t, err, errPermanentFailure, "aggregated error must join the failed node's own error")
	require.Contains(t, err.Error(), failName, "aggregated error must identify the failed node")

	for i := 0; i < nVolumes; i++ {
		diskName := fmt.Sprintf("cap-disk-%d", i)
		nodeDir := filepath.Join(outputDir, archive.SnapshotsDirName, archive.NodeDirName(capTestKind, diskName))
		dataPath := filepath.Join(nodeDir, archive.DataBlockName(".zst"))

		if i == failIdx {
			_, statErr := os.Stat(dataPath)
			require.True(t, os.IsNotExist(statErr), "the failed node %s must not have downloaded data", diskName)

			continue
		}

		assertNodeComplete(t, nodeDir)

		_, statErr := os.Stat(dataPath)
		require.NoError(t, statErr, "healthy node %s must have downloaded data despite the sibling failure", diskName)
	}
}

// ─── TestPipeline_MixedResumeStates_ConcurrentRun ────────────────────────────

// Namespace/name/geometry constants for the mixed-resume-states tree. Names
// carry a "mixed" prefix so they cannot be confused with any other test's
// fake-client fixtures in this package.
const (
	mixedNS       = "mixed-resume-ns"
	mixedRootSnap = "mixed-root"
	mixedVMSnap   = "mixed-vm-snap"

	mixedDiskDone          = "mixed-disk-done"
	mixedDiskBlockPartial  = "mixed-disk-block-partial"
	mixedDiskFSPartial     = "mixed-disk-fs-partial"
	mixedDiskManifestsOnly = "mixed-disk-manifests-only"
	mixedDiskPending       = "mixed-disk-pending"
)

// mixedBlockPartialBytes is the number of already-durable raw bytes seeded
// for mixed-disk-block-partial's single block chunk (production hardcodes
// block chunk geometry to volume.DefaultChunkSize, so a 300-byte fixture is
// always exactly one chunk; see block-chunk-size-hardcode-only).
const mixedBlockPartialBytes int64 = 100

// mixedLeafNames lists every volume-leaf name in the mixed-resume tree, in
// the order the fake client wires them as mixed-vm-snap's children. Used to
// size the aggregate-counter assertion and to drive fixture construction.
var mixedLeafNames = []string{
	mixedDiskDone,
	mixedDiskBlockPartial,
	mixedDiskFSPartial,
	mixedDiskManifestsOnly,
	mixedDiskPending,
}

// stringRecorder is a small concurrency-safe log used by
// TestPipeline_MixedResumeStates_ConcurrentRun to record which leaf names or
// HTTP requests occurred during a given pipeline.Run call. reset() discards
// prior entries so run 1's activity cannot leak into the run-2-only
// assertions the test makes about resume behavior.
type stringRecorder struct {
	mu      sync.Mutex
	entries []string
}

func (r *stringRecorder) record(s string) {
	r.mu.Lock()
	r.entries = append(r.entries, s)
	r.mu.Unlock()
}

func (r *stringRecorder) reset() {
	r.mu.Lock()
	r.entries = nil
	r.mu.Unlock()
}

func (r *stringRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.entries...)
}

// makeTrackedBlockServer serves rawData at /api/v1/block like makeBlockServer,
// additionally recording every GET Range header into rec so a test can assert
// exactly which byte ranges were (or were not) re-fetched across a resume run.
func makeTrackedBlockServer(t *testing.T, rawData []byte, rec *stringRecorder) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/block", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			rec.record(r.Header.Get("Range"))
		}

		http.ServeContent(w, r, "data", time.Time{}, bytes.NewReader(rawData))
	})

	srv := httptest.NewServer(mux)

	t.Cleanup(srv.Close)

	return srv
}

// makeTrackedFSServer serves a flat (no subdirectories) filesystem-volume
// listing of files at /api/v1/files/, serving producer-shaped source-hash HEAD
// requests, and recording only per-file body GETs into rec so a test can assert
// exactly which files were (or were not) re-fetched across a resume run.
// Modeled on makeE2EFSServer but flat and instrumented.
func makeTrackedFSServer(t *testing.T, files []fsE2EFile, rec *stringRecorder) *httptest.Server {
	t.Helper()

	fileMap := make(map[string][]byte, len(files))
	for _, f := range files {
		fileMap[f.rel] = f.content
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/files/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/files/" {
			items := make([]string, 0, len(files))

			for _, f := range files {
				items = append(items, fmt.Sprintf(
					`{"name":%q,"type":"file","uri":%q,"attributes":{"permissions":"0644","modtime":"2024-03-01T12:00:00Z","uid":0,"gid":0,"size":%d}}`,
					f.rel, f.rel, len(f.content)))
			}

			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"apiVersion":"v1","items":[`+strings.Join(items, ",")+`]}`)

			return
		}

		name := strings.TrimPrefix(r.URL.Path, "/api/v1/files/")

		content, ok := fileMap[name]
		if !ok {
			http.NotFound(w, r)

			return
		}

		if r.Method == http.MethodHead {
			if got := r.URL.Query()["attribute"]; len(got) != 1 || got[0] != "hash.md5" {
				t.Errorf("hash attribute query = %v, want [hash.md5]", got)
			}

			sum := md5.Sum(content)
			w.Header().Set("X-Attribute-Hash-Md5", fmt.Sprintf("%x", sum))
		} else {
			rec.record(name)
		}

		// The listing declares a "size" for every file, so each one downloads
		// via the durable chunked path (stageChunkedFile/DownloadBlockChunks),
		// which issues Range GETs — http.ServeContent (mirroring the real
		// data-exporter's sendFile idiom) is required to honor them.
		http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(content))
	})

	srv := httptest.NewServer(mux)

	t.Cleanup(srv.Close)

	return srv
}

// buildMixedResumeFakeClient constructs the fake kube client for the mixed-
// resume-states tree:
//
//	mixed-root (Snapshot)
//	  └─ mixed-vm-snap (VirtualMachineSnapshot, aggregator/intermediate node)
//	       ├─ mixed-disk-done            (VirtualDiskSnapshot, OwnDataRef → block)
//	       ├─ mixed-disk-block-partial   (VirtualDiskSnapshot, OwnDataRef → block)
//	       ├─ mixed-disk-fs-partial      (VirtualDiskSnapshot, OwnDataRef → fs)
//	       ├─ mixed-disk-manifests-only  (VirtualDiskSnapshot, OwnDataRef → block)
//	       └─ mixed-disk-pending         (VirtualDiskSnapshot, OwnDataRef → block)
//
// Every leaf is a non-aggregator with exactly one OwnDataRef, mirroring
// buildE2EFakeClient's disk-block/disk-fs leaves.
func buildMixedResumeFakeClient(t *testing.T) client.Client {
	t.Helper()

	scheme := buildScheme(t)

	root := snapObj{
		apiVersion: storageAPIVersion, kind: "Snapshot",
		namespace: mixedNS, name: mixedRootSnap, uid: "uid-mixed-root",
		sourceRef: namespaceSourceRefMap(mixedNS, "uid-mixed-ns"),
		children:  []map[string]interface{}{childRefMap(e2eVMAPIVersion, e2eVMKind, mixedVMSnap)},
	}.build()

	vmChildren := make([]map[string]interface{}, 0, len(mixedLeafNames))
	for _, name := range mixedLeafNames {
		vmChildren = append(vmChildren, childRefMap(e2eVMAPIVersion, e2eDiskKind, name))
	}

	// mixed-vm-snap is an intermediate node (domain children); it captures no own volume.
	vmSnap := snapObj{
		apiVersion: e2eVMAPIVersion, kind: e2eVMKind,
		namespace: mixedNS, name: mixedVMSnap, uid: "uid-" + mixedVMSnap,
		children: vmChildren,
	}.build()

	objs := []client.Object{root, vmSnap}

	for _, name := range mixedLeafNames {
		// Non-aggregator leaf with exactly one captured volume; no sourceRef → dir base = CR name.
		leafSnap := snapObj{
			apiVersion: e2eVMAPIVersion, kind: e2eDiskKind,
			namespace: mixedNS, name: name, uid: "uid-" + name,
			data: pvcData(mixedNS, "pvc-"+name, "uid-pvc-"+name, "vsc-"+name),
		}.build()

		objs = append(objs, leafSnap)
	}

	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()
}

// TestPipeline_MixedResumeStates_ConcurrentRun exercises the concurrent
// collectNodeTasks/processNode resume path against a tree where sibling
// leaves sit in every observed resume condition simultaneously: done,
// block-partial, fs-partial, manifests-only, and pending, all processed by ONE pipeline.Run
// with cfg.Workers=3. The existing single-state resume tests
// (TestPipeline_BlockResumeAfterMerge, TestPipeline_FSResumeAfterTar,
// TestPipeline_PartialChunkResume) each exercise exactly one resume state at
// a time with Workers=1; none combines mixed states across concurrently
// processed siblings, which is the gap this test closes.
//
// Fixture strategy: run the full tree ONCE to completion, so every leaf
// becomes a done node through the real download+finalize path (a genuinely
// valid checksum/snapshot.yaml, not a hand-rolled one). Then, mimicking a
// crash mid-run, four of the five leaves are rolled back to a specific
// partial/pending state by deleting their finished artifacts and — for the
// two partial cases — re-creating the exact staging layout a real
// interrupted download would have left (same technique as
// TestPipeline_PartialChunkResume for the block chunk dir); the fifth leaf
// (mixed-disk-done) is left untouched. A second pipeline.Run then resumes
// the whole tree concurrently, and the fake OpenExport plus the block/FS test
// servers are instrumented to prove each node resumed correctly rather than
// restarting from zero.
func TestPipeline_MixedResumeStates_ConcurrentRun(t *testing.T) {
	t.Parallel()

	// ── Fixture content ────────────────────────────────────────────────────
	rawBlockDone := bytes.Repeat([]byte("D"), 300)
	rawBlockPartial := bytes.Repeat([]byte("P"), 300)
	rawBlockManifestsOnly := bytes.Repeat([]byte("M"), 300)
	rawBlockPending := bytes.Repeat([]byte("N"), 300)

	fsPartialStaged := fsE2EFile{rel: "one.txt", content: []byte("hello-one-content")}
	fsPartialMissing := fsE2EFile{rel: "two.txt", content: []byte("hello-two-content!!")}
	fsFiles := []fsE2EFile{fsPartialStaged, fsPartialMissing}

	codec, err := compress.New("zstd", 0)
	require.NoError(t, err)

	// ── Instrumentation. The two "tracked" recorders are reset right before
	// run 2 so only its requests are captured for the resume assertions. ────
	openExportCalls := &stringRecorder{}
	blockPartialRanges := &stringRecorder{}
	fsPartialRequests := &stringRecorder{}

	doneSrv := makeBlockServer(t, rawBlockDone)
	defer doneSrv.Close()

	blockPartialSrv := makeTrackedBlockServer(t, rawBlockPartial, blockPartialRanges)
	fsPartialSrv := makeTrackedFSServer(t, fsFiles, fsPartialRequests)

	manifestsOnlySrv := makeBlockServer(t, rawBlockManifestsOnly)
	defer manifestsOnlySrv.Close()

	pendingSrv := makeBlockServer(t, rawBlockPending)
	defer pendingSrv.Close()

	openExport := func(_ context.Context, namespace string, leafRef aggapi.NodeRef, _ string) (*exporter.Export, error) {
		openExportCalls.record(leafRef.Name)

		switch leafRef.Name {
		case mixedDiskDone:
			return exporter.NewExport(namespace, "de-mixed-done", "Block", doneSrv.URL, exporter.NewFetcher(doneSrv.Client())), nil
		case mixedDiskBlockPartial:
			return exporter.NewExport(namespace, "de-mixed-block-partial", "Block", blockPartialSrv.URL, exporter.NewFetcher(blockPartialSrv.Client())), nil
		case mixedDiskFSPartial:
			return exporter.NewExport(namespace, "de-mixed-fs-partial", "Filesystem", fsPartialSrv.URL, exporter.NewFetcher(fsPartialSrv.Client())), nil
		case mixedDiskManifestsOnly:
			return exporter.NewExport(namespace, "de-mixed-manifests-only", "Block", manifestsOnlySrv.URL, exporter.NewFetcher(manifestsOnlySrv.Client())), nil
		case mixedDiskPending:
			return exporter.NewExport(namespace, "de-mixed-pending", "Block", pendingSrv.URL, exporter.NewFetcher(pendingSrv.Client())), nil
		default:
			return nil, fmt.Errorf("mixed-resume: unexpected leaf %q", leafRef.Name)
		}
	}

	c := buildMixedResumeFakeClient(t)
	outputDir := t.TempDir()

	cfg := pipeline.Config{
		Namespace:            mixedNS,
		RootSnapshot:         mixedRootSnap,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 2,
		Compression:          codec,
		KubeClient:           c,
		OpenExport:           openExport,
	}

	// ── Run 1: complete the whole tree normally ─────────────────────────────
	require.NoError(t, runPipeline(context.Background(), cfg))

	vmDir := filepath.Join(outputDir, archive.SnapshotsDirName, archive.NodeDirName(e2eVMKind, mixedVMSnap))

	leafDir := func(name string) string {
		return filepath.Join(vmDir, archive.SnapshotsDirName, archive.NodeDirName(e2eDiskKind, name))
	}

	doneDir := leafDir(mixedDiskDone)
	blockPartialDir := leafDir(mixedDiskBlockPartial)
	fsPartialDir := leafDir(mixedDiskFSPartial)
	manifestsOnlyDir := leafDir(mixedDiskManifestsOnly)
	pendingDir := leafDir(mixedDiskPending)

	for _, d := range []string{doneDir, blockPartialDir, fsPartialDir, manifestsOnlyDir, pendingDir} {
		assertNodeComplete(t, d)
	}

	// ── Roll four of the five leaves back to distinct partial resume states,
	// simulating a crash mid-run. mixed-disk-done is left untouched. ────────

	// mixed-disk-block-partial: drop the merged block file and snapshot.yaml,
	// re-create data.bin.d/ with chunk 0 (the only chunk, at the hardcoded
	// volume.DefaultChunkSize geometry) durably partial: mixedBlockPartialBytes
	// already on disk, the rest missing. Re-stamp the identity marker finalize
	// removed, so the crash residue matches a real interrupted run (marker
	// present, snapshot.yaml absent).
	reseedResumeMarkerFromSnapshotYAML(t, blockPartialDir)
	require.NoError(t, os.Remove(filepath.Join(blockPartialDir, archive.DataBlockName(codec.Ext()))))
	require.NoError(t, os.Remove(filepath.Join(blockPartialDir, archive.SnapshotYAMLName)))

	blockChunkDir := filepath.Join(blockPartialDir, archive.BlockChunksDirName)
	require.NoError(t, os.MkdirAll(blockChunkDir, 0o755))

	require.NoError(t, os.WriteFile(
		filepath.Join(blockChunkDir, archive.ChunkFileName(0, codec.Ext())+".part"),
		rawBlockPartial[:mixedBlockPartialBytes],
		0o644,
	))
	// A durable ".part.offset" sidecar must accompany the ".part" file so
	// partialChunkSize trusts this partial prefix instead of truncating it to
	// zero (see download-resume-part-trusted-prefix).
	require.NoError(t, os.WriteFile(
		filepath.Join(blockChunkDir, archive.ChunkFileName(0, codec.Ext())+".part.offset"),
		[]byte(strconv.FormatInt(mixedBlockPartialBytes, 10)),
		0o644,
	))
	require.NoError(t, archive.WriteChunkMeta(blockChunkDir, archive.ChunkMeta{
		ChunkSize: volume.DefaultChunkSize,
		TotalSize: int64(len(rawBlockPartial)),
	}))

	// mixed-disk-fs-partial: drop data.tar and snapshot.yaml, re-create
	// data.tar.d/ with "one.txt.zst" already staged; "two.txt" is left
	// missing so only it must be re-fetched on resume. Re-stamp the marker
	// finalize removed (crash residue).
	reseedResumeMarkerFromSnapshotYAML(t, fsPartialDir)
	require.NoError(t, os.Remove(filepath.Join(fsPartialDir, archive.FsTarName)))
	require.NoError(t, os.Remove(filepath.Join(fsPartialDir, archive.SnapshotYAMLName)))

	fsStagingDir := filepath.Join(fsPartialDir, archive.FsTarStagingDirName)
	require.NoError(t, os.MkdirAll(fsStagingDir, 0o755))

	stagedFrame, err := codec.EncodeFrame(fsPartialStaged.content)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(fsStagingDir, fsPartialStaged.rel+codec.Ext()),
		stagedFrame,
		0o644,
	))

	// mixed-disk-manifests-only: drop the merged block file and
	// snapshot.yaml; manifests/ (created by run 1) is left in place with no
	// volume artifact and no staging dir of any kind. Re-stamp the marker
	// finalize removed (crash residue) so the manifests-only dir is not treated
	// as a marker-less foreign dir.
	reseedResumeMarkerFromSnapshotYAML(t, manifestsOnlyDir)
	require.NoError(t, os.Remove(filepath.Join(manifestsOnlyDir, archive.DataBlockName(codec.Ext()))))
	require.NoError(t, os.Remove(filepath.Join(manifestsOnlyDir, archive.SnapshotYAMLName)))

	// mixed-disk-pending: remove the whole node directory so it starts from
	// nothing on the second run.
	require.NoError(t, os.RemoveAll(pendingDir))

	// The aggregator and the root committed to the five leaf envelopes just
	// rolled back, and bottom-up publication could not have written either
	// ancestor envelope while a descendant's was still missing. Roll them back
	// too, so the fixture is a state a crash can actually produce rather than a
	// stale parent commitment no publication transaction authorizes.
	reseedResumeMarkerAndDropEnvelopesUpTo(t, outputDir, vmDir)

	// Isolate run 2's instrumentation: everything captured so far belongs to
	// run 1's full download and must not pollute the resume assertions below.
	openExportCalls.reset()
	blockPartialRanges.reset()
	fsPartialRequests.reset()

	var buf bytes.Buffer

	sink := progress.New(&buf, false, progress.WithInterval(time.Hour))

	cfg.Workers = 3
	cfg.Progress = sink

	// ── Run 2: resume the whole tree concurrently from the mixed states ─────
	require.NoError(t, runPipeline(context.Background(), cfg))

	sink.Wait()

	// (a) Every node — root, the intermediate aggregator, and every leaf —
	// must be complete and pass VerifyNode; every leaf's decoded content must
	// match what its server actually holds, proving a correct download
	// occurred wherever one was needed.
	assertNodeComplete(t, outputDir)
	assertNodeComplete(t, vmDir)

	for _, d := range []string{doneDir, blockPartialDir, fsPartialDir, manifestsOnlyDir, pendingDir} {
		assertNodeComplete(t, d)
	}

	// The resumed run must also end marker-free once every node finalizes.
	assertNoIdentityMarkers(t, outputDir)

	require.Equal(t, rawBlockDone, e2eDecodeZstdFile(t, filepath.Join(doneDir, archive.DataBlockName(codec.Ext()))),
		"mixed-disk-done data must be untouched by run 2")
	require.Equal(t, rawBlockPartial, e2eDecodeZstdFile(t, filepath.Join(blockPartialDir, archive.DataBlockName(codec.Ext()))),
		"mixed-disk-block-partial must decode to the original bytes after resume")
	require.Equal(t, rawBlockManifestsOnly, e2eDecodeZstdFile(t, filepath.Join(manifestsOnlyDir, archive.DataBlockName(codec.Ext()))),
		"mixed-disk-manifests-only must download correctly")
	require.Equal(t, rawBlockPending, e2eDecodeZstdFile(t, filepath.Join(pendingDir, archive.DataBlockName(codec.Ext()))),
		"mixed-disk-pending must download correctly from scratch")

	fsTarPath := filepath.Join(fsPartialDir, archive.FsTarName)
	for _, f := range fsFiles {
		compressed, tarErr := readTarEntry(t, fsTarPath, f.rel+codec.Ext())
		require.NoError(t, tarErr, "tar must have entry for %s", f.rel)
		require.Equal(t, f.content, e2eDecodeZstdBytes(t, compressed), "fs file %s content mismatch after resume", f.rel)
	}

	// (b) The already-Done leaf must never be handed to OpenExport again.
	calls := openExportCalls.snapshot()
	require.NotContains(t, calls, mixedDiskDone,
		"OpenExport must not be called for the already-complete leaf")

	for _, name := range []string{mixedDiskBlockPartial, mixedDiskFSPartial, mixedDiskManifestsOnly, mixedDiskPending} {
		require.Contains(t, calls, name,
			"OpenExport must be called for %s to fetch its missing data", name)
	}

	// (c) Partial nodes resumed from their pre-seeded progress instead of
	// restarting from zero.
	blockRanges := blockPartialRanges.snapshot()

	for _, hdr := range blockRanges {
		require.NotEqual(t, "bytes=0-299", hdr,
			"the durable partial prefix was pre-seeded and must not be re-fetched from byte zero on resume")
	}

	require.Contains(t, blockRanges, fmt.Sprintf("bytes=%d-%d", mixedBlockPartialBytes, int64(len(rawBlockPartial))-1),
		"only the still-missing suffix must be fetched on resume")

	fsRequests := fsPartialRequests.snapshot()
	require.NotContains(t, fsRequests, fsPartialStaged.rel,
		"the pre-staged fs file must not be re-fetched on resume")
	require.Contains(t, fsRequests, fsPartialMissing.rel,
		"the missing fs file must be fetched on resume")

	// (d) The aggregate volume counter equals the number of volume leaves in
	// the run (5), and by the end of run 2 every one of them has settled.
	require.Contains(t, buf.String(), fmt.Sprintf("(%d/%d volumes)", len(mixedLeafNames), len(mixedLeafNames)),
		"aggregate volume counter must reach N/M == total volume leaves")
}

// seedLeftoverBlockChunkDir creates a populated flat block chunk staging
// directory (data.bin.d/) inside nodeDir, simulating the residue of a crash in
// volume.MergeBlockChunks between committing the verified data.bin* file and
// os.RemoveAll'ing the chunk dir. It returns the chunk dir path. The chunk
// contents are never read by the already-merged skip branch, so any bytes will
// do.
func seedLeftoverBlockChunkDir(t *testing.T, nodeDir string) string {
	t.Helper()

	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	require.NoError(t, os.MkdirAll(chunkDir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(chunkDir, archive.ChunkFileName(0, ".zst")),
		[]byte("stale-compressed-chunk-bytes"),
		0o644,
	))

	return chunkDir
}

// TestPipeline_BlockAlreadyMerged_OwnDataRef_RemovesLeftoverChunkDir covers the
// downloadOwnDataRefs already-merged skip branch: a node dir holding both a
// verified, merged data.bin.zst and a leftover chunk dir (the MergeBlockChunks
// verify->commit->RemoveAll crash window) must resume to Done with the chunk dir
// removed, so the compressed copy of the volume cannot leak forever. The
// no-leftover row pins that a normal already-merged node (no chunk dir) is
// unchanged.
func TestPipeline_BlockAlreadyMerged_OwnDataRef_RemovesLeftoverChunkDir(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		seedChunkDir bool
	}{
		{name: "leftover chunk dir removed", seedChunkDir: true},
		{name: "no chunk dir unchanged", seedChunkDir: false},
	}

	for _, tc := range cases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := buildFakeClient(t)
			outputDir := t.TempDir()

			// disk-snap is a non-aggregator with one OwnDataRef, so it flows through
			// downloadOwnDataRefs.
			diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
				archive.NodeDirName(childKind, diskSnapName))
			require.NoError(t, os.MkdirAll(filepath.Join(diskSnapDir, archive.ManifestsDirName), 0o755))
			seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())
			require.NoError(t, os.WriteFile(
				filepath.Join(diskSnapDir, archive.DataBlockName(".zst")),
				[]byte("pre-merged-block-data"),
				0o644,
			))

			chunkDir := filepath.Join(diskSnapDir, archive.BlockChunksDirName)
			if tc.seedChunkDir {
				seedLeftoverBlockChunkDir(t, diskSnapDir)
			}

			cfg := pipeline.Config{
				Namespace:    testNS,
				RootSnapshot: rootSnapshot,
				OutputDir:    outputDir,
				Workers:      1,
				KubeClient:   c,
				OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
					t.Error("OpenExport must not be called when data.bin.zst already exists")

					return nil, errors.New("unexpected OpenExport call")
				},
			}

			require.NoError(t, runPipeline(context.Background(), cfg))

			assertNodeComplete(t, diskSnapDir)

			_, statErr := os.Stat(chunkDir)
			require.True(t, os.IsNotExist(statErr),
				"the block chunk dir must not exist after an already-merged resume")
		})
	}
}

// TestPipeline_BlockAlreadyMerged_VolumeNode_RemovesLeftoverChunkDir covers the
// symmetric processVolumeNode (Binding leaf) already-merged skip branch. The
// partial state is produced by running the pipeline once, then re-stamping the
// identity marker finalize removed, re-creating a leftover chunk dir next to the
// merged file, and deleting snapshot.yaml — exactly the
// verify->commit->RemoveAll crash residue (marker present, snapshot.yaml absent).
func TestPipeline_BlockAlreadyMerged_VolumeNode_RemovesLeftoverChunkDir(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("A"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildOrphanLeafFakeClient(t)
	outputDir := t.TempDir()

	firstCfg := pipeline.Config{
		Namespace:            e2eNS,
		RootSnapshot:         e2eAggRootSnap,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			return exporter.NewExport(namespace, "de-agg-leaf", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), firstCfg))

	// The orphan leaf is a Binding node → processVolumeNode.
	leafDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(e2eDiskKind, "agg-snap"),
		archive.SnapshotsDirName, archive.NodeDirName("VolumeSnapshot", "pvc-agg"))
	assertNodeComplete(t, leafDir)

	chunkDir := seedLeftoverBlockChunkDir(t, leafDir)
	reseedResumeMarkerAndDropEnvelopesUpTo(t, outputDir, leafDir)

	secondCfg := firstCfg
	secondCfg.OpenExport = func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
		t.Error("OpenExport must not be called when the block volume is already merged")

		return nil, errors.New("unexpected OpenExport call")
	}

	require.NoError(t, runPipeline(context.Background(), secondCfg))

	assertNodeComplete(t, leafDir)

	_, statErr := os.Stat(chunkDir)
	require.True(t, os.IsNotExist(statErr),
		"the block chunk dir must be removed on the processVolumeNode already-merged skip path")
}

// TestPipeline_BlockChunkDirWithoutMergedFile_DownloadsNormally pins that the
// cleanup is confined to the already-merged branch: a chunk dir and stale
// AtomicWriter .tmp present WITHOUT an exact final data.bin[.<ext>] file are a
// normal interrupted download, so the skip branch must not fire and the volume
// must download normally.
func TestPipeline_BlockChunkDirWithoutMergedFile_DownloadsNormally(t *testing.T) {
	t.Parallel()

	rawBlock := bytes.Repeat([]byte("D"), 600)
	srv := makeBlockServer(t, rawBlock)

	defer srv.Close()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	require.NoError(t, os.MkdirAll(filepath.Join(diskSnapDir, archive.ManifestsDirName), 0o755))
	seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())
	// An empty chunk dir and stale unpublished temp with NO exact final payload:
	// the resume sweep removes the .tmp, ClassifyBlockPayload reports not-found,
	// the already-merged branch is skipped, and download proceeds.
	require.NoError(t, os.MkdirAll(filepath.Join(diskSnapDir, archive.BlockChunksDirName), 0o755))
	tmpPath := filepath.Join(diskSnapDir, archive.DataBlockName(".zst")+".tmp")
	require.NoError(t, os.WriteFile(tmpPath, []byte("unverified merged bytes"), 0o644))

	var openExportCalled atomic.Bool

	cfg := pipeline.Config{
		Namespace:            testNS,
		RootSnapshot:         rootSnapshot,
		OutputDir:            outputDir,
		Workers:              1,
		PerVolumeConcurrency: 1,
		KubeClient:           c,
		OpenExport: func(_ context.Context, namespace string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			openExportCalled.Store(true)

			return exporter.NewExport(namespace, "de-normal", "Block", srv.URL, exporter.NewFetcher(srv.Client())), nil
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg))

	require.True(t, openExportCalled.Load(),
		"an unpublished .tmp must not make the pipeline skip the volume download")

	assertNodeComplete(t, diskSnapDir)

	_, statErr := os.Stat(tmpPath)
	require.True(t, errors.Is(statErr, os.ErrNotExist), "unpublished .tmp must not survive resume")

	compressed, err := os.ReadFile(filepath.Join(diskSnapDir, archive.DataBlockName(".zst")))
	require.NoError(t, err)
	require.Equal(t, rawBlock, decodeZstdBlock(t, compressed),
		"the normally-downloaded block must decode to the original bytes")
}

// TestPipeline_BlockAlreadyMerged_RemoveAllFailure_StillCompletes pins that a
// best-effort chunk-dir cleanup failure is logged as a WARN and never fails an
// otherwise complete node (code-style §5): the download itself is already done
// (the merged file is durable). The chunk dir is made 0o555 (readable so the
// resume scan's WalkDir still succeeds, but not writable so os.RemoveAll cannot
// unlink its entry). Permission bits are not enforced for root, so skip there.
func TestPipeline_BlockAlreadyMerged_RemoveAllFailure_StillCompletes(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("permission bits are not enforced for root; cannot force os.RemoveAll to fail")
	}

	t.Parallel()

	c := buildFakeClient(t)
	outputDir := t.TempDir()

	diskSnapDir := filepath.Join(outputDir, archive.SnapshotsDirName,
		archive.NodeDirName(childKind, diskSnapName))
	require.NoError(t, os.MkdirAll(filepath.Join(diskSnapDir, archive.ManifestsDirName), 0o755))
	seedResumeIdentityMarker(t, diskSnapDir, diskSnapMarkerIdentity())
	require.NoError(t, os.WriteFile(
		filepath.Join(diskSnapDir, archive.DataBlockName(".zst")),
		[]byte("pre-merged-block-data"),
		0o644,
	))

	chunkDir := seedLeftoverBlockChunkDir(t, diskSnapDir)
	require.NoError(t, os.Chmod(chunkDir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(chunkDir, 0o755) })

	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	cfg := pipeline.Config{
		Namespace:    testNS,
		RootSnapshot: rootSnapshot,
		OutputDir:    outputDir,
		Workers:      1,
		KubeClient:   c,
		Log:          logger,
		OpenExport: func(_ context.Context, _ string, _ aggapi.NodeRef, _ string) (*exporter.Export, error) {
			t.Error("OpenExport must not be called when data.bin.zst already exists")

			return nil, errors.New("unexpected OpenExport call")
		},
	}

	require.NoError(t, runPipeline(context.Background(), cfg),
		"a failed best-effort chunk-dir cleanup must not fail an otherwise complete node")

	assertNodeComplete(t, diskSnapDir)

	require.Contains(t, buf.String(), "failed to remove leftover block chunk dir after merge",
		"a RemoveAll failure must be logged as a WARN")

	_, statErr := os.Stat(chunkDir)
	require.NoError(t, statErr, "the unremovable chunk dir must still be present")
}
