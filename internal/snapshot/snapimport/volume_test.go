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

package snapimport

import (
	gotar "archive/tar"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// recordingDoer captures the requests putBlock/postFinished send and returns canned responses.
type recordingDoer struct {
	headers []http.Header
	methods []string
	// resumeOffset is what the importer reports as already written via the HEAD probe.
	resumeOffset int64
}

func (d *recordingDoer) HTTPDo(req *http.Request) (*http.Response, error) {
	d.headers = append(d.headers, req.Header.Clone())
	d.methods = append(d.methods, req.Method)

	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}

	respHeader := http.Header{}

	// The block importer answers a resume probe (HEAD) with the current write offset.
	if req.Method == http.MethodHead {
		respHeader.Set("X-Next-Offset", strconv.FormatInt(d.resumeOffset, 10))

		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     respHeader,
		}, nil
	}

	return &http.Response{
		StatusCode: http.StatusCreated,
		Status:     "201 Created",
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     respHeader,
	}, nil
}

func TestPutBlock_SendsRequiredHeaders(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.raw")

	payload := []byte("rawblockbytes")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	doer := &recordingDoer{}

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, int64(len(payload)), nil); err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	putIdx := -1
	for i, m := range doer.methods {
		if m == http.MethodPut {
			putIdx = i

			break
		}
	}

	if putIdx < 0 {
		t.Fatalf("expected a PUT request, methods=%v", doer.methods)
	}

	// The SVDM importer's CheckRequiredHeaders rejects PUTs missing any of these.
	required := []string{"X-Content-Length", "X-Offset", "X-Attribute-Permissions", "X-Attribute-Uid", "X-Attribute-Gid"}
	for _, h := range required {
		if doer.headers[putIdx].Get(h) == "" {
			t.Errorf("missing required header %q on block PUT", h)
		}
	}
}

func TestPutBlock_ResumesFromServerOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.raw")

	payload := []byte("0123456789abc") // 13 bytes
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	doer := &recordingDoer{resumeOffset: 5}

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, int64(len(payload)), nil); err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	putIdx := -1
	for i, m := range doer.methods {
		if m == http.MethodPut {
			putIdx = i

			break
		}
	}

	if putIdx < 0 {
		t.Fatalf("expected a PUT request, methods=%v", doer.methods)
	}

	// The PUT must resume from the server-reported offset, not restart at 0.
	if got := doer.headers[putIdx].Get("X-Offset"); got != "5" {
		t.Errorf("X-Offset = %q, want 5 (resume from server offset)", got)
	}
}

func TestPutBlock_RejectsOversizeServerOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.raw")

	payload := []byte("0123456789abc") // 13 bytes
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	// Importer reports more bytes than the archive has: a mismatched reused DataImport.
	doer := &recordingDoer{resumeOffset: 20}

	err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, int64(len(payload)), nil)
	if err == nil {
		t.Fatal("expected error for oversize server offset, got nil")
	}

	for _, m := range doer.methods {
		if m == http.MethodPut {
			t.Error("no PUT should be issued when the server offset exceeds the archive size")
		}
	}
}

func completedDataImportObj(namespace, name string) *unstructured.Unstructured {
	obj := dataImportObj(namespace, name, false)
	_ = unstructured.SetNestedSlice(obj.Object, readyConditions(conditionCompleted), "status", "conditions")
	_ = unstructured.SetNestedMap(obj.Object, map[string]interface{}{"name": "vsc-1"}, "status", "dataArtifactRef")

	return obj
}

func TestUploadVolumeData_SkipsCompleted(t *testing.T) {
	// DataFile is set so the block-data preflight passes; the file is never opened because
	// the completed-import short-circuit returns before any upload.
	leaf := PlannedNode{
		APIVersion: "snapshot.storage.k8s.io/v1",
		Kind:       "VolumeSnapshot",
		Name:       "pvc-1",
		DataFile:   filepath.Join(t.TempDir(), "data.bin"),
	}

	dyn := newFakeDataImportDyn(completedDataImportObj(targetNS, "pvc-1"))
	imp := newTestVolumeImporter(dyn) // sc is nil: reaching the HTTP upload would panic.

	if err := imp.UploadVolumeData(context.Background(), leaf, "pvc-1", targetNS, nil); err != nil {
		t.Fatalf("UploadVolumeData on an already-completed import must be a no-op: %v", err)
	}
}

func newFakeDataImportDyn(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	gvrToListKind := map[schema.GroupVersionResource]string{
		dataImportGVR: "DataImportList",
	}

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
}

func dataImportObj(namespace, name string, expired bool) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": dataImportGVR.GroupVersion().String(),
		"kind":       dataImportKind,
		"metadata":   map[string]interface{}{"namespace": namespace, "name": name},
	}}

	if expired {
		_ = unstructured.SetNestedSlice(obj.Object, readyConditions(conditionExpired), "status", "conditions")
	}

	return obj
}

func countDataImportActions(dyn *dynamicfake.FakeDynamicClient, verb string) int {
	n := 0
	for _, a := range dyn.Actions() {
		if a.GetVerb() == verb && a.GetResource() == dataImportGVR {
			n++
		}
	}

	return n
}

func newTestVolumeImporter(dyn *dynamicfake.FakeDynamicClient) *clusterVolumeImporter {
	return &clusterVolumeImporter{
		dyn:  dyn,
		ttl:  "1h",
		poll: time.Millisecond,
		wait: 2 * time.Second,
		log:  discardLogger(),
	}
}

func TestEnsureDataImport_ReusesHealthy(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")

	dyn := newFakeDataImportDyn(dataImportObj(targetNS, "pvc-1", false))
	imp := newTestVolumeImporter(dyn)

	name, err := imp.EnsureDataImport(context.Background(), leaf, targetNS)
	if err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	if name != "pvc-1" {
		t.Errorf("name = %q, want pvc-1", name)
	}

	if c := countDataImportActions(dyn, "create"); c != 0 {
		t.Errorf("a healthy DataImport must be reused, not recreated (creates=%d)", c)
	}

	if d := countDataImportActions(dyn, "delete"); d != 0 {
		t.Errorf("a healthy DataImport must not be deleted (deletes=%d)", d)
	}
}

// volumeSnapshotLeaf builds a CSI VolumeSnapshot data leaf carrying the captured volume
// metadata that EnsureDataImport echoes into the Mode A DataImport spec.
func volumeSnapshotLeaf(name string) PlannedNode {
	return PlannedNode{
		APIVersion:       "snapshot.storage.k8s.io/v1",
		Kind:             "VolumeSnapshot",
		Name:             name,
		StorageClassName: "sc-1",
		Size:             "10Gi",
		VolumeMode:       volumeModeFilesystem,
	}
}

func TestEnsureDataImport_BuildsModeASpec(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")

	dyn := newFakeDataImportDyn()
	imp := newTestVolumeImporter(dyn)

	if _, err := imp.EnsureDataImport(context.Background(), leaf, targetNS); err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	got, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("created DataImport not found: %v", err)
	}

	if v, _, _ := unstructured.NestedString(got.Object, "spec", "storageClassName"); v != "sc-1" {
		t.Errorf("spec.storageClassName = %q, want sc-1", v)
	}

	if v, _, _ := unstructured.NestedString(got.Object, "spec", "size"); v != "10Gi" {
		t.Errorf("spec.size = %q, want 10Gi", v)
	}

	if v, _, _ := unstructured.NestedString(got.Object, "spec", "volumeMode"); v != volumeModeFilesystem {
		t.Errorf("spec.volumeMode = %q, want %q", v, volumeModeFilesystem)
	}

	// The legacy dataArtifactType field must be gone — the controller infers the artifact.
	if _, found, _ := unstructured.NestedString(got.Object, "spec", "dataArtifactType"); found {
		t.Error("spec.dataArtifactType must not be set (removed in the kind-targetRef rework)")
	}

	group, _, _ := unstructured.NestedString(got.Object, "spec", "targetRef", "group")
	kind, _, _ := unstructured.NestedString(got.Object, "spec", "targetRef", "kind")
	refName, _, _ := unstructured.NestedString(got.Object, "spec", "targetRef", "name")

	if group != "snapshot.storage.k8s.io" || kind != "VolumeSnapshot" || refName != "pvc-1" {
		t.Errorf("targetRef = {group:%q, kind:%q, name:%q}, want {snapshot.storage.k8s.io, VolumeSnapshot, pvc-1}", group, kind, refName)
	}

	// targetRef must not carry the removed plural "resource" key.
	if _, found, _ := unstructured.NestedString(got.Object, "spec", "targetRef", "resource"); found {
		t.Error("spec.targetRef.resource must not be set (renamed to kind)")
	}
}

func TestEnsureDataImport_RejectsMissingVolumeMetadata(t *testing.T) {
	// A data leaf without storageClassName/size means a malformed archive; EnsureDataImport
	// must fail fast instead of creating a CEL-rejected Mode A DataImport.
	leaf := PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "pvc-1"}

	dyn := newFakeDataImportDyn()
	imp := newTestVolumeImporter(dyn)

	if _, err := imp.EnsureDataImport(context.Background(), leaf, targetNS); err == nil {
		t.Fatal("expected error for missing volume metadata, got nil")
	}

	if c := countDataImportActions(dyn, "create"); c != 0 {
		t.Errorf("no DataImport must be created when volume metadata is missing (creates=%d)", c)
	}
}

func TestEnsureDataImport_AlignsTTLOnReuse(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")

	existing := dataImportObj(targetNS, "pvc-1", false)
	_ = unstructured.SetNestedField(existing.Object, "2m", "spec", "ttl")

	dyn := newFakeDataImportDyn(existing)
	imp := newTestVolumeImporter(dyn) // ttl: "1h"

	if _, err := imp.EnsureDataImport(context.Background(), leaf, targetNS); err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	if c := countDataImportActions(dyn, "create"); c != 0 {
		t.Errorf("a healthy DataImport must be reused, not recreated (creates=%d)", c)
	}

	got, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get DataImport: %v", err)
	}

	ttl, _, _ := unstructured.NestedString(got.Object, "spec", "ttl")
	if ttl != "1h" {
		t.Errorf("spec.ttl = %q, want 1h (aligned to the current run's --ttl)", ttl)
	}
}

func TestEnsureDataImport_RecreatesExpired(t *testing.T) {
	leaf := volumeSnapshotLeaf("pvc-1")

	dyn := newFakeDataImportDyn(dataImportObj(targetNS, "pvc-1", true))
	imp := newTestVolumeImporter(dyn)

	name, err := imp.EnsureDataImport(context.Background(), leaf, targetNS)
	if err != nil {
		t.Fatalf("EnsureDataImport: %v", err)
	}

	if name != "pvc-1" {
		t.Errorf("name = %q, want pvc-1", name)
	}

	if d := countDataImportActions(dyn, "delete"); d != 1 {
		t.Errorf("expired DataImport must be deleted (deletes=%d)", d)
	}

	if c := countDataImportActions(dyn, "create"); c != 1 {
		t.Errorf("a fresh DataImport must be created after expiry (creates=%d)", c)
	}

	got, err := dyn.Resource(dataImportGVR).Namespace(targetNS).Get(context.Background(), "pvc-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("DataImport not present after recreate: %v", err)
	}

	if conditionTrue(got, conditionExpired) {
		t.Errorf("recreated DataImport must not carry the Expired condition")
	}
}

// TestSendVolumeData_FSLeaf_UsesTarFile is the regression test for the wiring bug
// introduced in import-fs-tar-source: UploadVolumeData's Filesystem branch was calling
// importFSFromTar with leaf.DataFile (the block-data glob result, always empty for FS
// leaves), instead of leaf.TarFile. With an empty DataFile, os.Open("") fails immediately.
// This test calls sendVolumeData directly with FilesystemData=true, DataFile="" and a real
// TarFile path; it would fail before the fix (os.Open("")) and must pass after.
func TestSendVolumeData_FSLeaf_UsesTarFile(t *testing.T) {
	// Build a real data.tar with one zstd-compressed file entry.
	content := []byte("hello filesystem import regression")
	compressed := zstdCompress(t, content)

	var tarBuf bytes.Buffer

	tw := gotar.NewWriter(&tarBuf)
	addTarEntry(t, tw, "file.txt.zst", compressed, 0o644, 1000, 2000, time.Now())

	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}

	dir := t.TempDir()
	tarPath := filepath.Join(dir, archive.FsTarName)

	if err := os.WriteFile(tarPath, tarBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write data.tar: %v", err)
	}

	// PlannedNode carries FilesystemData=true and an empty DataFile (the wiring bug target).
	// Before the fix, sendVolumeData called importFSFromTar with leaf.DataFile (""),
	// causing os.Open("") to fail.
	leaf := PlannedNode{
		APIVersion:     "snapshot.storage.k8s.io/v1",
		Kind:           "VolumeSnapshot",
		Name:           "pvc-1",
		FilesystemData: true,
		DataFile:       "",
		TarFile:        tarPath,
	}

	var mu sync.Mutex

	var putPaths []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)

		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			mu.Lock()
			putPaths = append(putPaths, strings.TrimPrefix(r.URL.Path, "/api/v1/files/"))
			mu.Unlock()

			w.WriteHeader(http.StatusCreated)
		case http.MethodPost:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	imp := &clusterVolumeImporter{log: discardLogger()}

	if err := imp.sendVolumeData(context.Background(), plainHTTPDoer{}, srv.URL, volumeModeFilesystem, leaf, targetNS, "pvc-1", nil); err != nil {
		t.Fatalf("sendVolumeData with FS leaf and valid TarFile: %v", err)
	}

	mu.Lock()
	n := len(putPaths)
	mu.Unlock()

	// At least one PUT must have reached the server (the decompressed "file.txt" entry).
	if n == 0 {
		t.Error("expected at least one PUT (FS entry uploaded via TarFile), got none")
	}
}
