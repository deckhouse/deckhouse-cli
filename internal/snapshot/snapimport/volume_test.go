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
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
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

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, int64(len(payload))); err != nil {
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

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, int64(len(payload))); err != nil {
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

	err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, int64(len(payload)))
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

	if err := imp.UploadVolumeData(context.Background(), leaf, "pvc-1", targetNS); err != nil {
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
	leaf := PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "pvc-1"}

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

func TestEnsureDataImport_AlignsTTLOnReuse(t *testing.T) {
	leaf := PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "pvc-1"}

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
	leaf := PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "pvc-1"}

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
