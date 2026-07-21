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
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
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

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), nil); err != nil {
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

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), nil); err != nil {
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

// TestPutBlock_ReportsProgressBytes verifies that putBlock calls onProgress with the number
// of bytes written per PUT, summing to the total uploaded size over the whole transfer.
func TestPutBlock_ReportsProgressBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.raw")

	payload := []byte("rawblockdata0123456789") // 22 bytes
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write data: %v", err)
	}

	var reported int

	doer := &recordingDoer{}

	if err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), func(n int) { reported += n }); err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	if reported != len(payload) {
		t.Errorf("reported bytes = %d, want %d (total bytes uploaded)", reported, len(payload))
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

	err := putBlock(context.Background(), doer, "https://importer.local/api/v1/block", path, "", int64(len(payload)), discardLogger(), nil)
	if err == nil {
		t.Fatal("expected error for oversize server offset, got nil")
	}

	for _, m := range doer.methods {
		if m == http.MethodPut {
			t.Error("no PUT should be issued when the server offset exceeds the archive size")
		}
	}
}

// fakeBlockImporter is a minimal httptest.Server handler mimicking the storage-volume-
// data-manager import_block handler's HEAD/PUT/POST contract closely enough to exercise
// putBlock end to end: HEAD reports the current durable write offset, PUT appends the
// request body at that offset (rejecting an offset mismatch, mirroring the real
// handler's 409) and reports the new offset, and POST .../finished is a no-op success.
// It deliberately has no on-disk device and no independent Content-Length bound check —
// net/http's own enforcement of req.ContentLength (see transfer.go) and putBlockCompressed's
// own post-loop safety-net read are what this file's size-mismatch tests exercise.
type fakeBlockImporter struct {
	mu      sync.Mutex
	written []byte
}

// seed pre-populates the durable buffer, simulating bytes a previous, interrupted run
// already delivered before a fresh putBlock call resumes from that offset.
func (f *fakeBlockImporter) seed(prefix []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.written = append([]byte(nil), prefix...)
}

// received returns a copy of every byte durably written so far.
func (f *fakeBlockImporter) received() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]byte(nil), f.written...)
}

func (f *fakeBlockImporter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodHead:
		f.mu.Lock()
		n := int64(len(f.written))
		f.mu.Unlock()

		w.Header().Set("X-Next-Offset", strconv.FormatInt(n, 10))
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		offset, _ := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
		expectedTotal, _ := strconv.ParseInt(r.Header.Get("X-Content-Length"), 10, 64)

		f.mu.Lock()
		cur := int64(len(f.written))
		f.mu.Unlock()

		if offset != cur {
			http.Error(w, "offset mismatch", http.StatusConflict)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		f.mu.Lock()
		f.written = append(f.written, body...)
		next := int64(len(f.written))
		f.mu.Unlock()

		w.Header().Set("X-Next-Offset", strconv.FormatInt(next, 10))

		if next == expectedTotal {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	case http.MethodPost:
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// blockCodecCases enumerates the codecs putBlock must round-trip, pairing compress.New's
// codec name with the file extension putBlock/compress.NewReader dispatch on.
var blockCodecCases = []struct {
	codec string
	ext   string
}{
	{"zstd", ".zst"},
	{"gzip", ".gz"},
	{"lz4", ".lz4"},
	{"none", ""},
}

// writeEncodedBlockFile writes payload to path encoded as two concatenated codec frames
// (mirroring multi-chunk block-download output), or verbatim for "none" (a raw block
// file carries no framing).
func writeEncodedBlockFile(t *testing.T, path, codecName string, payload []byte) {
	t.Helper()

	if codecName == "none" {
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatalf("write raw block file: %v", err)
		}

		return
	}

	codec, err := compress.New(codecName, 0)
	if err != nil {
		t.Fatalf("compress.New(%q): %v", codecName, err)
	}

	var buf bytes.Buffer

	mid := len(payload) / 2
	for _, part := range [][]byte{payload[:mid], payload[mid:]} {
		frame, encErr := codec.EncodeFrame(part)
		if encErr != nil {
			t.Fatalf("EncodeFrame: %v", encErr)
		}

		buf.Write(frame)
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write encoded block file: %v", err)
	}
}

// TestPutBlock_FreshUploadRoundTrip verifies a from-scratch upload (HEAD offset 0) for
// every codec putBlock supports, including the raw ("none") fast path, against a real
// net/http round trip (httptest.Server), asserting the server receives exactly the
// original plaintext and onProgress sums to the full transferred size.
func TestPutBlock_FreshUploadRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("block-device-bytes-"), 4000)

	for _, tc := range blockCodecCases {
		t.Run(tc.codec, func(t *testing.T) {
			dir := t.TempDir()
			dataFile := filepath.Join(dir, "data.bin"+tc.ext)

			writeEncodedBlockFile(t, dataFile, tc.codec, payload)

			imp := &fakeBlockImporter{}
			srv := httptest.NewServer(imp)
			defer srv.Close()

			var reported int

			if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, int64(len(payload)), discardLogger(), func(n int) { reported += n }); err != nil {
				t.Fatalf("putBlock: %v", err)
			}

			if got := imp.received(); !bytes.Equal(got, payload) {
				t.Fatalf("server received %d bytes not matching the original %d-byte payload", len(got), len(payload))
			}

			if reported != len(payload) {
				t.Errorf("reported progress bytes = %d, want %d (total payload size)", reported, len(payload))
			}
		})
	}
}

// TestPutBlockCompressed_ResumesViaFastForward verifies that resuming from a nonzero
// HEAD-reported offset re-derives the correct decompressed position via
// io.CopyN(io.Discard, ...) fast-forward and uploads only the remainder, so that the
// pre-seeded prefix (standing in for a prior, interrupted run's durable bytes) plus this
// call's PUT concatenate back to the exact original plaintext. seedLen is deliberately
// not aligned to either encoded frame's boundary, proving the mechanism does not depend
// on frame geometry.
func TestPutBlockCompressed_ResumesViaFastForward(t *testing.T) {
	payload := bytes.Repeat([]byte("resume-me-please-"), 3000)
	seedLen := len(payload)/2 + 7

	for _, tc := range blockCodecCases {
		if tc.ext == "" {
			continue // the raw path's resume behavior is covered by TestPutBlock_ResumesFromServerOffset.
		}

		t.Run(tc.codec, func(t *testing.T) {
			dir := t.TempDir()
			dataFile := filepath.Join(dir, "data.bin"+tc.ext)

			writeEncodedBlockFile(t, dataFile, tc.codec, payload)

			imp := &fakeBlockImporter{}
			imp.seed(payload[:seedLen])

			srv := httptest.NewServer(imp)
			defer srv.Close()

			if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, tc.ext, int64(len(payload)), discardLogger(), nil); err != nil {
				t.Fatalf("putBlock (resume): %v", err)
			}

			if got := imp.received(); !bytes.Equal(got, payload) {
				t.Fatalf("resumed upload produced %d bytes not matching the original %d-byte payload", len(got), len(payload))
			}
		})
	}
}

// TestPutBlock_SkipsDecodeWhenOffsetEqualsTotal verifies that an offset already equal to
// totalSize (a fully-transferred-but-not-yet-finalized resume) never opens dataFile or
// builds a decode reader: pointing dataFile at a path that does not exist would fail
// immediately if putBlock tried to open it, so a nil return proves the short-circuit
// fired before any file I/O.
func TestPutBlock_SkipsDecodeWhenOffsetEqualsTotal(t *testing.T) {
	imp := &fakeBlockImporter{}
	imp.seed(bytes.Repeat([]byte("x"), 100))

	srv := httptest.NewServer(imp)
	defer srv.Close()

	missing := filepath.Join(t.TempDir(), "data.bin.zst")

	if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, missing, ".zst", 100, discardLogger(), nil); err != nil {
		t.Fatalf("putBlock with offset==totalSize must skip decoding entirely, got error: %v", err)
	}
}

// TestPutBlockCompressed_TooSmallDeclaredSizeErrors verifies the under-declared-size
// safety net: when totalSize is smaller than the archive's actual decompressed content,
// putBlockCompressed's post-loop probe read must catch the leftover bytes and fail
// loudly instead of silently truncating a successful-looking upload.
func TestPutBlockCompressed_TooSmallDeclaredSizeErrors(t *testing.T) {
	payload := bytes.Repeat([]byte("extra-bytes-beyond-declared-total-"), 200)

	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data.bin.zst")
	writeEncodedBlockFile(t, dataFile, "zstd", payload)

	srv := httptest.NewServer(&fakeBlockImporter{})
	defer srv.Close()

	shortTotal := int64(len(payload) - 500)

	err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".zst", shortTotal, discardLogger(), nil)
	if err == nil {
		t.Fatal("expected an error for an under-declared totalSize, got nil")
	}
}

// TestPutBlockCompressed_TooLargeDeclaredSizeErrors verifies the over-declared-size
// safety net: when totalSize is larger than the archive's actual decompressed content,
// the explicit req.ContentLength must make net/http refuse to send a short body, so
// putBlockCompressed surfaces a clear wrapped error instead of hanging or letting the
// server reject the request opaquely.
func TestPutBlockCompressed_TooLargeDeclaredSizeErrors(t *testing.T) {
	payload := bytes.Repeat([]byte("short-archive-"), 50)

	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data.bin.zst")
	writeEncodedBlockFile(t, dataFile, "zstd", payload)

	imp := &fakeBlockImporter{}
	srv := httptest.NewServer(imp)
	defer srv.Close()

	longTotal := int64(len(payload) + 5000)

	err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".zst", longTotal, discardLogger(), nil)
	if err == nil {
		t.Fatal("expected an error for an over-declared totalSize, got nil")
	}

	// Guard against a false pass for an unrelated reason: the server must never have
	// been told the transfer completed (it would only see longTotal bytes if net/http's
	// own short-body detection failed to fire and the request were sent anyway).
	if got := int64(len(imp.received())); got >= longTotal {
		t.Fatalf("server received %d bytes, want fewer than the over-declared total %d (net/http should have refused to send a short body)", got, longTotal)
	}
}

// TestPutBlockCompressed_NoTempFilesCreated asserts the streaming decode path creates
// zero temporary files anywhere under the archive directory during the whole upload —
// the entire point of decoding directly into the PUT body instead of materializing a
// decompressed copy first.
func TestPutBlockCompressed_NoTempFilesCreated(t *testing.T) {
	payload := bytes.Repeat([]byte("no-temp-files-please-"), 5000)

	dir := t.TempDir()
	dataFile := filepath.Join(dir, "data.bin.zst")
	writeEncodedBlockFile(t, dataFile, "zstd", payload)

	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir before upload: %v", err)
	}

	srv := httptest.NewServer(&fakeBlockImporter{})
	defer srv.Close()

	if err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".zst", int64(len(payload)), discardLogger(), nil); err != nil {
		t.Fatalf("putBlock: %v", err)
	}

	after, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir after upload: %v", err)
	}

	if len(before) != len(after) {
		t.Fatalf("archive directory entry count changed during upload: before=%d after=%d (a temp file was left behind)", len(before), len(after))
	}

	for i := range before {
		if before[i].Name() != after[i].Name() {
			t.Fatalf("archive directory contents changed during upload: before=%q after=%q", before[i].Name(), after[i].Name())
		}
	}
}

func completedDataImportObj(namespace, name string) *unstructured.Unstructured {
	obj := dataImportObj(namespace, name, false)
	_ = unstructured.SetNestedSlice(obj.Object, readyConditions(conditionCompleted), "status", "conditions")
	_ = unstructured.SetNestedMap(obj.Object, map[string]interface{}{"name": "vsc-1"}, "status", "data", "artifact")

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

	if err := imp.UploadVolumeData(context.Background(), leaf, "pvc-1", targetNS, nil, nil); err != nil {
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

// volumeSnapshotLeaf builds a CSI VolumeSnapshot data leaf carrying the captured scratch-volume
// parameters EnsureDataImport sends as the PopulateData DataImport's spec.storageParams.
func volumeSnapshotLeaf(name string) PlannedNode {
	return PlannedNode{
		APIVersion:       "snapshot.storage.k8s.io/v1",
		Kind:             "VolumeSnapshot",
		Name:             name,
		StorageClassName: "sc-fast",
		Size:             "10Gi",
		VolumeMode:       "Block",
	}
}

func TestEnsureDataImport_BuildsPopulateDataSpec(t *testing.T) {
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

	if v, _, _ := unstructured.NestedString(got.Object, "spec", "mode"); v != "PopulateData" {
		t.Errorf("spec.mode = %q, want PopulateData", v)
	}

	apiVersion, _, _ := unstructured.NestedString(got.Object, "spec", "snapshotRef", "apiVersion")
	kind, _, _ := unstructured.NestedString(got.Object, "spec", "snapshotRef", "kind")
	refName, _, _ := unstructured.NestedString(got.Object, "spec", "snapshotRef", "name")

	if apiVersion != "snapshot.storage.k8s.io/v1" || kind != "VolumeSnapshot" || refName != "pvc-1" {
		t.Errorf("snapshotRef = {apiVersion:%q, kind:%q, name:%q}, want {snapshot.storage.k8s.io/v1, VolumeSnapshot, pvc-1}", apiVersion, kind, refName)
	}

	sc, _, _ := unstructured.NestedString(got.Object, "spec", "storageParams", "storageClassName")
	size, _, _ := unstructured.NestedString(got.Object, "spec", "storageParams", "size")
	volumeMode, _, _ := unstructured.NestedString(got.Object, "spec", "storageParams", "volumeMode")

	if sc != "sc-fast" || size != "10Gi" || volumeMode != "Block" {
		t.Errorf("storageParams = {storageClassName:%q, size:%q, volumeMode:%q}, want {sc-fast, 10Gi, Block}", sc, size, volumeMode)
	}

	// The obsolete Mode A fields must not be sent (the DataImport CRD prunes unknown fields).
	if _, found, _ := unstructured.NestedString(got.Object, "spec", "dataArtifactType"); found {
		t.Error("spec.dataArtifactType must not be set (removed from the DataImport contract)")
	}

	if _, found, _ := unstructured.NestedMap(got.Object, "spec", "targetRef"); found {
		t.Error("spec.targetRef must not be set (replaced by spec.snapshotRef)")
	}
}

func TestEnsureDataImport_RejectsMissingStorageParams(t *testing.T) {
	// A data leaf without captured storage parameters means a malformed/stale archive;
	// EnsureDataImport must fail fast instead of creating an incomplete PopulateData DataImport.
	leaf := PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "pvc-1"}

	dyn := newFakeDataImportDyn()
	imp := newTestVolumeImporter(dyn)

	if _, err := imp.EnsureDataImport(context.Background(), leaf, targetNS); err == nil {
		t.Fatal("expected error for missing storage parameters, got nil")
	}

	if c := countDataImportActions(dyn, "create"); c != 0 {
		t.Errorf("no DataImport must be created when storage parameters are missing (creates=%d)", c)
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

	if err := imp.sendVolumeData(context.Background(), plainHTTPDoer{}, srv.URL, volumeModeFilesystem, leaf, targetNS, "pvc-1", nil, nil); err != nil {
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
