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

package source

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

// encodeChunk gzip-compresses and base64-encodes a JSON array of objects,
// returning the encoded data and the base64(sha256(compressed)) checksum.
func encodeChunk(t *testing.T, objs []map[string]interface{}) (data, checksum string) {
	t.Helper()

	raw, err := json.Marshal(objs)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var buf bytes.Buffer

	w := gzip.NewWriter(&buf)

	if _, err := w.Write(raw); err != nil {
		t.Fatalf("gzip.Write: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("gzip.Close: %v", err)
	}

	compressed := buf.Bytes()
	sum := sha256.Sum256(compressed)

	return base64.StdEncoding.EncodeToString(compressed),
		base64.StdEncoding.EncodeToString(sum[:])
}

// makeManifestCheckpoint builds a ManifestCheckpoint CR for the fake client.
func makeManifestCheckpoint(name string, chunks []snapshotapi.ChunkInfo) *snapshotapi.ManifestCheckpoint {
	return &snapshotapi.ManifestCheckpoint{
		TypeMeta: metav1.TypeMeta{
			APIVersion: snapshotapi.SnapshotterGroup + "/" + snapshotapi.Version,
			Kind:       "ManifestCheckpoint",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: snapshotapi.ManifestCheckpointStatus{
			TotalObjects: totalObjects(chunks),
			Chunks:       chunks,
		},
	}
}

// totalObjects sums ObjectsCount across all chunks.
func totalObjects(chunks []snapshotapi.ChunkInfo) int {
	n := 0
	for _, c := range chunks {
		n += c.ObjectsCount
	}

	return n
}

// makeChunkCR builds a ManifestCheckpointContentChunk CR for the fake client.
func makeChunkCR(name, checkpointName, data, checksum string, index, count int) *snapshotapi.ManifestCheckpointContentChunk {
	return &snapshotapi.ManifestCheckpointContentChunk{
		TypeMeta: metav1.TypeMeta{
			APIVersion: snapshotapi.SnapshotterGroup + "/" + snapshotapi.Version,
			Kind:       "ManifestCheckpointContentChunk",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: snapshotapi.ManifestCheckpointContentChunkSpec{
			CheckpointName: checkpointName,
			Index:          index,
			Data:           data,
			ObjectsCount:   count,
			Checksum:       checksum,
		},
	}
}

// kubeObj returns a minimal Kubernetes-style object map with the given identity.
func kubeObj(apiVersion, kind, namespace, name string) map[string]interface{} {
	meta := map[string]interface{}{"name": name}
	if namespace != "" {
		meta["namespace"] = namespace
	}

	return map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   meta,
	}
}

// TestFetchNodeManifests_SingleChunk verifies that a checkpoint with one chunk
// is decoded correctly and checksum validated.
func TestFetchNodeManifests_SingleChunk(t *testing.T) {
	scheme := makeScheme(t)

	objs := []map[string]interface{}{
		kubeObj("v1", "ConfigMap", "default", "app-config"),
		kubeObj("v1", "Secret", "default", "app-secret"),
	}

	data, checksum := encodeChunk(t, objs)

	mc := makeManifestCheckpoint("mcp-1", []snapshotapi.ChunkInfo{
		{Name: "chunk-0", Index: 0, ObjectsCount: 2, Checksum: checksum},
	})
	chunkCR := makeChunkCR("chunk-0", "mcp-1", data, checksum, 0, 2)

	c := buildFakeClient(scheme, []client.Object{mc, chunkCR}, nil)
	src := NewKubeManifestSource(c)

	result, err := src.FetchNodeManifests(context.Background(), "mcp-1")
	if err != nil {
		t.Fatalf("FetchNodeManifests: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("got %d objects, want 2", len(result))
	}

	if result[0].GetName() != "app-config" {
		t.Errorf("object 0 name: got %q, want %q", result[0].GetName(), "app-config")
	}

	if result[1].GetName() != "app-secret" {
		t.Errorf("object 1 name: got %q, want %q", result[1].GetName(), "app-secret")
	}
}

// TestFetchNodeManifests_MultipleChunksOrdered verifies that chunks are
// assembled in ascending index order regardless of their order in the status slice.
func TestFetchNodeManifests_MultipleChunksOrdered(t *testing.T) {
	scheme := makeScheme(t)

	objs0 := []map[string]interface{}{kubeObj("v1", "ConfigMap", "default", "cm-0")}
	objs1 := []map[string]interface{}{kubeObj("v1", "ConfigMap", "default", "cm-1")}

	data0, checksum0 := encodeChunk(t, objs0)
	data1, checksum1 := encodeChunk(t, objs1)

	// List chunks in reverse order to verify sorting.
	mc := makeManifestCheckpoint("mcp-ord", []snapshotapi.ChunkInfo{
		{Name: "chunk-1", Index: 1, ObjectsCount: 1, Checksum: checksum1},
		{Name: "chunk-0", Index: 0, ObjectsCount: 1, Checksum: checksum0},
	})
	chunk0 := makeChunkCR("chunk-0", "mcp-ord", data0, checksum0, 0, 1)
	chunk1 := makeChunkCR("chunk-1", "mcp-ord", data1, checksum1, 1, 1)

	c := buildFakeClient(scheme, []client.Object{mc, chunk0, chunk1}, nil)
	src := NewKubeManifestSource(c)

	result, err := src.FetchNodeManifests(context.Background(), "mcp-ord")
	if err != nil {
		t.Fatalf("FetchNodeManifests: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("got %d objects, want 2", len(result))
	}

	if result[0].GetName() != "cm-0" {
		t.Errorf("object 0 name: got %q, want %q", result[0].GetName(), "cm-0")
	}

	if result[1].GetName() != "cm-1" {
		t.Errorf("object 1 name: got %q, want %q", result[1].GetName(), "cm-1")
	}
}

// TestFetchNodeManifests_PlumbingExcluded verifies that SnapshotContent and
// VolumeSnapshotContent objects are removed from the result.
func TestFetchNodeManifests_PlumbingExcluded(t *testing.T) {
	scheme := makeScheme(t)

	objs := []map[string]interface{}{
		kubeObj("v1", "ConfigMap", "default", "user-cm"),
		kubeObj("storage.deckhouse.io/v1alpha1", "SnapshotContent", "", "sc-123"),
		kubeObj("snapshot.storage.k8s.io/v1", "VolumeSnapshotContent", "", "vsc-456"),
	}

	data, checksum := encodeChunk(t, objs)

	mc := makeManifestCheckpoint("mcp-plumb", []snapshotapi.ChunkInfo{
		{Name: "chunk-p", Index: 0, ObjectsCount: 3, Checksum: checksum},
	})
	chunkCR := makeChunkCR("chunk-p", "mcp-plumb", data, checksum, 0, 3)

	c := buildFakeClient(scheme, []client.Object{mc, chunkCR}, nil)
	src := NewKubeManifestSource(c)

	result, err := src.FetchNodeManifests(context.Background(), "mcp-plumb")
	if err != nil {
		t.Fatalf("FetchNodeManifests: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("got %d objects after filtering, want 1", len(result))
	}

	if result[0].GetName() != "user-cm" {
		t.Errorf("surviving object: got %q, want %q", result[0].GetName(), "user-cm")
	}
}

// TestFetchNodeManifests_BadChecksum verifies that a checksum mismatch is
// reported as ErrChecksumMismatch.
func TestFetchNodeManifests_BadChecksum(t *testing.T) {
	scheme := makeScheme(t)

	objs := []map[string]interface{}{kubeObj("v1", "ConfigMap", "default", "cm")}
	data, _ := encodeChunk(t, objs)

	wrongChecksum := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

	mc := makeManifestCheckpoint("mcp-bad-cs", []snapshotapi.ChunkInfo{
		{Name: "chunk-bad", Index: 0, ObjectsCount: 1, Checksum: wrongChecksum},
	})
	chunkCR := makeChunkCR("chunk-bad", "mcp-bad-cs", data, wrongChecksum, 0, 1)

	c := buildFakeClient(scheme, []client.Object{mc, chunkCR}, nil)
	src := NewKubeManifestSource(c)

	_, err := src.FetchNodeManifests(context.Background(), "mcp-bad-cs")
	if err == nil {
		t.Fatal("expected error on bad checksum, got nil")
	}

	if !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("expected ErrChecksumMismatch, got: %v", err)
	}
}

// TestFetchNodeManifests_CorruptGzip verifies that a chunk payload that is not
// valid gzip returns an error.
func TestFetchNodeManifests_CorruptGzip(t *testing.T) {
	scheme := makeScheme(t)

	garbage := base64.StdEncoding.EncodeToString([]byte("this is not gzip"))

	mc := makeManifestCheckpoint("mcp-corrupt", []snapshotapi.ChunkInfo{
		{Name: "chunk-corrupt", Index: 0, ObjectsCount: 1},
	})
	chunkCR := makeChunkCR("chunk-corrupt", "mcp-corrupt", garbage, "", 0, 1)

	c := buildFakeClient(scheme, []client.Object{mc, chunkCR}, nil)
	src := NewKubeManifestSource(c)

	_, err := src.FetchNodeManifests(context.Background(), "mcp-corrupt")
	if err == nil {
		t.Fatal("expected error on corrupt gzip, got nil")
	}
}

// TestFetchNodeManifests_NoChecksum verifies that a chunk without a checksum
// field is accepted without validation.
func TestFetchNodeManifests_NoChecksum(t *testing.T) {
	scheme := makeScheme(t)

	objs := []map[string]interface{}{kubeObj("v1", "Pod", "default", "test-pod")}
	data, _ := encodeChunk(t, objs)

	mc := makeManifestCheckpoint("mcp-nocs", []snapshotapi.ChunkInfo{
		{Name: "chunk-nocs", Index: 0, ObjectsCount: 1},
	})
	chunkCR := makeChunkCR("chunk-nocs", "mcp-nocs", data, "", 0, 1)

	c := buildFakeClient(scheme, []client.Object{mc, chunkCR}, nil)
	src := NewKubeManifestSource(c)

	result, err := src.FetchNodeManifests(context.Background(), "mcp-nocs")
	if err != nil {
		t.Fatalf("FetchNodeManifests: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("got %d objects, want 1", len(result))
	}

	if result[0].GetKind() != "Pod" {
		t.Errorf("object kind: got %q, want Pod", result[0].GetKind())
	}
}
