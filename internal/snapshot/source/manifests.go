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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

// ErrChecksumMismatch is returned when a chunk's computed checksum does not match
// the value recorded in the chunk spec.
var ErrChecksumMismatch = errors.New("checksum mismatch")

// plumbingKinds holds the Kubernetes kinds excluded from manifests/ output.
// These infrastructure objects have their identity captured in snapshot.yaml
// and must not appear alongside the user's own-scope manifests.
var plumbingKinds = map[string]struct{}{
	"SnapshotContent":       {},
	"VolumeSnapshotContent": {},
}

// ManifestSource retrieves the own-scope manifests for a snapshot node.
// The interface is intentionally narrow so that an aggregated-API or HTTP-based
// backend can replace the Kubernetes client implementation without changing callers.
type ManifestSource interface {
	FetchNodeManifests(ctx context.Context, manifestCheckpointName string) ([]unstructured.Unstructured, error)
}

// KubeManifestSource is the production ManifestSource backed by the Kubernetes API.
// It reads ManifestCheckpoint and ManifestCheckpointContentChunk CRs, assembles
// chunks in ascending index order, validates per-chunk SHA-256 checksums when present,
// and filters out plumbing kinds before returning.
type KubeManifestSource struct {
	client client.Client
}

// NewKubeManifestSource constructs a KubeManifestSource backed by c.
// The caller must have registered snapshotapi.AddToScheme with the client scheme.
func NewKubeManifestSource(c client.Client) *KubeManifestSource {
	return &KubeManifestSource{client: c}
}

// FetchNodeManifests implements ManifestSource.
// It fetches the named ManifestCheckpoint, resolves each chunk in index order,
// decodes base64(gzip(json[])) payloads, and returns user objects with plumbing
// kinds removed.
func (s *KubeManifestSource) FetchNodeManifests(ctx context.Context, manifestCheckpointName string) ([]unstructured.Unstructured, error) {
	mc := new(snapshotapi.ManifestCheckpoint)
	if err := s.client.Get(ctx, types.NamespacedName{Name: manifestCheckpointName}, mc); err != nil {
		return nil, fmt.Errorf("get ManifestCheckpoint %q: %w", manifestCheckpointName, err)
	}

	// Work on a copy so we do not mutate the status slice order.
	chunks := make([]snapshotapi.ChunkInfo, len(mc.Status.Chunks))
	copy(chunks, mc.Status.Chunks)
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Index < chunks[j].Index
	})

	result := make([]unstructured.Unstructured, 0, mc.Status.TotalObjects)

	for _, info := range chunks {
		objs, err := s.fetchChunk(ctx, info)
		if err != nil {
			return nil, fmt.Errorf("chunk %q (index %d): %w", info.Name, info.Index, err)
		}

		result = append(result, objs...)
	}

	return filterPlumbing(result), nil
}

// fetchChunk fetches one ManifestCheckpointContentChunk by name, verifies its
// optional checksum, decompresses the gzip payload, and decodes the JSON array.
func (s *KubeManifestSource) fetchChunk(ctx context.Context, info snapshotapi.ChunkInfo) ([]unstructured.Unstructured, error) {
	chunk := new(snapshotapi.ManifestCheckpointContentChunk)
	if err := s.client.Get(ctx, types.NamespacedName{Name: info.Name}, chunk); err != nil {
		return nil, fmt.Errorf("get ManifestCheckpointContentChunk %q: %w", info.Name, err)
	}

	compressed, err := base64.StdEncoding.DecodeString(chunk.Spec.Data)
	if err != nil {
		return nil, fmt.Errorf("base64 decode chunk data: %w", err)
	}

	if chunk.Spec.Checksum != "" {
		if err := verifyChecksum(compressed, chunk.Spec.Checksum); err != nil {
			return nil, err
		}
	}

	gr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("open gzip reader: %w", err)
	}

	defer func() { _ = gr.Close() }()

	raw, err := io.ReadAll(gr)
	if err != nil {
		return nil, fmt.Errorf("decompress chunk: %w", err)
	}

	return decodeJSONObjects(raw)
}

// verifyChecksum checks that sha256(compressed) matches expected.
//
// Real state-snapshotter producers encode the checksum as lowercase hex
// (hex.EncodeToString). This function accepts both hex (primary) and
// base64.StdEncoding (compat) to tolerate any encoding already in the wild.
// It compares raw digest bytes, so the same hash is accepted regardless of
// how it was serialised. If expected is neither valid hex nor valid base64,
// a non-ErrChecksumMismatch error is returned so callers can distinguish a
// real corruption from a format problem.
func verifyChecksum(compressed []byte, expected string) error {
	sum := sha256.Sum256(compressed)

	// Try hex first — the format emitted by production state-snapshotter controllers.
	if expectedBytes, err := hex.DecodeString(expected); err == nil {
		if !bytes.Equal(sum[:], expectedBytes) {
			return fmt.Errorf("%w: got %q, want %q", ErrChecksumMismatch, hex.EncodeToString(sum[:]), expected)
		}

		return nil
	}

	// Fall back to base64 for any producer that serialises the digest differently.
	if expectedBytes, err := base64.StdEncoding.DecodeString(expected); err == nil {
		if !bytes.Equal(sum[:], expectedBytes) {
			return fmt.Errorf("%w: got %q, want %q", ErrChecksumMismatch, hex.EncodeToString(sum[:]), expected)
		}

		return nil
	}

	return fmt.Errorf("chunk checksum has unrecognized encoding: %q", expected)
}

// decodeJSONObjects unmarshals a JSON array of Kubernetes object maps into
// a slice of unstructured.Unstructured values.
func decodeJSONObjects(data []byte) ([]unstructured.Unstructured, error) {
	var rawItems []json.RawMessage
	if err := json.Unmarshal(data, &rawItems); err != nil {
		return nil, fmt.Errorf("unmarshal object array: %w", err)
	}

	objs := make([]unstructured.Unstructured, 0, len(rawItems))

	for i, rawItem := range rawItems {
		var obj map[string]interface{}
		if err := json.Unmarshal(rawItem, &obj); err != nil {
			return nil, fmt.Errorf("unmarshal object %d: %w", i, err)
		}

		objs = append(objs, unstructured.Unstructured{Object: obj})
	}

	return objs, nil
}

// filterPlumbing returns a copy of objs with plumbing kinds removed.
func filterPlumbing(objs []unstructured.Unstructured) []unstructured.Unstructured {
	filtered := make([]unstructured.Unstructured, 0, len(objs))

	for _, obj := range objs {
		if _, skip := plumbingKinds[obj.GetKind()]; !skip {
			filtered = append(filtered, obj)
		}
	}

	return filtered
}
