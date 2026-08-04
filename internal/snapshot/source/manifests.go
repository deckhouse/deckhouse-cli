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
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
)

// plumbingKinds holds the Kubernetes kinds excluded from manifests/ output.
// These infrastructure objects have their identity captured in snapshot.yaml
// and must not appear alongside the user's own-scope manifests.
var plumbingKinds = map[string]struct{}{
	"SnapshotContent":       {},
	"VolumeSnapshotContent": {},
}

// ManifestSource retrieves the own-scope manifests for a snapshot node.
// The interface is intentionally narrow so that an aggregated-API backend can
// replace any other implementation without changing callers. Nodes are addressed
// by aggapi.NodeRef rather than by a low-level ManifestCheckpoint name.
type ManifestSource interface {
	FetchNodeManifests(ctx context.Context, ref aggapi.NodeRef) ([]unstructured.Unstructured, error)
}

// AggregatedManifestSource is the production ManifestSource backed by the
// state-snapshotter aggregated subresource API. It performs a single
// manifests-download GET per node and returns the decoded objects with plumbing
// kinds removed. The server already returns a clean JSON array (status preserved,
// namespace made relative), so no chunk assembly, gzip decode, or checksum
// verification is needed on the client side.
type AggregatedManifestSource struct {
	client *aggapi.Client
}

// NewAggregatedManifestSource constructs an AggregatedManifestSource backed by c.
func NewAggregatedManifestSource(c *aggapi.Client) *AggregatedManifestSource {
	return &AggregatedManifestSource{client: c}
}

// FetchNodeManifests implements ManifestSource. It GETs the node's
// manifests-download subresource and returns user objects with plumbing kinds removed.
func (s *AggregatedManifestSource) FetchNodeManifests(ctx context.Context, ref aggapi.NodeRef) ([]unstructured.Unstructured, error) {
	raw, err := s.client.NodeManifestsDownload(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("download manifests for %s/%s: %w", ref.Kind, ref.Name, err)
	}

	objs, err := decodeJSONObjects(raw)
	if err != nil {
		return nil, fmt.Errorf("decode manifests for %s/%s: %w", ref.Kind, ref.Name, err)
	}

	return filterPlumbing(objs), nil
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
