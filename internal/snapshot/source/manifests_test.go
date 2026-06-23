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
	"io"
	"net/http"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	restfake "k8s.io/client-go/rest/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
)

// kubeObjJSON renders a minimal Kubernetes object JSON with the given identity.
func kubeObjJSON(apiVersion, kind, namespace, name string) string {
	if namespace == "" {
		return `{"apiVersion":"` + apiVersion + `","kind":"` + kind +
			`","metadata":{"name":"` + name + `"}}`
	}

	return `{"apiVersion":"` + apiVersion + `","kind":"` + kind +
		`","metadata":{"name":"` + name + `","namespace":"` + namespace + `"}}`
}

// fakeDownloadSource builds an AggregatedManifestSource whose manifests-download GET
// returns the given HTTP status and body. The RESTMapper resolves the core Snapshot
// kind so downloadPath can be built without a live discovery client.
func fakeDownloadSource(status int, body string) *AggregatedManifestSource {
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(schema.GroupVersionKind{
		Group:   aggapi.StorageGroup,
		Version: "v1alpha1",
		Kind:    "Snapshot",
	}, meta.RESTScopeNamespace)

	restClient := &restfake.RESTClient{
		NegotiatedSerializer: scheme.Codecs,
		GroupVersion:         schema.GroupVersion{Group: aggapi.StorageGroup, Version: "v1alpha1"},
		VersionedAPIPath:     "/",
		Client: restfake.CreateHTTPClient(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: status,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
		}),
	}

	return NewAggregatedManifestSource(aggapi.NewClient(restClient, mapper))
}

// snapshotRef returns a core Snapshot node ref for the aggregated source tests.
func snapshotRef() aggapi.NodeRef {
	return aggapi.NodeRef{
		APIVersion: aggapi.StorageGroup + "/v1alpha1",
		Kind:       "Snapshot",
		Name:       "my-snap",
		Namespace:  "test-ns",
	}
}

// TestAggregatedFetchNodeManifests_Decodes verifies that the aggregated source GETs
// the node's manifests-download body, decodes the JSON array, and returns the objects.
func TestAggregatedFetchNodeManifests_Decodes(t *testing.T) {
	body := "[" +
		kubeObjJSON("v1", "ConfigMap", "default", "app-config") + "," +
		kubeObjJSON("v1", "Secret", "default", "app-secret") +
		"]"

	src := fakeDownloadSource(http.StatusOK, body)

	result, err := src.FetchNodeManifests(context.Background(), snapshotRef())
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

// TestAggregatedFetchNodeManifests_FiltersPlumbing verifies that SnapshotContent and
// VolumeSnapshotContent objects are removed from the downloaded result.
func TestAggregatedFetchNodeManifests_FiltersPlumbing(t *testing.T) {
	body := "[" +
		kubeObjJSON("v1", "ConfigMap", "default", "user-cm") + "," +
		kubeObjJSON("storage.deckhouse.io/v1alpha1", "SnapshotContent", "", "sc-123") + "," +
		kubeObjJSON("snapshot.storage.k8s.io/v1", "VolumeSnapshotContent", "", "vsc-456") +
		"]"

	src := fakeDownloadSource(http.StatusOK, body)

	result, err := src.FetchNodeManifests(context.Background(), snapshotRef())
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

// TestAggregatedFetchNodeManifests_ServerError verifies that a non-2xx download
// response is surfaced as an error.
func TestAggregatedFetchNodeManifests_ServerError(t *testing.T) {
	body := `{"kind":"Status","apiVersion":"v1","status":"Failure","code":500,"message":"boom"}`

	src := fakeDownloadSource(http.StatusInternalServerError, body)

	_, err := src.FetchNodeManifests(context.Background(), snapshotRef())
	if err == nil {
		t.Fatal("expected error on server failure, got nil")
	}
}

// TestDecodeJSONObjects covers a valid array, an empty array, and invalid JSON.
func TestDecodeJSONObjects(t *testing.T) {
	t.Run("valid array", func(t *testing.T) {
		objs, err := decodeJSONObjects([]byte("[" + kubeObjJSON("v1", "Pod", "default", "p") + "]"))
		if err != nil {
			t.Fatalf("decodeJSONObjects: %v", err)
		}

		if len(objs) != 1 || objs[0].GetKind() != "Pod" {
			t.Fatalf("got %#v, want a single Pod", objs)
		}
	})

	t.Run("empty array", func(t *testing.T) {
		objs, err := decodeJSONObjects([]byte("[]"))
		if err != nil {
			t.Fatalf("decodeJSONObjects: %v", err)
		}

		if len(objs) != 0 {
			t.Fatalf("got %d objects, want 0", len(objs))
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		if _, err := decodeJSONObjects([]byte("not json")); err == nil {
			t.Fatal("expected error on invalid JSON, got nil")
		}
	})
}

// TestFilterPlumbing verifies that plumbing kinds are dropped and user kinds kept.
func TestFilterPlumbing(t *testing.T) {
	objs, err := decodeJSONObjects([]byte("[" +
		kubeObjJSON("v1", "ConfigMap", "default", "cm") + "," +
		kubeObjJSON("storage.deckhouse.io/v1alpha1", "SnapshotContent", "", "sc") + "," +
		kubeObjJSON("snapshot.storage.k8s.io/v1", "VolumeSnapshotContent", "", "vsc") +
		"]"))
	if err != nil {
		t.Fatalf("decodeJSONObjects: %v", err)
	}

	filtered := filterPlumbing(objs)

	if len(filtered) != 1 {
		t.Fatalf("got %d objects after filtering, want 1", len(filtered))
	}

	if filtered[0].GetKind() != "ConfigMap" {
		t.Errorf("surviving kind: got %q, want ConfigMap", filtered[0].GetKind())
	}
}
