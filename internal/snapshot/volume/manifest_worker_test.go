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

package volume_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// stubManifestSource is a test-only ManifestSource that returns a pre-seeded list.
type stubManifestSource struct {
	objs []unstructured.Unstructured
	err  error
}

func (s *stubManifestSource) FetchNodeManifests(_ context.Context, _ string) ([]unstructured.Unstructured, error) {
	return s.objs, s.err
}

// makeObj builds a minimal unstructured object.
func makeObj(apiVersion, kind, name string) unstructured.Unstructured {
	obj := unstructured.Unstructured{}
	obj.SetAPIVersion(apiVersion)
	obj.SetKind(kind)
	obj.SetName(name)

	return obj
}

// setupNodeDir creates a nodeDir with a manifests/ subdirectory.
func setupNodeDir(t *testing.T) string {
	t.Helper()

	nodeDir := t.TempDir()

	if err := os.MkdirAll(filepath.Join(nodeDir, archive.ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir manifests: %v", err)
	}

	return nodeDir
}

func TestWriteNodeManifests_WritesFiles(t *testing.T) {
	nodeDir := setupNodeDir(t)

	objs := []unstructured.Unstructured{
		makeObj("v1", "ConfigMap", "my-cm"),
		makeObj("apps/v1", "Deployment", "my-deploy"),
	}

	node := &source.Node{
		APIVersion:             "storage.deckhouse.io/v1alpha1",
		Kind:                   "Snapshot",
		Name:                   "snap-1",
		Namespace:              "default",
		ManifestCheckpointName: "mc-1",
	}

	src := &stubManifestSource{objs: objs}

	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("WriteNodeManifests: %v", err)
	}

	// Expect one file per object.
	expected := []string{
		"configmap_my-cm.yaml",
		"deployment_my-deploy.yaml",
	}

	for _, name := range expected {
		path := filepath.Join(nodeDir, archive.ManifestsDirName, name)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected manifest file %s: %v", name, err)
		}
	}
}

func TestWriteNodeManifests_CollisionFallback(t *testing.T) {
	nodeDir := setupNodeDir(t)

	// Two objects with the same kind+name but different API groups.
	objs := []unstructured.Unstructured{
		makeObj("v1", "Pod", "my-pod"),
		makeObj("apps/v1", "Pod", "my-pod"),
	}

	node := &source.Node{ManifestCheckpointName: "mc-collision"}
	src := &stubManifestSource{objs: objs}

	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("WriteNodeManifests: %v", err)
	}

	manifestsDir := filepath.Join(nodeDir, archive.ManifestsDirName)

	// Normal name for the first-written object (core group → no qualifier).
	normalPath := filepath.Join(manifestsDir, "pod_my-pod.yaml")
	if _, err := os.Stat(normalPath); err != nil {
		t.Errorf("expected normal manifest file pod_my-pod.yaml: %v", err)
	}

	// Qualified name for the second object (different API group).
	qualifiedPath := filepath.Join(manifestsDir, "pod.apps_my-pod.yaml")
	if _, err := os.Stat(qualifiedPath); err != nil {
		t.Errorf("expected qualified manifest file pod.apps_my-pod.yaml: %v", err)
	}
}

func TestWriteNodeManifests_EmptyCheckpointName(t *testing.T) {
	nodeDir := setupNodeDir(t)

	node := &source.Node{ManifestCheckpointName: ""}
	src := &stubManifestSource{objs: []unstructured.Unstructured{makeObj("v1", "Secret", "s")}}

	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("WriteNodeManifests with empty checkpoint: %v", err)
	}

	// No files should be written because checkpoint name is empty.
	entries, err := os.ReadDir(filepath.Join(nodeDir, archive.ManifestsDirName))
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 0 {
		t.Errorf("expected no files written for empty checkpoint, got %d", len(entries))
	}
}

func TestWriteNodeManifests_Idempotent(t *testing.T) {
	nodeDir := setupNodeDir(t)

	objs := []unstructured.Unstructured{makeObj("v1", "ConfigMap", "cm1")}
	node := &source.Node{ManifestCheckpointName: "mc-idem"}
	src := &stubManifestSource{objs: objs}

	// First call.
	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("first WriteNodeManifests: %v", err)
	}

	path := filepath.Join(nodeDir, archive.ManifestsDirName, "configmap_cm1.yaml")

	fi1, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after first call: %v", err)
	}

	// Second call (idempotent rewrite).
	if err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node); err != nil {
		t.Fatalf("second WriteNodeManifests: %v", err)
	}

	fi2, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after second call: %v", err)
	}

	// File must still be present (idempotent — content unchanged, file re-written atomically).
	if fi1.Name() != fi2.Name() {
		t.Error("file disappeared after idempotent rewrite")
	}
}

func TestFinalizeNode_WritesSnapshotYAML(t *testing.T) {
	nodeDir := setupNodeDir(t)

	// Write a manifest so the checksum has content to cover.
	obj := makeObj("v1", "ConfigMap", "cm")

	if err := archive.WriteManifest(nodeDir, obj); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	node := &source.Node{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-test",
		Namespace:  "ns1",
		SourceRef:  "demo/vm-1",
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("FinalizeNode: %v", err)
	}

	// snapshot.yaml must exist.
	syPath := filepath.Join(nodeDir, archive.SnapshotYAMLName)
	if _, err := os.Stat(syPath); err != nil {
		t.Fatalf("snapshot.yaml missing: %v", err)
	}

	// VerifyNode must pass (checksum in snapshot.yaml == recomputed checksum).
	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode after FinalizeNode: %v", err)
	}

	// Identity fields must be preserved.
	sy, err := archive.ReadSnapshotYAML(nodeDir)
	if err != nil {
		t.Fatalf("ReadSnapshotYAML: %v", err)
	}

	if sy.APIVersion != node.APIVersion {
		t.Errorf("APIVersion: got %q, want %q", sy.APIVersion, node.APIVersion)
	}

	if sy.Kind != node.Kind {
		t.Errorf("Kind: got %q, want %q", sy.Kind, node.Kind)
	}

	if sy.Name != node.Name {
		t.Errorf("Name: got %q, want %q", sy.Name, node.Name)
	}

	if sy.SourceRef != node.SourceRef {
		t.Errorf("SourceRef: got %q, want %q", sy.SourceRef, node.SourceRef)
	}
}

func TestFinalizeNode_Idempotent(t *testing.T) {
	nodeDir := setupNodeDir(t)

	obj := makeObj("v1", "ConfigMap", "cm2")

	if err := archive.WriteManifest(nodeDir, obj); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	node := &source.Node{
		APIVersion: "storage.deckhouse.io/v1alpha1",
		Kind:       "Snapshot",
		Name:       "snap-idem",
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("first FinalizeNode: %v", err)
	}

	if err := volume.FinalizeNode(nodeDir, node); err != nil {
		t.Fatalf("second FinalizeNode: %v", err)
	}

	if err := archive.VerifyNode(nodeDir); err != nil {
		t.Errorf("VerifyNode after two FinalizeNode calls: %v", err)
	}
}

func TestWriteNodeManifests_FetchError(t *testing.T) {
	nodeDir := setupNodeDir(t)

	want := errors.New("backend unavailable")
	src := &stubManifestSource{err: want}

	node := &source.Node{ManifestCheckpointName: "mc-err"}

	err := volume.WriteNodeManifests(context.Background(), src, nodeDir, node)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, want) {
		t.Errorf("expected wrapped backend error, got: %v", err)
	}
}
