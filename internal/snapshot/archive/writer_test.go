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

package archive

import (
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// makeCompleteNode creates a node directory that satisfies VerifyNode (has a manifest
// and a matching snapshot.yaml). Returns the absolute path of the node directory.
func makeCompleteNode(t *testing.T, parentDir, kind, name string) string {
	t.Helper()

	nodeDir := filepath.Join(parentDir, NodeDirName(kind, name))

	if err := os.MkdirAll(filepath.Join(nodeDir, ManifestsDirName), 0o755); err != nil {
		t.Fatalf("mkdir manifests: %v", err)
	}

	mfPath := filepath.Join(nodeDir, ManifestsDirName, ManifestFileName(kind, name, ""))

	if err := os.WriteFile(mfPath, []byte("apiVersion: v1\nkind: "+kind+"\nmetadata:\n  name: "+name+"\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	checksum, err := ComputeNodeChecksum(nodeDir)
	if err != nil {
		t.Fatalf("compute checksum: %v", err)
	}

	sy := SnapshotYAML{
		APIVersion: "v1",
		Kind:       kind,
		Name:       name,
		Checksum:   checksum,
	}

	if err := WriteSnapshotYAML(nodeDir, sy); err != nil {
		t.Fatalf("write snapshot.yaml: %v", err)
	}

	return nodeDir
}

// kubeObj builds a minimal Unstructured object for use in tests.
func kubeObj(apiVersion, kind, name string) unstructured.Unstructured {
	return unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name": name,
			},
		},
	}
}

// TestCollisionNodeDir verifies the path format for a collision directory.
func TestCollisionNodeDir(t *testing.T) {
	got := CollisionNodeDir("/output", "Pod", "my-pod", "abcd1234")
	want := "/output/pod_my-pod__abcd1234"

	if got != want {
		t.Errorf("CollisionNodeDir: got %q, want %q", got, want)
	}
}

// TestCollisionNodeDir_CreateAndUse demonstrates the collision workflow:
// primary is complete → new content goes to the collision-suffix path.
func TestCollisionNodeDir_CreateAndUse(t *testing.T) {
	tmp := t.TempDir()

	// The "old" complete node.
	_ = makeCompleteNode(t, tmp, "Pod", "my-pod")

	// Verify the primary dir is complete.
	primaryDir := filepath.Join(tmp, NodeDirName("Pod", "my-pod"))
	if err := VerifyNode(primaryDir); err != nil {
		t.Fatalf("expected primary dir to be complete: %v", err)
	}

	// Pipeline detects a different snapshot; allocate a collision path.
	collisionDir := CollisionNodeDir(tmp, "Pod", "my-pod", "deadbeef")

	if err := EnsureDir(collisionDir); err != nil {
		t.Fatalf("EnsureDir collision: %v", err)
	}

	if err := EnsureDir(filepath.Join(collisionDir, ManifestsDirName)); err != nil {
		t.Fatalf("EnsureDir collision manifests: %v", err)
	}

	if _, err := os.Stat(filepath.Join(collisionDir, ManifestsDirName)); err != nil {
		t.Errorf("collision manifests/ not created: %v", err)
	}

	want := filepath.Join(tmp, "pod_my-pod__deadbeef")
	if collisionDir != want {
		t.Errorf("collisionDir: got %q, want %q", collisionDir, want)
	}
}

// TestWriteManifest_Normal verifies that a manifest is written to manifests/.
func TestWriteManifest_Normal(t *testing.T) {
	tmp := t.TempDir()
	nodeDir := filepath.Join(tmp, NodeDirName("Pod", "web"))

	if err := EnsureDir(filepath.Join(nodeDir, ManifestsDirName)); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}

	obj := kubeObj("v1", "ConfigMap", "app-config")

	if err := WriteManifest(nodeDir, obj); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	want := filepath.Join(nodeDir, ManifestsDirName, "configmap_app-config.yaml")
	if _, err := os.Stat(want); err != nil {
		t.Errorf("manifest file not found: %v", err)
	}

	data, err := os.ReadFile(want)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	if len(data) == 0 {
		t.Error("manifest file is empty")
	}
}

// TestWriteManifest_Idempotent verifies that rewriting the same object does not create duplicates.
func TestWriteManifest_Idempotent(t *testing.T) {
	tmp := t.TempDir()
	nodeDir := filepath.Join(tmp, NodeDirName("Pod", "web"))

	if err := EnsureDir(filepath.Join(nodeDir, ManifestsDirName)); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}

	obj := kubeObj("v1", "ConfigMap", "my-cm")

	if err := WriteManifest(nodeDir, obj); err != nil {
		t.Fatalf("first WriteManifest: %v", err)
	}

	if err := WriteManifest(nodeDir, obj); err != nil {
		t.Fatalf("second WriteManifest: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(nodeDir, ManifestsDirName))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	if len(entries) != 1 {
		t.Errorf("expected 1 manifest file, got %d", len(entries))
	}
}

// TestWriteManifest_CollisionAPIGroup verifies that two objects with the same kind/name
// but different API groups produce two distinct files.
func TestWriteManifest_CollisionAPIGroup(t *testing.T) {
	tmp := t.TempDir()
	nodeDir := filepath.Join(tmp, NodeDirName("Pod", "web"))

	if err := EnsureDir(filepath.Join(nodeDir, ManifestsDirName)); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}

	// First object: core ConfigMap (apiVersion "v1", group "").
	coreObj := kubeObj("v1", "ConfigMap", "shared")

	if err := WriteManifest(nodeDir, coreObj); err != nil {
		t.Fatalf("WriteManifest core: %v", err)
	}

	// Second object: same kind+name but different apiGroup.
	extObj := kubeObj("extensions/v1", "ConfigMap", "shared")

	if err := WriteManifest(nodeDir, extObj); err != nil {
		t.Fatalf("WriteManifest ext: %v", err)
	}

	manifestsDir := filepath.Join(nodeDir, ManifestsDirName)
	entries, err := os.ReadDir(manifestsDir)

	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	if len(entries) != 2 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("expected 2 manifest files, got %d: %v", len(entries), names)
	}

	// The normal form is taken by the first object.
	normalFile := filepath.Join(manifestsDir, ManifestFileName("ConfigMap", "shared", ""))
	if _, err := os.Stat(normalFile); err != nil {
		t.Errorf("normal form file not found: %v", err)
	}

	// The collision form uses the second object's apiGroup.
	qualifiedFile := filepath.Join(manifestsDir, ManifestFileName("ConfigMap", "shared", "extensions"))
	if _, err := os.Stat(qualifiedFile); err != nil {
		t.Errorf("qualified form file not found: %v", err)
	}
}

// TestWriteManifest_Tree tests a small multi-manifest node.
func TestWriteManifest_Tree(t *testing.T) {
	tmp := t.TempDir()
	nodeDir := filepath.Join(tmp, NodeDirName("VirtualDisk", "disk-a"))

	if err := EnsureDir(filepath.Join(nodeDir, ManifestsDirName)); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}

	if err := EnsureDir(filepath.Join(nodeDir, SnapshotsDirName)); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}

	objs := []unstructured.Unstructured{
		kubeObj("v1", "ConfigMap", "cfg"),
		kubeObj("apps/v1", "Deployment", "app"),
		kubeObj("v1", "Service", "svc"),
	}

	for _, obj := range objs {
		if err := WriteManifest(nodeDir, obj); err != nil {
			t.Fatalf("WriteManifest %s/%s: %v", obj.GetKind(), obj.GetName(), err)
		}
	}

	entries, err := os.ReadDir(filepath.Join(nodeDir, ManifestsDirName))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	if len(entries) != len(objs) {
		t.Errorf("expected %d manifest files, got %d", len(objs), len(entries))
	}

	// snapshots/ must exist because withSnapshots=true.
	if _, err := os.Stat(filepath.Join(nodeDir, SnapshotsDirName)); err != nil {
		t.Errorf("snapshots/ not created: %v", err)
	}
}
