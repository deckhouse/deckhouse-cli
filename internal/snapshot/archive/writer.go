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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	sigsyaml "sigs.k8s.io/yaml"
)

// CollisionNodeDir returns the path for a node directory with a short-checksum suffix:
//
//	<parentDir>/<NodeDirName(kind,name)>__<short>
//
// Use this when the primary directory already holds complete data for a different snapshot
// and the new node's own short checksum disambiguates the two.
func CollisionNodeDir(parentDir, kind, name, short string) string {
	return filepath.Join(parentDir, NodeDirName(kind, name)+"__"+short)
}

// WriteManifest serialises obj as uncompressed YAML and writes it atomically into
// <nodeDir>/manifests/. The filename is determined by ManifestFileName:
//
//   - Normal: <kindlower>_<name>.yaml.
//   - Collision fallback: if a file with the same kind/name already exists but belongs
//     to a different API group, <kindlower>.<apiGroup>_<name>.yaml is used instead.
//
// Rewriting the same object (same kind, name, and API group) is idempotent.
func WriteManifest(nodeDir string, obj unstructured.Unstructured) error {
	return writeManifest(nil, nodeDir, obj)
}

// WriteManifestRooted writes a manifest through destination's locked view.
func WriteManifestRooted(
	destination *RootedDestination,
	nodeDir string,
	obj unstructured.Unstructured,
) error {
	return writeManifest(destination, nodeDir, obj)
}

func writeManifest(
	destination *RootedDestination,
	nodeDir string,
	obj unstructured.Unstructured,
) error {
	kind := obj.GetKind()
	name := obj.GetName()
	normalPath := filepath.Join(nodeDir, ManifestsDirName, ManifestFileName(kind, name, ""))

	existingGroup, err := readManifestAPIGroupAt(destination, normalPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("check manifest %s: %w", normalPath, err)
		}

		return writeManifestYAMLAt(destination, normalPath, obj)
	}

	// File exists; use the qualified name only when the API group differs.
	newGroup := extractAPIGroup(obj.GetAPIVersion())
	if newGroup == existingGroup {
		return writeManifestYAMLAt(destination, normalPath, obj)
	}

	qualifiedPath := filepath.Join(nodeDir, ManifestsDirName, ManifestFileName(kind, name, newGroup))

	return writeManifestYAMLAt(destination, qualifiedPath, obj)
}

func writeManifestYAMLAt(
	destination *RootedDestination,
	path string,
	obj unstructured.Unstructured,
) error {
	data, err := sigsyaml.Marshal(obj.Object)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	if destination == nil {
		return WriteFileAtomic(path, bytes.NewReader(data))
	}

	return WriteFileAtomicRooted(context.Background(), destination, path, bytes.NewReader(data))
}

func readManifestAPIGroupAt(destination *RootedDestination, path string) (string, error) {
	var (
		data []byte
		err  error
	)

	if destination == nil {
		data, err = os.ReadFile(path)
	} else {
		data, err = destination.ReadFile(path)
	}

	if err != nil {
		return "", err
	}

	var obj map[string]interface{}
	if err := sigsyaml.Unmarshal(data, &obj); err != nil {
		return "", fmt.Errorf("unmarshal manifest %s: %w", path, err)
	}

	apiVersion, _ := obj["apiVersion"].(string)

	return extractAPIGroup(apiVersion), nil
}

// WriteSnapshotYAMLRooted writes snapshot.yaml atomically through destination.
func WriteSnapshotYAMLRooted(
	ctx context.Context,
	destination *RootedDestination,
	nodeDir string,
	snapshot SnapshotYAML,
) error {
	data, err := sigsyaml.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot.yaml: %w", err)
	}

	path := filepath.Join(nodeDir, SnapshotYAMLName)

	return WriteFileAtomicRooted(ctx, destination, path, bytes.NewReader(data))
}

// extractAPIGroup returns the API group portion of an apiVersion string.
// For core-group resources (e.g. "v1") an empty string is returned.
// For group-versioned resources (e.g. "apps/v1") the group part is returned.
func extractAPIGroup(apiVersion string) string {
	if i := strings.IndexByte(apiVersion, '/'); i >= 0 {
		return apiVersion[:i]
	}

	return ""
}
