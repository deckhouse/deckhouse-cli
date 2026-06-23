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

// Package snapimport implements the `d8 snapshot import` command: it reconstructs a
// snapshot tree in a target namespace from a local archive produced by
// `d8 snapshot download`, walking the tree bottom-up and, per node, creating an
// import-mode CR, importing volume data for data leaves (via SVDM DataImport), and
// POSTing the node's manifests plus its direct child refs to the state-snapshotter
// manifests-and-children-refs-upload aggregated subresource.
package snapimport

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// ChildRef is a direct-child reference for a manifests-and-children-refs-upload payload.
// The child namespace is implicit (it is always the upload target namespace), mirroring
// the server-side SnapshotChildRef shape.
type ChildRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

// PlannedNode is one archive node resolved for import. Nodes are returned by BuildPlan
// in post-order (deepest descendants first, root last) so that data leaves and child
// SnapshotContents materialise before their parents reference them.
type PlannedNode struct {
	// Dir is the absolute path of the node directory in the archive.
	Dir string
	// APIVersion/Kind/Name identify the snapshot CR for this node (from snapshot.yaml).
	APIVersion string
	Kind       string
	Name       string
	// SourceNamespace is the namespace recorded in the archive (informational; the import
	// always targets the user-supplied namespace).
	SourceNamespace string
	// Manifests are the node's own captured manifests (from manifests/), the same shape
	// the server returned from manifests-download.
	Manifests []unstructured.Unstructured
	// Children are the direct child snapshot refs (from snapshots/<child>/snapshot.yaml).
	Children []ChildRef
	// DataFile is the absolute path to the node's single-volume block data file
	// (data.bin[.<ext>]) when present; empty when the node carries no importable
	// block volume data.
	DataFile string
	// FilesystemData is true when the node carries filesystem-volume data (data.tar),
	// which the CLI cannot yet re-import.
	FilesystemData bool
}

// Ref returns the node's aggregated-API node ref (target namespace applied by the caller).
func (n PlannedNode) Ref(namespace string) aggapi.NodeRef {
	return aggapi.NodeRef{
		APIVersion: n.APIVersion,
		Kind:       n.Kind,
		Name:       n.Name,
		Namespace:  namespace,
	}
}

// HasBlockData reports whether the node carries a single-volume block data file.
func (n PlannedNode) HasBlockData() bool {
	return n.DataFile != ""
}

// BuildPlan walks the archive rooted at rootDir and returns its nodes in post-order
// (leaves first, root last). Each node's own manifests, direct child refs, and volume
// data file (if any) are resolved.
func BuildPlan(rootDir string) ([]PlannedNode, error) {
	rootDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("resolve archive path: %w", err)
	}

	var plan []PlannedNode
	if err := appendPostOrder(rootDir, &plan); err != nil {
		return nil, err
	}

	return plan, nil
}

// appendPostOrder visits children first (sorted for determinism), then the node itself.
func appendPostOrder(dir string, plan *[]PlannedNode) error {
	node, err := readNode(dir)
	if err != nil {
		return err
	}

	childDirs, err := childNodeDirs(dir)
	if err != nil {
		return err
	}

	for _, childDir := range childDirs {
		if err := appendPostOrder(childDir, plan); err != nil {
			return err
		}

		childNode, err := readNode(childDir)
		if err != nil {
			return err
		}

		node.Children = append(node.Children, ChildRef{
			APIVersion: childNode.APIVersion,
			Kind:       childNode.Kind,
			Name:       childNode.Name,
		})
	}

	*plan = append(*plan, node)

	return nil
}

// readNode reads a single node directory's snapshot.yaml, own manifests and data file.
func readNode(dir string) (PlannedNode, error) {
	sy, err := archive.ReadSnapshotYAML(dir)
	if err != nil {
		return PlannedNode{}, fmt.Errorf("read node %s: %w", dir, err)
	}

	if sy.Kind == "" || sy.Name == "" || sy.APIVersion == "" {
		return PlannedNode{}, fmt.Errorf("node %s: snapshot.yaml missing apiVersion/kind/name", dir)
	}

	manifests, err := readManifests(dir)
	if err != nil {
		return PlannedNode{}, fmt.Errorf("node %s: %w", dir, err)
	}

	node := PlannedNode{
		Dir:             dir,
		APIVersion:      sy.APIVersion,
		Kind:            sy.Kind,
		Name:            sy.Name,
		SourceNamespace: sy.Namespace,
		Manifests:       manifests,
	}

	blockData, found, err := archive.FindBlockData(dir)
	if err != nil {
		return PlannedNode{}, fmt.Errorf("node %s: %w", dir, err)
	}

	if found {
		node.DataFile = blockData
	}

	if _, statErr := os.Stat(filepath.Join(dir, archive.FsTarName)); statErr == nil {
		node.FilesystemData = true
	}

	return node, nil
}

// readManifests parses every <dir>/manifests/*.yaml file into an unstructured object.
func readManifests(dir string) ([]unstructured.Unstructured, error) {
	manifestsDir := filepath.Join(dir, archive.ManifestsDirName)

	entries, err := os.ReadDir(manifestsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("read manifests dir: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".yaml" {
			continue
		}

		names = append(names, e.Name())
	}

	sort.Strings(names)

	manifests := make([]unstructured.Unstructured, 0, len(names))
	for _, name := range names {
		data, readErr := os.ReadFile(filepath.Join(manifestsDir, name))
		if readErr != nil {
			return nil, fmt.Errorf("read manifest %s: %w", name, readErr)
		}

		var obj map[string]interface{}
		if err := sigsyaml.Unmarshal(data, &obj); err != nil {
			return nil, fmt.Errorf("unmarshal manifest %s: %w", name, err)
		}

		manifests = append(manifests, unstructured.Unstructured{Object: obj})
	}

	return manifests, nil
}

// childNodeDirs returns the sorted absolute paths of direct child node directories under
// <dir>/snapshots/. Returns nil when the node has no children.
func childNodeDirs(dir string) ([]string, error) {
	snapshotsDir := filepath.Join(dir, archive.SnapshotsDirName)

	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("read snapshots dir %s: %w", snapshotsDir, err)
	}

	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, filepath.Join(snapshotsDir, e.Name()))
		}
	}

	sort.Strings(dirs)

	return dirs, nil
}
