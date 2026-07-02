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

// Package localscan discovers and parses an offline snapshot archive directory
// written by d8 snapshot download. It reads each node's snapshot.yaml and
// returns an in-memory Node tree with identity and volume metadata populated.
// The package is fully offline: it never contacts a cluster.
package localscan

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// Node represents a single snapshot node discovered in an offline archive
// directory tree. Each node corresponds to one directory produced by
// d8 snapshot download, containing a snapshot.yaml and optional child nodes
// under a snapshots/ subdirectory.
type Node struct {
	// Kind is the kind of the snapshot CR recorded in snapshot.yaml
	// (e.g. "Snapshot", "DemoVirtualDiskSnapshot", "VolumeSnapshot").
	Kind string
	// Name is the metadata.name of the snapshot CR.
	Name string
	// Namespace is the namespace of the snapshot CR.
	// Empty for cluster-scoped resources.
	Namespace string
	// Path is the directory path relative to the scanned root directory.
	// The root node has Path == ".".
	Path string
	// Volumes lists the captured PVC volumes owned by this node, as recorded
	// in snapshot.yaml. Empty for aggregator or manifest-only nodes.
	Volumes []archive.VolumeInfo
	// Children are the nested snapshot nodes discovered under this node's
	// snapshots/ subdirectory. Nil when the subdirectory is absent.
	Children []*Node
	// YAML is the full parsed snapshot.yaml for this node.
	YAML archive.SnapshotYAML
}

// VolumeCount returns the total number of captured volumes owned by n and
// all of its descendants. Volume ownership lives in the node that actually
// captured the data (a domain disk/VM-snapshot node or an orphan-PVC leaf),
// never in an aggregator, so a plain len(n.Volumes) on the root undercounts
// any archive whose aggregator itself owns no data.
func (n *Node) VolumeCount() int {
	count := len(n.Volumes)

	for _, child := range n.Children {
		count += child.VolumeCount()
	}

	return count
}

// Scan walks the archive directory rooted at root, reads each node's
// snapshot.yaml via archive.ReadSnapshotYAML, and returns the root Node
// with its nested children tree fully populated.
//
// A missing snapshots/ subdirectory in any node yields zero children for that
// node and is not an error. A non-directory root, or a root whose snapshot.yaml
// cannot be read, yields a wrapped error.
func Scan(root string) (*Node, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("scan root %s: %w", root, err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("scan root %s: not a directory", root)
	}

	return scanDir(root, root)
}

// scanDir reads snapshot.yaml from dir, recursively discovers child nodes
// under dir/snapshots/, and returns the populated Node. root is the top-level
// scan root used to compute relative Path values.
func scanDir(root, dir string) (*Node, error) {
	sy, err := archive.ReadSnapshotYAML(dir)
	if err != nil {
		return nil, fmt.Errorf("read node at %s: %w", dir, err)
	}

	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return nil, fmt.Errorf("relative path for %s: %w", dir, err)
	}

	node := &Node{
		Kind:      sy.Kind,
		Name:      sy.Name,
		Namespace: sy.Namespace,
		Path:      rel,
		Volumes:   sy.Volumes,
		YAML:      sy,
	}

	snapshotsDir := filepath.Join(dir, archive.SnapshotsDirName)

	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return node, nil
		}

		return nil, fmt.Errorf("read snapshots dir %s: %w", snapshotsDir, err)
	}

	node.Children = make([]*Node, 0, len(entries))

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		childDir := filepath.Join(snapshotsDir, entry.Name())

		child, err := scanDir(root, childDir)
		if err != nil {
			return nil, err
		}

		node.Children = append(node.Children, child)
	}

	return node, nil
}
