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

// Package treeview renders a generic node tree to an io.Writer as an indented
// tree(1)-style ASCII tree using box-drawing connectors.
// It has no dependency on the snapshot source model, the cluster, or the filesystem.
package treeview

import (
	"fmt"
	"io"
)

// Node is an input-agnostic tree node. Callers map their domain model into this
// struct; the renderer imposes no dependency on k8s or internal snapshot packages.
//
// Rendering order under any parent: Children are emitted first (in slice order),
// then Volumes (in slice order). All entries share the same last-sibling connector.
type Node struct {
	// Label is the display text for this node.
	Label string

	// Children are the direct child sub-trees rendered under this node.
	Children []Node

	// Volumes are leaf string entries rendered after Children under this node.
	Volumes []string
}

// tree(1)-style box-drawing connector and continuation prefix strings.
const (
	connMid  = "├── "
	connLast = "└── "
	contMid  = "│   "
	contLast = "    "
)

// Render writes the tree rooted at root to w using tree(1)-style box-drawing
// connectors. The root label is printed with no connector; nested entries use
// ├── / └── connectors with │ / space continuation indentation based on depth
// and last-sibling position. Output is deterministic for a given input.
func Render(w io.Writer, root Node) error {
	if _, err := fmt.Fprintln(w, root.Label); err != nil {
		return fmt.Errorf("writing root label: %w", err)
	}

	return renderEntries(w, root, "")
}

// renderEntries writes all children (sub-trees) and volumes (leaf strings) of n
// to w with the given indent prefix. Children appear before volumes; last-sibling
// state is derived from the combined count so connectors are consistent across
// both groups.
func renderEntries(w io.Writer, n Node, prefix string) error {
	total := len(n.Children) + len(n.Volumes)

	for i, child := range n.Children {
		conn, cont := lineStyle(i == total-1)

		if _, err := fmt.Fprintln(w, prefix+conn+child.Label); err != nil {
			return fmt.Errorf("writing child label: %w", err)
		}

		if err := renderEntries(w, child, prefix+cont); err != nil {
			return err
		}
	}

	for j, vol := range n.Volumes {
		conn, _ := lineStyle(len(n.Children)+j == total-1)

		if _, err := fmt.Fprintln(w, prefix+conn+vol); err != nil {
			return fmt.Errorf("writing volume label: %w", err)
		}
	}

	return nil
}

// lineStyle returns the connector and continuation prefix for a tree entry.
// isLast is true when the entry is the last sibling among children+volumes.
func lineStyle(isLast bool) (string, string) {
	if isLast {
		return connLast, contLast
	}

	return connMid, contMid
}
