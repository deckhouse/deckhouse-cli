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
	"errors"
	"fmt"
)

// ErrNodeNotFound is returned by FindNode when no node in the tree matches the
// supplied kind and name.
var ErrNodeNotFound = errors.New("node not found in snapshot tree")

// ErrAmbiguousNode is returned by FindNode when more than one node in the tree
// matches the supplied kind and name.
var ErrAmbiguousNode = errors.New("ambiguous node: multiple nodes match kind and name")

// FindNode searches the tree rooted at root for the unique node whose Kind and Name
// both match. It returns the node and the ordered ancestor chain from the root down to
// the node's parent (nil when the match is the root itself).
//
// For domain snapshot nodes Name is the snapshot CR's metadata.name (e.g. nss-child-…).
// For VolumeSnapshot orphan leaf nodes Kind is "VolumeSnapshot" and Name is the captured
// PVC name (Node.Name == dataRef.Target.Name set by BuildTree).
//
// Returns ErrNodeNotFound when no node matches; returns ErrAmbiguousNode when more than
// one node matches.
//
// FindNode operates solely on the in-memory tree; it never fetches from the cluster.
func FindNode(root *Node, kind, name string) (*Node, []*Node, error) {
	var matches []*Node

	collectMatches(root, kind, name, &matches)

	switch len(matches) {
	case 0:
		return nil, nil, fmt.Errorf("kind=%s name=%s: %w", kind, name, ErrNodeNotFound)
	case 1:
		n := matches[0]
		return n, buildAncestorChain(n), nil
	default:
		return nil, nil, fmt.Errorf("kind=%s name=%s (%d matches): %w", kind, name, len(matches), ErrAmbiguousNode)
	}
}

// collectMatches does a DFS and appends every node whose Kind and Name match to out.
func collectMatches(n *Node, kind, name string, out *[]*Node) {
	if n.Kind == kind && n.Name == name {
		*out = append(*out, n)
	}

	for _, child := range n.Children {
		collectMatches(child, kind, name, out)
	}
}

// buildAncestorChain returns the ordered list of ancestors from the root down to
// n.Parent. Returns nil when n is the root (n.Parent == nil).
func buildAncestorChain(n *Node) []*Node {
	if n.Parent == nil {
		return nil
	}

	var rev []*Node

	for cur := n.Parent; cur != nil; cur = cur.Parent {
		rev = append(rev, cur)
	}

	// rev is [parent, grandparent, …, root]; reverse for [root, …, parent].
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}

	return rev
}
