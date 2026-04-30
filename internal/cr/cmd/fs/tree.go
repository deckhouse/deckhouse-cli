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

package fs

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/imagefs"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/output"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

type treeNode struct {
	name     string
	entry    imagefs.Entry
	isDir    bool
	children map[string]*treeNode
}

func newTreeCmd(opts *registry.Options) *cobra.Command {
	var (
		maxDepth  int
		dirsFirst bool
		showSize  bool
	)
	cmd := &cobra.Command{
		Use:   "tree IMAGE [PATH]",
		Short: "Show the filesystem of a container image as a tree",
		Long: `Render the filesystem of a container image as a tree.

PATH (if given) becomes the tree root and is normalized the same way as in
"fs ls" (leading "./" or "/" stripped, "..", trailing "/" cleaned).

The merged filesystem is rendered.`,
		Args:              cobra.RangeArgs(1, 2),
		ValidArgsFunction: completion.ImageThenInImagePath(),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref := args[0]
			root := ""
			if len(args) == 2 {
				root = imagefs.NormalizeScopePath(args[1])
				if root == "." {
					root = ""
				}
			}

			img, err := registry.Fetch(cmd.Context(), ref, opts)
			if err != nil {
				return err
			}

			entries, err := imagefs.MergedFS(img)
			if err != nil {
				return err
			}

			tree := buildTree(entries, root)
			rootLabel := "/"
			if root != "" {
				rootLabel = "/" + root
			}
			return writeTreeText(cmd.OutOrStdout(), tree, rootLabel, maxDepth, dirsFirst, showSize)
		},
	}
	cmd.Flags().IntVarP(&maxDepth, "depth", "L", 0, "Max depth to descend (0 = unlimited)")
	cmd.Flags().BoolVar(&dirsFirst, "dirsfirst", false, "List directories before files")
	cmd.Flags().BoolVar(&showSize, "size", false, "Show file sizes (human-readable: B / KB / MB)")
	return cmd
}

func buildTree(entries []imagefs.Entry, root string) *treeNode {
	tree := &treeNode{name: root, isDir: true, children: make(map[string]*treeNode)}
	for _, e := range entries {
		if e.Type == imagefs.TypeWhiteout {
			continue
		}
		rel := e.Path
		if root != "" {
			if rel == root {
				tree.entry = e
				continue
			}
			prefix := root + "/"
			if !strings.HasPrefix(rel, prefix) {
				continue
			}
			rel = strings.TrimPrefix(rel, prefix)
		}
		insert(tree, rel, e)
	}
	return tree
}

func insert(parent *treeNode, rel string, entry imagefs.Entry) {
	parts := strings.Split(rel, "/")
	cur := parent
	for i, part := range parts {
		if part == "" {
			continue
		}
		child, ok := cur.children[part]
		if !ok {
			child = &treeNode{name: part, children: make(map[string]*treeNode)}
			cur.children[part] = child
		}
		// isDir grows monotonically: a node that has been observed as a
		// directory (either via a Dir entry, or because it has descendants)
		// must stay a directory even if a later, conflicting entry with the
		// same path claims to be a regular file. Without this guard, an
		// unsorted input or a malformed tar carrying both "etc" (file) and
		// "etc/passwd" (file under it) could flip "etc" back to non-dir
		// after its children were registered, hiding the subtree.
		isDirNow := i < len(parts)-1 || entry.IsDir()
		child.isDir = child.isDir || isDirNow
		if i == len(parts)-1 {
			child.entry = entry
		}
		cur = child
	}
}

func writeTreeText(w io.Writer, tree *treeNode, rootLabel string, maxDepth int, dirsFirst, showSize bool) error {
	if _, err := fmt.Fprintln(w, rootLabel); err != nil {
		return err
	}
	return writeSubtree(w, tree, "", 1, maxDepth, dirsFirst, showSize)
}

func writeSubtree(w io.Writer, node *treeNode, prefix string, depth, maxDepth int, dirsFirst, showSize bool) error {
	if maxDepth > 0 && depth > maxDepth {
		return nil
	}
	names := make([]string, 0, len(node.children))
	for n := range node.children {
		names = append(names, n)
	}
	sortChildren(names, node.children, dirsFirst)

	for i, name := range names {
		child := node.children[name]
		isLast := i == len(names)-1
		branch := "├── "
		nextPrefix := prefix + "│   "
		if isLast {
			branch = "└── "
			nextPrefix = prefix + "    "
		}
		displayName := name
		if child.isDir {
			displayName += "/"
		}
		line := prefix + branch + displayName
		if !child.isDir && showSize {
			line += "  " + output.HumanSize(child.entry.Size)
		}
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
		if child.isDir {
			if err := writeSubtree(w, child, nextPrefix, depth+1, maxDepth, dirsFirst, showSize); err != nil {
				return err
			}
		}
	}
	return nil
}

func sortChildren(names []string, children map[string]*treeNode, dirsFirst bool) {
	sort.Slice(names, func(i, j int) bool {
		ni, nj := names[i], names[j]
		if dirsFirst {
			di, dj := children[ni].isDir, children[nj].isDir
			if di != dj {
				return di
			}
		}
		return ni < nj
	})
}
