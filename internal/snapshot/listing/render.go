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

package listing

import (
	"encoding/json"
	"fmt"
	"io"

	sigsyaml "sigs.k8s.io/yaml"
)

const (
	FormatHuman = "human"
	FormatJSON  = "json"
	FormatYAML  = "yaml"
)

// Render writes the tree to w in the requested format (human, json, yaml).
func Render(w io.Writer, t *Tree, format string) error {
	switch format {
	case FormatJSON:
		return renderJSON(w, t)
	case FormatYAML:
		return renderYAML(w, t)
	default:
		return renderHuman(w, t)
	}
}

func renderJSON(w io.Writer, t *Tree) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return enc.Encode(t)
}

func renderYAML(w io.Writer, t *Tree) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal for yaml: %w", err)
	}

	out, err := sigsyaml.JSONToYAML(data)
	if err != nil {
		return fmt.Errorf("convert to yaml: %w", err)
	}

	_, err = w.Write(out)

	return err
}

func renderHuman(w io.Writer, t *Tree) error {
	switch t.Source.Kind {
	case "cluster":
		fmt.Fprintf(w, "Source:    cluster\n")
		fmt.Fprintf(w, "Cluster:   %s\n", t.Source.Cluster)
		fmt.Fprintf(w, "Namespace: %s\n", t.Source.Namespace)
		fmt.Fprintf(w, "Snapshot:  %s\n", t.Source.Snapshot)
	default:
		fmt.Fprintf(w, "Source:    archive\n")
		fmt.Fprintf(w, "Directory: %s\n", t.Source.ArchiveDir)
		fmt.Fprintf(w, "ArchiveID: %s\n", t.Source.ArchiveID)

		if t.Complete {
			fmt.Fprintf(w, "Complete:  yes\n")
		} else {
			fmt.Fprintf(w, "Complete:  no (COMPLETE sentinel absent)\n")
		}
	}

	fmt.Fprintf(w, "Selection: %s\n", t.Selection)
	fmt.Fprintln(w)

	if t.Root != nil {
		printNodeHuman(w, t.Root, "", true, true)
	}

	return nil
}

// connector returns the tree branch glyph for a sibling entry.
func connector(isLast bool) string {
	if isLast {
		return "└─ "
	}

	return "├─ "
}

// continuation returns the vertical-line prefix used under a sibling entry.
func continuation(isLast bool) string {
	if isLast {
		return "   "
	}

	return "│  "
}

// printNodeHuman recursively renders a node using box-drawing connectors.
// prefix is the accumulated column string from parent levels.
// isLast is true when this node is the last sibling in its parent's list.
// isRoot is true only for the top-level node, which prints without a connector.
// The model is pre-deduped by dedupTree, so nv.Objects is iterated directly.
func printNodeHuman(w io.Writer, nv *NodeView, prefix string, isLast, isRoot bool) {
	countStr := ""
	if nv.ObjectCount >= 0 {
		countStr = fmt.Sprintf("  (%d objects)", nv.ObjectCount)
	}

	if isRoot {
		fmt.Fprintf(w, "%s  %s/%s%s\n", nv.ID, nv.Kind, nv.Name, countStr)
	} else {
		fmt.Fprintf(w, "%s%s%s  %s/%s%s\n", prefix, connector(isLast), nv.ID, nv.Kind, nv.Name, countStr)
	}

	childPrefix := prefix
	if !isRoot {
		childPrefix += continuation(isLast)
	}

	total := len(nv.Objects) + len(nv.Children)
	idx := 0

	for _, obj := range nv.Objects {
		idx++

		name := obj.Name
		if obj.Namespace != "" {
			name = obj.Namespace + "/" + obj.Name
		}

		fmt.Fprintf(w, "%s%s%s %s %s\n", childPrefix, connector(idx == total), obj.APIVersion, obj.Kind, name)
	}

	for _, child := range nv.Children {
		idx++
		printNodeHuman(w, child, childPrefix, idx == total, false)
	}
}
