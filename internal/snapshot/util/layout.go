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

package util

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	v1alpha1 "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

// On-disk export bundle layout (also consumed by import). The CLI is status-driven: it never parses
// index.json, it only stores it verbatim and follows the per-node URLs published in CR status.
//
//	<dir>/index.json                  the opaque hierarchy index blob (stored as-is, never parsed)
//	<dir>/view.json                   the stable SnapshotView projection (for `d8 snapshot list --dir`)
//	<dir>/manifests/<nodeID>.json     each node's own manifests
//	<dir>/data/<nodeID>.img           each Block data node's raw volume image
//	<dir>/data/<nodeID>/...           each Filesystem data node's directory tree
const (
	indexFileName    = "index.json"
	viewFileName     = "view.json"
	manifestsDirName = "manifests"
	dataDirName      = "data"
)

// IndexPath returns the opaque index blob path within a bundle dir.
func IndexPath(dir string) string { return filepath.Join(dir, indexFileName) }

// ViewPath returns the SnapshotView projection path within a bundle dir.
func ViewPath(dir string) string { return filepath.Join(dir, viewFileName) }

// ManifestPath returns a node's manifest file path within a bundle dir.
func ManifestPath(dir, nodeID string) string {
	return filepath.Join(dir, manifestsDirName, sanitizeID(nodeID)+".json")
}

// DataPath returns a Block data node's raw image path within a bundle dir.
func DataPath(dir, nodeID string) string {
	return filepath.Join(dir, dataDirName, sanitizeID(nodeID)+".img")
}

// DataDirPath returns a Filesystem data node's directory-tree root within a bundle dir.
func DataDirPath(dir, nodeID string) string {
	return filepath.Join(dir, dataDirName, sanitizeID(nodeID))
}

// EnsureBundleDirs creates the manifests/ and data/ subdirs.
func EnsureBundleDirs(dir string) error {
	for _, sub := range []string{manifestsDirName, dataDirName} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return err
		}
	}
	return nil
}

// WriteFileAtomic writes data to path via a temp file + rename.
func WriteFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func sanitizeID(id string) string {
	return strings.NewReplacer("/", "_", string(os.PathSeparator), "_").Replace(id)
}

// ParseView unmarshals and validates a SnapshotView blob (the stable projection consumed by
// `d8 snapshot list`). Unlike the opaque index, the view IS meant to be parsed by clients.
func ParseView(raw []byte) (*v1alpha1.SnapshotView, error) {
	view := &v1alpha1.SnapshotView{}
	if err := json.Unmarshal(raw, view); err != nil {
		return nil, fmt.Errorf("parse view: %w", err)
	}
	if view.Root.Name == "" {
		return nil, fmt.Errorf("view is empty (no root)")
	}
	return view, nil
}

// RenderView renders a SnapshotView as an indented tree, annotating data nodes with their volume
// metadata. It walks only the stable view shape; the internal index is never consulted.
func RenderView(w io.Writer, view *v1alpha1.SnapshotView) {
	var walk func(n *v1alpha1.SnapshotViewNode, prefix string, root, last bool)
	walk = func(n *v1alpha1.SnapshotViewNode, prefix string, root, last bool) {
		branch := "├── "
		childPrefix := prefix + "│   "
		if last {
			branch = "└── "
			childPrefix = prefix + "    "
		}
		if root {
			branch = ""
		}
		fmt.Fprintf(w, "%s%s%s/%s [%s]%s\n", prefix, branch, n.Kind, n.Name, n.Namespace, viewDataSuffix(n))
		for i := range n.Children {
			walk(&n.Children[i], childPrefix, false, i == len(n.Children)-1)
		}
	}
	walk(&view.Root, "", true, true)
}

func viewDataSuffix(n *v1alpha1.SnapshotViewNode) string {
	if !n.HasData {
		return ""
	}
	parts := []string{}
	if n.VolumeMode != "" {
		parts = append(parts, n.VolumeMode)
	}
	if n.SizeBytes > 0 {
		parts = append(parts, humanBytes(n.SizeBytes))
	}
	if len(parts) == 0 {
		return " (data)"
	}
	return " (data: " + strings.Join(parts, ", ") + ")"
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
