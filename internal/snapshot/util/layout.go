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
	"sort"
	"strings"

	v1alpha1 "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
)

// On-disk export bundle layout (also consumed by import):
//
//	<dir>/index.json                  the hierarchy index
//	<dir>/manifests/<nodeID>.json     each node's own manifests
//	<dir>/data/<nodeID>.img           each data node's block-volume image
const (
	indexFileName    = "index.json"
	manifestsDirName = "manifests"
	dataDirName      = "data"
)

// IndexPath returns the index file path within a bundle dir.
func IndexPath(dir string) string { return filepath.Join(dir, indexFileName) }

// ManifestPath returns a node's manifest file path within a bundle dir.
func ManifestPath(dir, nodeID string) string {
	return filepath.Join(dir, manifestsDirName, sanitizeID(nodeID)+".json")
}

// DataPath returns a data node's block image path within a bundle dir.
func DataPath(dir, nodeID string) string {
	return filepath.Join(dir, dataDirName, sanitizeID(nodeID)+".img")
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

// ReadIndexFile reads and parses the bundle index.
func ReadIndexFile(dir string) (*v1alpha1.Index, error) {
	raw, err := os.ReadFile(IndexPath(dir))
	if err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}
	return ParseIndex(raw)
}

// ParseIndex unmarshals and validates an index blob.
func ParseIndex(raw []byte) (*v1alpha1.Index, error) {
	idx := &v1alpha1.Index{}
	if err := json.Unmarshal(raw, idx); err != nil {
		return nil, fmt.Errorf("parse index: %w", err)
	}
	if idx.Version == "" || len(idx.Snapshots) == 0 {
		return nil, fmt.Errorf("index is empty or missing version")
	}
	if idx.Version != v1alpha1.IndexVersion {
		return nil, fmt.Errorf("unsupported index version %q (this CLI understands %q)", idx.Version, v1alpha1.IndexVersion)
	}
	return idx, nil
}

func sanitizeID(id string) string {
	return strings.NewReplacer("/", "_", string(os.PathSeparator), "_").Replace(id)
}

// PrintTree renders the snapshot hierarchy as an indented tree, annotating data nodes with their
// volume metadata. PrintArchive renders the same content as a flat, machine-friendly listing.
func PrintTree(w io.Writer, idx *v1alpha1.Index) {
	byID := map[string]*v1alpha1.IndexSnapshot{}
	for i := range idx.Snapshots {
		byID[idx.Snapshots[i].ID] = &idx.Snapshots[i]
	}
	var walk func(id, prefix string, last bool)
	walk = func(id, prefix string, last bool) {
		n := byID[id]
		if n == nil {
			return
		}
		branch := "├── "
		childPrefix := prefix + "│   "
		if last {
			branch = "└── "
			childPrefix = prefix + "    "
		}
		if prefix == "" {
			branch = ""
		}
		fmt.Fprintf(w, "%s%s%s/%s [%s]%s\n", prefix, branch, n.Kind, n.Name, n.Namespace, dataSuffix(n))
		children := append([]string(nil), n.Children...)
		sort.Strings(children)
		for i, c := range children {
			walk(c, childPrefix, i == len(children)-1)
		}
	}
	walk(idx.RootSnapshot.ID, "", true)
}

// PrintArchive renders a flat per-node listing.
func PrintArchive(w io.Writer, idx *v1alpha1.Index) {
	fmt.Fprintf(w, "version: %s\nroot: %s\n", idx.Version, idx.RootSnapshot.ID)
	for i := range idx.Snapshots {
		n := &idx.Snapshots[i]
		fmt.Fprintf(w, "- %s\t%s/%s [%s]%s\n", n.ID, n.Kind, n.Name, n.Namespace, dataSuffix(n))
	}
}

func dataSuffix(n *v1alpha1.IndexSnapshot) string {
	if !n.HasData || n.Data == nil {
		return ""
	}
	d := n.Data
	parts := []string{}
	if d.VolumeMode != "" {
		parts = append(parts, d.VolumeMode)
	}
	if d.StorageClassName != "" {
		parts = append(parts, "sc="+d.StorageClassName)
	}
	if d.Size > 0 {
		parts = append(parts, humanBytes(d.Size))
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
