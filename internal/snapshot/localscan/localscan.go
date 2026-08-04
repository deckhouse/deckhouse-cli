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

const (
	defaultScanMaxDepth = 64
	defaultScanMaxNodes = 10_000

	// maxReportedSkippedDirs bounds how many skipped-directory paths the reporting scan
	// variant retains for the caller's warning log. SkippedDirs.Total is never truncated,
	// only the Paths sample used for the human-readable message.
	maxReportedSkippedDirs = 32
)

// ErrScanBudget is returned when an archive tree exceeds a configured scan limit.
var ErrScanBudget = errors.New("local snapshot scan budget exceeded")

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
	return ScanWithLimits(root, DefaultScanLimits())
}

// ScanVerified walks an archive with the default traversal limits and verifies every node's
// content checksum and structural metadata before returning the tree.
func ScanVerified(root string) (*Node, error) {
	return ScanVerifiedWithOptions(root, archive.SnapshotYAMLReadOptions{})
}

// ScanVerifiedWithOptions verifies every node under an explicit snapshot.yaml compatibility policy.
func ScanVerifiedWithOptions(
	root string,
	options archive.SnapshotYAMLReadOptions,
) (*Node, error) {
	return ScanVerifiedWithLimitsAndOptions(root, DefaultScanLimits(), options)
}

// SkippedDirs reports archive child directories a scan skipped because they carry no
// snapshot.yaml — leftovers of an interrupted-and-resumed download redirected to a collision
// path (see archive.CollisionNodeDir), not scannable nodes. Paths is truncated to
// maxReportedSkippedDirs; Total counts every skip regardless of truncation.
type SkippedDirs struct {
	Paths []string
	Total int
}

// ScanVerifiedWithOptionsReportingSkips verifies every node under an explicit snapshot.yaml
// compatibility policy exactly like ScanVerifiedWithOptions, additionally reporting archive
// child directories skipped for carrying no snapshot.yaml (see treeScanner.recordSkippedDir).
func ScanVerifiedWithOptionsReportingSkips(
	root string,
	options archive.SnapshotYAMLReadOptions,
) (*Node, SkippedDirs, error) {
	return scanReportingSkips(root, DefaultScanLimits(), options, true)
}

// ScanVerifiedWithLimitsAndOptions verifies every node subject to explicit traversal limits
// and snapshot.yaml compatibility policy.
func ScanVerifiedWithLimitsAndOptions(
	root string,
	limits ScanLimits,
	options archive.SnapshotYAMLReadOptions,
) (*Node, error) {
	return scan(root, limits, options, true)
}

// ScanWithOptions scans an archive under an explicit snapshot.yaml compatibility policy.
func ScanWithOptions(
	root string,
	options archive.SnapshotYAMLReadOptions,
) (*Node, error) {
	return ScanWithLimitsAndOptions(root, DefaultScanLimits(), options)
}

// DefaultScanLimits returns the traversal limits used by Scan: at most
// 10,000 nodes and 64 child directories below the root.
func DefaultScanLimits() ScanLimits {
	return ScanLimits{
		MaxDepth: defaultScanMaxDepth,
		MaxNodes: defaultScanMaxNodes,
	}
}

// ScanLimits bounds the number and depth of archive nodes visited by ScanWithLimits.
// The root is at depth zero and counts toward MaxNodes.
type ScanLimits struct {
	MaxDepth int
	MaxNodes int
}

// ScanWithLimits scans an archive tree subject to explicit traversal limits.
func ScanWithLimits(root string, limits ScanLimits) (*Node, error) {
	return ScanWithLimitsAndOptions(root, limits, archive.SnapshotYAMLReadOptions{})
}

// ScanWithLimitsAndOptions scans with explicit traversal and snapshot.yaml compatibility policy.
func ScanWithLimitsAndOptions(
	root string,
	limits ScanLimits,
	options archive.SnapshotYAMLReadOptions,
) (*Node, error) {
	return scan(root, limits, options, false)
}

func scan(
	root string,
	limits ScanLimits,
	options archive.SnapshotYAMLReadOptions,
	verifyIntegrity bool,
) (*Node, error) {
	node, _, err := scanReportingSkips(root, limits, options, verifyIntegrity)

	return node, err
}

func scanReportingSkips(
	root string,
	limits ScanLimits,
	options archive.SnapshotYAMLReadOptions,
	verifyIntegrity bool,
) (*Node, SkippedDirs, error) {
	if limits.MaxDepth < 0 {
		return nil, SkippedDirs{}, fmt.Errorf("local snapshot scan maxDepth must be non-negative: %w", ErrScanBudget)
	}

	if limits.MaxNodes <= 0 {
		return nil, SkippedDirs{}, fmt.Errorf("local snapshot scan maxNodes must be positive: %w", ErrScanBudget)
	}

	info, err := os.Stat(root)
	if err != nil {
		return nil, SkippedDirs{}, fmt.Errorf("scan root %s: %w", root, err)
	}

	if !info.IsDir() {
		return nil, SkippedDirs{}, fmt.Errorf("scan root %s: not a directory", root)
	}

	scanner := treeScanner{
		root:                root,
		limits:              limits,
		snapshotReadOptions: options,
		verifyIntegrity:     verifyIntegrity,
	}

	node, err := scanner.scanDir(root, 0)
	if err != nil {
		return nil, SkippedDirs{}, err
	}

	return node, scanner.skips(), nil
}

type treeScanner struct {
	root                string
	limits              ScanLimits
	snapshotReadOptions archive.SnapshotYAMLReadOptions
	verifyIntegrity     bool
	nodeCount           int
	skippedDirs         []string
	skippedDirsTotal    int
}

// recordSkippedDir records a child directory skipped for carrying no snapshot.yaml. The
// sample kept for the caller's warning log is bounded by maxReportedSkippedDirs; the total
// count is not, so a warning can always report exactly how many were skipped.
func (s *treeScanner) recordSkippedDir(path string) {
	s.skippedDirsTotal++

	if len(s.skippedDirs) < maxReportedSkippedDirs {
		s.skippedDirs = append(s.skippedDirs, path)
	}
}

// skips returns the skipped-directory report accumulated over the traversal so far.
func (s *treeScanner) skips() SkippedDirs {
	return SkippedDirs{Paths: s.skippedDirs, Total: s.skippedDirsTotal}
}

// scanDir reads snapshot.yaml from dir and discovers child nodes under dir/snapshots/.
func (s *treeScanner) scanDir(dir string, depth int) (*Node, error) {
	if depth > s.limits.MaxDepth {
		return nil, fmt.Errorf(
			"local snapshot scan maxDepth %d exceeded at %s (depth %d; root depth is 0): %w",
			s.limits.MaxDepth,
			dir,
			depth,
			ErrScanBudget,
		)
	}

	if s.nodeCount >= s.limits.MaxNodes {
		return nil, fmt.Errorf(
			"local snapshot scan maxNodes %d exceeded while adding %s: %w",
			s.limits.MaxNodes,
			dir,
			ErrScanBudget,
		)
	}

	s.nodeCount++

	sy, err := archive.ReadSnapshotYAMLWithOptions(dir, s.snapshotReadOptions)
	if err != nil {
		return nil, fmt.Errorf("read node at %s: %w", dir, err)
	}

	if s.verifyIntegrity {
		if err := archive.VerifyNodeWithOptions(dir, s.snapshotReadOptions); err != nil {
			return nil, fmt.Errorf("verify node at %s: %w", dir, err)
		}

		if err := archive.ValidateNodeMetadataWithOptions(dir, s.snapshotReadOptions); err != nil {
			return nil, fmt.Errorf("validate node metadata at %s: %w", dir, err)
		}
	}

	rel, err := filepath.Rel(s.root, dir)
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

		// A child directory with no snapshot.yaml is not a finalized node: it is a
		// collision/resume directory archive.CollisionNodeDir left behind for an
		// interrupted-and-resumed download (see archive/resume.go). Lstat, never Stat,
		// so a symlinked snapshot.yaml is NOT treated as present here — scanDir still
		// recurses into it and its no-follow RootedSource open fails as a non-regular
		// artifact, rather than the entry being silently skipped.
		if _, statErr := os.Lstat(filepath.Join(childDir, archive.SnapshotYAMLName)); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				s.recordSkippedDir(childDir)

				continue
			}

			return nil, fmt.Errorf("check snapshot.yaml presence in %s: %w", childDir, statErr)
		}

		child, err := s.scanDir(childDir, depth+1)
		if err != nil {
			return nil, err
		}

		node.Children = append(node.Children, child)
	}

	return node, nil
}
