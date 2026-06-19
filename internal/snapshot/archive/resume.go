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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// NodeState classifies the on-disk progress of a planned node directory.
type NodeState int

const (
	// NodeStatePending means the node directory does not yet exist or holds no
	// usable progress.  The pipeline should start the node from scratch.
	NodeStatePending NodeState = iota

	// NodeStateBlockPartial means the chunk directory (BlockChunksDirName) exists
	// and at least some chunk files may be present.  The block downloader should
	// skip already-present chunks and resume from where it left off.
	NodeStateBlockPartial

	// NodeStateFSPartial means the flat FS tar staging dir (FsTarStagingDirName) or
	// the multi-volume data/ directory exists but snapshot.yaml is absent.
	// The filesystem downloader will skip already-staged raw files and resume assembly.
	NodeStateFSPartial

	// NodeStateManifestsOnly means manifests/ (or data.img.zst) is present but
	// there is no volume download in progress and no snapshot.yaml.  The pipeline
	// should call FinalizeNode once volume data is ready.
	NodeStateManifestsOnly

	// NodeStateDone means snapshot.yaml exists and VerifyNode passes.  The
	// pipeline should skip this node entirely.
	NodeStateDone
)

// NodeIdentity describes the planned identity of a snapshot node.  It is used
// for collision detection: if a complete primary directory holds data for a
// different identity, the new node is redirected to a collision-suffixed path.
type NodeIdentity struct {
	APIVersion string
	Kind       string
	// Name is the CR metadata.name used for resume identity matching (stored in
	// snapshot.yaml and compared by matchesIdentity). It is NOT the on-disk dir name.
	Name string
	// DirName is the on-disk directory-name component: NodeDirName(Kind, DirName).
	// For domain snapshot nodes it is the source-ref .name (the captured object name);
	// for orphan leaf volume nodes it is the captured PVC name.
	// When empty, Name is used as the fallback (root nodes that use ScanAbsolute
	// with a user-supplied path and domain nodes without a source annotation).
	DirName   string
	Namespace string
	SourceRef string
}

// NodeResumePlan is the result of scanning one planned node on disk.
type NodeResumePlan struct {
	// TargetDir is the absolute path to use for this node.  For a collision-
	// redirected node this will be CollisionNodeDir(...) rather than the primary
	// directory.
	TargetDir string

	// State describes the on-disk condition found during scanning.
	State NodeState
}

// nodeDirComponent returns the directory-name component for id.
// It uses id.DirName when set and falls back to id.Name (for nodes without a
// source annotation and for backward compatibility with code that does not set DirName).
func nodeDirComponent(id NodeIdentity) string {
	if id.DirName != "" {
		return id.DirName
	}

	return id.Name
}

// ScanNode inspects parentDir for an existing node directory whose name is
// NodeDirName(id.Kind, nodeDirComponent(id)), removes any stale *.tmp files,
// and returns a NodeResumePlan describing the on-disk state for the planned node.
//
// The directory name is derived from id.DirName (the source object name) when set,
// falling back to id.Name (the CR name) for nodes without a source annotation.
// Identity matching (matchesIdentity) still uses id.Name and id.SourceRef, which
// are the values written into snapshot.yaml.
//
// Collision rule: if the primary directory is complete (VerifyNode passes) but
// its stored identity does not match id, the primary directory belongs to a
// different node.  ScanNode returns NodeStatePending with TargetDir set to
// CollisionNodeDir(parentDir, id.Kind, nodeDirComponent(id), short), where short
// is derived from the existing complete node's checksum.  This prevents the
// pipeline from overwriting unrelated completed data.
func ScanNode(parentDir string, id NodeIdentity) (NodeResumePlan, error) {
	primaryDir := filepath.Join(parentDir, NodeDirName(id.Kind, nodeDirComponent(id)))

	_, statErr := os.Stat(primaryDir)
	if errors.Is(statErr, os.ErrNotExist) {
		return NodeResumePlan{TargetDir: primaryDir, State: NodeStatePending}, nil
	}

	if statErr != nil {
		return NodeResumePlan{}, fmt.Errorf("stat node dir %s: %w", primaryDir, statErr)
	}

	if err := removeTmpFiles(primaryDir); err != nil {
		return NodeResumePlan{}, err
	}

	if verifyErr := VerifyNode(primaryDir); verifyErr == nil {
		return classifyCompleteDir(parentDir, primaryDir, id)
	}

	return classifyPartialDir(primaryDir)
}

// classifyCompleteDir handles the case where the primary directory passes
// VerifyNode.  It checks identity and may redirect to a collision path.
func classifyCompleteDir(parentDir, primaryDir string, id NodeIdentity) (NodeResumePlan, error) {
	sy, err := ReadSnapshotYAML(primaryDir)
	if err != nil {
		return NodeResumePlan{}, fmt.Errorf("read snapshot.yaml in %s: %w", primaryDir, err)
	}

	if matchesIdentity(sy, id) {
		return NodeResumePlan{TargetDir: primaryDir, State: NodeStateDone}, nil
	}

	// Primary dir is complete but belongs to a different node.
	// Redirect the new node to a stable collision-suffixed path so the existing
	// complete data is not overwritten.
	short := ShortChecksum(sy.Checksum.Hex)
	collisionDir := CollisionNodeDir(parentDir, id.Kind, nodeDirComponent(id), short)

	return NodeResumePlan{TargetDir: collisionDir, State: NodeStatePending}, nil
}

// classifyPartialDir classifies an existing but incomplete node directory.
func classifyPartialDir(primaryDir string) (NodeResumePlan, error) {
	// Block chunk staging dir (single-volume block download in progress).
	if _, err := os.Stat(filepath.Join(primaryDir, BlockChunksDirName)); err == nil {
		return NodeResumePlan{TargetDir: primaryDir, State: NodeStateBlockPartial}, nil
	}

	// Flat FS tar staging dir (single-volume filesystem download in progress).
	if _, err := os.Stat(filepath.Join(primaryDir, FsTarStagingDirName)); err == nil {
		return NodeResumePlan{TargetDir: primaryDir, State: NodeStateFSPartial}, nil
	}

	// Multi-volume data/ directory (multi-volume layout, block or FS).
	if _, err := os.Stat(filepath.Join(primaryDir, DataDirName)); err == nil {
		return NodeResumePlan{TargetDir: primaryDir, State: NodeStateFSPartial}, nil
	}

	return NodeResumePlan{TargetDir: primaryDir, State: NodeStateManifestsOnly}, nil
}

// matchesIdentity reports whether the stored SnapshotYAML identity equals id.
func matchesIdentity(sy SnapshotYAML, id NodeIdentity) bool {
	return sy.APIVersion == id.APIVersion &&
		sy.Kind == id.Kind &&
		sy.Name == id.Name &&
		sy.Namespace == id.Namespace &&
		sy.SourceRef == id.SourceRef
}

// ErrIdentityMismatch is returned by ScanAbsolute when the target directory
// contains a complete snapshot whose stored identity does not match the planned node.
// The caller must choose a different output path rather than overwriting the data.
var ErrIdentityMismatch = errors.New("output directory belongs to a different snapshot")

// ScanAbsolute classifies the on-disk state of an absolute node directory path,
// removing stale *.tmp files.  Unlike ScanNode it does not derive the path from
// a parent directory + NodeDirName convention, and it does not redirect to a
// collision-suffixed path on identity mismatch.  Instead it returns ErrIdentityMismatch
// so the caller can abort and ask the user to choose a different output path.
//
// Suitable for the root output directory where the path name is user-controlled.
func ScanAbsolute(nodeDir string, id NodeIdentity) (NodeResumePlan, error) {
	_, statErr := os.Stat(nodeDir)

	if errors.Is(statErr, os.ErrNotExist) {
		return NodeResumePlan{TargetDir: nodeDir, State: NodeStatePending}, nil
	}

	if statErr != nil {
		return NodeResumePlan{}, fmt.Errorf("stat %s: %w", nodeDir, statErr)
	}

	if err := removeTmpFiles(nodeDir); err != nil {
		return NodeResumePlan{}, err
	}

	if verifyErr := VerifyNode(nodeDir); verifyErr == nil {
		sy, err := ReadSnapshotYAML(nodeDir)
		if err != nil {
			return NodeResumePlan{}, fmt.Errorf("read snapshot.yaml in %s: %w", nodeDir, err)
		}

		if !matchesIdentity(sy, id) {
			return NodeResumePlan{}, fmt.Errorf("%w: %s contains %s/%s, expected %s/%s",
				ErrIdentityMismatch, nodeDir, sy.Kind, sy.Name, id.Kind, id.Name)
		}

		return NodeResumePlan{TargetDir: nodeDir, State: NodeStateDone}, nil
	}

	return classifyPartialDir(nodeDir)
}

// removeTmpFiles deletes every *.tmp file found anywhere under dir.
func removeTmpFiles(dir string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}

		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return fmt.Errorf("remove stale tmp %s: %w", path, removeErr)
		}

		return nil
	})
}
