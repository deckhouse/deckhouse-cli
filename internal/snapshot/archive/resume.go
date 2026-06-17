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
	"regexp"
	"sort"
	"strconv"
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

	// NodeStateFSPartial means the data/ directory exists but snapshot.yaml is
	// absent.  Already-present per-file .zst files will be skipped by the
	// filesystem downloader.
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
	Name       string
	Namespace  string
	SourceRef  string
}

// NodeResumePlan is the result of scanning one planned node on disk.
type NodeResumePlan struct {
	// TargetDir is the absolute path to use for this node.  For a collision-
	// redirected node this will be CollisionNodeDir(...) rather than the primary
	// directory.
	TargetDir string

	// State describes the on-disk condition found during scanning.
	State NodeState

	// PresentChunkIndices lists the zero-based indices for which chunk_NNNNN.zst
	// already exists inside BlockChunksDirName.  Populated only when State is
	// NodeStateBlockPartial; nil otherwise.
	PresentChunkIndices []int
}

// chunkNameRe matches "chunk_NNNNN.zst" and captures the five-digit decimal index.
var chunkNameRe = regexp.MustCompile(`^chunk_(\d{5})\.zst$`)

// ScanNode inspects parentDir for an existing node directory whose name is
// NodeDirName(id.Kind, id.Name), removes any stale *.tmp files, and returns a
// NodeResumePlan describing the on-disk state for the planned node.
//
// Collision rule: if the primary directory is complete (VerifyNode passes) but
// its stored identity does not match id, the primary directory belongs to a
// different node.  ScanNode returns NodeStatePending with TargetDir set to
// CollisionNodeDir(parentDir, id.Kind, id.Name, short), where short is derived
// from the existing complete node's checksum.  This prevents the pipeline from
// overwriting unrelated completed data.
func ScanNode(parentDir string, id NodeIdentity) (NodeResumePlan, error) {
	primaryDir := filepath.Join(parentDir, NodeDirName(id.Kind, id.Name))

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
	collisionDir := CollisionNodeDir(parentDir, id.Kind, id.Name, short)

	return NodeResumePlan{TargetDir: collisionDir, State: NodeStatePending}, nil
}

// classifyPartialDir classifies an existing but incomplete node directory.
func classifyPartialDir(primaryDir string) (NodeResumePlan, error) {
	chunkDir := filepath.Join(primaryDir, BlockChunksDirName)

	if _, err := os.Stat(chunkDir); err == nil {
		indices, err := presentChunkIndices(chunkDir)
		if err != nil {
			return NodeResumePlan{}, err
		}

		return NodeResumePlan{
			TargetDir:           primaryDir,
			State:               NodeStateBlockPartial,
			PresentChunkIndices: indices,
		}, nil
	}

	dataDir := filepath.Join(primaryDir, DataDirName)

	if _, err := os.Stat(dataDir); err == nil {
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

// presentChunkIndices scans chunkDir for files named chunk_NNNNN.zst and
// returns the sorted list of their zero-based indices.
func presentChunkIndices(chunkDir string) ([]int, error) {
	entries, err := os.ReadDir(chunkDir)
	if err != nil {
		return nil, fmt.Errorf("read chunk dir %s: %w", chunkDir, err)
	}

	indices := make([]int, 0, len(entries))

	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		m := chunkNameRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}

		idx, parseErr := strconv.Atoi(m[1])
		if parseErr != nil {
			continue
		}

		indices = append(indices, idx)
	}

	sort.Ints(indices)

	return indices, nil
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
