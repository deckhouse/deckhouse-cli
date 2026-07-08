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
	"bytes"
	"crypto/sha256"
	"encoding/json"
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

	// NodeStateManifestsOnly means manifests/ is present but there is no volume
	// download in progress (no chunk dir, no FS staging dir, no data.bin or data.tar)
	// and no snapshot.yaml.  The pipeline should call FinalizeNode once volume data is ready.
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

	// A partial dir is resumable only with proven identity. On a mismatched or
	// absent-but-non-empty marker the node is redirected to a stable collision
	// path (mirroring classifyCompleteDir) instead of resuming into another
	// snapshot's bytes.
	return classifyPartialDir(primaryDir, id, func(mm partialMismatch) (NodeResumePlan, error) {
		collisionDir := CollisionNodeDir(parentDir, id.Kind, nodeDirComponent(id), mm.short)

		return NodeResumePlan{TargetDir: collisionDir, State: NodeStatePending}, nil
	})
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

// partialMismatch carries the information ScanNode/ScanAbsolute need to react to
// a partial directory that cannot be proven to belong to the planned node.
type partialMismatch struct {
	// short is a stable collision-suffix ScanNode appends to redirect the node to
	// a fresh path. It is derived from the FOREIGN marker's identity when a
	// mismatched marker is present (so re-runs redirect to the same path), or from
	// the PLANNED identity when the dir has no marker at all (still deterministic).
	short string
	// detail is a human-readable clause for ScanAbsolute's ErrIdentityMismatch.
	detail string
}

// classifyPartialDir classifies an existing but incomplete node directory.
//
// A partial directory carries no snapshot.yaml (that file — the other identity
// record — is written only at finalize), so its identity is proven solely by the
// identity marker written on first touch (WriteNodeIdentityMarker):
//
//   - marker present and matching id       -> resume (classifyPartialResumable);
//   - marker present but mismatched         -> foreign; onMismatch redirects
//     (ScanNode) or rejects (ScanAbsolute);
//   - marker absent, dir holds node content -> unverifiable (a tree predating
//     this feature, or a foreign dir such as scenario B's merged-but-unmarked
//     data.bin); onMismatch, paying a one-time re-download rather than risk
//     resuming into another snapshot's bytes;
//   - marker absent, dir effectively empty  -> a fresh dir (e.g. the root output
//     dir holding only the download lock, or a --node scaffold dir); resume as a
//     pending node and let the pipeline stamp the marker on first touch.
//
// This is the resume-identity invariant (inv. #9): a partial dir is resumable
// only with proven identity.
func classifyPartialDir(dir string, id NodeIdentity, onMismatch func(partialMismatch) (NodeResumePlan, error)) (NodeResumePlan, error) {
	marker, found, err := ReadNodeIdentityMarker(dir)
	if err != nil {
		return NodeResumePlan{}, err
	}

	if found {
		if markerMatchesIdentity(marker, id) {
			return classifyPartialResumable(dir)
		}

		return onMismatch(partialMismatch{
			short:  identityMarkerShort(marker),
			detail: fmt.Sprintf("contains a partial download of %s/%s", marker.Kind, marker.Name),
		})
	}

	populated, err := dirHasNodeArtifacts(dir)
	if err != nil {
		return NodeResumePlan{}, err
	}

	if !populated {
		return classifyPartialResumable(dir)
	}

	return onMismatch(partialMismatch{
		short:  identityMarkerShort(markerFromIdentity(id)),
		detail: "contains an unverifiable partial download (no identity marker)",
	})
}

// classifyPartialResumable classifies a partial dir whose identity is already
// proven (matching marker, or a genuinely fresh dir). It branches purely on the
// on-disk staging layout, exactly as the pre-identity-marker resume scan did.
func classifyPartialResumable(dir string) (NodeResumePlan, error) {
	// Block chunk staging dir (single-volume block download in progress). This
	// fires on the DIRECTORY'S EXISTENCE alone, not on any chunk having
	// finalized — a chunk dir holding only durable in-flight "*.part" raw
	// partials (see volume.downloadChunk's sub-chunk resume) is exactly as
	// much "in progress" as one with finalized chunk_NNNNN files, and must
	// resume rather than restart from scratch.
	if _, err := os.Stat(filepath.Join(dir, BlockChunksDirName)); err == nil {
		return NodeResumePlan{TargetDir: dir, State: NodeStateBlockPartial}, nil
	}

	// Flat FS tar staging dir (single-volume filesystem download in progress).
	if _, err := os.Stat(filepath.Join(dir, FsTarStagingDirName)); err == nil {
		return NodeResumePlan{TargetDir: dir, State: NodeStateFSPartial}, nil
	}

	// Multi-volume data/ directory (multi-volume layout, block or FS).
	if _, err := os.Stat(filepath.Join(dir, DataDirName)); err == nil {
		return NodeResumePlan{TargetDir: dir, State: NodeStateFSPartial}, nil
	}

	return NodeResumePlan{TargetDir: dir, State: NodeStateManifestsOnly}, nil
}

// dirHasNodeArtifacts reports whether dir already holds any snapshot-download
// artifact the pipeline writes into a node directory (manifests/, block/FS
// staging dirs, a merged data.bin*/data.tar, the data/ multi-volume dir, or a
// snapshot.yaml). It probes ONLY these fixed pipeline-owned names, so files the
// pipeline does not own — the download advisory lock, the identity marker
// itself, or unrelated user files — never make a genuinely fresh dir look
// populated (which would wrongly block a first-time download).
func dirHasNodeArtifacts(dir string) (bool, error) {
	for _, name := range []string{
		ManifestsDirName,
		BlockChunksDirName,
		FsTarStagingDirName,
		DataDirName,
		FsTarName,
		SnapshotYAMLName,
	} {
		_, err := os.Stat(filepath.Join(dir, name))
		if err == nil {
			return true, nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("stat %s in %s: %w", name, dir, err)
		}
	}

	_, blockFound, err := FindBlockData(dir)
	if err != nil {
		return false, fmt.Errorf("find block data in %s: %w", dir, err)
	}

	return blockFound, nil
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

	// A partial dir under a user-controlled path is resumable only with proven
	// identity; on a mismatched or absent-but-non-empty marker reject with
	// ErrIdentityMismatch so the caller can pick a different output path (the
	// same contract as the complete-dir mismatch path above).
	return classifyPartialDir(nodeDir, id, func(mm partialMismatch) (NodeResumePlan, error) {
		return NodeResumePlan{}, fmt.Errorf("%w: %s %s, expected %s/%s",
			ErrIdentityMismatch, nodeDir, mm.detail, id.Kind, id.Name)
	})
}

// NodeIdentityMarker is the on-disk identity sidecar (NodeIdentityMarkerName)
// written into a node directory on first touch. Its fields are exactly the
// identity fields matchesIdentity compares (the on-disk DirName is intentionally
// excluded — it is a naming detail, not an identity).
type NodeIdentityMarker struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Namespace  string `json:"namespace,omitempty"`
	SourceRef  string `json:"sourceRef,omitempty"`
}

// WriteNodeIdentityMarker writes the identity marker for id into dir, but ONLY
// when no marker is already present. The marker records the FIRST toucher's
// identity — precisely the identity a later resume must match — so an existing
// marker is left untouched and this is safe to call on every reconcile of the
// same node. The write is crash-safe (WriteFileAtomic: .tmp -> fsync -> rename
// -> dir fsync).
func WriteNodeIdentityMarker(dir string, id NodeIdentity) error {
	markerPath := filepath.Join(dir, NodeIdentityMarkerName)

	_, statErr := os.Stat(markerPath)
	if statErr == nil {
		return nil
	}

	if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("stat identity marker %s: %w", markerPath, statErr)
	}

	data, err := json.Marshal(markerFromIdentity(id))
	if err != nil {
		return fmt.Errorf("marshal identity marker: %w", err)
	}

	if err := WriteFileAtomic(markerPath, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write identity marker %s: %w", markerPath, err)
	}

	return nil
}

// ReadNodeIdentityMarker reads the identity marker from dir. found is false with
// a nil error when the marker is absent.
func ReadNodeIdentityMarker(dir string) (NodeIdentityMarker, bool, error) {
	markerPath := filepath.Join(dir, NodeIdentityMarkerName)

	data, err := os.ReadFile(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return NodeIdentityMarker{}, false, nil
	}

	if err != nil {
		return NodeIdentityMarker{}, false, fmt.Errorf("read identity marker %s: %w", markerPath, err)
	}

	var marker NodeIdentityMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return NodeIdentityMarker{}, false, fmt.Errorf("unmarshal identity marker %s: %w", markerPath, err)
	}

	return marker, true, nil
}

// markerFromIdentity projects a NodeIdentity onto the marker's identity fields.
func markerFromIdentity(id NodeIdentity) NodeIdentityMarker {
	return NodeIdentityMarker{
		APIVersion: id.APIVersion,
		Kind:       id.Kind,
		Name:       id.Name,
		Namespace:  id.Namespace,
		SourceRef:  id.SourceRef,
	}
}

// markerMatchesIdentity reports whether the stored marker equals id on every
// identity field (the same fields matchesIdentity compares for snapshot.yaml).
func markerMatchesIdentity(m NodeIdentityMarker, id NodeIdentity) bool {
	return m.APIVersion == id.APIVersion &&
		m.Kind == id.Kind &&
		m.Name == id.Name &&
		m.Namespace == id.Namespace &&
		m.SourceRef == id.SourceRef
}

// identityMarkerShort derives a stable short collision-suffix from a marker's
// identity, used when a partial dir must be redirected but no node checksum
// exists yet (snapshot.yaml is absent). The digest is over the identity fields
// only, so it is deterministic across runs — a re-run redirects to the same path.
func identityMarkerShort(m NodeIdentityMarker) string {
	sum := sha256.Sum256([]byte(strings.Join(
		[]string{m.APIVersion, m.Kind, m.Name, m.Namespace, m.SourceRef}, "\x00")))

	return ShortChecksum(fmt.Sprintf("%x", sum[:]))
}

// removeTmpFiles deletes every stale *.tmp file left by an interrupted
// AtomicWriter under dir — EXCEPT inside an FS tar staging directory, whose
// subtree is skipped entirely.
//
// The FS staging subtree (data.tar.d/ and any multi-volume data/<pvc>.tar.d/,
// see fsStagingDirSuffix) is the ONLY place user-provided file bytes exist as
// loose files on disk, and at codec none a staged user blob is written under its
// verbatim server-provided name — which may legitimately end in ".tmp"
// (inv. #10a). Sweeping it here would delete that blob on every resume scan and
// force a needless re-download. The staging path owns its own transient cleanup
// instead: stageCompressedFile removes its per-file "<dest>.tmp" before each
// stage, downloadChunk removes its per-chunk "<final>.tmp", and the whole
// staging dir is os.RemoveAll'd on tar assembly — so excluding it here loses no
// required cleanup. Internal ".tmp" outside that subtree (snapshot.yaml.tmp,
// manifests/*.tmp, identity.json.tmp, block chunk-dir "<final>.tmp") is still
// swept.
func removeTmpFiles(dir string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			if strings.HasSuffix(d.Name(), fsStagingDirSuffix) {
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(d.Name(), ".tmp") {
			return nil
		}

		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return fmt.Errorf("remove stale tmp %s: %w", path, removeErr)
		}

		return nil
	})
}
