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
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ObservedState is a human-readable, NON-AUTHORITATIVE label describing what the
// resume scan saw on disk for a planned node directory. It exists solely for log
// output (the pipeline's "resume_state" attribute) so an operator can see how a
// node was classified; it MUST NOT drive any resume decision.
//
// The pipeline re-proves every real resume decision from disk probes at each
// site — FindBlockData / a data.tar stat / chunk geometry re-derivation — so a
// stale or approximate label here can never cause wrong data to be reused. Only
// NodeResumePlan.Done gates whether a node is skipped. In particular the
// collision-redirect paths report ObservedPending for a fresh redirect target
// they do not scan the contents of; that is fine precisely because nothing reads
// the label to decide anything.
type ObservedState string

const (
	// ObservedPending: the node directory does not exist, is effectively empty
	// (a genuinely fresh dir), or the node was redirected to a not-yet-scanned
	// collision path.
	ObservedPending ObservedState = "pending"

	// ObservedBlockPartial: a block chunk staging dir (BlockChunksDirName) is
	// present, i.e. a single-volume block download was in progress.
	ObservedBlockPartial ObservedState = "block_partial"

	// ObservedFSPartial: an FS tar staging dir (FsTarStagingDirName) or the
	// multi-volume data/ directory is present, i.e. a filesystem download was in
	// progress.
	ObservedFSPartial ObservedState = "fs_partial"

	// ObservedManifestsOnly: the directory exists (proven-fresh or manifests-only)
	// with no volume-staging artifact and no snapshot.yaml.
	ObservedManifestsOnly ObservedState = "manifests_only"

	// ObservedDone: snapshot.yaml is present, VerifyNode passed for the planned
	// identity, and its directory durability was confirmed — the node is complete.
	ObservedDone ObservedState = "done"
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
	// UID is the snapshot CR's metadata.uid. It is the identity component that ties a
	// directory to the exact snapshot CR (Variant A: readable dir base from source name,
	// uniqueness/resume identity from the CR identity incl UID). matchesIdentity and the
	// collision discriminator use it.
	UID string
}

// NodeResumePlan is the result of scanning one planned node on disk.
type NodeResumePlan struct {
	// TargetDir is the absolute path to use for this node.  For a collision-
	// redirected node this will be CollisionNodeDir(...) rather than the primary
	// directory.
	TargetDir string

	// Done is the ONLY resume decision the pipeline consumes: true means the node
	// directory already holds a complete, identity-verified download whose
	// snapshot.yaml directory durability was confirmed, so the pipeline skips it
	// entirely. Every not-done node is (re)driven through the normal download path,
	// which re-proves what to (re)fetch from disk probes — those probes, NOT this
	// plan, are the single source of truth for resume.
	Done bool

	// Observed is a NON-AUTHORITATIVE label of what the scan saw on disk (see
	// ObservedState). It is log-only and never an input to any resume decision.
	Observed ObservedState
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

// ScanNode is ScanNodeContext with a non-cancellable context.
func ScanNode(parentDir string, id NodeIdentity) (NodeResumePlan, error) {
	return ScanNodeContext(context.Background(), parentDir, id)
}

// ScanNodeContext inspects parentDir for an existing node directory whose name is
// NodeDirName(id.Kind, nodeDirComponent(id)), removes any stale *.tmp files,
// and returns a NodeResumePlan describing the on-disk state for the planned node.
//
// The directory name is derived from id.DirName (the source object name) when set,
// falling back to id.Name (the CR name) for nodes without a source annotation.
// Identity matching (matchesIdentity) uses id.APIVersion/Kind/Name/Namespace/UID,
// which are the values written into snapshot.yaml.
//
// Collision rule: if the primary directory is complete (VerifyNode passes) but
// its stored identity does not match id, the primary directory belongs to a
// different node.  ScanNode returns a not-done plan with TargetDir set to
// CollisionNodeDir(parentDir, id.Kind, nodeDirComponent(id), short), where short
// is derived from the existing complete node's checksum.  This prevents the
// pipeline from overwriting unrelated completed data.
func ScanNodeContext(ctx context.Context, parentDir string, id NodeIdentity) (NodeResumePlan, error) {
	return scanNodeContext(ctx, nil, parentDir, id)
}

// ScanNodeRootedContext is ScanNodeContext rooted in destination's locked view.
func ScanNodeRootedContext(
	ctx context.Context,
	destination *RootedDestination,
	parentDir string,
	id NodeIdentity,
) (NodeResumePlan, error) {
	if destination == nil {
		return NodeResumePlan{}, errors.New("scan rooted node: destination is nil")
	}

	return scanNodeContext(ctx, destination, parentDir, id)
}

func scanNodeContext(
	ctx context.Context,
	destination *RootedDestination,
	parentDir string,
	id NodeIdentity,
) (NodeResumePlan, error) {
	primaryDir := filepath.Join(parentDir, NodeDirName(id.Kind, nodeDirComponent(id)))

	_, statErr := resumeStat(destination, primaryDir)
	if errors.Is(statErr, os.ErrNotExist) {
		return NodeResumePlan{TargetDir: primaryDir, Observed: ObservedPending}, nil
	}

	if statErr != nil {
		return NodeResumePlan{}, fmt.Errorf("stat node dir %s: %w", primaryDir, statErr)
	}

	if err := removeTmpFilesAt(destination, primaryDir); err != nil {
		return NodeResumePlan{}, err
	}

	verifyErr := resumeVerifyNode(destination, primaryDir)
	if verifyErr == nil {
		return classifyCompleteDir(ctx, destination, parentDir, primaryDir, id)
	}

	// A PRESENT snapshot.yaml whose recorded checksum no longer matches the
	// on-disk payload is post-finalize corruption (bit rot / partial disk
	// failure / manual edit), NOT an unfinished download. Routing it into
	// classifyPartialDir would let the pipeline's "already merged/complete"
	// skip re-finalize the corrupt bytes and silently re-stamp a fresh checksum
	// over them, laundering a mismatch VerifyNode correctly detected (inv. #9,
	// code-style §6a "existence is not validity"). Surface it instead of
	// resuming into it.
	if errors.Is(verifyErr, ErrChecksumMismatch) {
		return classifyChecksumMismatchDir(destination, primaryDir, id, func(sy SnapshotYAML) (NodeResumePlan, error) {
			collisionDir := CollisionNodeDir(parentDir, id.Kind, nodeDirComponent(id), ShortChecksum(sy.Checksum.Hex))

			return NodeResumePlan{TargetDir: collisionDir, Observed: ObservedPending}, nil
		})
	}

	// Any other VerifyNode failure keeps its existing handling. In particular
	// the crash window (data committed, snapshot.yaml never written ->
	// ErrSnapshotYAMLMissing) and I/O errors flow here: a partial dir is
	// resumable only with proven identity. On a mismatched or absent-but-
	// non-empty marker the node is redirected to a stable collision path
	// (mirroring classifyCompleteDir) instead of resuming into another
	// snapshot's bytes.
	return classifyPartialDir(destination, primaryDir, id, func(mm partialMismatch) (NodeResumePlan, error) {
		collisionDir := CollisionNodeDir(parentDir, id.Kind, nodeDirComponent(id), mm.short)

		return NodeResumePlan{TargetDir: collisionDir, Observed: ObservedPending}, nil
	})
}

// classifyChecksumMismatchDir handles a node directory that HAS a snapshot.yaml
// (so it was finalized at least once) whose recorded checksum no longer matches
// its on-disk payload.
//
// This is distinct from the crash window (data committed, snapshot.yaml never
// written -> VerifyNode returns ErrSnapshotYAMLMissing), where re-finalizing is
// the correct resume behavior and stays untouched.
//
// If the stored identity matches the planned node, the corruption is in THIS
// snapshot's own dir: surface a clear, operator-facing error naming the dir and
// both short digests (stored vs computed) instead of re-blessing the corrupt
// bytes — option (a) of the resume-checksum-mismatch task, the safe choice for a
// backup tool. Otherwise the dir is a foreign finalized-but-corrupt node that
// merely collided on the directory name; onForeign redirects (ScanNode) or
// rejects (ScanAbsolute) it exactly as a foreign VALID dir is handled, so
// unrelated data is never overwritten.
func classifyChecksumMismatchDir(
	destination *RootedDestination,
	nodeDir string,
	id NodeIdentity,
	onForeign func(sy SnapshotYAML) (NodeResumePlan, error),
) (NodeResumePlan, error) {
	sy, err := resumeReadSnapshotYAML(destination, nodeDir)
	if err != nil {
		return NodeResumePlan{}, fmt.Errorf("read snapshot.yaml in %s: %w", nodeDir, err)
	}

	if !matchesIdentity(sy, id) {
		return onForeign(sy)
	}

	// VerifyNode already recomputed the digest successfully to reach the
	// mismatch verdict, so this recompute succeeds in practice; fall back to a
	// placeholder rather than masking the mismatch with a recompute-time error.
	computed := "unavailable"
	if got, csErr := resumeComputeNodeChecksum(destination, nodeDir); csErr == nil {
		computed = got.Short
	}

	return NodeResumePlan{}, fmt.Errorf(
		"%w: node directory %s no longer matches its recorded checksum "+
			"(recorded %s, computed %s): its manifests or volume data were modified "+
			"after the node was finalized; delete the node directory to re-download it, "+
			"or choose a different output directory",
		ErrChecksumMismatch, nodeDir, ShortChecksum(sy.Checksum.Hex), computed)
}

// classifyCompleteDir handles the case where the primary directory passes
// VerifyNode.  It checks identity and may redirect to a collision path.
func classifyCompleteDir(
	ctx context.Context,
	destination *RootedDestination,
	parentDir, primaryDir string,
	id NodeIdentity,
) (NodeResumePlan, error) {
	sy, err := resumeReadSnapshotYAML(destination, primaryDir)
	if err != nil {
		return NodeResumePlan{}, fmt.Errorf("read snapshot.yaml in %s: %w", primaryDir, err)
	}

	if matchesIdentity(sy, id) {
		if err := confirmSnapshotYAMLDurability(ctx, destination, primaryDir); err != nil {
			return NodeResumePlan{}, err
		}

		if err := healNodeIdentityMarkerAt(destination, primaryDir); err != nil {
			return NodeResumePlan{}, err
		}

		return NodeResumePlan{TargetDir: primaryDir, Done: true, Observed: ObservedDone}, nil
	}

	// Primary dir is complete but belongs to a different node.
	// Redirect the new node to a stable collision-suffixed path so the existing
	// complete data is not overwritten.
	short := ShortChecksum(sy.Checksum.Hex)
	collisionDir := CollisionNodeDir(parentDir, id.Kind, nodeDirComponent(id), short)

	return NodeResumePlan{TargetDir: collisionDir, Observed: ObservedPending}, nil
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
func classifyPartialDir(
	destination *RootedDestination,
	dir string,
	id NodeIdentity,
	onMismatch func(partialMismatch) (NodeResumePlan, error),
) (NodeResumePlan, error) {
	marker, found, err := readNodeIdentityMarkerAt(destination, dir)
	if err != nil {
		return NodeResumePlan{}, err
	}

	if found {
		if markerMatchesIdentity(marker, id) {
			return classifyPartialResumable(destination, dir)
		}

		return onMismatch(partialMismatch{
			short:  identityMarkerShort(marker),
			detail: fmt.Sprintf("contains a partial download of %s/%s", marker.Kind, marker.Name),
		})
	}

	populated, err := dirHasNodeArtifacts(destination, dir)
	if err != nil {
		return NodeResumePlan{}, err
	}

	if !populated {
		return classifyPartialResumable(destination, dir)
	}

	return onMismatch(partialMismatch{
		short:  identityMarkerShort(markerFromIdentity(id)),
		detail: "contains an unverifiable partial download (no identity marker)",
	})
}

// classifyPartialResumable classifies a partial dir whose identity is already
// proven (matching marker, or a genuinely fresh dir). It branches purely on the
// on-disk staging layout, exactly as the pre-identity-marker resume scan did.
func classifyPartialResumable(destination *RootedDestination, dir string) (NodeResumePlan, error) {
	// Block chunk staging dir (single-volume block download in progress). This
	// fires on the DIRECTORY'S EXISTENCE alone, not on any chunk having
	// finalized — a chunk dir holding only durable in-flight "*.part" raw
	// partials (see volume.downloadChunk's sub-chunk resume) is exactly as
	// much "in progress" as one with finalized chunk_NNNNN files, and must
	// resume rather than restart from scratch.
	if _, err := resumeStat(destination, filepath.Join(dir, BlockChunksDirName)); err == nil {
		return NodeResumePlan{TargetDir: dir, Observed: ObservedBlockPartial}, nil
	}

	// Flat FS tar staging dir (single-volume filesystem download in progress).
	if _, err := resumeStat(destination, filepath.Join(dir, FsTarStagingDirName)); err == nil {
		return NodeResumePlan{TargetDir: dir, Observed: ObservedFSPartial}, nil
	}

	// Multi-volume data/ directory (multi-volume layout, block or FS).
	if _, err := resumeStat(destination, filepath.Join(dir, DataDirName)); err == nil {
		return NodeResumePlan{TargetDir: dir, Observed: ObservedFSPartial}, nil
	}

	return NodeResumePlan{TargetDir: dir, Observed: ObservedManifestsOnly}, nil
}

// dirHasNodeArtifacts reports whether dir already holds any snapshot-download
// artifact the pipeline writes into a node directory (manifests/, block/FS
// staging dirs, a merged data.bin*/data.tar, the data/ multi-volume dir, or a
// snapshot.yaml). It probes ONLY these fixed pipeline-owned names, so files the
// pipeline does not own — the download advisory lock, the identity marker
// itself, or unrelated user files — never make a genuinely fresh dir look
// populated (which would wrongly block a first-time download).
func dirHasNodeArtifacts(destination *RootedDestination, dir string) (bool, error) {
	for _, name := range []string{
		ManifestsDirName,
		BlockChunksDirName,
		FsTarStagingDirName,
		DataDirName,
		FsTarName,
		SnapshotYAMLName,
	} {
		_, err := resumeStat(destination, filepath.Join(dir, name))
		if err == nil {
			return true, nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("stat %s in %s: %w", name, dir, err)
		}
	}

	_, blockFound, err := resumeFindBlockData(destination, dir)
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
		sy.UID == id.UID
}

// ErrIdentityMismatch is returned by ScanAbsolute when the target directory
// contains a complete snapshot whose stored identity does not match the planned node.
// The caller must choose a different output path rather than overwriting the data.
var ErrIdentityMismatch = errors.New("output directory belongs to a different snapshot")

// ScanAbsolute is ScanAbsoluteContext with a non-cancellable context.
func ScanAbsolute(nodeDir string, id NodeIdentity) (NodeResumePlan, error) {
	return ScanAbsoluteContext(context.Background(), nodeDir, id)
}

// ScanAbsoluteContext classifies the on-disk state of an absolute node directory path,
// removing stale *.tmp files.  Unlike ScanNode it does not derive the path from
// a parent directory + NodeDirName convention, and it does not redirect to a
// collision-suffixed path on identity mismatch.  Instead it returns ErrIdentityMismatch
// so the caller can abort and ask the user to choose a different output path.
//
// Suitable for the root output directory where the path name is user-controlled.
func ScanAbsoluteContext(ctx context.Context, nodeDir string, id NodeIdentity) (NodeResumePlan, error) {
	return scanAbsoluteContext(ctx, nil, nodeDir, id)
}

// ScanAbsoluteRootedContext is ScanAbsoluteContext rooted in destination.
func ScanAbsoluteRootedContext(
	ctx context.Context,
	destination *RootedDestination,
	nodeDir string,
	id NodeIdentity,
) (NodeResumePlan, error) {
	if destination == nil {
		return NodeResumePlan{}, errors.New("scan rooted absolute node: destination is nil")
	}

	return scanAbsoluteContext(ctx, destination, nodeDir, id)
}

func scanAbsoluteContext(
	ctx context.Context,
	destination *RootedDestination,
	nodeDir string,
	id NodeIdentity,
) (NodeResumePlan, error) {
	_, statErr := resumeStat(destination, nodeDir)

	if errors.Is(statErr, os.ErrNotExist) {
		return NodeResumePlan{TargetDir: nodeDir, Observed: ObservedPending}, nil
	}

	if statErr != nil {
		return NodeResumePlan{}, fmt.Errorf("stat %s: %w", nodeDir, statErr)
	}

	if err := removeTmpFilesAt(destination, nodeDir); err != nil {
		return NodeResumePlan{}, err
	}

	verifyErr := resumeVerifyNode(destination, nodeDir)
	if verifyErr == nil {
		sy, err := resumeReadSnapshotYAML(destination, nodeDir)
		if err != nil {
			return NodeResumePlan{}, fmt.Errorf("read snapshot.yaml in %s: %w", nodeDir, err)
		}

		if !matchesIdentity(sy, id) {
			return NodeResumePlan{}, fmt.Errorf("%w: %s contains %s/%s, expected %s/%s",
				ErrIdentityMismatch, nodeDir, sy.Kind, sy.Name, id.Kind, id.Name)
		}

		if err := confirmSnapshotYAMLDurability(ctx, destination, nodeDir); err != nil {
			return NodeResumePlan{}, err
		}

		if err := healNodeIdentityMarkerAt(destination, nodeDir); err != nil {
			return NodeResumePlan{}, err
		}

		return NodeResumePlan{TargetDir: nodeDir, Done: true, Observed: ObservedDone}, nil
	}

	// A PRESENT snapshot.yaml whose recorded checksum no longer matches the
	// on-disk payload is post-finalize corruption, not an unfinished download:
	// surface it rather than resuming into it and re-blessing the corrupt bytes
	// (inv. #9). A foreign finalized-but-corrupt occupant is rejected with
	// ErrIdentityMismatch, exactly as a foreign VALID dir is above, so the
	// caller can pick a different output path.
	if errors.Is(verifyErr, ErrChecksumMismatch) {
		return classifyChecksumMismatchDir(destination, nodeDir, id, func(sy SnapshotYAML) (NodeResumePlan, error) {
			return NodeResumePlan{}, fmt.Errorf("%w: %s contains %s/%s, expected %s/%s",
				ErrIdentityMismatch, nodeDir, sy.Kind, sy.Name, id.Kind, id.Name)
		})
	}

	// A partial dir under a user-controlled path is resumable only with proven
	// identity; on a mismatched or absent-but-non-empty marker reject with
	// ErrIdentityMismatch so the caller can pick a different output path (the
	// same contract as the complete-dir mismatch path above).
	return classifyPartialDir(destination, nodeDir, id, func(mm partialMismatch) (NodeResumePlan, error) {
		return NodeResumePlan{}, fmt.Errorf("%w: %s %s, expected %s/%s",
			ErrIdentityMismatch, nodeDir, mm.detail, id.Kind, id.Name)
	})
}

func confirmSnapshotYAMLDurability(
	ctx context.Context,
	destination *RootedDestination,
	nodeDir string,
) error {
	path := filepath.Join(nodeDir, SnapshotYAMLName)
	if err := resumeConfirmFileDurability(ctx, destination, path); err != nil {
		return fmt.Errorf("confirm snapshot.yaml durability in %s: %w", nodeDir, err)
	}

	return nil
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
	UID        string `json:"uid,omitempty"`
}

// WriteNodeIdentityMarker writes the identity marker for id into dir, but ONLY
// when no marker is already present. The marker records the FIRST toucher's
// identity — precisely the identity a later resume must match — so an existing
// marker is left untouched and this is safe to call on every reconcile of the
// same node. The write is crash-safe (WriteFileAtomic: .tmp -> fsync -> rename
// -> dir fsync).
func WriteNodeIdentityMarker(dir string, id NodeIdentity) error {
	return writeNodeIdentityMarkerAt(context.Background(), nil, dir, id)
}

// WriteNodeIdentityMarkerRooted writes the marker through destination.
func WriteNodeIdentityMarkerRooted(
	ctx context.Context,
	destination *RootedDestination,
	dir string,
	id NodeIdentity,
) error {
	if destination == nil {
		return errors.New("write rooted node identity marker: destination is nil")
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("write rooted node identity marker: %w", err)
	}

	// Identity marker writes historically used WriteFileAtomic's background
	// context. Keep directory-sync test hooks scoped to snapshot publication;
	// destination binding loss is still checked at every rooted operation.
	return writeNodeIdentityMarkerAt(context.Background(), destination, dir, id)
}

func writeNodeIdentityMarkerAt(
	ctx context.Context,
	destination *RootedDestination,
	dir string,
	id NodeIdentity,
) error {
	markerPath := filepath.Join(dir, NodeIdentityMarkerName)

	_, statErr := resumeStat(destination, markerPath)
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

	if err := resumeWriteFileAtomic(ctx, destination, markerPath, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write identity marker %s: %w", markerPath, err)
	}

	return nil
}

// ReadNodeIdentityMarker reads the identity marker from dir. found is false with
// a nil error when the marker is absent.
func ReadNodeIdentityMarker(dir string) (NodeIdentityMarker, bool, error) {
	return readNodeIdentityMarkerAt(nil, dir)
}

func readNodeIdentityMarkerAt(
	destination *RootedDestination,
	dir string,
) (NodeIdentityMarker, bool, error) {
	markerPath := filepath.Join(dir, NodeIdentityMarkerName)

	data, err := resumeReadFile(destination, markerPath)
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

// healNodeIdentityMarker removes the resume identity marker from a node
// directory the scan has just proven is the planned node's OWN completed dir
// (Done=true). FinalizeNode normally removes the marker once snapshot.yaml is
// durable; this self-heals the crash window between that write and the remove,
// and archives produced by older builds that never removed it. Only a Done dir
// is healed — a FOREIGN complete dir keeps its marker so its owner snapshot's
// next run can still resume it.
//
// A missing marker is fine (os.ErrNotExist ignored, keeping this idempotent); a
// real I/O error propagates, exactly like removeTmpFiles' error on the same
// scan path. Removal is checksum-neutral (collectNodeFiles excludes the marker),
// so it cannot perturb the checksum VerifyNode just validated.
func healNodeIdentityMarkerAt(destination *RootedDestination, nodeDir string) error {
	markerPath := filepath.Join(nodeDir, NodeIdentityMarkerName)
	if err := resumeRemove(destination, markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove identity marker %s: %w", markerPath, err)
	}

	return nil
}

// markerFromIdentity projects a NodeIdentity onto the marker's identity fields.
func markerFromIdentity(id NodeIdentity) NodeIdentityMarker {
	return NodeIdentityMarker{
		APIVersion: id.APIVersion,
		Kind:       id.Kind,
		Name:       id.Name,
		Namespace:  id.Namespace,
		UID:        id.UID,
	}
}

// markerMatchesIdentity reports whether the stored marker equals id on every
// identity field (the same fields matchesIdentity compares for snapshot.yaml).
func markerMatchesIdentity(m NodeIdentityMarker, id NodeIdentity) bool {
	return m.APIVersion == id.APIVersion &&
		m.Kind == id.Kind &&
		m.Name == id.Name &&
		m.Namespace == id.Namespace &&
		m.UID == id.UID
}

// identityMarkerShort derives a stable short collision-suffix from a marker's
// identity, used when a partial dir must be redirected but no node checksum
// exists yet (snapshot.yaml is absent). The digest is over the identity fields
// only (the snapshot CR identity incl UID — the canonical snapshot identity), so it
// is deterministic across runs and distinct for two nodes sharing a source-name dir
// base — a re-run redirects to the same path.
func identityMarkerShort(m NodeIdentityMarker) string {
	sum := sha256.Sum256([]byte(strings.Join(
		[]string{m.APIVersion, m.Kind, m.Name, m.Namespace, m.UID}, "\x00")))

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
func removeTmpFilesAt(destination *RootedDestination, dir string) error {
	if destination != nil {
		return removeRootedTmpFiles(destination, dir)
	}

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

func removeRootedTmpFiles(destination *RootedDestination, dir string) error {
	entries, err := destination.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if entry.IsDir() {
			if strings.HasSuffix(entry.Name(), fsStagingDirSuffix) {
				continue
			}

			if err := removeRootedTmpFiles(destination, path); err != nil {
				return err
			}

			continue
		}

		if !strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}

		if err := destination.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale tmp %s: %w", path, err)
		}
	}

	return nil
}

func resumeStat(destination *RootedDestination, path string) (os.FileInfo, error) {
	if destination == nil {
		return os.Stat(path)
	}

	return destination.Stat(path)
}

func resumeVerifyNode(destination *RootedDestination, path string) error {
	if destination == nil {
		return VerifyNode(path)
	}

	return destination.VerifyNode(path)
}

func resumeReadSnapshotYAML(destination *RootedDestination, path string) (SnapshotYAML, error) {
	if destination == nil {
		return ReadSnapshotYAML(path)
	}

	return destination.ReadSnapshotYAML(path)
}

func resumeComputeNodeChecksum(destination *RootedDestination, path string) (NodeChecksum, error) {
	if destination == nil {
		return ComputeNodeChecksum(path)
	}

	return destination.ComputeNodeChecksum(path)
}

func resumeFindBlockData(destination *RootedDestination, path string) (string, bool, error) {
	if destination == nil {
		return FindBlockData(path)
	}

	payload, found, err := destination.FindBlockData(path)

	return payload.Path, found, err
}

func resumeConfirmFileDurability(
	ctx context.Context,
	destination *RootedDestination,
	path string,
) error {
	if destination == nil {
		return ConfirmFileDurability(ctx, path)
	}

	return ConfirmRootedFileDurability(ctx, destination, path)
}

func resumeWriteFileAtomic(
	ctx context.Context,
	destination *RootedDestination,
	path string,
	reader io.Reader,
) error {
	if destination == nil {
		return WriteFileAtomicContext(ctx, path, reader)
	}

	return WriteFileAtomicRooted(ctx, destination, path, reader)
}

func resumeReadFile(destination *RootedDestination, path string) ([]byte, error) {
	if destination == nil {
		return os.ReadFile(path)
	}

	return destination.ReadFile(path)
}

func resumeRemove(destination *RootedDestination, path string) error {
	if destination == nil {
		return os.Remove(path)
	}

	return destination.Remove(path)
}
