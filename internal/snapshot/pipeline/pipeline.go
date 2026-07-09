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

package pipeline

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/deckhouse/deckhouse-cli/internal/progress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// nodeTask is a planned work item for one snapshot node.
type nodeTask struct {
	node    *source.Node
	nodeDir string // final target directory (may differ from primary on collision)
	// done is the resume scan's single consumed decision: a complete,
	// identity-verified node is skipped. observed is a non-authoritative label of
	// what the scan saw on disk, carried only for the "resume_state" log line.
	done     bool
	observed archive.ObservedState
}

// streamKey identifies a pre-created progress stream.
// Variant A (decision #9) guarantees at most one dataRef per node, so a node
// key alone is enough to address its single stream — there is no per-pvc
// discriminator.
type streamKey struct {
	node *source.Node
}

// streamHandle pairs a pre-created progress.Stream with the number of bytes
// seedStreamFromDisk already credited to it (see that function) before the
// real transfer starts. downloadBlock/downloadFS pass Seeded to
// skipSeededBytes to build an onProgress wrapper that discards exactly that
// many bytes of the resume-skip logic's re-derivation of the SAME
// already-committed bytes, instead of resetting the stream with
// SetCurrent(0) — the reset is what produced a visible dip back to 0% at the
// waiting->active transition, and the displayed value must never move backward
// across that transition.
// Seeded is 0 for a from-scratch stream (nothing to skip) and for a stream
// looked up when progress tracking is disabled.
type streamHandle struct {
	stream progress.Stream
	seeded int64
}

// Run builds the snapshot tree, scans the output directory for resume state, and
// downloads all missing node data with bounded concurrency.
// The per-node download phase is BEST-EFFORT: nodes are independent (own
// DataExport, own output subdir, no shared mutable state), so one node's
// failure — a permanently broken volume, a DataExport timeout — is recorded
// and does NOT cancel sibling nodes still downloading; aborting healthy nodes
// seconds from completion is the wrong trade-off for a backup/download tool.
// Run aggregates every per-node failure into a single errors.Join error. A
// genuine ctx cancellation (SIGINT, or the caller cancelling its parent
// context) still aborts all in-flight nodes promptly; in that case Run
// returns ctx.Err() instead of the aggregated per-node error — but only when
// at least one node actually failed. If every node already succeeded before
// the cancellation was observed, Run reports success: a late-arriving
// cancellation must never turn a fully successful download into a reported
// failure.
func Run(ctx context.Context, cfg Config) error {
	// Generate the per-run ownership ID before applyDefaults builds the
	// production OpenExport closure (which captures it), so every DataExport this
	// run creates is stamped and only this run releases it (inv #10b).
	if cfg.RunID == "" {
		runID, err := newDownloadRunID()
		if err != nil {
			return fmt.Errorf("pipeline: %w", err)
		}

		cfg.RunID = runID
	}

	cfg = applyDefaults(cfg)

	if cfg.OpenExport == nil {
		return fmt.Errorf("pipeline: OpenExport must be set (supply SafeClient+AggClient or set OpenExport directly)")
	}

	if cfg.ManifestSource == nil {
		return fmt.Errorf("pipeline: ManifestSource must be set (supply AggClient or set ManifestSource directly)")
	}

	root, err := source.BuildTree(ctx, cfg.KubeClient, cfg.Namespace, cfg.RootSnapshot)
	if err != nil {
		return fmt.Errorf("build snapshot tree: %w", err)
	}

	processRoot, startDir, err := resolveSubtreeRoot(root, cfg)
	if err != nil {
		return err
	}

	tasks, err := collectNodeTasks(processRoot, startDir)
	if err != nil {
		return fmt.Errorf("scan output directory: %w", err)
	}

	// Redirect any sibling nodes that resolve to the SAME on-disk directory in
	// this run to deterministic collision paths BEFORE the worker errgroup
	// starts. Two sibling CRs referencing one source object map to one
	// <parent>/snapshots/<kind>_<source>/ dir; without this guard the Workers
	// errgroup would run them concurrently — two writers over one chunk/staging
	// dir and snapshot.yaml, the single-writer violation the cross-process flock
	// cannot catch inside one process (inv. #10b).
	tasks, err = dedupeSiblingTargetDirs(tasks, cfg.Log)
	if err != nil {
		return fmt.Errorf("dedupe sibling target directories: %w", err)
	}

	// Pre-create one progress stream per volume leaf before the worker errgroup
	// starts, so every bar appears immediately (docker-pull style).
	streams := precreateStreams(tasks, cfg)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Workers)

	// nodeErrs collects one error per failed node. Guarded by nodeErrsMu because
	// up to cfg.Workers goroutines append to it concurrently.
	var (
		nodeErrsMu sync.Mutex
		nodeErrs   []error
	)

	for _, t := range tasks {
		task := t

		g.Go(func() error {
			if err := processNode(gctx, cfg, task, streams); err != nil {
				nodeErrsMu.Lock()

				nodeErrs = append(nodeErrs, err)

				nodeErrsMu.Unlock()
			}

			// Always return nil: a per-node failure must never cancel gctx, or it
			// would abort every OTHER node's in-flight download too (the exact
			// behavior this best-effort design replaces). Failures are aggregated
			// via nodeErrs instead.
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		// Unreachable in practice: every g.Go closure above unconditionally
		// returns nil. Handled rather than discarded in case that ever changes.
		nodeErrs = append(nodeErrs, err)
	}

	// Defensive sweep: guarantee every pre-created stream is terminally settled
	// before Run returns, so a caller's sink.Wait() can never block forever.
	// Several early-return paths (semaphore-acquire failure, OpenExport failure,
	// a manifest/stat error before a stream is even looked up) can leave a
	// pre-created stream's Done/Fail never called — see the design_refs proof
	// that an mpb bar only unblocks Progress.Wait() once SetTotal(_, true) has
	// run, which happens only inside Stream.Done/Fail. Fail() is safe to call
	// unconditionally here: both sink implementations gate on "first terminal
	// call wins" (see ttyStream.finalize / plainStream.finalize), so a stream
	// already settled by its own code path is left untouched by this sweep —
	// it only settles the ones nothing else finalized.
	failUnsettledStreams(streams)

	// A genuine ctx cancellation (SIGINT/parent cancel) takes priority over the
	// aggregated per-node errors: every in-flight node was aborted for the same
	// reason, so ctx.Err() is the more useful signal to the caller. But only
	// when there is at least one per-node failure to explain — if every node
	// already succeeded (nodeErrs empty), a cancellation that merely arrived in
	// the window between the last node finishing and Run returning must not
	// turn a fully successful download into a reported failure.
	if len(nodeErrs) > 0 && ctx.Err() != nil {
		return ctx.Err()
	}

	return errors.Join(nodeErrs...)
}

// failUnsettledStreams calls Fail on every pre-created stream once the worker
// errgroup has fully drained. It is the primary fix for the "double Ctrl-C"
// deadlock: without it, any early-return path that skips a stream's own
// Done/Fail call leaves that stream's bar incomplete forever, so a subsequent
// sink.Wait() never returns. Calling Fail unconditionally is safe because
// Stream.Done/Fail is specified to let the first terminal call win — a stream
// already settled (by success or by its own Fail) is not double-counted or
// re-settled.
func failUnsettledStreams(streams map[streamKey]streamHandle) {
	for _, h := range streams {
		h.stream.Fail()
	}
}

// precreateStreams creates one progress.Stream per volume download before any worker
// goroutine starts. The map is keyed by node pointer so each call site can
// retrieve its handle without a second NewStream call.
//
// Streams for nodes that are already complete (task.done) are marked Done
// immediately so they render as already-complete (docker-pull "Already exists" style).
//
// Returns nil when cfg.Progress is nil (progress disabled), which causes all
// lookupStream calls to return a zero streamHandle (nil stream) and behave
// as no-ops.
func precreateStreams(tasks []nodeTask, cfg Config) map[streamKey]streamHandle {
	if cfg.Progress == nil {
		return nil
	}

	// Count the exact number of streams to pre-allocate the map.
	nStreams := 0

	for _, t := range tasks {
		n := t.node

		switch {
		case n.Binding != nil:
			nStreams++
		case len(n.OwnDataRefs) == 1:
			nStreams++
		}
	}

	// nStreams is already subtree-scoped (tasks comes from resolveSubtreeRoot), so
	// the live "N/M volumes downloaded" counter is automatically correct for a
	// --node selection with no extra plumbing.
	cfg.Progress.SetVolumeTotal(nStreams)

	out := make(map[streamKey]streamHandle, nStreams)

	for _, t := range tasks {
		node := t.node

		switch {
		case node.Binding != nil:
			// Orphan VolumeSnapshot leaf: one stream keyed on the node, name = leaf ref name.
			s := cfg.Progress.NewStream(node.Ref().Name, 0)

			if t.done {
				s.Done()

				out[streamKey{node: node}] = streamHandle{stream: s}
			} else {
				out[streamKey{node: node}] = streamHandle{stream: s, seeded: seedStreamFromDisk(cfg, s, t.nodeDir)}
			}

		case len(node.OwnDataRefs) == 1:
			// Non-aggregator with a single volume: one stream keyed on the node.
			s := cfg.Progress.NewStream(node.Ref().Name, 0)

			if t.done {
				s.Done()

				out[streamKey{node: node}] = streamHandle{stream: s}
			} else {
				out[streamKey{node: node}] = streamHandle{stream: s, seeded: seedStreamFromDisk(cfg, s, t.nodeDir)}
			}

		default:
			// Aggregator/manifest-only nodes: no stream.
		}
	}

	return out
}

// seedStreamFromDisk seeds s with already-committed on-disk bytes scanned
// from nodeDir's block chunk dir and/or filesystem staging dir, so the
// stream's FIRST rendered frame reflects real resume progress instead of
// starting at 0 — well before the DataExport becomes ready or any network
// call happens, so the first frame shows real resume progress rather than a
// spurious 0%. It is
// called once, right after the stream is created, for every task that is NOT
// already done (a done node's stream is marked Done immediately by
// the caller and never downloads anything, so seeding it would be wasted
// work).
//
// For the filesystem path this also seeds the stream's TOTAL (not only its
// current value) from the persisted sizes sidecar written by
// volume.DownloadFilesystemVolume the first time the listing was fetched
// (see volume.ScanFSStagingSizes) — without it, a resumed FS volume's
// denominator stays unknown ("???") until the DataExport becomes ready and
// the listing is re-fetched over the network, purely a leftover of the
// on-disk state predating this sidecar. It also credits files that have
// ALREADY been fully staged as a flat blob (their chunk directory was merged
// away by MergeBlockChunks), which ScanFSStagingProgress cannot see because
// that merge takes the only on-disk record of the file's raw size — the
// chunk dir's chunks.meta — with it.
//
// This is a pure best-effort display aid: a scan error is logged and
// swallowed rather than failing stream creation, since correctness never
// depends on it. It returns the total number of bytes it credited via
// s.IncrBy (0 if the scans found nothing to seed, e.g. a from-scratch
// volume). The caller (precreateStreams) stores this in the stream's
// streamHandle.seeded so downloadBlock/downloadFS can later wrap onProgress
// with skipSeededBytes(seeded, ...): rather than resetting the stream with
// SetCurrent(0) once the real transfer starts — which handed crediting back
// to the existing resume-skip logic inside DownloadBlockChunks/
// stageCompressedFile but visibly dipped the displayed value back to 0%
// first, which must never happen — skipSeededBytes discards
// exactly the first `seeded` bytes that resume-skip logic re-derives for the
// SAME already-committed state, so the displayed value only ever moves
// forward from the seed onward while the final total still lands exactly on
// the volume size.
func seedStreamFromDisk(cfg Config, s progress.Stream, nodeDir string) int64 {
	ext := cfg.Compression.Ext()
	dest := flatDest(nodeDir, ext)

	var seeded int64

	blockCommitted, blockTotal, err := volume.ScanBlockChunkProgress(dest.chunkDir, ext)
	if err != nil {
		cfg.Log.Warn("failed to scan block resume progress for seeding",
			slog.String("dir", dest.chunkDir),
			slog.String("error", err.Error()))
	} else if blockCommitted > 0 {
		if blockTotal > 0 {
			s.SetTotal(blockTotal)
		}

		s.IncrBy(int(blockCommitted))

		seeded += blockCommitted
	}

	fsCommitted, err := volume.ScanFSStagingProgress(dest.fsTarStagingDir, ext)
	if err != nil {
		cfg.Log.Warn("failed to scan filesystem resume progress for seeding",
			slog.String("dir", dest.fsTarStagingDir),
			slog.String("error", err.Error()))

		return seeded
	}

	if fsCommitted > 0 {
		s.IncrBy(int(fsCommitted))

		seeded += fsCommitted
	}

	sizesTotal, stagedBytes, sizesFound, err := volume.ScanFSStagingSizes(dest.fsTarStagingDir, ext)
	if err != nil {
		cfg.Log.Warn("failed to scan filesystem sizes sidecar for seeding",
			slog.String("dir", dest.fsTarStagingDir),
			slog.String("error", err.Error()))

		return seeded
	}

	if !sizesFound {
		return seeded
	}

	if sizesTotal > 0 {
		s.SetTotal(sizesTotal)
	}

	if stagedBytes > 0 {
		s.IncrBy(int(stagedBytes))

		seeded += stagedBytes
	}

	return seeded
}

// lookupStream returns the pre-created streamHandle for the given node, or a
// zero streamHandle (nil stream, 0 seeded) when streams is nil (progress
// disabled) or the key is absent.
func lookupStream(streams map[streamKey]streamHandle, node *source.Node) streamHandle {
	if streams == nil {
		return streamHandle{}
	}

	return streams[streamKey{node: node}]
}

// collectNodeTasks performs a depth-first traversal of the snapshot tree, computing
// the target directory and resume state for each node.
//
// The root node uses ScanAbsolute (user-controlled path, no collision redirect).
// Child nodes use ScanNode (naming-convention path, collision-aware).
func collectNodeTasks(root *source.Node, outputDir string) ([]nodeTask, error) {
	rootPlan, err := archive.ScanAbsolute(outputDir, nodeIdentity(root))
	if err != nil {
		return nil, fmt.Errorf("scan root directory %s: %w", outputDir, err)
	}

	var tasks []nodeTask

	if err := collectDFS(root, rootPlan, &tasks); err != nil {
		return nil, err
	}

	return tasks, nil
}

// collectDFS appends a nodeTask for node and recursively visits its children.
// plan carries the already-computed resume state and target directory for node.
func collectDFS(node *source.Node, plan archive.NodeResumePlan, tasks *[]nodeTask) error {
	*tasks = append(*tasks, nodeTask{
		node:     node,
		nodeDir:  plan.TargetDir,
		done:     plan.Done,
		observed: plan.Observed,
	})

	if len(node.Children) == 0 {
		return nil
	}

	// Children live inside plan.TargetDir/snapshots/ (uses the possibly-redirected dir).
	snapshotsDir := filepath.Join(plan.TargetDir, archive.SnapshotsDirName)

	for _, child := range node.Children {
		childPlan, err := archive.ScanNode(snapshotsDir, nodeIdentity(child))
		if err != nil {
			return fmt.Errorf("scan child %s/%s: %w", child.Kind, child.Name, err)
		}

		if err := collectDFS(child, childPlan, tasks); err != nil {
			return err
		}
	}

	return nil
}

// dedupeSiblingTargetDirs detects sibling nodes that resolve to the SAME on-disk
// directory within a single run and redirects the duplicates to deterministic
// collision paths, so the worker errgroup never runs two writers over one node
// directory (inv. #10b).
//
// Why this is needed despite ScanNode's own cross-run collision redirect:
// collectDFS computes each child's plan with archive.ScanNode independently, and
// the on-disk directory name derives from the SOURCE object name
// (nodeIdentity.DirName). Two sibling snapshot CRs referencing the SAME source
// object therefore map to the same <parent>/snapshots/<kind>_<source>/ directory
// and, on a FRESH run, both classify not-done/pending (nothing on disk yet) — so
// ScanNode cannot tell them apart and the Workers errgroup would process them
// concurrently: two writers over one chunk dir / staging dir / snapshot.yaml.
// That is exactly the single-writer violation the cross-process advisory flock
// prevents across processes, but INSIDE one process where the flock cannot help.
// ScanNode's redirect protects ACROSS runs (a complete/partial dir owned by a
// different snapshot); this guard protects WITHIN one run — both are needed.
//
// The grouping key is the naming-convention primary directory
// (conventionPrimaryDir), NOT the possibly-already-redirected task.nodeDir: on a
// resumed run ScanNode may have moved a later sibling off the primary onto a
// checksum-derived collision path, so comparing final TargetDirs would miss the
// collision and the sibling would never resume its own partial data. Keying on
// the stable convention dir keeps the first-occurrence-keeps-primary decision
// and the duplicate's own-identity collision suffix identical on every run.
// Iterating in the collected (deterministic DFS) order makes "first" stable.
// When no two siblings share a convention dir every group has one member and the
// list is returned unchanged (zero behavior change).
func dedupeSiblingTargetDirs(tasks []nodeTask, log *slog.Logger) ([]nodeTask, error) {
	firstAt := make(map[string]*source.Node, len(tasks))
	out := make([]nodeTask, 0, len(tasks))

	for i := 0; i < len(tasks); {
		task := tasks[i]
		primary := conventionPrimaryDir(task)

		if first, dup := firstAt[primary]; dup {
			// Consume the whole DFS-preorder subtree of the duplicate and replace
			// it with the re-scanned subtree rooted at the collision path.
			end := subtreeEnd(tasks, i)

			redirected, err := redirectDuplicateSubtree(task, first, log)
			if err != nil {
				return nil, err
			}

			out = append(out, redirected...)
			i = end

			continue
		}

		firstAt[primary] = task.node
		out = append(out, task)
		i++
	}

	return out, nil
}

// conventionPrimaryDir returns the naming-convention primary directory for task
// — the path ScanNode/ScanAbsolute would use for the node ABSENT any collision
// redirect: <parent>/<kindlower>_<sourceName>. Because it is derived from the
// node's own Kind + source-name component (never from task.nodeDir's basename),
// it is identical whether or not collectDFS's ScanNode already redirected
// task.nodeDir to a checksum-suffixed path on a resumed run — making it the
// stable grouping key for within-run duplicate detection across runs. filepath.Dir
// recovers the parent identically for a primary dir and a "<primary>__<short>"
// collision dir, since the redirect only appends to the basename.
func conventionPrimaryDir(task nodeTask) string {
	return filepath.Join(filepath.Dir(task.nodeDir), archive.NodeDirName(task.node.Kind, nodeDirOf(task.node)))
}

// subtreeEnd returns the exclusive end index of the DFS-preorder subtree rooted
// at tasks[i]: the first index > i whose node directory is NOT nested under
// tasks[i].nodeDir. Because collectDFS emits nodes in preorder, a node's whole
// subtree occupies the contiguous block [i, subtreeEnd(tasks, i)). The trailing
// separator on the prefix prevents a sibling whose name shares a prefix (e.g.
// "foo" vs "foobar") from being mistaken for a descendant.
func subtreeEnd(tasks []nodeTask, i int) int {
	prefix := tasks[i].nodeDir + string(os.PathSeparator)

	j := i + 1
	for j < len(tasks) && strings.HasPrefix(tasks[j].nodeDir, prefix) {
		j++
	}

	return j
}

// redirectDuplicateSubtree redirects a duplicate sibling node (and its whole
// subtree) off the shared primary directory to a collision path keyed on the
// duplicate's OWN CR identity, then re-scans the subtree at the new location so
// the returned tasks' state/TargetDir reflect any existing collision-dir
// contents.
//
// The suffix is nodeCollisionShort(node) — first 8 hex of
// sha256(kind\x00namespace\x00name), the node's own identity, NOT a data
// checksum (none exists yet) and NOT random — so a resumed run recomputes the
// same collision path and resumes the duplicate's own partial data from it
// (inv. #10b). ScanAbsolute classifies the collision root (rejecting a foreign
// occupant with ErrIdentityMismatch, astronomically unlikely under an
// identity-derived suffix); collectDFS then re-scans descendants via ScanNode so
// the whole subtree moves WITH the redirected node instead of being stranded
// under the first occupant's directory.
func redirectDuplicateSubtree(task nodeTask, first *source.Node, log *slog.Logger) ([]nodeTask, error) {
	node := task.node
	parentDir := filepath.Dir(task.nodeDir)
	sourceName := nodeDirOf(node)
	collisionDir := archive.CollisionNodeDir(parentDir, node.Kind, sourceName, nodeCollisionShort(node))

	log.Warn("sibling snapshot nodes resolve to the same target directory; redirecting the duplicate to a collision path",
		slog.String("shared_dir", conventionPrimaryDir(task)),
		slog.String("source_name", sourceName),
		slog.String("first_kind", first.Kind),
		slog.String("first_name", first.Name),
		slog.String("duplicate_kind", node.Kind),
		slog.String("duplicate_name", node.Name),
		slog.String("collision_dir", collisionDir))

	plan, err := archive.ScanAbsolute(collisionDir, nodeIdentity(node))
	if err != nil {
		return nil, fmt.Errorf("scan collision dir %s for %s/%s: %w", collisionDir, node.Kind, node.Name, err)
	}

	var redirected []nodeTask
	if err := collectDFS(node, plan, &redirected); err != nil {
		return nil, err
	}

	return redirected, nil
}

// nodeCollisionShort derives a stable 8-hex collision suffix from a node's own
// CR identity (kind/namespace/name). It is deterministic — not random and not a
// data checksum — so within-run duplicate redirection lands on the same path on
// every run, letting a resumed run recompute the path and resume the node's own
// partial data (inv. #10b). It mirrors archive.identityMarkerShort's construction
// (NUL-joined identity fields, first 8 hex of sha256 via archive.ShortChecksum).
func nodeCollisionShort(node *source.Node) string {
	sum := sha256.Sum256([]byte(strings.Join(
		[]string{node.Kind, node.Namespace, node.Name}, "\x00")))

	return archive.ShortChecksum(fmt.Sprintf("%x", sum[:]))
}

// resolveSubtreeRoot returns the node to start processing from and its on-disk
// directory. When neither SelectedNodeKind nor SelectedNodeName is set in cfg it
// returns (root, cfg.OutputDir) for a full-tree download. When both are set it
// finds the node in the already-built tree, scaffolds content-free ancestor
// directories under cfg.OutputDir so the selected node sits at its real path, and
// returns (selectedNode, selectedNodeDir).
func resolveSubtreeRoot(root *source.Node, cfg Config) (*source.Node, string, error) {
	if cfg.SelectedNodeKind == "" || cfg.SelectedNodeName == "" {
		return root, cfg.OutputDir, nil
	}

	selected, ancestors, err := source.FindNode(root, cfg.SelectedNodeKind, cfg.SelectedNodeName)
	if err != nil {
		return nil, "", fmt.Errorf("find node %s/%s: %w", cfg.SelectedNodeKind, cfg.SelectedNodeName, err)
	}

	selectedDir, err := buildSubtreeScaffold(cfg.OutputDir, selected, ancestors)
	if err != nil {
		return nil, "", fmt.Errorf("scaffold for %s/%s: %w", cfg.SelectedNodeKind, cfg.SelectedNodeName, err)
	}

	return selected, selectedDir, nil
}

// buildSubtreeScaffold creates the content-free ancestor directory chain so the
// selected node lands at its real path under outputDir, and returns the absolute
// directory path for the selected node.
//
// When the selected node is the root (len(ancestors) == 0) outputDir is returned
// directly — no scaffold directories are created because the root already occupies
// the user-supplied output directory.
//
// For deeper selections the path is built ancestor-by-ancestor:
//
//	outputDir/
//	  snapshots/<ancestor[1]-dir>/        ← scaffold (no content)
//	    snapshots/<ancestor[2]-dir>/      ← scaffold (no content)
//	      …
//	        snapshots/<selected-dir>/     ← subtree root (returned)
//
// Scaffold directories are created with archive.EnsureDir (os.MkdirAll). They hold
// no snapshot.yaml, no manifests/, no data, and no sibling subtrees.
func buildSubtreeScaffold(outputDir string, selected *source.Node, ancestors []*source.Node) (string, error) {
	if len(ancestors) == 0 {
		// selected IS the root; it occupies outputDir directly.
		return outputDir, nil
	}

	// Walk ancestors after the root (ancestors[0]), which is represented by outputDir.
	current := outputDir

	for _, anc := range ancestors[1:] {
		current = filepath.Join(current, archive.SnapshotsDirName, archive.NodeDirName(anc.Kind, nodeDirOf(anc)))

		if err := archive.EnsureDir(current); err != nil {
			return "", fmt.Errorf("create scaffold dir %s: %w", current, err)
		}
	}

	// Place the selected node inside the last ancestor's snapshots/ subdirectory.
	selectedDir := filepath.Join(current, archive.SnapshotsDirName, archive.NodeDirName(selected.Kind, nodeDirOf(selected)))

	if err := archive.EnsureDir(selectedDir); err != nil {
		return "", fmt.Errorf("create subtree root dir %s: %w", selectedDir, err)
	}

	return selectedDir, nil
}

// nodeDirOf returns the directory-name component for node. It returns node.SourceName
// when set and falls back to node.Name, mirroring the DirName logic in nodeIdentity.
func nodeDirOf(node *source.Node) string {
	if node.SourceName != "" {
		return node.SourceName
	}

	return node.Name
}

// processNode executes all download and finalization steps for one node task.
// It is called concurrently by the worker pool.
//
// Volume nodes (task.node.Binding != nil) are handled by processVolumeNode.
// Snapshot nodes with OwnDataRefs download their own volume data directly into
// the node directory (flat for 1 ref; multi-volume for >1). Aggregator snapshot
// nodes (no OwnDataRefs, may have orphan leaf children) write manifests only.
func processNode(ctx context.Context, cfg Config, task nodeTask, streams map[streamKey]streamHandle) error {
	if task.done {
		// Streams for done nodes were already marked Done in precreateStreams.
		cfg.Log.Info("node already complete, skipping",
			slog.String("kind", task.node.Kind),
			slog.String("name", task.node.Name))

		return nil
	}

	cfg.Log.Info("processing node",
		slog.String("kind", task.node.Kind),
		slog.String("name", task.node.Name),
		slog.String("resume_state", string(task.observed)))

	if task.node.Binding != nil {
		return processVolumeNode(ctx, cfg, task, streams)
	}

	// Snapshot node: ensure subdirs, write manifests, then download own data if present.
	withSnapshots := len(task.node.Children) > 0
	if err := ensureNodeSubdirs(task.nodeDir, nodeIdentity(task.node), withSnapshots); err != nil {
		return fmt.Errorf("ensure subdirs for %s/%s: %w", task.node.Kind, task.node.Name, err)
	}

	if err := volume.WriteNodeManifests(ctx, cfg.ManifestSource, task.nodeDir, task.node); err != nil {
		return fmt.Errorf("write manifests for %s/%s: %w", task.node.Kind, task.node.Name, err)
	}

	if len(task.node.OwnDataRefs) > 0 {
		if err := downloadOwnDataRefs(ctx, cfg, task.node, task.nodeDir, streams); err != nil {
			return fmt.Errorf("download own volumes for %s/%s: %w", task.node.Kind, task.node.Name, err)
		}
	}

	if err := volume.FinalizeNode(task.nodeDir, task.node); err != nil {
		return fmt.Errorf("finalize %s/%s: %w", task.node.Kind, task.node.Name, err)
	}

	cfg.Log.Info("node complete",
		slog.String("kind", task.node.Kind),
		slog.String("name", task.node.Name))

	return nil
}

// processVolumeNode handles a volume node (task.node.Binding != nil).
// It writes the captured PVC manifest, applies the block-resume guard, downloads
// the volume data, and finalizes the node directory.
// Volume nodes are always leaves: no snapshots/ subdirectory is created.
func processVolumeNode(ctx context.Context, cfg Config, task nodeTask, streams map[streamKey]streamHandle) error {
	if err := ensureNodeSubdirs(task.nodeDir, nodeIdentity(task.node), false); err != nil {
		return fmt.Errorf("ensure subdirs for %s/%s: %w", task.node.Kind, task.node.Name, err)
	}

	if err := volume.WriteVolumeManifest(ctx, cfg.ManifestSource, task.nodeDir, task.node); err != nil {
		return fmt.Errorf("write volume manifest for %s/%s: %w", task.node.Kind, task.node.Name, err)
	}

	dest := flatDest(task.nodeDir, cfg.Compression.Ext())

	_, blockAlreadyMerged, err := archive.FindBlockData(task.nodeDir)
	if err != nil {
		return fmt.Errorf("find block data in %s: %w", task.nodeDir, err)
	}

	fsTarDone, err := fsTarComplete(dest.fsTarPath)
	if err != nil {
		return fmt.Errorf("check fs tar in %s: %w", task.nodeDir, err)
	}

	handle := lookupStream(streams, task.node)

	switch {
	case blockAlreadyMerged:
		cfg.Log.Info("block volume already merged, skipping download",
			slog.String("kind", task.node.Kind),
			slog.String("name", task.node.Name))

		// The skip branch OWNS the leftover-chunk-dir cleanup for the crash
		// window in volume.MergeBlockChunks between committing the merged file
		// and removing the chunk dir (inv. #1); no later run ever revisits it.
		removeMergedBlockChunkDir(cfg, dest.chunkDir)

		if handle.stream != nil {
			handle.stream.Done()
		}

	case fsTarDone:
		cfg.Log.Info("fs tar already complete, skipping download",
			slog.String("kind", task.node.Kind),
			slog.String("name", task.node.Name))

		if handle.stream != nil {
			handle.stream.Done()
		}

	default:
		if err := downloadVolumeBinding(ctx, cfg, task.node.Ref(), task.node.Namespace, dest, handle); err != nil {
			return fmt.Errorf("download volume for %s/%s: %w", task.node.Kind, task.node.Name, err)
		}
	}

	if err := volume.FinalizeNode(task.nodeDir, task.node); err != nil {
		return fmt.Errorf("finalize %s/%s: %w", task.node.Kind, task.node.Name, err)
	}

	cfg.Log.Info("node complete",
		slog.String("kind", task.node.Kind),
		slog.String("name", task.node.Name))

	return nil
}

// downloadOwnDataRefs downloads the OwnDataRef volume for a non-aggregator
// snapshot node into nodeDir using the flat layout — data.bin[.<ext>] /
// data.tar directly in nodeDir, with the block-resume guard (skip if any
// data.bin* file already exists).
//
// Variant A (decision #9, .agent/implementer-prompt.md:125-140) guarantees a
// SnapshotContent carries AT MOST ONE dataRef, so refs has length 0 or 1 in
// every real payload. len(refs) > 1 is therefore a contract violation from an
// unexpected producer, not a supported multi-volume layout — reject it loudly
// instead of guessing at a per-pvc destination.
func downloadOwnDataRefs(
	ctx context.Context,
	cfg Config,
	node *source.Node,
	nodeDir string,
	streams map[streamKey]streamHandle,
) error {
	refs := node.OwnDataRefs

	if len(refs) > 1 {
		return fmt.Errorf("node %s/%s carries %d dataRefs but Variant A allows at most one per SnapshotContent (see decision #9); refusing to guess a multi-volume layout",
			node.Kind, node.Name, len(refs))
	}

	if len(refs) == 0 {
		return nil
	}

	// Flat single-volume layout: reuse the same paths as leaf volume nodes.
	dest := flatDest(nodeDir, cfg.Compression.Ext())
	handle := lookupStream(streams, node)

	_, found, err := archive.FindBlockData(nodeDir)
	if err != nil {
		return fmt.Errorf("find block data in %s: %w", nodeDir, err)
	}

	if found {
		cfg.Log.Info("block volume already merged, skipping download",
			slog.String("kind", node.Kind),
			slog.String("name", node.Name))

		// The skip branch OWNS the leftover-chunk-dir cleanup for the crash
		// window in volume.MergeBlockChunks between committing the merged file
		// and removing the chunk dir (inv. #1); no later run ever revisits it.
		removeMergedBlockChunkDir(cfg, dest.chunkDir)

		if handle.stream != nil {
			handle.stream.Done()
		}

		return nil
	}

	fsTarDone, err := fsTarComplete(dest.fsTarPath)
	if err != nil {
		return fmt.Errorf("check fs tar in %s: %w", nodeDir, err)
	}

	if fsTarDone {
		cfg.Log.Info("fs tar already complete, skipping download",
			slog.String("kind", node.Kind),
			slog.String("name", node.Name))

		if handle.stream != nil {
			handle.stream.Done()
		}

		return nil
	}

	return downloadVolumeBinding(ctx, cfg, node.Ref(), node.Namespace, dest, handle)
}

// removeMergedBlockChunkDir deletes a leftover block chunk staging directory
// (data.bin.d/) found next to an already-merged data.bin* file, compensating the
// crash window in volume.MergeBlockChunks between committing the merged file
// (aw.Commit) and removing the chunk dir (os.RemoveAll). A hard kill in that
// window leaves BOTH the durable merged file and a full compressed copy of the
// volume in the chunk dir; every later resume takes the blockAlreadyMerged skip
// branch and nothing else ever revisits the chunk dir, so without this cleanup
// the staging copy leaks permanently (inv. #1 — the skip branch owns leftover
// cleanup). os.RemoveAll is a no-op when the chunk dir is absent, so the normal
// (no-leftover) path is unchanged.
//
// A RemoveAll failure is logged as a WARN and swallowed, never returned: the
// download is already complete (the merged file is durable), so losing
// best-effort cleanup must not fail an otherwise successful node (code-style §5).
func removeMergedBlockChunkDir(cfg Config, chunkDir string) {
	if err := os.RemoveAll(chunkDir); err != nil {
		cfg.Log.Warn("failed to remove leftover block chunk dir after merge",
			slog.String("dir", chunkDir),
			slog.String("error", err.Error()))
	}
}

// ensureNodeSubdirs creates manifests/ and, when the node has children, snapshots/
// inside nodeDir, and stamps the node's identity marker on first touch.
//
// The identity marker is written BEFORE any chunk/staging/volume data lands, so a
// later resume can prove a partial (not-yet-finalized) dir belongs to THIS
// snapshot — snapshot.yaml, the only other identity record, is written just at
// finalize (inv. #9). archive.WriteNodeIdentityMarker is a no-op when a marker
// already exists, so it is safe to call on every reconcile of the same node; it
// is checksum-neutral (excluded from ComputeNodeChecksum) and survives the
// stale-*.tmp resume sweep. The marker is short-lived: volume.FinalizeNode
// removes it once snapshot.yaml is durably written (and the Done scan branches
// self-heal any crash-window leftover), so a finalized node never keeps a stray
// identity.json.
func ensureNodeSubdirs(nodeDir string, id archive.NodeIdentity, withSnapshots bool) error {
	if err := archive.EnsureDir(filepath.Join(nodeDir, archive.ManifestsDirName)); err != nil {
		return err
	}

	if err := archive.WriteNodeIdentityMarker(nodeDir, id); err != nil {
		return fmt.Errorf("write identity marker in %s: %w", nodeDir, err)
	}

	if !withSnapshots {
		return nil
	}

	return archive.EnsureDir(filepath.Join(nodeDir, archive.SnapshotsDirName))
}

// volumeDestPaths holds the resolved absolute paths for one volume's output
// within a node directory. The caller constructs it via flatDest.
type volumeDestPaths struct {
	// chunkDir is the directory that receives block chunk files during download.
	chunkDir string
	// blockPath is the merged block output file (data.bin[.<ext>]).
	blockPath string
	// fsTarPath is the final assembled tar file (data.tar).
	fsTarPath string
	// fsTarStagingDir is the temporary directory for raw per-file downloads
	// (data.tar.d/).
	fsTarStagingDir string
}

// flatDest returns the single-volume flat destination paths for nodeDir.
// ext is codec.Ext() and determines the block file name suffix.
// Used for leaf volume nodes and snapshot nodes with exactly one OwnDataRef —
// the only two shapes Variant A (decision #9) allows.
func flatDest(nodeDir, ext string) volumeDestPaths {
	return volumeDestPaths{
		chunkDir:        filepath.Join(nodeDir, archive.BlockChunksDirName),
		blockPath:       filepath.Join(nodeDir, archive.DataBlockName(ext)),
		fsTarPath:       filepath.Join(nodeDir, archive.FsTarName),
		fsTarStagingDir: filepath.Join(nodeDir, archive.FsTarStagingDirName),
	}
}

// downloadVolumeBinding opens a DataExport for the snapshot leaf identified by
// leafRef, downloads the volume data (block or filesystem) to dest, and releases
// the DataExport on completion or error.
//
// handle carries the pre-created progress handle for this volume (nil stream
// when progress tracking is disabled) plus the number of bytes
// seedStreamFromDisk already credited to it (handle.seeded), which
// downloadBlock/downloadFS thread into skipSeededBytes. handle.stream is
// marked Done on success or Fail on error/cancellation for every return path
// from the point the stream semaphore is acquired onward (via defer),
// including an OpenExport failure. The one path this function cannot itself
// cover is failing to acquire the semaphore in the first place (e.g. ctx
// cancelled while queued behind MaxParallelDownloads) — that case, and any
// other future early-return gap, is caught by Run's post-g.Wait() defensive
// sweep over every pre-created stream.
//
// leafRef addresses the snapshot leaf CR that the DataExport controller will
// resolve via leaf.status.boundSnapshotContentName → SnapshotContent → dataRef.
// For CSI VolumeSnapshot visibility-leaves leafRef.Kind == "VolumeSnapshot"; for
// domain snapshot nodes it carries the domain group and kind.
//
// namespace is the Kubernetes namespace for the DataExport.
// dest specifies where block chunks, the merged block file, and filesystem files go.
func downloadVolumeBinding(
	ctx context.Context,
	cfg Config,
	leafRef aggapi.NodeRef,
	namespace string,
	dest volumeDestPaths,
	handle streamHandle,
) error {
	stream := handle.stream

	// Acquire one slot from the global stream semaphore before opening the
	// DataExport. This caps the number of concurrently active volume-stream
	// downloads across all nodes, independently of the node-level Workers errgroup
	// and the per-volume PerVolumeConcurrency errgroup. Cancelling ctx (e.g. on
	// sibling error or SIGINT) unblocks a waiting Acquire.
	if err := cfg.streamSem.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("acquire stream semaphore for %s/%s: %w", leafRef.Kind, leafRef.Name, err)
	}

	defer cfg.streamSem.Release(1)

	// downloadErr captures this function's terminal outcome; it is a plain
	// local (not a named return — nonamedreturns is enforced repo-wide) set on
	// every return path below and read only by the deferred Fail-or-Done
	// closure immediately following. Registering that closure HERE — right
	// after the semaphore is acquired, rather than after cfg.OpenExport
	// returns — ensures an OpenExport failure (e.g. ctx cancelled mid-
	// WaitReady) still settles the stream locally instead of relying solely on
	// Run's post-g.Wait() sweep to catch it.
	var downloadErr error

	if stream != nil {
		defer func() {
			if downloadErr != nil {
				stream.Fail()

				return
			}

			stream.Done()
		}()
	}

	// Register the release-by-name defer BEFORE calling cfg.OpenExport, so it
	// runs on EVERY return path — including when OpenExport itself fails, e.g.
	// ctx is cancelled while still polling WaitReady. cfg.OpenExport's
	// production implementation creates the DataExport CR (EnsureDataExport)
	// BEFORE waiting for it to become Ready (WaitReady); a cancellation during
	// that wait previously returned before any cleanup defer was registered,
	// permanently leaking the DataExport until its TTL expired. Releasing by
	// the deterministic name (rather than through the *exporter.Export OpenExport
	// would have returned) works even when OpenExport never returned one:
	// exporter.ReleaseDataExport treats NotFound as success, so this defer is a
	// safe no-op on the paths where no DataExport was ever created.
	//
	// The release timeout is deliberately derived FRESH, right here inside the
	// closure, at the moment it actually runs — NOT once up front before
	// cfg.OpenExport. This closure only executes at function return, i.e. after
	// the full OpenExport (EnsureDataExport + WaitReady) AND the entire volume
	// transfer have already completed, which routinely exceeds ReleaseTimeout for
	// any real-sized volume. A single timeout created before that work would
	// already be expired by the time release is attempted, failing the release
	// Get immediately even on a fully successful download (live-reproduced).
	// Deriving from
	// context.WithoutCancel(ctx) keeps release running when ctx itself is
	// cancelled (e.g. by errgroup on sibling error or by SIGINT).
	defer func() {
		if cfg.KeepExports {
			cfg.Log.Info("leaving DataExport in cluster (--cleanup=false)",
				slog.String("leaf", leafRef.Name))

			return
		}

		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ReleaseTimeout)
		defer releaseCancel()

		deName := exporter.DataExportName(leafRef.Name)
		if relErr := exporter.ReleaseDataExport(releaseCtx, cfg.KubeClient, cfg.Log, namespace, deName, cfg.RunID); relErr != nil {
			cfg.Log.Warn("failed to release DataExport",
				slog.String("leaf", leafRef.Name),
				slog.String("error", relErr.Error()))
		}
	}()

	exp, err := cfg.OpenExport(ctx, namespace, leafRef, cfg.TTL)
	if err != nil {
		downloadErr = fmt.Errorf("open DataExport for %s/%s: %w", leafRef.Kind, leafRef.Name, err)

		return downloadErr
	}

	// Flip the bar from "waiting for export…" to the live byte-counter display
	// now that the DataExport is ready and bytes are about to flow.
	if stream != nil {
		stream.Activate()
	}

	cfg.Log.Info("downloading volume",
		slog.String("leaf", leafRef.Name),
		slog.String("volume_mode", exp.VolumeMode()))

	switch exp.VolumeMode() {
	case "Block":
		downloadErr = downloadBlock(ctx, cfg, dest, exp, stream, handle.seeded)

	case "Filesystem":
		downloadErr = downloadFS(ctx, cfg, dest, exp, stream, handle.seeded)

	default:
		downloadErr = fmt.Errorf("unsupported volume mode %q for leaf %s/%s", exp.VolumeMode(), leafRef.Kind, leafRef.Name)
	}

	return downloadErr
}

// downloadBlock downloads a block volume's chunks and merges them. seeded is
// the number of bytes seedStreamFromDisk already credited to stream before
// the transfer started (0 if stream was not seeded, e.g. progress disabled
// or a from-scratch volume); it is threaded into skipSeededBytes so
// DownloadBlockChunks' own resume-skip crediting — which re-derives and
// re-credits those SAME already-committed bytes from the same chunkDir state
// — does not double-count them, without ever resetting stream's displayed
// value back to 0 (see skipSeededBytes).
func downloadBlock(ctx context.Context, cfg Config, dest volumeDestPaths, exp *exporter.Export, stream progress.Stream, seeded int64) error {
	blockURL, err := exporter.BlockURL(exp.BaseURL())
	if err != nil {
		return fmt.Errorf("build block URL: %w", err)
	}

	totalSize, err := exp.Fetcher().HeadVolume(ctx, blockURL)
	if err != nil {
		return fmt.Errorf("HEAD block volume: %w", err)
	}

	// Update the stream's expected total now that we know the volume size.
	var onProgress func(n int)

	if stream != nil {
		// A fresh HEAD total below the value seedStreamFromDisk already
		// credited proves that seed was computed from an on-disk chunk
		// geometry DownloadBlockChunks' ensureChunkGeometry is about to purge
		// (a changed --chunk-size or a shrunk volume): after that purge the
		// resume-skip crediting re-derives from byte zero, so leaving the seed
		// in place would strand the bar above 100% until Done() forces it back
		// down. Reset the displayed value to 0 BEFORE lowering the total, so
		// the bar never renders current > total, and neutralize the seed so
		// skipSeededBytes forwards every re-downloaded byte (inv. #9c). The
		// HEAD size is authoritative even at 0, so there is no total > 0 guard.
		// A valid seed (seeded <= totalSize) is left untouched: no dip,
		// monotonic forward progress preserved — the displayed value must never
		// move backward (inv. #7).
		if seeded > totalSize {
			stream.SetCurrent(0)

			seeded = 0
		}

		stream.SetTotal(totalSize)
		onProgress = skipSeededBytes(seeded, stream.IncrBy)
	}

	if err := volume.DownloadBlockChunks(ctx, cfg.Log, dest.chunkDir, blockURL, totalSize, cfg.ChunkSize, cfg.PerVolumeConcurrency, exp.Fetcher(), cfg.Compression, onProgress); err != nil {
		return fmt.Errorf("download block chunks: %w", err)
	}

	return volume.MergeBlockChunks(ctx, dest.chunkDir, dest.blockPath, totalSize, cfg.ChunkSize, cfg.Compression.Ext())
}

// downloadFS downloads a filesystem volume's files and assembles the tar.
// seeded plays the same role as in downloadBlock: the number of bytes
// seedStreamFromDisk already credited to stream, threaded into
// skipSeededBytes so the per-file/per-chunk resume-skip crediting inside
// DownloadFilesystemVolume/stageCompressedFile — which re-derives and
// re-credits those SAME already-committed bytes once the listing completes —
// does not double-count them.
func downloadFS(ctx context.Context, cfg Config, dest volumeDestPaths, exp *exporter.Export, stream progress.Stream, seeded int64) error {
	filesURL, err := exporter.FilesURL(exp.BaseURL())
	if err != nil {
		return fmt.Errorf("build files URL: %w", err)
	}

	var (
		onProgress func(n int)
		setTotal   func(total int64)
	)

	if stream != nil {
		// skip is (re)built inside setTotal from the effective (possibly
		// clamped) seed. DownloadFilesystemVolume calls setTotal exactly once,
		// after the listing establishes the total and BEFORE it spawns any
		// file-staging worker, so the workers that later call onProgress observe
		// the built skip through the errgroup goroutine-start happens-before
		// edge — the write is sequenced before the first g.Go, so no lock is
		// needed and -race stays clean. No bytes are ever credited before
		// setTotal runs, so the nil guard only covers the early-return paths
		// (tar already complete) where onProgress is never called at all.
		var skip func(n int)

		setTotal = func(total int64) {
			effSeeded := seeded

			// A positive fresh listing total below the seeded value proves the
			// sizes sidecar the seed came from is stale (a volume that changed
			// between runs): reset the display to 0 BEFORE lowering the total so
			// the bar never renders current > total, and drop the effective seed
			// so every re-staged byte is forwarded (inv. #9c). total <= 0 means
			// the listing omitted per-file sizes (size unknown), never proof the
			// volume shrank, so it is NOT treated as stale. A valid seed
			// (seeded <= total) is untouched — no dip (inv. #7).
			if total > 0 && seeded > total {
				stream.SetCurrent(0)

				effSeeded = 0
			}

			skip = skipSeededBytes(effSeeded, stream.IncrBy)
			stream.SetTotal(total)
		}

		onProgress = func(n int) {
			if skip != nil {
				skip(n)
			}
		}
	}

	return volume.DownloadFilesystemVolume(ctx, cfg.Log, dest.fsTarPath, dest.fsTarStagingDir, filesURL, cfg.PerVolumeConcurrency, cfg.ChunkSize, exp.Fetcher(), cfg.Compression, setTotal, onProgress)
}

// skipSeededBytes wraps onProgress so that credits toward the first `seeded`
// bytes are discarded and only bytes beyond that are forwarded. It replaces
// the previous stream.SetCurrent(0) reset that downloadBlock/downloadFS used
// to hand crediting back to the block/fs resume-skip logic: zeroing the
// stream visibly dipped the displayed value back to 0% right at the
// waiting->active transition, before that resume-skip logic re-climbed it —
// a backward move the displayed value must never make. Here the stream keeps
// whatever seedStreamFromDisk already credited it with, and this wrapper absorbs the
// resume-skip logic's re-derivation of those SAME bytes — the net effect is
// identical (the final total still lands on seeded + (totalSize - seeded) ==
// totalSize) but the displayed value now only ever moves forward.
//
// onProgress may be invoked concurrently by multiple chunk/file workers;
// position is a single atomic counter so each call claims a distinct,
// non-overlapping [before, after) slice of the byte stream regardless of
// interleaving. Which specific bytes land on the "seeded" side of the cut
// versus the "new" side depends on call order and is not guaranteed to align
// with which bytes were literally pre-existing on disk — only the aggregate
// counts (seeded bytes discarded, totalSize-seeded bytes forwarded) and
// monotonicity are guaranteed, which is all the displayed bar needs.
// Returns onProgress unchanged when there is nothing to skip (seeded <= 0)
// or onProgress is nil, so the from-scratch (unseeded) and progress-disabled
// paths are exactly as before.
func skipSeededBytes(seeded int64, onProgress func(n int)) func(n int) {
	if onProgress == nil || seeded <= 0 {
		return onProgress
	}

	var position atomic.Int64

	return func(n int) {
		if n <= 0 {
			return
		}

		after := position.Add(int64(n))
		before := after - int64(n)

		switch {
		case after <= seeded:
			// Entirely within the seeded region: already reflected in the
			// stream's current value, discard.
			return
		case before >= seeded:
			// Entirely beyond the seeded region: genuinely new, forward as-is.
			onProgress(n)
		default:
			// Straddles the boundary: forward only the tail beyond `seeded`.
			onProgress(int(after - seeded))
		}
	}
}

// nodeIdentity converts a source.Node into an archive.NodeIdentity for resume scanning.
// DirName is set to node.SourceName (the captured object name from the source-ref
// annotation) when available, falling back to the CR name for nodes without a
// source annotation. The on-disk directory derives from DirName; identity
// matching (snapshot.yaml fields) always uses Name (the CR name) and SourceRef.
func nodeIdentity(node *source.Node) archive.NodeIdentity {
	dirName := node.SourceName
	if dirName == "" {
		dirName = node.Name
	}

	return archive.NodeIdentity{
		APIVersion: node.APIVersion,
		Kind:       node.Kind,
		Name:       node.Name,
		DirName:    dirName,
		Namespace:  node.Namespace,
		SourceRef:  node.SourceRef,
	}
}

// fsTarComplete reports whether the assembled filesystem tar at tarPath already
// exists. Returns (true, nil) when found, (false, nil) when absent, and
// (false, err) for any other stat error.
func fsTarComplete(tarPath string) (bool, error) {
	_, err := os.Stat(tarPath)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return false, err
}
