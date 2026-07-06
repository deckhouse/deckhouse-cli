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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

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
	state   archive.NodeState
}

// streamKey identifies a pre-created progress stream.
// pvcName is empty for Binding nodes and single-OwnDataRef nodes; it holds the PVC
// name for multi-OwnDataRef nodes (matching multiDest naming).
type streamKey struct {
	node    *source.Node
	pvcName string
}

// Run builds the snapshot tree, scans the output directory for resume state, and
// downloads all missing node data with bounded concurrency.
// The first node error cancels all in-flight work.
func Run(ctx context.Context, cfg Config) error {
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

	// Pre-create one progress stream per volume leaf before the worker errgroup
	// starts, so every bar appears immediately (docker-pull style).
	streams := precreateStreams(tasks, cfg)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Workers)

	for _, t := range tasks {
		task := t

		g.Go(func() error {
			return processNode(gctx, cfg, task, streams)
		})
	}

	return g.Wait()
}

// precreateStreams creates one progress.Stream per volume download before any worker
// goroutine starts. The map is keyed by (node pointer, pvcName) so each call site can
// retrieve its handle without a second NewStream call.
//
// Streams for nodes that are already complete (NodeStateDone) are marked Done
// immediately so they render as already-complete (docker-pull "Already exists" style).
//
// Returns nil when cfg.Progress is nil (progress disabled), which causes all
// lookupStream calls to return nil and behave as no-ops.
func precreateStreams(tasks []nodeTask, cfg Config) map[streamKey]progress.Stream {
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
		case len(n.OwnDataRefs) > 1:
			nStreams += len(n.OwnDataRefs)
		}
	}

	// nStreams is already subtree-scoped (tasks comes from resolveSubtreeRoot), so
	// the live "N/M volumes downloaded" counter is automatically correct for a
	// --node selection with no extra plumbing.
	cfg.Progress.SetVolumeTotal(nStreams)

	out := make(map[streamKey]progress.Stream, nStreams)

	for _, t := range tasks {
		node := t.node

		switch {
		case node.Binding != nil:
			// Orphan VolumeSnapshot leaf: one stream keyed on the node, name = leaf ref name.
			s := cfg.Progress.NewStream(node.Ref().Name, 0)
			if t.state == archive.NodeStateDone {
				s.Done()
			}

			out[streamKey{node: node}] = s

		case len(node.OwnDataRefs) == 1:
			// Non-aggregator with a single volume: one stream keyed on the node.
			s := cfg.Progress.NewStream(node.Ref().Name, 0)
			if t.state == archive.NodeStateDone {
				s.Done()
			}

			out[streamKey{node: node}] = s

		case len(node.OwnDataRefs) > 1:
			// Non-aggregator with multiple volumes: one stream per PVC.
			for _, ref := range node.OwnDataRefs {
				pvc := ref.Target.Name
				s := cfg.Progress.NewStream(pvc, 0)

				if t.state == archive.NodeStateDone {
					s.Done()
				}

				out[streamKey{node: node, pvcName: pvc}] = s
			}

			// Aggregator/manifest-only nodes: no stream.
		}
	}

	return out
}

// lookupStream returns the pre-created progress.Stream for the given node and
// optional pvcName, or nil when streams is nil (progress disabled) or the key
// is absent.
func lookupStream(streams map[streamKey]progress.Stream, node *source.Node, pvcName string) progress.Stream {
	if streams == nil {
		return nil
	}

	return streams[streamKey{node: node, pvcName: pvcName}]
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
		node:    node,
		nodeDir: plan.TargetDir,
		state:   plan.State,
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
func processNode(ctx context.Context, cfg Config, task nodeTask, streams map[streamKey]progress.Stream) error {
	if task.state == archive.NodeStateDone {
		// Streams for NodeStateDone nodes were already marked Done in precreateStreams.
		cfg.Log.Info("node already complete, skipping",
			slog.String("kind", task.node.Kind),
			slog.String("name", task.node.Name))

		return nil
	}

	cfg.Log.Info("processing node",
		slog.String("kind", task.node.Kind),
		slog.String("name", task.node.Name),
		slog.String("resume_state", nodeStateName(task.state)))

	if task.node.Binding != nil {
		return processVolumeNode(ctx, cfg, task, streams)
	}

	// Snapshot node: ensure subdirs, write manifests, then download own data if present.
	withSnapshots := len(task.node.Children) > 0
	if err := ensureNodeSubdirs(task.nodeDir, withSnapshots); err != nil {
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
func processVolumeNode(ctx context.Context, cfg Config, task nodeTask, streams map[streamKey]progress.Stream) error {
	if err := ensureNodeSubdirs(task.nodeDir, false); err != nil {
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

	stream := lookupStream(streams, task.node, "")

	switch {
	case blockAlreadyMerged:
		cfg.Log.Info("block volume already merged, skipping download",
			slog.String("kind", task.node.Kind),
			slog.String("name", task.node.Name))

		if stream != nil {
			stream.Done()
		}

	case fsTarDone:
		cfg.Log.Info("fs tar already complete, skipping download",
			slog.String("kind", task.node.Kind),
			slog.String("name", task.node.Name))

		if stream != nil {
			stream.Done()
		}

	default:
		if err := downloadVolumeBinding(ctx, cfg, task.node.Ref(), task.node.Namespace, dest, stream); err != nil {
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

// downloadOwnDataRefs downloads all OwnDataRef volumes for a non-aggregator snapshot
// node into nodeDir.
//
//   - One OwnDataRef: flat layout — data.bin[.<ext>] / data.tar directly in nodeDir,
//     with the block-resume guard (skip if any data.bin* file already exists).
//   - Two or more OwnDataRefs: multi-volume layout — data/<pvc>.bin[.<ext>] / data/<pvc>.tar
//     per volume. The resume guard is per-pvc (skip if the specific MultiVolumeBlockName exists).
//     DataExport lifecycle is independent per volume.
func downloadOwnDataRefs(
	ctx context.Context,
	cfg Config,
	node *source.Node,
	nodeDir string,
	streams map[streamKey]progress.Stream,
) error {
	refs := node.OwnDataRefs

	if len(refs) == 1 {
		// Flat single-volume layout: reuse the same paths as leaf volume nodes.
		dest := flatDest(nodeDir, cfg.Compression.Ext())
		stream := lookupStream(streams, node, "")

		_, found, err := archive.FindBlockData(nodeDir)
		if err != nil {
			return fmt.Errorf("find block data in %s: %w", nodeDir, err)
		}

		if found {
			cfg.Log.Info("block volume already merged, skipping download",
				slog.String("kind", node.Kind),
				slog.String("name", node.Name))

			if stream != nil {
				stream.Done()
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

			if stream != nil {
				stream.Done()
			}

			return nil
		}

		return downloadVolumeBinding(ctx, cfg, node.Ref(), node.Namespace, dest, stream)
	}

	// Multi-volume layout: one DataExport per binding. Each binding shares the same
	// snapshot leaf ref (node.Ref()); the pvc name is used only for output file naming.
	for i := range refs {
		ref := &refs[i]
		pvc := ref.Target.Name
		dest := multiDest(nodeDir, pvc, cfg.Compression.Ext())
		stream := lookupStream(streams, node, pvc)

		_, statErr := os.Stat(dest.blockPath)
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("stat %s: %w", dest.blockPath, statErr)
		}

		if statErr == nil {
			cfg.Log.Info("block volume already merged, skipping",
				slog.String("pvc", pvc))

			if stream != nil {
				stream.Done()
			}

			continue
		}

		fsTarDone, err := fsTarComplete(dest.fsTarPath)
		if err != nil {
			return fmt.Errorf("check fs tar for pvc %s: %w", pvc, err)
		}

		if fsTarDone {
			cfg.Log.Info("fs tar already complete, skipping",
				slog.String("pvc", pvc))

			if stream != nil {
				stream.Done()
			}

			continue
		}

		if err := downloadVolumeBinding(ctx, cfg, node.Ref(), node.Namespace, dest, stream); err != nil {
			return fmt.Errorf("download volume for pvc %s: %w", pvc, err)
		}
	}

	return nil
}

// ensureNodeSubdirs creates manifests/ and, when the node has children, snapshots/
// inside nodeDir.
func ensureNodeSubdirs(nodeDir string, withSnapshots bool) error {
	if err := archive.EnsureDir(filepath.Join(nodeDir, archive.ManifestsDirName)); err != nil {
		return err
	}

	if !withSnapshots {
		return nil
	}

	return archive.EnsureDir(filepath.Join(nodeDir, archive.SnapshotsDirName))
}

// volumeDestPaths holds the resolved absolute paths for one volume's output
// within a node directory. The caller constructs it via flatDest or multiDest.
type volumeDestPaths struct {
	// chunkDir is the directory that receives block chunk files during download.
	chunkDir string
	// blockPath is the merged block output file (data.bin[.<ext>] or data/<pvc>.bin[.<ext>]).
	blockPath string
	// fsTarPath is the final assembled tar file (data.tar or data/<pvc>.tar).
	fsTarPath string
	// fsTarStagingDir is the temporary directory for raw per-file downloads
	// (data.tar.d/ or data/<pvc>.tar.d/).
	fsTarStagingDir string
}

// flatDest returns the single-volume flat destination paths for nodeDir.
// ext is codec.Ext() and determines the block file name suffix.
// Used for leaf volume nodes and snapshot nodes with exactly one OwnDataRef.
func flatDest(nodeDir, ext string) volumeDestPaths {
	return volumeDestPaths{
		chunkDir:        filepath.Join(nodeDir, archive.BlockChunksDirName),
		blockPath:       filepath.Join(nodeDir, archive.DataBlockName(ext)),
		fsTarPath:       filepath.Join(nodeDir, archive.FsTarName),
		fsTarStagingDir: filepath.Join(nodeDir, archive.FsTarStagingDirName),
	}
}

// multiDest returns the per-pvc multi-volume destination paths for nodeDir.
// ext is codec.Ext() and determines the block file name suffix.
// Used for snapshot nodes with more than one OwnDataRef.
func multiDest(nodeDir, pvc, ext string) volumeDestPaths {
	return volumeDestPaths{
		chunkDir:        filepath.Join(nodeDir, archive.BlockChunksDirNameFor(pvc)),
		blockPath:       filepath.Join(nodeDir, archive.MultiVolumeBlockName(pvc, ext)),
		fsTarPath:       filepath.Join(nodeDir, archive.MultiVolumeTarName(pvc)),
		fsTarStagingDir: filepath.Join(nodeDir, archive.MultiVolumeTarStagingDirName(pvc)),
	}
}

// downloadVolumeBinding opens a DataExport for the snapshot leaf identified by
// leafRef, downloads the volume data (block or filesystem) to dest, and releases
// the DataExport on completion or error.
//
// stream is the pre-created progress handle for this volume; it is marked Done on
// success or Fail on error when downloadVolumeBinding returns (via defer, once the
// DataExport has opened) and must not be nil-checked by the caller. Pass nil when
// progress tracking is disabled.
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
	stream progress.Stream,
) error {
	// Acquire one slot from the global stream semaphore before opening the
	// DataExport. This caps the number of concurrently active volume-stream
	// downloads across all nodes, independently of the node-level Workers errgroup
	// and the per-volume PerVolumeConcurrency errgroup. Cancelling ctx (e.g. on
	// sibling error or SIGINT) unblocks a waiting Acquire.
	if err := cfg.streamSem.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("acquire stream semaphore for %s/%s: %w", leafRef.Kind, leafRef.Name, err)
	}

	defer cfg.streamSem.Release(1)

	// cleanupCtx is deliberately not derived from ctx so that release still runs
	// when ctx is cancelled (e.g. by errgroup on sibling error or by SIGINT).
	// A bounded timeout prevents release from hanging forever.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cleanupCancel()

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
	defer func() {
		if cfg.KeepExports {
			cfg.Log.Info("leaving DataExport in cluster (--cleanup=false)",
				slog.String("leaf", leafRef.Name))

			return
		}

		deName := exporter.DataExportName(leafRef.Name)
		if relErr := exporter.ReleaseDataExport(cleanupCtx, cfg.KubeClient, namespace, deName); relErr != nil {
			cfg.Log.Warn("failed to release DataExport",
				slog.String("leaf", leafRef.Name),
				slog.String("error", relErr.Error()))
		}
	}()

	exp, err := cfg.OpenExport(ctx, namespace, leafRef, cfg.TTL)
	if err != nil {
		return fmt.Errorf("open DataExport for %s/%s: %w", leafRef.Kind, leafRef.Name, err)
	}

	// Mark the pre-created stream Done on success or Fail on error when we
	// return. downloadErr is a plain local (not a named return — nonamedreturns
	// is enforced repo-wide) set on every path below and read only by this
	// single deferred closure, so the terminal Stream call always matches the
	// function's real outcome instead of unconditionally reporting success.
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
		downloadErr = downloadBlock(ctx, cfg, dest, exp, stream)

	case "Filesystem":
		var (
			onProgress func(n int)
			setTotal   func(total int64)
		)

		if stream != nil {
			onProgress = stream.IncrBy
			setTotal = stream.SetTotal
		}

		downloadErr = downloadFS(ctx, cfg, dest.fsTarPath, dest.fsTarStagingDir, exp, setTotal, onProgress)

	default:
		downloadErr = fmt.Errorf("unsupported volume mode %q for leaf %s/%s", exp.VolumeMode(), leafRef.Kind, leafRef.Name)
	}

	return downloadErr
}

func downloadBlock(ctx context.Context, cfg Config, dest volumeDestPaths, exp *exporter.Export, stream progress.Stream) error {
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
		stream.SetTotal(totalSize)
		onProgress = stream.IncrBy
	}

	if err := volume.DownloadBlockChunks(ctx, cfg.Log, dest.chunkDir, blockURL, totalSize, cfg.ChunkSize, cfg.PerVolumeConcurrency, exp.Fetcher(), cfg.Compression, onProgress); err != nil {
		return fmt.Errorf("download block chunks: %w", err)
	}

	return volume.MergeBlockChunks(ctx, dest.chunkDir, dest.blockPath, totalSize, cfg.ChunkSize, cfg.Compression.Ext())
}

func downloadFS(ctx context.Context, cfg Config, tarPath, stagingDir string, exp *exporter.Export, setTotal func(total int64), onProgress func(n int)) error {
	filesURL, err := exporter.FilesURL(exp.BaseURL())
	if err != nil {
		return fmt.Errorf("build files URL: %w", err)
	}

	return volume.DownloadFilesystemVolume(ctx, cfg.Log, tarPath, stagingDir, filesURL, cfg.PerVolumeConcurrency, exp.Fetcher(), cfg.Compression, setTotal, onProgress)
}

// nodeStateName returns a human-readable label for a NodeState, used in log output
// so that the classification produced by the resume scan is visible to operators.
func nodeStateName(s archive.NodeState) string {
	switch s {
	case archive.NodeStatePending:
		return "pending"
	case archive.NodeStateBlockPartial:
		return "block_partial"
	case archive.NodeStateFSPartial:
		return "fs_partial"
	case archive.NodeStateManifestsOnly:
		return "manifests_only"
	case archive.NodeStateDone:
		return "done"
	default:
		return "unknown"
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
