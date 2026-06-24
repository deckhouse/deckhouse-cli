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

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.Workers)

	for _, t := range tasks {
		task := t

		g.Go(func() error {
			return processNode(gctx, cfg, task)
		})
	}

	return g.Wait()
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
func processNode(ctx context.Context, cfg Config, task nodeTask) error {
	if task.state == archive.NodeStateDone {
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
		return processVolumeNode(ctx, cfg, task)
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
		if err := downloadOwnDataRefs(ctx, cfg, task.node, task.nodeDir); err != nil {
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
func processVolumeNode(ctx context.Context, cfg Config, task nodeTask) error {
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

	switch {
	case blockAlreadyMerged:
		cfg.Log.Info("block volume already merged, skipping download",
			slog.String("kind", task.node.Kind),
			slog.String("name", task.node.Name))
	case fsTarDone:
		cfg.Log.Info("fs tar already complete, skipping download",
			slog.String("kind", task.node.Kind),
			slog.String("name", task.node.Name))
	default:
		if err := downloadVolumeBinding(ctx, cfg, task.node.Ref(), task.node.Namespace, dest); err != nil {
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
) error {
	refs := node.OwnDataRefs

	if len(refs) == 1 {
		// Flat single-volume layout: reuse the same paths as leaf volume nodes.
		dest := flatDest(nodeDir, cfg.Compression.Ext())

		_, found, err := archive.FindBlockData(nodeDir)
		if err != nil {
			return fmt.Errorf("find block data in %s: %w", nodeDir, err)
		}

		if found {
			cfg.Log.Info("block volume already merged, skipping download",
				slog.String("kind", node.Kind),
				slog.String("name", node.Name))

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

			return nil
		}

		return downloadVolumeBinding(ctx, cfg, node.Ref(), node.Namespace, dest)
	}

	// Multi-volume layout: one DataExport per binding. Each binding shares the same
	// snapshot leaf ref (node.Ref()); the pvc name is used only for output file naming.
	for i := range refs {
		ref := &refs[i]
		pvc := ref.Target.Name
		dest := multiDest(nodeDir, pvc, cfg.Compression.Ext())

		_, statErr := os.Stat(dest.blockPath)
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("stat %s: %w", dest.blockPath, statErr)
		}

		if statErr == nil {
			cfg.Log.Info("block volume already merged, skipping",
				slog.String("pvc", pvc))

			continue
		}

		fsTarDone, err := fsTarComplete(dest.fsTarPath)
		if err != nil {
			return fmt.Errorf("check fs tar for pvc %s: %w", pvc, err)
		}

		if fsTarDone {
			cfg.Log.Info("fs tar already complete, skipping",
				slog.String("pvc", pvc))

			continue
		}

		if err := downloadVolumeBinding(ctx, cfg, node.Ref(), node.Namespace, dest); err != nil {
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
) error {
	exp, err := cfg.OpenExport(ctx, namespace, leafRef, cfg.TTL)
	if err != nil {
		return fmt.Errorf("open DataExport for %s/%s: %w", leafRef.Kind, leafRef.Name, err)
	}

	// cleanupCtx is deliberately not derived from ctx so that release still runs
	// when ctx is cancelled (e.g. by errgroup on sibling error or by SIGINT).
	// A bounded timeout prevents release from hanging forever.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cleanupCancel()

	defer func() {
		if relErr := exp.Release(cleanupCtx, cfg.KubeClient); relErr != nil {
			cfg.Log.Warn("failed to release DataExport",
				slog.String("leaf", leafRef.Name),
				slog.String("error", relErr.Error()))
		}
	}()

	cfg.Log.Info("downloading volume",
		slog.String("leaf", leafRef.Name),
		slog.String("volume_mode", exp.VolumeMode()))

	switch exp.VolumeMode() {
	case "Block":
		return downloadBlock(ctx, cfg, dest, exp)
	case "Filesystem":
		return downloadFS(ctx, cfg, dest.fsTarPath, dest.fsTarStagingDir, exp)
	default:
		return fmt.Errorf("unsupported volume mode %q for leaf %s/%s", exp.VolumeMode(), leafRef.Kind, leafRef.Name)
	}
}

func downloadBlock(ctx context.Context, cfg Config, dest volumeDestPaths, exp *exporter.Export) error {
	blockURL, err := exporter.BlockURL(exp.BaseURL())
	if err != nil {
		return fmt.Errorf("build block URL: %w", err)
	}

	totalSize, err := exp.Fetcher().HeadVolume(ctx, blockURL)
	if err != nil {
		return fmt.Errorf("HEAD block volume: %w", err)
	}

	if err := volume.DownloadBlockChunks(ctx, cfg.Log, dest.chunkDir, blockURL, totalSize, cfg.ChunkSize, cfg.PerVolumeConcurrency, exp.Fetcher(), cfg.Compression); err != nil {
		return fmt.Errorf("download block chunks: %w", err)
	}

	return volume.MergeBlockChunks(dest.chunkDir, dest.blockPath, totalSize, cfg.ChunkSize, cfg.Compression.Ext())
}

func downloadFS(ctx context.Context, cfg Config, tarPath, stagingDir string, exp *exporter.Export) error {
	filesURL, err := exporter.FilesURL(exp.BaseURL())
	if err != nil {
		return fmt.Errorf("build files URL: %w", err)
	}

	return volume.DownloadFilesystemVolume(ctx, cfg.Log, tarPath, stagingDir, filesURL, cfg.PerVolumeConcurrency, exp.Fetcher(), cfg.Compression)
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
