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
	"fmt"
	"log/slog"
	"path/filepath"

	"golang.org/x/sync/errgroup"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// nodeTask is a planned work item for one snapshot node.
type nodeTask struct {
	node          *source.Node
	nodeDir       string // final target directory (may differ from primary on collision)
	state         archive.NodeState
	presentChunks []int // non-nil only for NodeStateBlockPartial
}

// Run builds the snapshot tree, scans the output directory for resume state, and
// downloads all missing node data with bounded concurrency.
// The first node error cancels all in-flight work.
func Run(ctx context.Context, cfg Config) error {
	cfg = applyDefaults(cfg)

	if cfg.OpenExport == nil {
		return fmt.Errorf("pipeline: OpenExport must be set (supply SafeClient or set OpenExport directly)")
	}

	enc, err := compress.NewEncoder(cfg.ZstdLevel)
	if err != nil {
		return fmt.Errorf("create zstd encoder: %w", err)
	}

	root, err := source.BuildTree(ctx, cfg.KubeClient, cfg.Namespace, cfg.RootSnapshot)
	if err != nil {
		return fmt.Errorf("build snapshot tree: %w", err)
	}

	tasks, err := collectNodeTasks(root, cfg.OutputDir)
	if err != nil {
		return fmt.Errorf("scan output directory: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, cfg.Workers)

	for _, t := range tasks {
		task := t

		g.Go(func() error {
			select {
			case sem <- struct{}{}:
			case <-gctx.Done():
				return gctx.Err()
			}

			defer func() { <-sem }()

			return processNode(gctx, cfg, enc, task)
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
		node:          node,
		nodeDir:       plan.TargetDir,
		state:         plan.State,
		presentChunks: plan.PresentChunkIndices,
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

// processNode executes all download and finalization steps for one node task.
// It is called concurrently by the worker pool.
func processNode(ctx context.Context, cfg Config, enc *compress.Encoder, task nodeTask) error {
	if task.state == archive.NodeStateDone {
		cfg.Log.Info("node already complete, skipping",
			slog.String("kind", task.node.Kind),
			slog.String("name", task.node.Name))

		return nil
	}

	if err := ensureNodeSubdirs(task.nodeDir, len(task.node.Children) > 0); err != nil {
		return fmt.Errorf("ensure subdirs for %s/%s: %w", task.node.Kind, task.node.Name, err)
	}

	if err := volume.WriteNodeManifests(ctx, cfg.ManifestSource, task.nodeDir, task.node); err != nil {
		return fmt.Errorf("write manifests for %s/%s: %w", task.node.Kind, task.node.Name, err)
	}

	if len(task.node.DataRefs) > 0 {
		if err := downloadVolume(ctx, cfg, enc, task.node, task.nodeDir, task.node.DataRefs[0]); err != nil {
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

// downloadVolume creates the shadow VS/VSC pair for the artifact, opens a
// DataExport, downloads the volume data (block or filesystem), and cleans up
// the shadow pair and export on completion or error.
func downloadVolume(
	ctx context.Context,
	cfg Config,
	enc *compress.Encoder,
	node *source.Node,
	nodeDir string,
	binding snapshotapi.SnapshotDataBinding,
) error {
	if binding.Artifact.Kind != exporter.ArtifactKindVolumeSnapshotContent {
		return fmt.Errorf("unsupported artifact kind %q for %s/%s (want VolumeSnapshotContent)",
			binding.Artifact.Kind, node.Kind, node.Name)
	}

	artifactName := binding.Artifact.Name

	shadowVS, err := exporter.EnsureShadowPair(ctx, cfg.KubeClient, node.Namespace, artifactName)
	if err != nil {
		return fmt.Errorf("ensure shadow pair for artifact %s: %w", artifactName, err)
	}

	defer func() {
		if cleanErr := exporter.CleanupShadowPair(ctx, cfg.KubeClient, node.Namespace, artifactName); cleanErr != nil {
			cfg.Log.Warn("failed to cleanup shadow pair",
				slog.String("artifact", artifactName),
				slog.String("error", cleanErr.Error()))
		}
	}()

	exp, err := cfg.OpenExport(ctx, node.Namespace, shadowVS.Name, cfg.TTL)
	if err != nil {
		return fmt.Errorf("open DataExport for shadow VS %s: %w", shadowVS.Name, err)
	}

	defer func() {
		if relErr := exp.Release(ctx, cfg.KubeClient); relErr != nil {
			cfg.Log.Warn("failed to release DataExport",
				slog.String("shadow_vs", shadowVS.Name),
				slog.String("error", relErr.Error()))
		}
	}()

	cfg.Log.Info("downloading volume",
		slog.String("node", node.Kind+"/"+node.Name),
		slog.String("volume_mode", exp.VolumeMode()))

	switch exp.VolumeMode() {
	case "Block":
		return downloadBlock(ctx, cfg, enc, nodeDir, exp)
	case "Filesystem":
		return downloadFS(ctx, cfg, enc, nodeDir, exp)
	default:
		return fmt.Errorf("unsupported volume mode %q for %s/%s", exp.VolumeMode(), node.Kind, node.Name)
	}
}

func downloadBlock(ctx context.Context, cfg Config, enc *compress.Encoder, nodeDir string, exp *exporter.Export) error {
	blockURL, err := exporter.BlockURL(exp.BaseURL())
	if err != nil {
		return fmt.Errorf("build block URL: %w", err)
	}

	totalSize, err := exp.Fetcher().HeadVolume(ctx, blockURL)
	if err != nil {
		return fmt.Errorf("HEAD block volume: %w", err)
	}

	if err := volume.DownloadBlockChunks(ctx, cfg.Log, nodeDir, blockURL, totalSize, cfg.ChunkSize, cfg.PerVolumeConcurrency, exp.Fetcher(), enc); err != nil {
		return fmt.Errorf("download block chunks: %w", err)
	}

	return volume.MergeBlockChunks(nodeDir, totalSize, cfg.ChunkSize)
}

func downloadFS(ctx context.Context, cfg Config, enc *compress.Encoder, nodeDir string, exp *exporter.Export) error {
	filesURL, err := exporter.FilesURL(exp.BaseURL())
	if err != nil {
		return fmt.Errorf("build files URL: %w", err)
	}

	return volume.DownloadFilesystemVolume(ctx, cfg.Log, nodeDir, filesURL, cfg.PerVolumeConcurrency, exp.Fetcher(), enc)
}

// nodeIdentity converts a source.Node into an archive.NodeIdentity for resume scanning.
func nodeIdentity(node *source.Node) archive.NodeIdentity {
	return archive.NodeIdentity{
		APIVersion: node.APIVersion,
		Kind:       node.Kind,
		Name:       node.Name,
		Namespace:  node.Namespace,
		SourceRef:  node.SourceRef,
	}
}
