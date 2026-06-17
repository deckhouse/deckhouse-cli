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
	corev1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	sigsyaml "sigs.k8s.io/yaml"

	snapshotapi "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
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
		blockPath := filepath.Join(task.nodeDir, archive.DataBlockName)

		_, statErr := os.Stat(blockPath)
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("stat %s: %w", blockPath, statErr)
		}

		blockAlreadyMerged := statErr == nil
		if blockAlreadyMerged {
			cfg.Log.Info("block volume already merged, skipping download",
				slog.String("kind", task.node.Kind),
				slog.String("name", task.node.Name))
		}

		if !blockAlreadyMerged {
			if err := downloadVolume(ctx, cfg, enc, task.node, task.nodeDir, task.node.DataRefs[0]); err != nil {
				return fmt.Errorf("download volume for %s/%s: %w", task.node.Kind, task.node.Name, err)
			}
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

	meta, err := resolveShadowMeta(ctx, cfg, nodeDir, binding.Target)
	if err != nil {
		return fmt.Errorf("resolve shadow metadata for artifact %s: %w", artifactName, err)
	}

	shadowVS, err := exporter.EnsureShadowPair(ctx, cfg.KubeClient, node.Namespace, artifactName, meta)
	if err != nil {
		return fmt.Errorf("ensure shadow pair for artifact %s: %w", artifactName, err)
	}

	// cleanupCtx is deliberately not derived from ctx so that cleanup still runs
	// when ctx is cancelled (e.g. by errgroup on sibling error or by SIGINT).
	// A bounded timeout prevents cleanup from hanging forever.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cleanupCancel()

	defer func() {
		if cleanErr := exporter.CleanupShadowPair(cleanupCtx, cfg.KubeClient, node.Namespace, artifactName); cleanErr != nil {
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
		if relErr := exp.Release(cleanupCtx, cfg.KubeClient); relErr != nil {
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

// resolveShadowMeta resolves the storageClass and volumeMode of the source PVC
// (target) that was snapshotted. It tries a live API lookup first; if the PVC
// is gone it falls back to the captured manifest written under
// <nodeDir>/manifests/. Returns an error if both attempts fail or if the PVC
// does not carry both fields.
func resolveShadowMeta(
	ctx context.Context,
	cfg Config,
	nodeDir string,
	target snapshotapi.SnapshotSubjectRef,
) (exporter.ShadowMeta, error) {
	pvc := &corev1.PersistentVolumeClaim{}

	err := cfg.KubeClient.Get(ctx, types.NamespacedName{Namespace: target.Namespace, Name: target.Name}, pvc)
	if err != nil && !kubeerrors.IsNotFound(err) {
		return exporter.ShadowMeta{}, fmt.Errorf("get source PVC %s/%s: %w", target.Namespace, target.Name, err)
	}

	if err == nil {
		return shadowMetaFromPVC(pvc)
	}

	return shadowMetaFromManifest(nodeDir, target.Name)
}

// shadowMetaFromPVC extracts storageClass and volumeMode from a live PVC.
func shadowMetaFromPVC(pvc *corev1.PersistentVolumeClaim) (exporter.ShadowMeta, error) {
	var meta exporter.ShadowMeta

	if pvc.Spec.StorageClassName != nil && *pvc.Spec.StorageClassName != "" {
		meta.StorageClass = *pvc.Spec.StorageClassName
	}

	if pvc.Spec.VolumeMode != nil {
		meta.VolumeMode = string(*pvc.Spec.VolumeMode)
	}

	if meta.StorageClass == "" || meta.VolumeMode == "" {
		return exporter.ShadowMeta{}, fmt.Errorf(
			"source PVC %s/%s is missing required fields: storageClassName=%q volumeMode=%q",
			pvc.Namespace, pvc.Name, meta.StorageClass, meta.VolumeMode)
	}

	return meta, nil
}

// shadowMetaFromManifest reads the captured PVC manifest from
// <nodeDir>/manifests/persistentvolumeclaim_<pvcName>.yaml and extracts
// storageClass and volumeMode.
func shadowMetaFromManifest(nodeDir, pvcName string) (exporter.ShadowMeta, error) {
	manifestPath := filepath.Join(nodeDir, archive.ManifestsDirName,
		archive.ManifestFileName("PersistentVolumeClaim", pvcName, ""))

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return exporter.ShadowMeta{}, fmt.Errorf(
			"source PVC %q not found live and manifest not readable (%s): %w",
			pvcName, manifestPath, err)
	}

	var obj map[string]interface{}

	if err := sigsyaml.Unmarshal(data, &obj); err != nil {
		return exporter.ShadowMeta{}, fmt.Errorf("parse manifest %s: %w", manifestPath, err)
	}

	spec, _ := obj["spec"].(map[string]interface{})

	storageClass, _ := spec["storageClassName"].(string)
	volumeMode, _ := spec["volumeMode"].(string)

	if storageClass == "" || volumeMode == "" {
		return exporter.ShadowMeta{}, fmt.Errorf(
			"manifest %s is missing required fields: storageClassName=%q volumeMode=%q",
			manifestPath, storageClass, volumeMode)
	}

	return exporter.ShadowMeta{StorageClass: storageClass, VolumeMode: volumeMode}, nil
}
