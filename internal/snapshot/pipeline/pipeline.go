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

// Package pipeline orchestrates the snapshot manifest download workflow.
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// Options holds user-specified parameters for the download pipeline.
type Options struct {
	Namespace    string
	SnapshotName string
	OutputDir    string

	// NodeFilter is the node ID for subtree selection (--node flag).
	// Empty means the full snapshot is downloaded.
	NodeFilter string

	// ObjectFilter is the --object flag value for single-object filtering.
	// Format: <apiVersion>/<Kind>/<name> (e.g. "apps/v1/Deployment/my-deploy")
	ObjectFilter string
}

// BuildTreeFunc and FetchManifestsFunc are overridable seams for the tree build and manifest fetch steps.
var (
	BuildTreeFunc      = source.BuildTree
	FetchManifestsFunc = source.FetchManifests
)

// Run executes the full manifest-download pipeline.
func Run(ctx context.Context, sClient *safeClient.SafeClient, rtClient ctrlrtclient.Client, opts Options, log *slog.Logger) error {
	log.Info("checking Snapshot readiness", "namespace", opts.Namespace, "snapshot", opts.SnapshotName)

	root, selected, nodes, err := prepareTree(ctx, rtClient, opts, log)
	if err != nil {
		return err
	}

	mode := selectionModeFor(opts)
	meta := buildArchiveMeta(sClient, root, selected, nodes, opts, mode)

	log.Info("creating archive directory", "path", opts.OutputDir)

	w, err := archive.NewDirWriter(opts.OutputDir, meta)
	if err != nil {
		return fmt.Errorf("initialise archive at %s: %w", opts.OutputDir, err)
	}

	if err := writeNodeRecords(w, nodes); err != nil {
		return err
	}

	objFilter, err := source.BuildObjectFilter(opts.ObjectFilter)
	if err != nil {
		return err
	}

	dl := newDownloader(sClient, w, objFilter, log)

	for _, n := range nodes {
		if err := dl.processNode(ctx, n); err != nil {
			return err
		}
	}

	return finalize(w, mode, opts, log)
}

// downloader bundles the dependencies for per-node manifest fetching.
type downloader struct {
	sClient *safeClient.SafeClient
	writer  *archive.DirWriter
	filter  source.ObjectFilter
	log     *slog.Logger
}

func newDownloader(sc *safeClient.SafeClient, w *archive.DirWriter, f source.ObjectFilter, log *slog.Logger) *downloader {
	return &downloader{sClient: sc, writer: w, filter: f, log: log}
}

func (d *downloader) processNode(ctx context.Context, n *source.Node) error {
	d.log.Debug("fetching manifests", "node", n.ID)

	rawObjects, err := FetchManifestsFunc(ctx, d.sClient, n)
	if err != nil {
		return fmt.Errorf("fetch manifests for %s: %w", n.ID, err)
	}

	filtered := rawObjects
	if d.filter != nil {
		filtered = filtered[:0]

		for _, raw := range rawObjects {
			keep, err := d.filter(raw)
			if err != nil {
				return fmt.Errorf("apply object filter: %w", err)
			}

			if keep {
				filtered = append(filtered, raw)
			}
		}
	}

	for _, raw := range filtered {
		objRec, err := d.writer.AddObject(n.ID, raw)
		if err != nil {
			return fmt.Errorf("add object from %s: %w", n.ID, err)
		}

		if err := d.writer.AppendObject(objRec); err != nil {
			return err
		}
	}

	d.log.Debug("node done", "node", n.ID, "objects", len(filtered))

	return nil
}

// prepareTree reads the snapshot readiness, builds and selects the node tree.
func prepareTree(ctx context.Context, rtClient ctrlrtclient.Client, opts Options, log *slog.Logger) (*source.Node, *source.Node, []*source.Node, error) {
	root, err := BuildTreeFunc(ctx, rtClient, opts.Namespace, opts.SnapshotName)
	if err != nil {
		return nil, nil, nil, err
	}

	selected, err := source.SelectSubtree(root, source.TreeOptions{NodeFilter: opts.NodeFilter})
	if err != nil {
		return nil, nil, nil, err
	}

	nodes := source.FlatNodes(selected)

	log.Debug("snapshot tree ready", "nodes", len(nodes))

	return root, selected, nodes, nil
}

// selectionModeFor maps the CLI filter flags to an archive SelectionMode.
func selectionModeFor(opts Options) archive.SelectionMode {
	if opts.ObjectFilter != "" {
		return archive.SelectionObject
	}

	if opts.NodeFilter != "" {
		return archive.SelectionSubtree
	}

	return archive.SelectionFull
}

// writeNodeRecords appends a NodeRecord for every node in the list.
func writeNodeRecords(w *archive.DirWriter, nodes []*source.Node) error {
	for _, n := range nodes {
		if err := w.AppendNode(source.ToNodeRecord(n)); err != nil {
			return err
		}
	}

	return nil
}

// finalize builds the index, calls Finalize, and logs the result.
func finalize(w *archive.DirWriter, mode archive.SelectionMode, opts Options, log *slog.Logger) error {
	idx := buildIndex(mode, opts.NodeFilter != "")

	summary, err := w.Finalize(idx)
	if err != nil {
		return fmt.Errorf("finalise archive: %w", err)
	}

	log.Info("archive complete", "path", opts.OutputDir, "nodes", summary.Nodes, "objects", summary.Objects)

	return nil
}

// buildArchiveMeta constructs the Meta written to archive.json.
func buildArchiveMeta(
	sClient *safeClient.SafeClient,
	root, selected *source.Node,
	nodes []*source.Node,
	opts Options,
	mode archive.SelectionMode,
) archive.Meta {
	archiveID := fmt.Sprintf("a-%s", time.Now().UTC().Format("20060102-150405"))

	rootNodeID := root.ID
	if selected != nil {
		rootNodeID = selected.ID
	}

	serverHost := ""
	if sClient != nil {
		serverHost = sClient.ServerHost()
	}

	selectedNodeIDs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		selectedNodeIDs = append(selectedNodeIDs, n.ID)
	}

	return archive.Meta{
		Magic:         archive.Magic,
		SchemaVersion: archive.SchemaVersion,
		ArchiveID:     archiveID,
		CreatedAt:     time.Now().UTC(),
		CreatedBy: archive.Creator{
			Tool:    "d8",
			Version: "dev",
		},
		Source: archive.Source{
			Cluster: archive.Cluster{
				Server: serverHost,
			},
			Namespace: opts.Namespace,
			RootSnapshot: archive.SnapshotRef{
				APIVersion: root.APIVersion,
				Kind:       root.Kind,
				Resource:   root.Resource,
				Name:       root.Name,
			},
			RootSnapshotContent: archive.SnapshotContentRef{
				APIVersion: root.APIVersion,
				Kind:       "SnapshotContent",
				Name:       root.BoundSnapshotContentName,
			},
		},
		Selection: archive.Selection{
			Mode:            mode,
			RootNodeID:      rootNodeID,
			SelectedNodeIDs: selectedNodeIDs,
		},
	}
}

// buildIndex constructs the Index written to index.json.
// Summary counts are filled by DirWriter.Finalize.
func buildIndex(mode archive.SelectionMode, isPartial bool) archive.Index {
	return archive.Index{
		SchemaVersion: archive.SchemaVersion,
		Capabilities: archive.IndexCapabilities{
			Manifests:            true,
			Volumes:              false,
			RestoreFromArchive:   true,
			UploadableAsSnapshot: false,
			PartialSelection:     isPartial || mode != archive.SelectionFull,
			Resumable:            true,
		},
		ManifestModel: archive.IndexManifestModel{
			Format:      "json",
			Compression: "gzip-per-object",
			SourceKind:  "aggregated-subtree",
		},
		Catalogs: archive.IndexCatalogs{
			Nodes:   "indexes/nodes.jsonl",
			Objects: "indexes/objects.jsonl",
		},
		Paths: archive.IndexPaths{
			ManifestsRoot: "manifests/objects",
			DataRoot:      "data",
		},
	}
}
