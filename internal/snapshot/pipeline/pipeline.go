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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

	// Fresh forces a clean overwrite of an existing archive without prompting.
	Fresh bool

	// Retries is the number of download attempts per node (0 → default 3).
	Retries int

	// RetryDelay is the base delay between retries (0 → default 2s).
	// The actual delay doubles on each attempt (exponential back-off).
	RetryDelay time.Duration

	// OverwritePromptFn is called when an existing archive with a different
	// identity is found and Fresh is false. It returns true to proceed with
	// overwrite. When nil and Fresh is false, Run returns an error.
	OverwritePromptFn func(dir string) bool

	// IncludeManifests controls whether manifests are downloaded (default true).
	IncludeManifests bool

	// IncludeVolumes controls whether volume data is downloaded (default true).
	IncludeVolumes bool

	// DataExportTTL is the TTL for auto-created DataExport objects used during
	// volume download. Defaults to defaultDataExportTTL when empty.
	DataExportTTL string
}

const (
	defaultRetries       = 3
	defaultRetryDelay    = 2 * time.Second
	defaultDataExportTTL = "30m"
)

// BuildTreeFunc, FetchManifestsFunc, and DownloadNodeVolumesFunc are overridable seams for tests.
var (
	BuildTreeFunc           = source.BuildTree
	FetchManifestsFunc      = source.FetchManifests
	DownloadNodeVolumesFunc = downloadNodeVolumes
)

// failedNode records a download failure for one tree node.
type failedNode struct {
	ID  string
	Err error
}

// Run executes the full manifest-download pipeline with resume and retry support.
//
// Decision flow:
//   - absent/empty output dir                      → fresh download
//   - valid archive, same identity, COMPLETE+up-to-date → noop
//   - valid archive, same identity, incomplete/stale   → resume
//   - valid archive, different identity OR non-archive non-empty → overwrite (prompt or --fresh)
func Run(ctx context.Context, sClient *safeClient.SafeClient, rtClient ctrlrtclient.Client, opts Options, log *slog.Logger) error {
	retries := opts.Retries
	if retries <= 0 {
		retries = defaultRetries
	}

	retryDelay := opts.RetryDelay
	if retryDelay <= 0 {
		retryDelay = defaultRetryDelay
	}

	log.Info("checking Snapshot readiness", "namespace", opts.Namespace, "snapshot", opts.SnapshotName)

	root, selected, nodes, err := prepareTree(ctx, rtClient, opts, log)
	if err != nil {
		return err
	}

	mode := selectionModeFor(opts)
	liveMeta := buildArchiveMeta(sClient, root, selected, nodes, opts, mode)
	liveID := archive.IdentityOf(liveMeta)

	needFresh, err := resolveOutputDir(opts.OutputDir, liveID, opts, log)
	if err != nil {
		return err
	}

	var (
		w                *archive.DirWriter
		existingProgress map[string]archive.ProgressRecord
		existingVolProg  map[string]archive.VolumeProgressRecord
	)

	if needFresh {
		log.Info("creating archive directory", "path", opts.OutputDir)

		w, err = archive.NewDirWriter(opts.OutputDir, liveMeta)
		if err != nil {
			return fmt.Errorf("initialise archive at %s: %w", opts.OutputDir, err)
		}

		existingProgress = make(map[string]archive.ProgressRecord)
		existingVolProg = make(map[string]archive.VolumeProgressRecord)
	} else {
		w, existingProgress, existingVolProg, err = archive.OpenForResume(opts.OutputDir)
		if err != nil {
			return fmt.Errorf("open archive for resume at %s: %w", opts.OutputDir, err)
		}

		if isNoop(nodes, existingProgress, existingVolProg, opts.OutputDir) {
			w.Close()
			log.Info("archive already up to date", "path", opts.OutputDir)

			return nil
		}

		log.Info("resuming existing archive", "path", opts.OutputDir)
	}

	// Apply defaults for include flags: if both are false (zero values), enable both.
	if !opts.IncludeManifests && !opts.IncludeVolumes {
		opts.IncludeManifests = true
		opts.IncludeVolumes = true
	}

	if opts.DataExportTTL == "" {
		opts.DataExportTTL = defaultDataExportTTL
	}

	objFilter, err := source.BuildObjectFilter(opts.ObjectFilter)
	if err != nil {
		return err
	}

	dl := &downloader{
		sClient:             sClient,
		rtClient:            rtClient,
		writer:              w,
		filter:              objFilter,
		retries:             retries,
		retryDelay:          retryDelay,
		existingProgress:    existingProgress,
		existingVolProgress: existingVolProg,
		outputDir:           opts.OutputDir,
		opts:                opts,
		log:                 log,
	}

	var failed []failedNode

	for _, n := range nodes {
		if err := dl.processNode(ctx, n); err != nil {
			failed = append(failed, failedNode{ID: n.ID, Err: err})
			log.Warn("node failed, continuing best-effort", "node", n.ID, "err", err)
		}
	}

	liveNodeRecs := source.ToNodeRecords(nodes)
	complete := len(failed) == 0
	idx := buildIndex(mode, opts.NodeFilter != "", opts.IncludeVolumes)

	summary, err := w.Finalize(idx, liveNodeRecs, complete)
	if err != nil {
		return fmt.Errorf("finalise archive: %w", err)
	}

	if complete {
		log.Info("archive complete", "path", opts.OutputDir, "nodes", summary.Nodes, "objects", summary.Objects, "volumes", summary.Volumes)

		return nil
	}

	return buildFailureSummary(failed, opts)
}

// downloader bundles the dependencies for per-node manifest and volume fetching.
type downloader struct {
	sClient             *safeClient.SafeClient
	rtClient            ctrlrtclient.Client
	writer              *archive.DirWriter
	filter              source.ObjectFilter
	retries             int
	retryDelay          time.Duration
	existingProgress    map[string]archive.ProgressRecord
	existingVolProgress map[string]archive.VolumeProgressRecord
	outputDir           string
	opts                Options
	log                 *slog.Logger
}

// processNode downloads manifests and/or volumes for n, skipping it if already
// satisfied by the existing progress. Retries transient errors up to d.retries times.
func (d *downloader) processNode(ctx context.Context, n *source.Node) error {
	if isNodeSatisfied(n, d.existingProgress, d.existingVolProgress, d.outputDir) {
		d.log.Debug("node already satisfied, skipping", "node", n.ID)

		return nil
	}

	var lastErr error

	for attempt := range d.retries {
		if attempt > 0 {
			delay := d.retryDelay * time.Duration(1<<uint(attempt-1))
			d.log.Debug("retrying node", "node", n.ID, "attempt", attempt+1, "delay", delay)
			time.Sleep(delay)
		}

		lastErr = d.fetchAndStoreNode(ctx, n)
		if lastErr == nil {
			return nil
		}

		if errors.Is(lastErr, context.Canceled) || errors.Is(lastErr, context.DeadlineExceeded) {
			return lastErr
		}

		d.log.Warn("fetch attempt failed", "node", n.ID, "attempt", attempt+1, "err", lastErr)
	}

	return fmt.Errorf("node %s failed after %d attempt(s): %w", n.ID, d.retries, lastErr)
}

// fetchAndStoreNode performs a single fetch-and-store cycle for one node
// (manifests and/or volumes, depending on Options).
func (d *downloader) fetchAndStoreNode(ctx context.Context, n *source.Node) error {
	if d.opts.IncludeManifests {
		if err := d.fetchAndStoreManifests(ctx, n); err != nil {
			return err
		}
	}

	if d.opts.IncludeVolumes && n.HasData {
		if err := DownloadNodeVolumesFunc(ctx, d.rtClient, d.sClient, d.writer, n, d.existingVolProgress, d.opts, d.log); err != nil {
			return fmt.Errorf("download volumes for %s: %w", n.ID, err)
		}
	}

	return nil
}

// fetchAndStoreManifests downloads and persists manifests for one node.
func (d *downloader) fetchAndStoreManifests(ctx context.Context, n *source.Node) error {
	// Skip if manifests already complete for this node.
	if rec, ok := d.existingProgress[n.ID]; ok && rec.ContentRef == n.BoundSnapshotContentName {
		allBlobs := true

		for _, obj := range rec.Objects {
			if _, err := os.Stat(filepath.Join(d.outputDir, obj.Blob)); err != nil {
				allBlobs = false
				break
			}
		}

		if allBlobs {
			d.log.Debug("manifests already complete, skipping", "node", n.ID)
			return nil
		}
	}

	d.log.Debug("fetching manifests", "node", n.ID)

	rawObjects, err := FetchManifestsFunc(ctx, d.sClient, n)
	if err != nil {
		return fmt.Errorf("fetch manifests for %s: %w", n.ID, err)
	}

	filtered := rawObjects

	if d.filter != nil {
		filtered = filtered[:0]

		for _, raw := range rawObjects {
			keep, filterErr := d.filter(raw)
			if filterErr != nil {
				return fmt.Errorf("apply object filter: %w", filterErr)
			}

			if keep {
				filtered = append(filtered, raw)
			}
		}
	}

	objRecs := make([]archive.ObjectRecord, 0, len(filtered))

	for _, raw := range filtered {
		objRec, addErr := d.writer.AddObject(n.ID, raw)
		if addErr != nil {
			return fmt.Errorf("add object from %s: %w", n.ID, addErr)
		}

		objRecs = append(objRecs, objRec)
	}

	prec := archive.ProgressRecord{
		NodeID:     n.ID,
		ContentRef: n.BoundSnapshotContentName,
		Objects:    objRecs,
	}

	if err := d.writer.AppendProgress(prec); err != nil {
		return err
	}

	d.existingProgress[n.ID] = prec

	d.log.Debug("node manifests done", "node", n.ID, "objects", len(filtered))

	return nil
}

// resolveOutputDir inspects the output directory and decides whether to start
// fresh. It handles wiping when overwrite is confirmed via Fresh or the prompt.
// Returns needFresh=true for a clean write, false to resume.
func resolveOutputDir(dir string, liveID archive.ArchiveIdentity, opts Options, log *slog.Logger) (bool, error) {
	info, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
				return false, fmt.Errorf("create output dir %s: %w", dir, mkErr)
			}

			return true, nil
		}

		return false, fmt.Errorf("stat output dir %s: %w", dir, err)
	}

	if !info.IsDir() {
		return false, fmt.Errorf("output path %s exists and is not a directory", dir)
	}

	// Check if dir is effectively empty (only possibly COMPLETE or similar).
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, fmt.Errorf("read output dir %s: %w", dir, err)
	}

	if len(entries) == 0 {
		return true, nil
	}

	// Try to read as a d8 snapshot archive.
	r, openErr := archive.OpenDir(dir)
	if openErr != nil {
		// Not a d8 archive or damaged; fall through to overwrite handling.
		return false, handleOverwrite(dir, "not a snapshot archive (or archive.json is missing/invalid)", opts, log)
	}

	existingMeta, metaErr := r.Meta()
	if metaErr != nil || existingMeta.Magic != archive.Magic {
		return false, handleOverwrite(dir, "archive.json has unexpected content", opts, log)
	}

	existingID := archive.IdentityOf(existingMeta)
	if existingID.Equal(liveID) {
		// Same target: resume (caller will check noop after opening for resume).
		return false, nil
	}

	// Different identity: overwrite.
	reason := fmt.Sprintf("existing archive targets %s/%s (%s), requested %s/%s (%s)",
		existingMeta.Source.Namespace, existingMeta.Source.RootSnapshot.Name, existingMeta.Selection.Mode,
		liveID.Namespace, liveID.Snapshot, liveID.Mode)

	if err := handleOverwrite(dir, reason, opts, log); err != nil {
		return false, err
	}

	return true, nil
}

// handleOverwrite either wipes the directory (Fresh=true or prompt returned true)
// or returns an error instructing the user how to proceed.
func handleOverwrite(dir, reason string, opts Options, log *slog.Logger) error {
	if opts.Fresh {
		log.Info("overwriting existing directory (--fresh)", "path", dir, "reason", reason)

		return archive.WipeDir(dir)
	}

	if opts.OverwritePromptFn != nil && opts.OverwritePromptFn(dir) {
		log.Info("overwriting existing directory (confirmed)", "path", dir)

		return archive.WipeDir(dir)
	}

	if opts.OverwritePromptFn == nil {
		return fmt.Errorf("directory %q contains different content (%s); use --fresh to overwrite or choose a different -o", dir, reason)
	}

	return fmt.Errorf("overwrite of %q declined; choose a different -o or use --fresh", dir)
}

// isNoop returns true when all live nodes are satisfied by the existing
// progress and a COMPLETE sentinel is present.
func isNoop(nodes []*source.Node, progress map[string]archive.ProgressRecord, volProgress map[string]archive.VolumeProgressRecord, dir string) bool {
	if !archive.IsComplete(dir) {
		return false
	}

	for _, n := range nodes {
		if !isNodeSatisfied(n, progress, volProgress, dir) {
			return false
		}
	}

	return true
}

// isNodeSatisfied reports whether a live node's manifests and volumes are already
// complete on disk and the ContentRef matches the progress record.
func isNodeSatisfied(n *source.Node, progress map[string]archive.ProgressRecord, volProgress map[string]archive.VolumeProgressRecord, dir string) bool {
	rec, ok := progress[n.ID]
	if !ok {
		return false
	}

	if rec.ContentRef != n.BoundSnapshotContentName {
		return false
	}

	for _, obj := range rec.Objects {
		if _, err := os.Stat(filepath.Join(dir, obj.Blob)); err != nil {
			return false
		}
	}

	if n.HasData {
		for _, dr := range n.DataRefs {
			key := archive.VolumeProgressKey(n.ID, dr.VSCName)

			vrec, ok := volProgress[key]
			if !ok || !vrec.Complete {
				return false
			}
		}
	}

	return true
}

// buildFailureSummary formats an error describing which nodes failed and how
// to resume.
func buildFailureSummary(failed []failedNode, opts Options) error {
	lines := make([]string, 0, len(failed)+2)
	lines = append(lines, fmt.Sprintf("%d node(s) failed to download:", len(failed)))

	for _, f := range failed {
		lines = append(lines, fmt.Sprintf("  %s: %v", f.ID, f.Err))
	}

	lines = append(lines, fmt.Sprintf(
		"Re-run the same command to resume the remaining downloads:\n  d8 snapshot download %s %s -o %s",
		opts.Namespace, opts.SnapshotName, opts.OutputDir,
	))

	return errors.New(strings.Join(lines, "\n"))
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
				APIVersion:      root.APIVersion,
				Kind:            root.Kind,
				Resource:        root.Resource,
				Name:            root.Name,
				UID:             root.UID,
				ResourceVersion: root.ResourceVersion,
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
			ObjectFilter:    opts.ObjectFilter,
			SelectedNodeIDs: selectedNodeIDs,
		},
	}
}

// buildIndex constructs the Index written to index.json.
// Summary counts are filled by DirWriter.Finalize.
func buildIndex(mode archive.SelectionMode, isPartial, includeVolumes bool) archive.Index {
	return archive.Index{
		SchemaVersion: archive.SchemaVersion,
		Capabilities: archive.IndexCapabilities{
			Manifests:            true,
			Volumes:              includeVolumes,
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
