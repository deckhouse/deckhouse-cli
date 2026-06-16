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

package download

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	snaputil "github.com/deckhouse/deckhouse-cli/internal/snapshot/util"
)

// NewCommand builds "d8 snapshot export download".
func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "download <export-name> --dir <bundle-dir>",
		Short:         "Download a snapshot export bundle (index, view, manifests, volume data) resumably",
		Args:          cobra.ExactArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(ctx, log, cmd, args[0])
		},
	}
	cmd.Flags().StringP("namespace", "n", "", "namespace of the export (default: "+snaputil.DefaultNamespace+")")
	cmd.Flags().StringP("dir", "d", "", "local bundle directory to download into (required)")
	_ = cmd.MarkFlagRequired("dir")
	return cmd
}

// run is status-driven: it never parses the index. It stores status.indexURL verbatim, then follows
// the per-node URLs published in status.snapshots[] to fetch manifests and volume data.
func run(ctx context.Context, log *slog.Logger, cmd *cobra.Command, name string) error {
	ns := snaputil.ResolveNamespace(cmd)
	dir, _ := cmd.Flags().GetString("dir")
	out := cmd.OutOrStdout()
	progress := func(s string) { fmt.Fprintln(cmd.ErrOrStderr(), s) }

	sc, rt, api, err := snaputil.NewClients(cmd)
	if err != nil {
		return err
	}

	se, err := snaputil.WaitSnapshotExportReady(ctx, rt, ns, name, snaputil.DefaultTimeout, progress)
	if err != nil {
		return err
	}
	if se.Status.IndexURL == "" {
		return fmt.Errorf("SnapshotExport %s/%s is Ready but status.indexURL is not published", ns, name)
	}
	if len(se.Status.Snapshots) == 0 {
		return fmt.Errorf("SnapshotExport %s/%s is Ready but status.snapshots[] is empty", ns, name)
	}
	if err := snaputil.EnsureBundleDirs(dir); err != nil {
		return err
	}

	// 1. Index: stored verbatim as an opaque blob (never parsed by the CLI).
	rawIndex, err := api.Get(ctx, se.Status.IndexURL)
	if err != nil {
		return fmt.Errorf("download index: %w", err)
	}
	if err := snaputil.WriteFileAtomic(snaputil.IndexPath(dir), rawIndex); err != nil {
		return err
	}

	// 2. View: the stable projection for `d8 snapshot list --dir`. Best-effort — a missing /view
	// permission must not fail the data download.
	if err := saveView(ctx, rt, api, ns, se.Spec.SnapshotRef, dir); err != nil {
		progress(fmt.Sprintf("warning: could not save view.json (snapshot list --dir will be unavailable): %v", err))
	}

	// 3. Per-node manifests (data and dataless), following each node's own manifestsURL.
	dataNodes := 0
	for i := range se.Status.Snapshots {
		entry := &se.Status.Snapshots[i]
		if entry.ManifestsURL == "" {
			return fmt.Errorf("node %s has no manifestsURL in status", entry.SnapshotID)
		}
		raw, merr := api.Get(ctx, entry.ManifestsURL)
		if merr != nil {
			return fmt.Errorf("download manifests for %s: %w", entry.SnapshotID, merr)
		}
		if werr := snaputil.WriteFileAtomic(snaputil.ManifestPath(dir, entry.SnapshotID), raw); werr != nil {
			return werr
		}
		if entry.HasData {
			dataNodes++
		}
	}
	fmt.Fprintf(out, "downloaded index + view + manifests for %d node(s)\n", len(se.Status.Snapshots))

	// 4. Per-data-node volume data, by volume mode.
	for i := range se.Status.Snapshots {
		entry := &se.Status.Snapshots[i]
		if !entry.HasData {
			continue
		}
		if !entry.Ready || entry.DataURL == "" {
			return fmt.Errorf("data endpoint for node %s is not ready", entry.SnapshotID)
		}
		progress(fmt.Sprintf("downloading %s data for %s...", entry.VolumeMode, entry.SnapshotID))
		switch entry.VolumeMode {
		case snaputil.VolumeModeBlock:
			if derr := snaputil.DownloadBlock(ctx, sc, entry.DataURL, entry.DataCA, snaputil.DataPath(dir, entry.SnapshotID)); derr != nil {
				return fmt.Errorf("download data for %s: %w", entry.SnapshotID, derr)
			}
		case snaputil.VolumeModeFilesystem:
			if derr := snaputil.DownloadFilesystem(ctx, sc, entry.DataURL, entry.DataCA, snaputil.DataDirPath(dir, entry.SnapshotID), log); derr != nil {
				return fmt.Errorf("download data for %s: %w", entry.SnapshotID, derr)
			}
		default:
			return fmt.Errorf("node %s has unknown volumeMode %q", entry.SnapshotID, entry.VolumeMode)
		}
	}

	fmt.Fprintf(out, "export bundle downloaded to %s (%d node(s), %d with data)\n", dir, len(se.Status.Snapshots), dataNodes)
	return nil
}

// saveView fetches the server-side SnapshotView for the exported (sub)tree root and stores it as
// view.json. The /view URL is not published in status, so the CLI builds it from spec.snapshotRef.
func saveView(ctx context.Context, rt ctrlclient.Client, api *snaputil.APIClient, ns string, ref v1alpha1.SnapshotReference, dir string) error {
	resource, err := snaputil.ResolveResource(rt, ref.APIVersion, ref.Kind)
	if err != nil {
		return err
	}
	raw, err := api.Get(ctx, snaputil.ViewAbsPath(ns, resource, ref.Name))
	if err != nil {
		return err
	}
	if _, err := snaputil.ParseView(raw); err != nil {
		return err
	}
	return snaputil.WriteFileAtomic(snaputil.ViewPath(dir), raw)
}
