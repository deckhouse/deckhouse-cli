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
	"strings"

	"github.com/spf13/cobra"

	v1alpha1 "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	snaputil "github.com/deckhouse/deckhouse-cli/internal/snapshot/util"
)

// NewCommand builds "d8 snapshot export download".
func NewCommand(ctx context.Context, _ *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "download <export-name> --dir <bundle-dir>",
		Short:         "Download a snapshot export bundle (index, manifests, volume data) resumably",
		Args:          cobra.ExactArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(ctx, cmd, args[0])
		},
	}
	cmd.Flags().StringP("namespace", "n", "", "namespace of the export (default: "+snaputil.DefaultNamespace+")")
	cmd.Flags().StringP("dir", "d", "", "local bundle directory to download into (required)")
	cmd.Flags().Bool("archive", false, "print the hierarchy as a flat archive listing instead of a tree")
	_ = cmd.MarkFlagRequired("dir")
	return cmd
}

func run(ctx context.Context, cmd *cobra.Command, name string) error {
	ns := snaputil.ResolveNamespace(cmd)
	dir, _ := cmd.Flags().GetString("dir")
	archive, _ := cmd.Flags().GetBool("archive")
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
	if se.Status.IndexURL == "" || se.Status.ManifestsURL == "" {
		return fmt.Errorf("SnapshotExport %s/%s is Ready but index/manifests URLs are not published", ns, name)
	}
	if err := snaputil.EnsureBundleDirs(dir); err != nil {
		return err
	}

	// 1. Index.
	rawIndex, err := api.Get(ctx, se.Status.IndexURL)
	if err != nil {
		return fmt.Errorf("download index: %w", err)
	}
	if err := snaputil.WriteFileAtomic(snaputil.IndexPath(dir), rawIndex); err != nil {
		return err
	}
	idx, err := snaputil.ParseIndex(rawIndex)
	if err != nil {
		return err
	}
	if archive {
		snaputil.PrintArchive(out, idx)
	} else {
		snaputil.PrintTree(out, idx)
	}

	// Fail fast on unsupported volume modes before transferring data.
	if fs := filesystemDataNodes(idx); len(fs) > 0 {
		return fmt.Errorf("filesystem volume mode is not yet supported by the CLI; affected nodes: %s", strings.Join(fs, ", "))
	}

	// 2. Per-node manifests.
	for i := range idx.Snapshots {
		node := &idx.Snapshots[i]
		raw, merr := api.Get(ctx, snaputil.ManifestsNodePath(se.Status.ManifestsURL, node.ID))
		if merr != nil {
			return fmt.Errorf("download manifests for %s: %w", node.ID, merr)
		}
		if werr := snaputil.WriteFileAtomic(snaputil.ManifestPath(dir, node.ID), raw); werr != nil {
			return werr
		}
	}
	fmt.Fprintf(out, "downloaded index + manifests for %d node(s)\n", len(idx.Snapshots))

	// 3. Per-data-node block volume images (resumable).
	dataByID := snaputil.ExportDataEntryByID(se.Status.DataSnapshots)
	for i := range idx.Snapshots {
		node := &idx.Snapshots[i]
		if !node.HasData || node.Data == nil {
			continue
		}
		entry, ok := dataByID[node.ID]
		if !ok || !entry.Ready || entry.DataURL == "" {
			return fmt.Errorf("data endpoint for node %s is not ready", node.ID)
		}
		progress(fmt.Sprintf("downloading data for %s...", node.ID))
		if derr := snaputil.DownloadBlock(ctx, sc, entry.DataURL, entry.DataCA, snaputil.DataPath(dir, node.ID)); derr != nil {
			return fmt.Errorf("download data for %s: %w", node.ID, derr)
		}
	}

	fmt.Fprintf(out, "export bundle downloaded to %s\n", dir)
	return nil
}

func filesystemDataNodes(idx *v1alpha1.Index) []string {
	var fs []string
	for i := range idx.Snapshots {
		n := &idx.Snapshots[i]
		if n.HasData && n.Data != nil && n.Data.VolumeMode == snaputil.VolumeModeFilesystem {
			fs = append(fs, n.ID)
		}
	}
	return fs
}
