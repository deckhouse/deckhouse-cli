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

package upload

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	v1alpha1 "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	snaputil "github.com/deckhouse/deckhouse-cli/internal/snapshot/util"
)

// NewCommand builds "d8 snapshot import upload".
func NewCommand(ctx context.Context, _ *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "upload <import-name> --dir <bundle-dir>",
		Short:         "Upload a snapshot export bundle to a SnapshotImport resumably",
		Args:          cobra.ExactArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(ctx, cmd, args[0])
		},
	}
	cmd.Flags().StringP("namespace", "n", "", "namespace of the import (default: "+snaputil.DefaultNamespace+")")
	cmd.Flags().StringP("dir", "d", "", "local bundle directory to upload (required)")
	_ = cmd.MarkFlagRequired("dir")
	return cmd
}

func run(ctx context.Context, cmd *cobra.Command, name string) error {
	ns := snaputil.ResolveNamespace(cmd)
	dir, _ := cmd.Flags().GetString("dir")
	out := cmd.OutOrStdout()
	progress := func(s string) { fmt.Fprintln(cmd.ErrOrStderr(), s) }

	rawIndex, err := os.ReadFile(snaputil.IndexPath(dir))
	if err != nil {
		return fmt.Errorf("read index: %w", err)
	}
	idx, err := snaputil.ParseIndex(rawIndex)
	if err != nil {
		return err
	}
	if fs := filesystemDataNodes(idx); len(fs) > 0 {
		return fmt.Errorf("filesystem volume mode is not yet supported by the CLI; affected nodes: %s", strings.Join(fs, ", "))
	}

	sc, rt, api, err := snaputil.NewClients(cmd)
	if err != nil {
		return err
	}

	// The import must be created first (it carries the snapshot name + storage class mapping).
	if _, gerr := snaputil.GetSnapshotImport(ctx, rt, ns, name); gerr != nil {
		if apierrors.IsNotFound(gerr) {
			return fmt.Errorf("SnapshotImport %s/%s not found; create it first with 'd8 snapshot import create %s --snapshot <name>'", ns, name, name)
		}
		return gerr
	}

	imp, err := snaputil.WaitImportUploadURLs(ctx, rt, ns, name, snaputil.DefaultTimeout, progress)
	if err != nil {
		return err
	}

	// 1. Index (drives the controller's node resolution); reuse the bytes already read above.
	if uerr := api.UploadBlob(ctx, imp.Status.IndexUploadURL, rawIndex, true); uerr != nil {
		return fmt.Errorf("upload index: %w", uerr)
	}

	// 2. Per-node manifests, then the whole-tree commit.
	for i := range idx.Snapshots {
		node := &idx.Snapshots[i]
		mraw, rerr := os.ReadFile(snaputil.ManifestPath(dir, node.ID))
		if rerr != nil {
			return fmt.Errorf("read manifests for %s: %w", node.ID, rerr)
		}
		if uerr := api.UploadBlob(ctx, snaputil.ManifestsNodePath(imp.Status.ManifestsUploadURL, node.ID), mraw, true); uerr != nil {
			return fmt.Errorf("upload manifests for %s: %w", node.ID, uerr)
		}
	}
	if uerr := api.UploadBlob(ctx, imp.Status.ManifestsUploadURL, nil, true); uerr != nil {
		return fmt.Errorf("commit manifests: %w", uerr)
	}
	fmt.Fprintf(out, "uploaded index + manifests for %d node(s)\n", len(idx.Snapshots))

	// 3. Wait for per-data upload endpoints, then upload each block image.
	imp, err = snaputil.WaitImportDataUploadsReady(ctx, rt, ns, name, snaputil.DefaultTimeout, progress)
	if err != nil {
		return err
	}
	dataByID := snaputil.ImportDataEntryByID(imp.Status.DataSnapshots)
	for i := range idx.Snapshots {
		node := &idx.Snapshots[i]
		if !node.HasData || node.Data == nil {
			continue
		}
		entry, ok := dataByID[node.ID]
		if !ok || !entry.UploadReady || entry.UploadURL == "" {
			return fmt.Errorf("upload endpoint for node %s is not ready", node.ID)
		}
		progress(fmt.Sprintf("uploading data for %s...", node.ID))
		if uerr := snaputil.UploadBlock(ctx, sc, entry.UploadURL, entry.UploadCA, snaputil.DataPath(dir, node.ID)); uerr != nil {
			return fmt.Errorf("upload data for %s: %w", node.ID, uerr)
		}
	}

	// 4. Wait for the controller to capture + pre-provision the whole tree.
	if _, werr := snaputil.WaitImportReady(ctx, rt, ns, name, snaputil.DefaultTimeout, progress); werr != nil {
		return werr
	}
	fmt.Fprintf(out, "SnapshotImport %s/%s is Ready; snapshot %q restored\n", ns, name, idx.RootSnapshot.Name)
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
