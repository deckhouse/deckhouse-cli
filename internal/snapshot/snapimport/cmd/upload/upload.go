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

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

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

// run is status-driven: it uploads the index as an opaque blob, then follows the per-node URLs the
// controller publishes in status.snapshots[] (after server-side re-root) to upload manifests and
// volume data. The CLI never parses the index, so the node set always comes from status, not the
// bundle — which is what makes spec.childSnapshot re-root transparent to the client.
func run(ctx context.Context, cmd *cobra.Command, name string) error {
	ns := snaputil.ResolveNamespace(cmd)
	dir, _ := cmd.Flags().GetString("dir")
	out := cmd.OutOrStdout()
	progress := func(s string) { fmt.Fprintln(cmd.ErrOrStderr(), s) }

	rawIndex, err := os.ReadFile(snaputil.IndexPath(dir))
	if err != nil {
		return fmt.Errorf("read index: %w", err)
	}

	sc, rt, api, err := snaputil.NewClients(cmd)
	if err != nil {
		return err
	}

	// The import must be created first (it carries targetName + childSnapshot + storage class mapping).
	if _, gerr := snaputil.GetSnapshotImport(ctx, rt, ns, name); gerr != nil {
		if apierrors.IsNotFound(gerr) {
			return fmt.Errorf("SnapshotImport %s/%s not found; create it first with 'd8 snapshot import create %s --target <name>'", ns, name, name)
		}
		return gerr
	}

	imp, err := snaputil.WaitImportUploadURLs(ctx, rt, ns, name, snaputil.DefaultTimeout, progress)
	if err != nil {
		return err
	}

	// 1. Index (opaque blob, as-is). It drives the controller's node resolution + re-root.
	if uerr := api.UploadBlob(ctx, imp.Status.IndexUploadURL, rawIndex, true); uerr != nil {
		return fmt.Errorf("upload index: %w", uerr)
	}

	// 2. Wait for the per-node view (post re-root), then upload each node's manifests by its own URL.
	imp, err = snaputil.WaitImportSnapshots(ctx, rt, ns, name, snaputil.DefaultTimeout, progress)
	if err != nil {
		return err
	}
	for i := range imp.Status.Snapshots {
		entry := &imp.Status.Snapshots[i]
		mraw, rerr := os.ReadFile(snaputil.ManifestPath(dir, entry.SnapshotID))
		if rerr != nil {
			return fmt.Errorf("read manifests for %s: %w", entry.SnapshotID, rerr)
		}
		if uerr := api.UploadBlob(ctx, entry.ManifestsUploadURL, mraw, true); uerr != nil {
			return fmt.Errorf("upload manifests for %s: %w", entry.SnapshotID, uerr)
		}
	}
	// Top-level manifests commit flips ManifestsReceived once every per-node manifest is in.
	if uerr := api.UploadBlob(ctx, imp.Status.ManifestsUploadURL, nil, true); uerr != nil {
		return fmt.Errorf("commit manifests: %w", uerr)
	}
	fmt.Fprintf(out, "uploaded index + manifests for %d node(s)\n", len(imp.Status.Snapshots))

	// 3. Wait for per-data upload endpoints, then upload each node's volume data by volume mode.
	imp, err = snaputil.WaitImportDataUploadsReady(ctx, rt, ns, name, snaputil.DefaultTimeout, progress)
	if err != nil {
		return err
	}
	for i := range imp.Status.Snapshots {
		entry := &imp.Status.Snapshots[i]
		if entry.VolumeMode == "" {
			continue // dataless node
		}
		if !entry.UploadReady || entry.UploadURL == "" {
			return fmt.Errorf("upload endpoint for node %s is not ready", entry.SnapshotID)
		}
		progress(fmt.Sprintf("uploading %s data for %s...", entry.VolumeMode, entry.SnapshotID))
		switch entry.VolumeMode {
		case snaputil.VolumeModeBlock:
			if uerr := snaputil.UploadBlock(ctx, sc, entry.UploadURL, entry.UploadCA, snaputil.DataPath(dir, entry.SnapshotID)); uerr != nil {
				return fmt.Errorf("upload data for %s: %w", entry.SnapshotID, uerr)
			}
		case snaputil.VolumeModeFilesystem:
			if uerr := snaputil.UploadFilesystem(ctx, sc, entry.UploadURL, entry.UploadCA, snaputil.DataDirPath(dir, entry.SnapshotID)); uerr != nil {
				return fmt.Errorf("upload data for %s: %w", entry.SnapshotID, uerr)
			}
		default:
			return fmt.Errorf("node %s has unknown volumeMode %q", entry.SnapshotID, entry.VolumeMode)
		}
	}

	// 4. Wait for the controller to capture + pre-provision the (re-rooted) tree.
	imp, err = snaputil.WaitImportReady(ctx, rt, ns, name, snaputil.DefaultTimeout, progress)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "SnapshotImport %s/%s is Ready; snapshot %q restored\n", ns, name, imp.Spec.TargetName)
	return nil
}
