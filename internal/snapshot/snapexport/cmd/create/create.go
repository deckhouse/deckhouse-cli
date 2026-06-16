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

package create

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	v1alpha1 "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	snaputil "github.com/deckhouse/deckhouse-cli/internal/snapshot/util"
)

// NewCommand builds "d8 snapshot export create".
func NewCommand(ctx context.Context, _ *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "create <export-name> --snapshot <snapshot-name>",
		Short:         "Create a SnapshotExport for a snapshot hierarchy (or a typed subtree)",
		Args:          cobra.ExactArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(ctx, cmd, args[0])
		},
	}
	cmd.Flags().StringP("namespace", "n", "", "namespace of the snapshot and export (default: "+snaputil.DefaultNamespace+")")
	cmd.Flags().String("snapshot", "", "name of the snapshot to export (required)")
	cmd.Flags().String("kind", snaputil.DefaultSnapshotKind, "kind of the snapshot to export (e.g. Snapshot, DemoVirtualDiskSnapshot)")
	cmd.Flags().String("api-version", snaputil.DefaultSnapshotAPIVersion, "apiVersion of the snapshot to export")
	cmd.Flags().String("ttl", "30m", "idle time-to-live for the export's data endpoints")
	cmd.Flags().Bool("publish", false, "expose endpoints outside the cluster")
	cmd.Flags().Bool("wait", false, "wait until the export is Ready")
	_ = cmd.MarkFlagRequired("snapshot")
	return cmd
}

func run(ctx context.Context, cmd *cobra.Command, name string) error {
	ns := snaputil.ResolveNamespace(cmd)
	snapshot, _ := cmd.Flags().GetString("snapshot")
	kind, _ := cmd.Flags().GetString("kind")
	apiVersion, _ := cmd.Flags().GetString("api-version")
	ttl, _ := cmd.Flags().GetString("ttl")
	publish, _ := cmd.Flags().GetBool("publish")
	wait, _ := cmd.Flags().GetBool("wait")

	ref := v1alpha1.SnapshotReference{APIVersion: apiVersion, Kind: kind, Name: snapshot}

	_, rt, _, err := snaputil.NewClients(cmd)
	if err != nil {
		return err
	}
	if err := snaputil.EnsureSnapshotExport(ctx, rt, ns, name, ref, ttl, publish); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "SnapshotExport %s/%s created for %s %q\n", ns, name, kind, snapshot)

	if wait {
		if _, err := snaputil.WaitSnapshotExportReady(ctx, rt, ns, name, snaputil.DefaultTimeout,
			func(s string) { fmt.Fprintln(cmd.ErrOrStderr(), s) }); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "SnapshotExport %s/%s is Ready\n", ns, name)
	}
	return nil
}
