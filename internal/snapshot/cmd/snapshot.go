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

// Package cmd wires the "d8 snapshot" command group (export/import of Snapshot hierarchies).
package cmd

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	seCreate "github.com/deckhouse/deckhouse-cli/internal/snapshot/snapexport/cmd/create"
	seDelete "github.com/deckhouse/deckhouse-cli/internal/snapshot/snapexport/cmd/delete"
	seDownload "github.com/deckhouse/deckhouse-cli/internal/snapshot/snapexport/cmd/download"
	siCreate "github.com/deckhouse/deckhouse-cli/internal/snapshot/snapimport/cmd/create"
	siDelete "github.com/deckhouse/deckhouse-cli/internal/snapshot/snapimport/cmd/delete"
	siUpload "github.com/deckhouse/deckhouse-cli/internal/snapshot/snapimport/cmd/upload"
)

const cmdName = "snapshot"

// NewCommand builds the "d8 snapshot" group with export and import subgroups.
func NewCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           cmdName,
		Short:         "Export and import Snapshot hierarchies (SnapshotExport / SnapshotImport)",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			_ = cmd.Help()
		},
	}
	root.SetOut(os.Stdout)

	// Cancel in-flight waits/transfers on Ctrl-C / SIGTERM (the wait loops and sleep honor ctx).
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	logger := slog.Default()

	exportCmd := &cobra.Command{
		Use:   "export",
		Short: "Export a Snapshot hierarchy (SnapshotExport)",
		Run:   func(cmd *cobra.Command, _ []string) { _ = cmd.Help() },
	}
	exportCmd.AddCommand(
		seCreate.NewCommand(ctx, logger),
		seDownload.NewCommand(ctx, logger),
		seDelete.NewCommand(ctx, logger),
	)

	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Import a Snapshot hierarchy (SnapshotImport)",
		Run:   func(cmd *cobra.Command, _ []string) { _ = cmd.Help() },
	}
	importCmd.AddCommand(
		siCreate.NewCommand(ctx, logger),
		siUpload.NewCommand(ctx, logger),
		siDelete.NewCommand(ctx, logger),
	)

	root.AddCommand(exportCmd, importCmd)
	return root
}
