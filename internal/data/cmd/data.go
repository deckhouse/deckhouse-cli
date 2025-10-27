/*
Copyright 2025 Flant JSC

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

package cmd

import (
	"context"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	deCreate "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/cmd/create"
	deDelete "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/cmd/delete"
	deDownload "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/cmd/download"
	deList "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/cmd/list"

	diCreate "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/cmd/create"
	diDelete "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/cmd/delete"
	diUpload "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/cmd/upload"
)

const (
	cmdName = "data"
)

func NewCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           cmdName,
		Short:         "Data operations (export/import)",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			cmd.Help()
		},
	}

	root.SetOut(os.Stdout)

	ctx := context.Background()
	logger := slog.Default()

	exportCmd := &cobra.Command{
		Use:           "export",
		Short:         "Export data (DataExport)",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run:           func(cmd *cobra.Command, _ []string) { cmd.Help() },
	}
	exportCmd.AddCommand(
		deCreate.NewCommand(ctx, logger),
		deDelete.NewCommand(ctx, logger),
		deDownload.NewCommand(ctx, logger),
		deList.NewCommand(ctx, logger),
	)

	importCmd := &cobra.Command{
		Use:           "import",
		Short:         "Import data (DataImport)",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run:           func(cmd *cobra.Command, _ []string) { cmd.Help() },
	}
	importCmd.AddCommand(
		diCreate.NewCommand(ctx, logger),
		diDelete.NewCommand(ctx, logger),
		diUpload.NewCommand(ctx, logger),
	)

	root.AddCommand(exportCmd, importCmd)

	return root
}
