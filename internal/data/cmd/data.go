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
	"fmt"
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
			_ = cmd.Help()
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
		Run:           func(cmd *cobra.Command, _ []string) { _ = cmd.Help() },
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
		Run:           func(cmd *cobra.Command, _ []string) { _ = cmd.Help() },
	}
	importCmd.AddCommand(
		diCreate.NewCommand(ctx, logger),
		diDelete.NewCommand(ctx, logger),
		diUpload.NewCommand(ctx, logger),
	)

	// TODO: remove deprecated commands after migration period
	deprecatedCreate := &cobra.Command{
		Use:    "create",
		Hidden: true,
		RunE: func(c *cobra.Command, args []string) error {
			return fmt.Errorf("'d8 data create' has been removed, use 'd8 data export create'")
		},
	}

	deprecatedList := &cobra.Command{
		Use:    "list",
		Hidden: true,
		RunE: func(c *cobra.Command, args []string) error {
			return fmt.Errorf("'d8 data list' has been removed, use 'd8 data export list'")
		},
	}

	deprecatedDownload := &cobra.Command{
		Use:    "download",
		Hidden: true,
		RunE: func(c *cobra.Command, args []string) error {
			return fmt.Errorf("'d8 data download' has been removed, use 'd8 data export download'")
		},
	}

	deprecatedDelete := &cobra.Command{
		Use:    "delete",
		Hidden: true,
		RunE: func(c *cobra.Command, args []string) error {
			return fmt.Errorf("'d8 data delete' has been removed, use 'd8 data export delete'")
		},
	}

	root.AddCommand(exportCmd, importCmd, deprecatedCreate, deprecatedList, deprecatedDownload, deprecatedDelete)

	return root
}
