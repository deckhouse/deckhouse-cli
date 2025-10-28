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

// Hooks for tests (overridable)
var (
	exportCreateRun   = deCreate.Run
	exportListRun     = deList.Run
	exportDownloadRun = deDownload.Run
	exportDeleteRun   = deDelete.Run
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

	// TODO remove this section later
	// Backward-compat: `d8 data create` maps to `d8 data export create` with deprecation warning.
	deprecatedCreate := &cobra.Command{
		Use:   "create [flags] data_export_name volume_type/volume_name",
		Short: "Deprecated: use 'd8 data export create'",
		RunE: func(c *cobra.Command, args []string) error {
			c.Println("WARNING: 'd8 data create' is deprecated and will be removed. Use 'd8 data export create'.")
			return exportCreateRun(ctx, logger, c, args)
		},
	}
	deprecatedCreate.Flags().StringP("namespace", "n", "d8-data-exporter", "data volume namespace")
	deprecatedCreate.Flags().String("ttl", "2m", "Time to live")
	deprecatedCreate.Flags().Bool("publish", false, "Provide access outside of cluster")

	// TODO remove this section later
	// Backward-compat: `d8 data list` -> export list (deprecated)
	deprecatedList := &cobra.Command{
		Use:   "list [flags] data_export_name [/path/]",
		Short: "Deprecated: use 'd8 data export list'",
		RunE: func(c *cobra.Command, args []string) error {
			c.Println("WARNING: 'd8 data list' is deprecated and will be removed. Use 'd8 data export list'.")
			return exportListRun(ctx, logger, c, args)
		},
	}
	deprecatedList.Flags().StringP("namespace", "n", "d8-data-exporter", "data volume namespace")
	deprecatedList.Flags().Bool("publish", false, "Provide access outside of cluster")
	deprecatedList.Flags().String("ttl", "2m", "Time to live for auto-created DataExport")

	// TODO remove this section later
	// Backward-compat: `d8 data download` -> export download (deprecated)
	deprecatedDownload := &cobra.Command{
		Use:   "download [flags] [KIND/]data_export_name [path/file.ext]",
		Short: "Deprecated: use 'd8 data export download'",
		RunE: func(c *cobra.Command, args []string) error {
			c.Println("WARNING: 'd8 data download' is deprecated and will be removed. Use 'd8 data export download'.")
			return exportDownloadRun(ctx, logger, c, args)
		},
	}
	deprecatedDownload.Flags().StringP("namespace", "n", "d8-data-exporter", "data volume namespace")
	deprecatedDownload.Flags().StringP("output", "o", "", "file to save data (default: same as resource)")
	deprecatedDownload.Flags().Bool("publish", false, "Provide access outside of cluster")
	deprecatedDownload.Flags().String("ttl", "2m", "Time to live for auto-created DataExport")

	// TODO remove this section later
	// Backward-compat: `d8 data delete` -> export delete (deprecated)
	deprecatedDelete := &cobra.Command{
		Use:   "delete [flags] data_export_name",
		Short: "Deprecated: use 'd8 data export delete'",
		RunE: func(c *cobra.Command, args []string) error {
			c.Println("WARNING: 'd8 data delete' is deprecated and will be removed. Use 'd8 data export delete'.")
			return exportDeleteRun(ctx, logger, c, args)
		},
	}
	deprecatedDelete.Flags().StringP("namespace", "n", "d8-data-exporter", "data volume namespace")

	root.AddCommand(exportCmd, importCmd, deprecatedCreate, deprecatedList, deprecatedDownload, deprecatedDelete)

	return root
}
