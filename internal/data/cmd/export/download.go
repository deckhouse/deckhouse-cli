/*
Copyright 2024 Flant JSC

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

package export

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/data/adapters"
	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	deAPI "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	exportUC "github.com/deckhouse/deckhouse-cli/internal/data/usecase/export"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// NewDownloadCommand creates a new export download command
func NewDownloadCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	config := &DownloadConfig{}

	cmd := &cobra.Command{
		Use:     "download [flags] [KIND/]data_export_name [path/file.ext]",
		Short:   "Download exported data",
		Example: downloadExamples(),
		Args: func(_ *cobra.Command, args []string) error {
			_, _, err := dataio.ParseArgs(args)
			return err
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDownload(ctx, log, cmd, config, args)
		},
	}

	config.BindDownloadFlags(cmd)
	return cmd
}

func downloadExamples() string {
	resp := []string{
		"  # Start exporter + Download + Stop for Filesystem",
		"    ... download [flags] kind/volume_name path/file.ext [-o out_file.ext]",
		"    ... download -n target-namespace pvc/my-file-volume mydir/testdir/file.txt -o file.txt",
		"  # Start exporter + Download + Stop for Block",
		"    ... download [flags] kind/volume_name [-o out_file.ext]",
		"    ... download -n target-namespace vs/my-vs-volume -o file.txt",
	}
	return strings.Join(resp, "\n")
}

func runDownload(ctx context.Context, log *slog.Logger, cmd *cobra.Command, config *DownloadConfig, args []string) error {
	config.DataName, config.SrcPath, _ = dataio.ParseArgs(args)

	// Create K8s client
	flags := cmd.PersistentFlags()
	safeClient.SupportNoAuth = false
	sc, err := safeClient.NewSafeClient(flags)
	if err != nil {
		return err
	}

	rtClient, err := sc.NewRTClient(deAPI.AddToScheme)
	if err != nil {
		return err
	}

	// Build dependencies
	repo := adapters.NewDataExportRepository(rtClient)
	httpClient := adapters.NewSafeClientAdapter(sc)
	fs := adapters.NewOSFileSystem()
	logger := adapters.NewSlogAdapter(log)

	// Execute use case
	uc := exportUC.NewDownloadUseCase(repo, httpClient, fs, logger)
	result, err := uc.Execute(ctx, &exportUC.DownloadParams{
		DataName:  config.DataName,
		Namespace: config.Namespace,
		SrcPath:   config.SrcPath,
		DstPath:   config.DstPath,
		Publish:   config.Publish,
		TTL:       config.TTL,
	})

	if err != nil {
		return err
	}

	// Clean up auto-created DataExport
	if result.WasCreated {
		if dataio.AskYesNoWithTimeout("DataExport will auto-delete in 30 sec [press y+Enter to delete now, n+Enter to cancel]", time.Second*30) {
			if deleteErr := uc.DeleteCreatedExport(ctx, result.ExportName, config.Namespace); deleteErr != nil {
				log.Warn("Failed to delete DataExport", slog.String("name", result.ExportName), slog.String("error", deleteErr.Error()))
			}
		}
	}

	return nil
}

