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
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/data/adapters"
	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	deAPI "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	exportUC "github.com/deckhouse/deckhouse-cli/internal/data/usecase/export"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// NewListCommand creates a new export list command
func NewListCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	config := &ListConfig{}

	cmd := &cobra.Command{
		Use:     "list [flags] data_export_name [/path/]",
		Aliases: []string{"ls"},
		Short:   "List DataExported content information",
		Example: listExamples(),
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return fmt.Errorf("requires 1 or 2 arguments: data_export_name [/path/]")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(ctx, log, cmd, config, args)
		},
	}

	config.Config.BindFlags(cmd)
	return cmd
}

func listExamples() string {
	resp := []string{
		"  ... -n target-namespace list my-file-volume /mydir/testdir/",
		"  ... -n target-namespace list my-block-volume",
	}
	return strings.Join(resp, "\n")
}

func runList(ctx context.Context, log *slog.Logger, cmd *cobra.Command, config *ListConfig, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	config.DataName = args[0]
	if len(args) >= 2 {
		config.Path = args[1]
	}

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
	logger := adapters.NewSlogAdapter(log)

	// Execute use case
	uc := exportUC.NewListUseCase(repo, httpClient, logger)
	result, err := uc.Execute(ctx, &exportUC.ListParams{
		DataName:  config.DataName,
		Namespace: config.Namespace,
		Path:      config.Path,
		Publish:   config.Publish,
		TTL:       config.TTL,
	})

	if err != nil {
		return err
	}

	// Output content
	if result.Content != nil {
		if rc, ok := result.Content.(io.ReadCloser); ok {
			defer rc.Close()
		}
		if _, err := io.Copy(os.Stdout, result.Content); err != nil && err != io.EOF {
			return err
		}
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

