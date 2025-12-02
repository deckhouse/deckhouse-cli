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
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/data/adapters"
	deAPI "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	exportUC "github.com/deckhouse/deckhouse-cli/internal/data/usecase/export"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// NewDeleteCommand creates a new export delete command
func NewDeleteCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	config := &DeleteConfig{}

	cmd := &cobra.Command{
		Use:     "delete [flags] data_export_name",
		Short:   "Delete dataexport kubernetes resource",
		Example: deleteExamples(),
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires exactly 1 argument: data_export_name")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDelete(ctx, log, cmd, config, args)
		},
	}

	config.BindFlags(cmd)
	return cmd
}

func deleteExamples() string {
	resp := []string{
		"  ... -n target-namespace delete my-volume",
	}
	return strings.Join(resp, "\n")
}

func runDelete(ctx context.Context, log *slog.Logger, cmd *cobra.Command, config *DeleteConfig, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	config.Name = args[0]

	// Create K8s client
	flags := cmd.PersistentFlags()
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
	logger := adapters.NewSlogAdapter(log)

	// Execute use case
	uc := exportUC.NewDeleteUseCase(repo, logger)
	return uc.Execute(ctx, &exportUC.DeleteParams{
		Name:      config.Name,
		Namespace: config.Namespace,
	})
}

