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

package dataimport

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/data/adapters"
	diAPI "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
	importUC "github.com/deckhouse/deckhouse-cli/internal/data/usecase/import"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// NewDeleteCommand creates a new import delete command
func NewDeleteCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	config := &DeleteConfig{}

	cmd := &cobra.Command{
		Use:     "delete [flags] data_import_name",
		Short:   "Delete DataImport",
		Example: deleteExamples(),
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires exactly 1 argument: data_import_name")
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
		"  ... -n target-namespace delete my-import",
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

	rtClient, err := sc.NewRTClient(diAPI.AddToScheme)
	if err != nil {
		return err
	}

	// Build dependencies
	repo := adapters.NewDataImportRepository(rtClient)
	logger := adapters.NewSlogAdapter(log)

	// Execute use case
	uc := importUC.NewDeleteUseCase(repo, logger)
	return uc.Execute(ctx, &importUC.DeleteParams{
		Name:      config.Name,
		Namespace: config.Namespace,
	})
}

