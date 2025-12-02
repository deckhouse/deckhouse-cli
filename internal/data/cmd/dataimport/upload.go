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

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/data/adapters"
	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	diAPI "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
	importUC "github.com/deckhouse/deckhouse-cli/internal/data/usecase/import"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// NewUploadCommand creates a new import upload command
func NewUploadCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	config := &UploadConfig{}

	cmd := &cobra.Command{
		Use:     "upload [flags] data_import_name path/file.ext",
		Short:   "Upload a file to the provided url",
		Example: uploadExamples(),
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires exactly 1 argument: data_import_name")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runUpload(ctx, log, cmd, config, args)
		},
	}

	config.BindUploadFlags(cmd)
	return cmd
}

func uploadExamples() string {
	resp := []string{
		"  # Upload with resume (continue from server-reported offset)",
		"    ... upload NAME -n NAMESPACE -P -d /dst/path -f ./file --resume",
		"  # Upload without resume, split into 4 chunks",
		"    ... upload NAME -n NAMESPACE -P -d /dst/path -f ./file -c 4",
	}
	return strings.Join(resp, "\n")
}

func runUpload(ctx context.Context, log *slog.Logger, cmd *cobra.Command, config *UploadConfig, args []string) error {
	config.Name, _, _ = dataio.ParseArgs(args)

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
	httpClient := adapters.NewSafeClientAdapter(sc)
	fs := adapters.NewOSFileSystem()
	logger := adapters.NewSlogAdapter(log)

	// Execute use case
	uc := importUC.NewUploadUseCase(repo, httpClient, fs, logger)
	return uc.Execute(ctx, &importUC.UploadParams{
		Name:      config.Name,
		Namespace: config.Namespace,
		FilePath:  config.FilePath,
		DstPath:   config.DstPath,
		Publish:   config.Publish,
		Chunks:    config.Chunks,
		Resume:    config.Resume,
	})
}

