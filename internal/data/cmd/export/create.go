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

// NewCreateCommand creates a new export create command
func NewCreateCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	config := &CreateConfig{}

	cmd := &cobra.Command{
		Use:     "create [flags] data_export_name volume_type/volume_name",
		Short:   "Create dataexport kubernetes resource",
		Example: createExamples(),
		Args:    validateCreateArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(ctx, log, cmd, config, args)
		},
	}

	config.Config.BindFlags(cmd)
	return cmd
}

func createExamples() string {
	resp := []string{
		"  # Start data exporting for PVC 'test-pvc-name'",
		"    ... create export-name pvc/test-pvc-name",
		"  # Start data exporting with extra flags",
		"    ... create --kubeconfig='kube_tmp.conf' -n target-namespace --ttl 17m export-name pvc/test-pvc-name",
	}
	return strings.Join(resp, "\n")
}

func validateCreateArgs(_ *cobra.Command, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("requires exactly 2 arguments: data_export_name and volume_type/volume_name")
	}
	
	parts := strings.Split(args[1], "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid volume format, expect: <type>/<name>")
	}

	volumeKind := strings.ToLower(parts[0])
	switch volumeKind {
	case "pvc", "persistentvolumeclaim", "vs", "volumesnapshot", "vd", "virtualdisk", "vds", "virtualdisksnapshot":
		return nil
	default:
		return fmt.Errorf("invalid volume type; valid values: pvc | persistentvolumeclaim | vs | volumesnapshot | vd | virtualdisk | vds | virtualdisksnapshot")
	}
}

func runCreate(ctx context.Context, log *slog.Logger, cmd *cobra.Command, config *CreateConfig, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	config.Name = args[0]
	config.VolumeRef = args[1]

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
	uc := exportUC.NewCreateUseCase(repo, logger)
	return uc.Execute(ctx, &exportUC.CreateParams{
		Name:      config.Name,
		Namespace: config.Namespace,
		TTL:       config.TTL,
		VolumeRef: config.VolumeRef,
		Publish:   config.Publish,
	})
}

