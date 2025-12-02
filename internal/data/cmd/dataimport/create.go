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
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/internal/data/adapters"
	diAPI "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/data/domain"
	importUC "github.com/deckhouse/deckhouse-cli/internal/data/usecase/import"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// NewCreateCommand creates a new import create command
func NewCreateCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	config := &CreateConfig{}

	cmd := &cobra.Command{
		Use:     "create [flags] data_import_name",
		Short:   "Create DataImport",
		Example: createExamples(),
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("requires exactly 1 argument: data_import_name")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCreate(ctx, log, cmd, config, args)
		},
	}

	config.BindCreateFlags(cmd)
	return cmd
}

func createExamples() string {
	resp := []string{
		"  # Create DataImport",
		"    ... create my-import -n d8-storage-volume-data-manager -f - --ttl 2m --publish --wffc",
	}
	return strings.Join(resp, "\n")
}

func runCreate(ctx context.Context, log *slog.Logger, cmd *cobra.Command, config *CreateConfig, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
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

	// Read PVC spec from file
	data, err := os.ReadFile(config.PVCFilePath)
	if err != nil {
		return fmt.Errorf("read PVC file: %w", err)
	}

	pvcTpl := &diAPI.PersistentVolumeClaimTemplateSpec{}
	if err := yaml.Unmarshal(data, pvcTpl); err != nil {
		return fmt.Errorf("parse PVC: %w", err)
	}

	// Build domain PVC spec
	pvcSpec := buildPVCSpec(pvcTpl)

	namespace := config.Namespace
	if namespace == "" {
		if pvcTpl.Namespace == "" {
			return fmt.Errorf("namespace is required")
		}
		namespace = pvcTpl.Namespace
	}

	// Build dependencies
	repo := adapters.NewDataImportRepository(rtClient)
	logger := adapters.NewSlogAdapter(log)

	// Execute use case
	uc := importUC.NewCreateUseCase(repo, logger)
	return uc.Execute(ctx, &importUC.CreateParams{
		Name:      config.Name,
		Namespace: namespace,
		TTL:       config.TTL,
		Publish:   config.Publish,
		WFFC:      config.WFFC,
		PVCSpec:   pvcSpec,
	})
}

func buildPVCSpec(tpl *diAPI.PersistentVolumeClaimTemplateSpec) *domain.PVCSpec {
	if tpl == nil {
		return nil
	}

	accessModes := make([]string, len(tpl.PersistentVolumeClaimSpec.AccessModes))
	for i, mode := range tpl.PersistentVolumeClaimSpec.AccessModes {
		accessModes[i] = string(mode)
	}

	var storageClassName string
	if tpl.PersistentVolumeClaimSpec.StorageClassName != nil {
		storageClassName = *tpl.PersistentVolumeClaimSpec.StorageClassName
	}

	var storage string
	if requests := tpl.PersistentVolumeClaimSpec.Resources.Requests; requests != nil {
		if q, ok := requests[diAPI.ResourceStorage]; ok {
			storage = q.String()
		}
	}

	return &domain.PVCSpec{
		Name:             tpl.ObjectMeta.Name,
		Namespace:        tpl.ObjectMeta.Namespace,
		StorageClassName: storageClassName,
		AccessModes:      accessModes,
		Storage:          storage,
	}
}

