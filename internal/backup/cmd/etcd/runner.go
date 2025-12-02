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

package etcd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/backup/adapters"
	"github.com/deckhouse/deckhouse-cli/internal/backup/domain"
	"github.com/deckhouse/deckhouse-cli/internal/backup/usecase"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// Config holds configuration for etcd backup command
type Config struct {
	SnapshotPath string
	PodName      string
	Verbose      bool
}

// Runner executes etcd backup using clean architecture
type Runner struct {
	config *Config
}

// NewRunner creates a new Runner
func NewRunner(config *Config) *Runner {
	return &Runner{config: config}
}

// Run executes the backup
func (r *Runner) Run(ctx context.Context, cmd *cobra.Command) error {
	// Setup K8s client
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("get kubeconfig: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return fmt.Errorf("get context: %w", err)
	}

	restConfig, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return fmt.Errorf("setup Kubernetes client: %w", err)
	}

	// Build dependencies
	k8sClient := adapters.NewK8sClientAdapter(kubeCl, restConfig)
	fs := adapters.NewFileSystemAdapter()
	logger := adapters.NewSlogLogger()

	// Create use case
	uc := usecase.NewETCDBackupUseCase(k8sClient, fs, logger)

	// Execute
	params := &domain.ETCDBackupParams{
		SnapshotPath: r.config.SnapshotPath,
		PodName:      r.config.PodName,
		Verbose:      r.config.Verbose,
	}

	result, err := uc.Execute(ctx, params)
	if err != nil {
		return err
	}

	if !result.Success {
		return result.Error
	}

	return nil
}

