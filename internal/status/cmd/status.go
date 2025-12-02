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

package status

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/status/adapters"
	"github.com/deckhouse/deckhouse-cli/internal/status/usecase"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var statusLong = templates.LongDesc(`
Get status information about Deckhouse Kubernetes Platform

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	statusCmd := &cobra.Command{
		Use:           "status",
		Short:         "Get cluster status information",
		Long:          statusLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          runStatus,
	}

	addPersistentFlags(statusCmd.PersistentFlags())

	return statusCmd
}

func runStatus(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	// Setup K8s clients
	restConfig, kubeCl, err := setupK8sClients(cmd)
	if err != nil {
		return fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	dynamicCl, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Create provider factory
	factory := adapters.NewStatusProviderFactory(kubeCl, dynamicCl, restConfig)

	// Build usecase with all providers
	statusUC := usecase.NewStatusUseCase(
		factory.CreateMastersProvider(),
		factory.CreateDeckhousePodsProvider(),
		factory.CreateReleasesProvider(),
		factory.CreateEditionProvider(),
		factory.CreateSettingsProvider(),
		factory.CreateRegistryProvider(),
		factory.CreateClusterAlertsProvider(),
		factory.CreateCNIModulesProvider(),
		factory.CreateQueueProvider(),
	)

	// Execute and display results
	color.Cyan("\n┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
	color.Cyan("┃      Cluster Status Report     ┃")
	color.Cyan("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")

	report := statusUC.Execute(ctx)
	for _, section := range report.GetAllSections() {
		fmt.Println(section.Output)
	}

	return nil
}

func setupK8sClients(cmd *cobra.Command) (*rest.Config, *kubernetes.Clientset, error) {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get context: %w", err)
	}

	restConfig, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	return restConfig, kubeCl, nil
}
