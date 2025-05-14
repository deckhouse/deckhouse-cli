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

package list

import (
	"fmt"

	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/operatemodule"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/edit/flags"
)

var listLong = templates.LongDesc(`
List enabled Deckhouse Kubernetes Platform modules.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	listCmd := &cobra.Command{
		Use:           "list",
		Short:         "List enabled modules.",
		Long:          listLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          listModule,
	}
	flags.AddFlags(listCmd.Flags())
	return listCmd
}

func listModule(cmd *cobra.Command, _ []string) error {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	err = operatemodule.OptionsModule(config, kubeCl, "list.yaml")
	if err != nil {
		return fmt.Errorf("Error list modules: %w", err)
	}

	return nil
}
