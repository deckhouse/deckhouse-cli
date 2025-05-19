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

package disable

import (
	"fmt"

	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/operatemodule"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var disableLong = templates.LongDesc(`
Disable module using the ModuleConfig resource.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	disableCmd := &cobra.Command{
		Use:           "disable",
		Short:         "Disable module.",
		Long:          disableLong,
		ValidArgs:     []string{"module_name"},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          disableModule,
	}
	return disableCmd
}

func disableModule(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("module name is required")
	}
	moduleName := args[0]

	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, _, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("Failed to create dynamic client: %v", err)
	}

	err = operatemodule.OperateModule(dynamicClient, moduleName, operatemodule.ModuleDisabled)
	if err != nil {
		return fmt.Errorf("Error disable module: %w", err)
	}

	fmt.Println("Module", moduleName, "disabled")
	return nil
}
