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

package enable

import (
	"fmt"

	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/operatemodule"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var enableLong = templates.LongDesc(`
Enable module using the ModuleConfig resource.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	enableCmd := &cobra.Command{
		Use:           "enable",
		Short:         "Enable module.",
		Long:          enableLong,
		ValidArgs:     []string{"module_name"},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          enableModule,
	}
	return enableCmd
}

func enableModule(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("this command requires exactly 1 argument: module name")
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

	err = operatemodule.OperateModule(dynamicClient, moduleName, operatemodule.ModuleEnabled)
	if err != nil {
		return fmt.Errorf("Error enable module: %w", err)
	}

	fmt.Println("Module", moduleName, "enabled")
	return nil
}
