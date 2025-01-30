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
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/queue/flags"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/queue/operatequeue"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var listLong = templates.LongDesc(`
Dump all Deckhouse Kubernetes Platform queues.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	listCmd := &cobra.Command{
		Use:           "list",
		Short:         "Dump all queues.",
		Long:          listLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       flags.ValidateParameters,
		RunE:          listModule,
	}
	flags.AddFlags(listCmd.Flags())
	AddFlags(listCmd.Flags())
	return listCmd
}

func listModule(cmd *cobra.Command, args []string) error {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	empty, err := cmd.Flags().GetBool("show-empty")
	if err != nil {
		return fmt.Errorf("Failed to show empty queues from flag: %w", err)
	}

	format, err := cmd.Flags().GetString("output")
	if err != nil {
		return fmt.Errorf("Failed to get output format: %w", err)
	}

	pathFromOption := "list." + format
	if empty {
		pathFromOption = pathFromOption + "?showEmpty=true"
	}

	err = operatequeue.OperateQueue(config, kubeCl, pathFromOption)
	if err != nil {
		return fmt.Errorf("Error list queues: %w", err)
	}
	return err
}
