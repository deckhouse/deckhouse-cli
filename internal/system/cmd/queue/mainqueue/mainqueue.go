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

package mainqueue

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/queue/flags"
	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/queue/operatequeue"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var mainQueueLong = templates.LongDesc(`
Dump main Deckhouse Kubernetes Platform queues.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	listCmd := &cobra.Command{
		Use:           "main",
		Short:         "Dump main queue.",
		Long:          mainQueueLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       flags.ValidateParameters,
		RunE:          mainQueue,
	}
	flags.AddFlags(listCmd.Flags())
	return listCmd
}

func mainQueue(cmd *cobra.Command, _ []string) error {
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

	format, err := cmd.Flags().GetString("output")
	if err != nil {
		return fmt.Errorf("Failed to get output format: %w", err)
	}

	pathFromOption := "main." + format

	err = operatequeue.OperateQueue(config, kubeCl, pathFromOption, false)
	if err != nil {
		return fmt.Errorf("Error list main queue: %w", err)
	}

	return nil
}
