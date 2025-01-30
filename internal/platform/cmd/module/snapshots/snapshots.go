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

package snapshots

import (
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/module/operatemodule"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var snapshotsLong = templates.LongDesc(`
Dump module hooks snapshots.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	snapshotsCmd := &cobra.Command{
		Use:           "snapshots",
		Short:         "Dump shapshots.",
		Long:          snapshotsLong,
		ValidArgs:     []string{"module_name"},
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          snapshotsModule,
	}
	return snapshotsCmd
}

func snapshotsModule(cmd *cobra.Command, moduleName []string) error {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}

	config, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("Failed to setup Kubernetes client: %w", err)
	}
	pathFromOption := fmt.Sprintf("%s/snapshots.yaml", moduleName[0])
	err = operatemodule.OptionsModule(config, kubeCl, pathFromOption)
	if err != nil {
		return fmt.Errorf("Error snapshot module: %w", err)
	}
	return err
}
