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

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/deckhouse"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var snapshotsLong = templates.LongDesc(`
Dump module hooks snapshots.

© Flant JSC 2025`)

var outputFormats = []string{"yaml", "json"}

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
	utilk8s.AddOutputFlag(snapshotsCmd, "yaml", outputFormats...)

	return snapshotsCmd
}

func snapshotsModule(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("this command requires exactly 1 argument: module name")
	}

	moduleName := args[0]

	format, err := utilk8s.GetOutputFormat(cmd, outputFormats...)
	if err != nil {
		return err
	}

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

	pathFromOption := fmt.Sprintf("%s/snapshots.%s", moduleName, format)

	err = deckhouse.QueryAPI(config, kubeCl, pathFromOption)
	if err != nil {
		return fmt.Errorf("Error snapshot module: %w", err)
	}

	return nil
}
