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

package module

import (
	"os"
	"k8s.io/kubectl/pkg/util/templates"
	"github.com/spf13/cobra"
	module_enable "github.com/deckhouse/deckhouse-cli/internal/module/cmd/module-enable"
)


var moduleLong = templates.LongDesc(`
Enable, disable, list enabled, and show modules values in Kubernetes cluster.

© Flant JSC 2024`)

func NewCommand() *cobra.Command {
	moduleCmd := &cobra.Command{
		Use: "module", Short: "Edit modules in Kubernetes cluster. ",
		Long: editLong,
	}

        moduleCmd.AddCommand(
                module_enable.NewCommand(),
        )

        defaultKubeconfigPath := os.ExpandEnv("$HOME/.kube/config")
        if p := os.Getenv("KUBECONFIG"); p != "" {
                defaultKubeconfigPath = p
        }
	editCmd.PersistentFlags().StringVarP(&defaultKubeconfigPath, "kubeconfig", "k", "", "KubeConfig of the cluster. (default is $KUBECONFIG when it is set, $HOME/.kube/config otherwise)")

	return moduleCmd
}
