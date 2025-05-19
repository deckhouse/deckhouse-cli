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

package collect_debug_info

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/collect-debug-info/debugTar"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var collectDebugInfoCmdLong = templates.LongDesc(`
Collect debug info from Deckhouse Kubernetes Platform.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	collectDebugInfoCmd := &cobra.Command{
		Use:           "collect-debug-info",
		Short:         "Collect debug info.",
		Long:          collectDebugInfoCmdLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if term.IsTerminal(int(os.Stdout.Fd())) {
				return fmt.Errorf("Please provide output tar.gz to dump debug logs, ex. \"> dump-logs.tar.gz\"")
			}
			return nil
		},
		RunE: collectDebugInfo,
	}
	return collectDebugInfoCmd
}

func collectDebugInfo(cmd *cobra.Command, _ []string) error {
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

	err = debugTar.Tarball(config, kubeCl)
	if err != nil {
		return fmt.Errorf("Error collecting debug info: %w", err)
	}
	return err
}
