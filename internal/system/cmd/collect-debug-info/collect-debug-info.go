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

package collectdebuginfo

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/collect-debug-info/debugtar"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var collectDebugInfoCmdLong = templates.LongDesc(`
Collect debug info from Deckhouse Kubernetes Platform.

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
	var (
		excludeList []string
		listExclude bool
	)

	collectDebugInfoCmd := &cobra.Command{
		Use:           "collect-debug-info",
		Short:         "Collect debug info.",
		Long:          collectDebugInfoCmdLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			if listExclude {
				return nil
			}

			if term.IsTerminal(int(os.Stdout.Fd())) {
				return fmt.Errorf("output must be redirected to a file, e.g., \"> dump-logs.tar.gz\"")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return collectDebugInfo(cmd, listExclude, excludeList)
		},
	}
	collectDebugInfoCmd.Flags().StringSliceVar(&excludeList, "exclude", []string{}, "Exclude specific files from the debug archive. Use comma-separated values")
	collectDebugInfoCmd.Flags().BoolVarP(&listExclude, "list-exclude", "l", false, "List all files that can be excluded from the debug archive")
	return collectDebugInfoCmd
}

func printExcludableFiles() {
	fmt.Println("List of possible data to exclude:")
	for _, fileName := range debugtar.GetExcludableFiles() {
		fmt.Println(fileName)
	}
}

func collectDebugInfo(cmd *cobra.Command, listExclude bool, excludeList []string) error {
	if listExclude {
		printExcludableFiles()
		return nil
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

	if err = debugtar.Tarball(config, kubeCl, excludeList); err != nil {
		return fmt.Errorf("Error collecting debug info: %w", err)
	}
	return nil
}
