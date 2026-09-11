/*
Copyright 2026 Flant JSC

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

package virtualizationtar

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/collect-debug-info/debugtar"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var (
	virtualizationCmdLong = templates.LongDesc(`
		Collect a separate debug archive with detailed data from the d8-virtualization namespace.
	`)

	virtualizationCmdExample = templates.Examples(`
		# Collect the virtualization debug archive:
		d8 system collect-debug-info virtualization > deckhouse-debug-virtualization-$(date +"%Y_%m_%d").tar.gz

		# The --skip-ds-logs flag can be used to skip logs from DaemonSet-managed pods
		# (virt-handler, virtualization-dra, vm-route-forge, ...) to reduce archive size:
		d8 system collect-debug-info virtualization --skip-ds-logs > deckhouse-debug-virtualization-$(date +"%Y_%m_%d").tar.gz
	`)
)

func NewCommand() *cobra.Command {
	var (
		commandTimeout  time.Duration
		requestInterval time.Duration
		skipDsLogs      bool
	)

	virtualizationCmd := &cobra.Command{
		Use:           `virtualization [flags] > deckhouse-debug-virtualization-$(date +"%Y_%m_%d").tar.gz`,
		Short:         "Collect a separate virtualization debug archive.",
		Long:          virtualizationCmdLong,
		Example:       virtualizationCmdExample,
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE: func(_ *cobra.Command, _ []string) error {
			if term.IsTerminal(int(os.Stdout.Fd())) {
				return fmt.Errorf("output must be redirected to a file, e.g., \"> dump-logs.tar.gz\"")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			return collectVirtualizationDebugInfo(cmd, commandTimeout, requestInterval, skipDsLogs)
		},
	}

	virtualizationCmd.Flags().DurationVar(&commandTimeout, "command-timeout", 2*time.Minute, "Timeout for each individual debug command execution")
	virtualizationCmd.Flags().DurationVar(&requestInterval, "request-interval", 0, "Minimum interval between debug command executions to avoid overloading the cluster (e.g. 200ms, 500ms, 1s). Zero disables rate limiting (default 0s)")
	virtualizationCmd.Flags().BoolVar(&skipDsLogs, "skip-ds-logs", false, "Skip collecting logs from pods managed by a DaemonSet (virt-handler, virtualization-dra, vm-route-forge, ...) to reduce archive size on clusters with many nodes")

	return virtualizationCmd
}

func collectVirtualizationDebugInfo(cmd *cobra.Command, commandTimeout, requestInterval time.Duration, skipDsLogs bool) error {
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

	if err = debugtar.VirtualizationTarball(config, kubeCl, commandTimeout, requestInterval, skipDsLogs); err != nil {
		return fmt.Errorf("Error collecting virtualization debug info: %w", err)
	}

	return nil
}
