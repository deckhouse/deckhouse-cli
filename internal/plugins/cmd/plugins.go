/*
Copyright 2025 Flant JSC

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

// Package pluginscmd implements the `d8 plugins` command tree and the
// per-plugin wrapper command on top of the internal/plugins machinery.
package pluginscmd

import (
	"log/slog"

	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/cmd/errdetect"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	rppflags "github.com/deckhouse/deckhouse-cli/internal/rpp/flags"
)

// NewCommand returns the `d8 plugins` command tree for managing plugins.
func NewCommand(logger *dkplog.Logger) *cobra.Command {
	manager := plugins.NewManager(logger)

	cmd := &cobra.Command{
		Use:   "plugins",
		Short: "Manage Deckhouse CLI plugins",
		Long: "Manage Deckhouse CLI plugins.\n\n" +
			"Plugins are pulled from the in-cluster registry-packages-proxy, authenticated by the\n" +
			"current kubeconfig identity.\n\n" +
			"Update on demand with 'd8 plugins update <name>' or 'd8 plugins update all'.\n\n" +
			"Environment variables:\n" +
			"  " + flags.EnvSkipClusterChecks + "=1  skip cluster-side plugin requirement checks\n" +
			"  " + flags.EnvPluginsDir + "                plugins directory (same as --plugins-dir)\n" +
			"  " + rppflags.EnvEndpoint + "                   registry-packages-proxy base URL\n" +
			"  " + rppflags.EnvCAFile + "                    PEM CA bundle for proxy TLS verification\n" +
			"  KUBECONFIG                        path to the kubeconfig file",
		Hidden: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// The plugins directory was captured at registration time, BEFORE flag
			// parsing - re-read it here so --plugins-dir is honored (the env
			// path DECKHOUSE_CLI_PATH is applied earlier, at registration).
			manager.SetDirectory(flags.DeckhousePluginsDir)

			// init plugin services for subcommands after flags are parsed
			if err := manager.InitPluginServices(cmd.Context()); err != nil {
				return err
			}

			if err := manager.EnsureInstallRoot(); err != nil {
				logger.Warn("failed to ensure plugin root directory", slog.String("error", err.Error()))
			}

			return nil
		},
	}

	cmd.AddCommand(newListCommand(manager))
	cmd.AddCommand(newVersionsCommand(manager))
	cmd.AddCommand(newContractCommand(manager, logger))
	cmd.AddCommand(newInstallCommand(manager))
	cmd.AddCommand(newUpdateCommand(manager))
	cmd.AddCommand(newRemoveCommand(manager))

	flags.AddFlags(cmd.PersistentFlags())

	wrapProxyDiagnostics(cmd)

	return cmd
}

// wrapProxyDiagnostics turns recognized registry-packages-proxy failures into
// colored diagnostics at the command level (per pkg/diagnostic: classify in the
// command, never in root.go). It wraps every RunE in the tree; errdetect.Diagnose
// returns nil for non-proxy and already-diagnosed errors, leaving them untouched.
func wrapProxyDiagnostics(cmd *cobra.Command) {
	if cmd.RunE != nil {
		inner := cmd.RunE
		cmd.RunE = func(c *cobra.Command, args []string) error {
			err := inner(c, args)
			if diag := errdetect.Diagnose(err); diag != nil {
				return diag
			}

			return err
		}
	}

	for _, sub := range cmd.Commands() {
		wrapProxyDiagnostics(sub)
	}
}
