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

package distcmd

import (
	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/dist/cmd/errdetect"
	pluginscmd "github.com/deckhouse/deckhouse-cli/internal/plugins/cmd"
	pluginflags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	rppflags "github.com/deckhouse/deckhouse-cli/internal/rpp/flags"
)

// NewCommand returns the `d8 dist` command tree - management of the d8
// distribution: the deckhouse-cli binary itself and its plugins. It reaches
// the registry-packages-proxy with the caller's kubeconfig identity.
// builtinCommands are built-in command names that satisfy a plugin dependency
// of the same name (see pluginscmd.NewCommand).
func NewCommand(logger *dkplog.Logger, builtinCommands []string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dist",
		Short: "Manage the d8 distribution: the deckhouse-cli binary and its plugins",
		Long: "Manage the d8 distribution - the deckhouse-cli binary and its plugins.\n\n" +
			"Versions are served by the in-cluster registry-packages-proxy, authenticated by the\n" +
			"current kubeconfig identity.\n\n" +
			"Update the binary with 'd8 dist update'; manage plugins under 'd8 dist plugins'.\n\n" +
			"Environment variables:\n" +
			"  " + rppflags.EnvEndpoint + "  registry-packages-proxy base URL (otherwise discovered from the cluster)\n" +
			"  " + rppflags.EnvCAFile + "   PEM CA bundle to verify the proxy TLS certificate\n" +
			"  KUBECONFIG       path to the kubeconfig file",
	}

	cmd.AddCommand(newCheckCommand(logger))
	cmd.AddCommand(newUpdateCommand(logger))
	cmd.AddCommand(newUseCommand(logger))
	cmd.AddCommand(newVersionsCommand(logger))

	// Cluster access flags (kubeconfig/context, rpp-*) are owned by the dist
	// root for the whole tree; the plugins subtree adds only its own flags.
	pluginflags.AddKubeFlags(cmd.PersistentFlags())
	rppflags.AddFlags(cmd.PersistentFlags())

	// Wrap before mounting plugins: the plugins subtree classifies its
	// failures with its own errdetect.
	wrapProxyDiagnostics(cmd)

	cmd.AddCommand(pluginscmd.NewCommand(logger.Named("plugins"), builtinCommands))

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
