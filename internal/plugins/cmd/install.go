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

package pluginscmd

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
)

func newInstallCommand(manager *plugins.Manager) *cobra.Command {
	var (
		version                 string
		useMajor                int
		resolvePluginsConflicts bool
		force                   bool
	)

	cmd := &cobra.Command{
		Use:   "install <plugin-name>",
		Short: "Install a Deckhouse CLI plugin",
		Long: "Install a plugin: the newest version compatible with this cluster by default,\n" +
			"an exact one with --version.\n\n" +
			"A version already on disk is activated by repointing the 'current' symlink -\n" +
			"no download. Plugin requirements are always checked before the switch.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			ctx := cmd.Context()

			opts := []plugins.InstallOption{
				plugins.InstallWithVersion(version),
				plugins.InstallWithMajorVersion(useMajor),
			}

			if resolvePluginsConflicts {
				opts = append(opts, plugins.InstallWithResolvePluginsConflicts())
			}

			if force {
				opts = append(opts, plugins.InstallWithForce())
			}

			return manager.InstallPlugin(ctx, pluginName, opts...)
		},
	}

	cmd.Flags().StringVar(&version, "version", "", "Exact version to install. Skips compatibility selection and may install a pre-release.")
	cmd.Flags().IntVar(&useMajor, "use-major", -1, "Pin to a specific major version. By default an update stays within the installed plugin's major; pass this to cross majors.")
	cmd.Flags().BoolVar(&resolvePluginsConflicts, "resolve-plugins-conflicts", false, "Automatically install missing plugins this one requires.")
	cmd.Flags().BoolVar(&force, "force", false, "Reinstall even if the selected version is already installed (re-pull + re-verify).")

	return cmd
}
