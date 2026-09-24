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
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
)

func newInstallCommand(manager *plugins.Manager) *cobra.Command {
	var (
		version  string
		useMajor int
		force    bool
		all      bool
	)

	cmd := &cobra.Command{
		Use:   "install <plugin-name> | --all",
		Short: "Install or update Deckhouse CLI plugins",
		Long: "Install a plugin: the newest version compatible with this cluster by default,\n" +
			"an exact one with --version.\n\n" +
			"Installing a plugin that is already present updates it, so this is also how a\n" +
			"plugin is brought up to date; --all does that for every installed plugin at once,\n" +
			"each within its own current major.\n\n" +
			"Plugins this one depends on are installed/upgraded automatically. With --use-major\n" +
			"dependencies may also cross their own major to satisfy a constraint.\n\n" +
			"A version already on disk is activated by repointing the 'current' symlink -\n" +
			"no download. Plugin requirements are always checked before the switch.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validateInstallArgs(cmd, args, all); err != nil {
				return err
			}

			if all {
				return runInstallAll(cmd, manager, force)
			}

			opts := []plugins.InstallOption{
				plugins.InstallWithVersion(version),
				plugins.InstallWithMajorVersion(useMajor),
			}

			if force {
				opts = append(opts, plugins.InstallWithForce())
			}

			return manager.InstallPlugin(cmd.Context(), args[0], opts...)
		},
	}

	cmd.Flags().StringVar(&version, "version", "", "Exact version to install. Skips compatibility selection and may install a pre-release.")
	cmd.Flags().IntVar(&useMajor, "use-major", -1, "Pin to a specific major version. By default an install stays within the installed plugin's major; pass this to cross majors (dependencies may cross theirs too).")
	cmd.Flags().BoolVar(&force, "force", false, "Reinstall even if the selected version is already installed (re-pull + re-verify).")
	cmd.Flags().BoolVar(&all, "all", false, "Update every installed plugin instead of naming one, each within its own current major.")

	return cmd
}

// validateInstallArgs rejects the combinations that cannot mean anything, rather
// than letting them resolve to a silent surprise: --all takes no plugin name, and
// the options that pin one plugin to one version cannot apply to a whole set.
func validateInstallArgs(cmd *cobra.Command, args []string, all bool) error {
	if !all {
		if len(args) == 0 {
			return errors.New("provide a plugin name, or --all to update every installed plugin")
		}

		return nil
	}

	if len(args) > 0 {
		return fmt.Errorf("--all updates every installed plugin and takes no plugin name (got %q)", args[0])
	}

	for _, flag := range []string{"version", "use-major"} {
		if cmd.Flags().Changed(flag) {
			return fmt.Errorf("--%s pins a single plugin and cannot be combined with --all", flag)
		}
	}

	return nil
}

// runInstallAll updates every installed plugin. A per-plugin failure is reported by
// the manager as it happens and does not stop the rest, so the error here means at
// least one failed - the successes still stand.
func runInstallAll(cmd *cobra.Command, manager *plugins.Manager, force bool) error {
	out := cmd.OutOrStdout()

	fmt.Fprintln(out, "Updating all installed plugins...")

	var opts []plugins.InstallOption
	if force {
		opts = append(opts, plugins.InstallWithForce())
	}

	if err := manager.UpdateAll(cmd.Context(), opts...); err != nil {
		return err
	}

	fmt.Fprintln(out, "✓ All plugins updated successfully!")

	return nil
}
