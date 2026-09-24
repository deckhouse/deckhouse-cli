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
	"io"
	"log/slog"

	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/cmd/errdetect"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
)

const tableRule = "-------------------------------------------"

func newListCommand(manager *plugins.Manager, logger *dkplog.Logger) *cobra.Command {
	// sourceErr records why the registry could not be reached, so the installed half
	// of the listing still prints and the published half can explain itself.
	var sourceErr error

	return &cobra.Command{
		Use:   "list",
		Short: "List Deckhouse CLI plugins",
		Long: "Show the plugins installed locally and, when the transport can enumerate\n" +
			"them, the plugins published in the registry and ready to install.\n\n" +
			"The published set is read from the tags of the plugins repository, where each\n" +
			"plugin has an image tagged with its name. Only direct registry access (--source)\n" +
			"reaches that repository: the registry-packages-proxy addresses plugins by exact\n" +
			"name and does not serve the plugins path, so over the proxy this half is\n" +
			"reported as unsupported rather than attempted.\n\n" +
			"Whatever cannot be resolved is reported in place rather than dropped: a plugin\n" +
			"whose versions cannot be read is still listed by name, and an unreachable\n" +
			"registry leaves the installed list intact.",
		Args: cobra.NoArgs,
		// Replaces the parent hook: unlike its sibling commands, list has something
		// worth showing without the registry, so an unreachable cluster must not
		// suppress the installed list, which is read straight from disk.
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// The plugins directory was captured at registration time, BEFORE flag
			// parsing - re-read it here so --plugins-dir is honored (the env
			// path DECKHOUSE_CLI_PATH is applied earlier, at registration).
			manager.SetDirectory(flags.DeckhousePluginsDir)

			if err := manager.EnsureInstallRoot(); err != nil {
				logger.Warn("failed to ensure plugin root directory", slog.String("error", err.Error()))
			}

			if err := manager.InitPluginServices(cmd.Context()); err != nil {
				if diag := errdetect.Diagnose(err); diag != nil {
					err = diag
				}

				sourceErr = err
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			out := cmd.OutOrStdout()

			printInstalledPlugins(out, manager.List())

			if sourceErr != nil {
				printAvailablePlugins(out, nil, sourceErr)

				return nil
			}

			available, err := manager.AvailablePlugins(cmd.Context())
			printAvailablePlugins(out, available, err)

			return nil
		},
	}
}

// printInstalledPlugins renders the installed-plugins table.
func printInstalledPlugins(out io.Writer, installed []plugins.PluginInfo) {
	fmt.Fprintln(out, "Installed plugins:")
	fmt.Fprintln(out, tableRule)
	fmt.Fprintf(out, "%-20s %-15s %-40s\n", "NAME", "VERSION", "DESCRIPTION")
	fmt.Fprintln(out, tableRule)

	if len(installed) == 0 {
		fmt.Fprintln(out, "No plugins installed")
	} else {
		for _, plugin := range installed {
			fmt.Fprintf(out, "%-20s %-15s %-40s\n", plugin.Name, plugin.Version, plugin.Description)
		}
	}

	fmt.Fprintln(out)
	fmt.Fprintf(out, "Total: %d plugin(s) installed\n", len(installed))
}

// printAvailablePlugins renders the registry half of the listing. err is why the
// published set could not be enumerated at all; it is reported in place of the
// table, leaving the installed half above untouched.
func printAvailablePlugins(out io.Writer, available []plugins.RemotePluginInfo, err error) {
	fmt.Fprintln(out)
	fmt.Fprintln(out, "Available in the registry:")
	fmt.Fprintln(out, tableRule)

	if err != nil {
		fmt.Fprintf(out, "Could not list published plugins: %v\n", err)

		// Addressing a plugin by name works on every transport, so point at the
		// lookups that still work rather than leaving a dead end.
		if errors.Is(err, plugins.ErrCatalogUnsupported) {
			fmt.Fprintln(out, "Inspect a plugin by name with 'd8 dist plugins versions <name>',")
			fmt.Fprintln(out, "or pass --source to reach the registry directly, which can enumerate them.")
		}

		return
	}

	if len(available) == 0 {
		fmt.Fprintln(out, "No plugins found in the registry")

		return
	}

	fmt.Fprintf(out, "%-20s %-15s %-40s\n", "NAME", "LATEST", "STATUS")
	fmt.Fprintln(out, tableRule)

	for _, plugin := range available {
		fmt.Fprintf(out, "%-20s %-15s %-40s\n", plugin.Name, plugin.Version, remotePluginStatus(plugin))
	}

	fmt.Fprintln(out)
	fmt.Fprintf(out, "Total: %d plugin(s) published\n", len(available))
	fmt.Fprintln(out, "\nInstall a plugin with 'd8 dist plugins install <name>'.")
}

// remotePluginStatus is the STATUS cell: why the version is missing when it is,
// otherwise whether the plugin is already on disk.
func remotePluginStatus(plugin plugins.RemotePluginInfo) string {
	if plugin.Note != "" {
		return plugin.Note
	}

	if plugin.Installed {
		return "installed"
	}

	return ""
}
