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
	"fmt"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/plugins"
)

func newListCommand(manager *plugins.Manager) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List installed Deckhouse CLI plugins",
		Long: "Show installed plugins.\n\n" +
			"The registry-packages-proxy serves only allow-listed images by name and exposes no\n" +
			"catalog, so the set of available plugins cannot be listed - inspect a plugin by name\n" +
			"with 'd8 plugins versions <name>'.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			printInstalledPlugins(manager.List())

			return nil
		},
	}
}

// printInstalledPlugins renders the installed-plugins table.
func printInstalledPlugins(installed []plugins.PluginInfo) {
	fmt.Println("Installed Plugins:")
	fmt.Println("-------------------------------------------")
	fmt.Printf("%-20s %-15s %-40s\n", "NAME", "VERSION", "DESCRIPTION")
	fmt.Println("-------------------------------------------")

	if len(installed) == 0 {
		fmt.Println("No plugins installed")
	} else {
		for _, plugin := range installed {
			fmt.Printf("%-20s %-15s %-40s\n", plugin.Name, plugin.Version, plugin.Description)
		}
	}

	fmt.Println()
	fmt.Printf("Total: %d plugin(s) installed\n", len(installed))
	fmt.Println("\nThe registry serves no catalog; install a plugin by name with 'd8 plugins install <name>'.")
}
