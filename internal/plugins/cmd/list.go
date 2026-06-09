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
	var (
		showInstalledOnly bool
		showAvailableOnly bool
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List Deckhouse CLI plugins",
		Long: "Show installed plugins.\n\n" +
			"The registry-packages-proxy serves no catalog, so available plugins cannot be\n" +
			"listed - check a plugin by name with 'd8 plugins versions <name>' instead.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Prepare all data before printing
			data := manager.List(cmd.Context(), showInstalledOnly, showAvailableOnly)

			// Print all prepared data
			printPluginsList(data, showInstalledOnly, showAvailableOnly)

			return nil
		},
	}

	cmd.Flags().BoolVar(&showInstalledOnly, "installed", false, "Show only installed plugins")
	cmd.Flags().BoolVar(&showAvailableOnly, "available", false, "Show only available plugins from registry")

	return cmd
}

// printPluginsList prints all prepared data
func printPluginsList(data *plugins.ListResult, showInstalledOnly, showAvailableOnly bool) {
	// Print installed plugins section
	if !showAvailableOnly {
		printInstalledSection(data)
	}

	// Print available plugins section
	if !showInstalledOnly {
		printAvailableSection(data)
	}
}

// printInstalledSection prints the installed plugins section
func printInstalledSection(data *plugins.ListResult) {
	fmt.Println("Installed Plugins:")
	fmt.Println("-------------------------------------------")
	fmt.Printf("%-20s %-15s %-40s\n", "NAME", "VERSION", "DESCRIPTION")
	fmt.Println("-------------------------------------------")

	if len(data.Installed) == 0 {
		fmt.Println("No plugins installed")
	} else {
		for _, plugin := range data.Installed {
			fmt.Printf("%-20s %-15s %-40s\n", plugin.Name, plugin.Version, plugin.Description)
		}
	}

	fmt.Println()
	fmt.Printf("Total: %d plugin(s) installed\n", len(data.Installed))
	fmt.Println()
}

// printAvailableSection prints the available plugins section
func printAvailableSection(data *plugins.ListResult) {
	fmt.Println("Available Plugins in Registry:")
	fmt.Println("-------------------------------------------")

	// Handle registry error
	if data.RegistryError != nil {
		fmt.Println()
		fmt.Println("⚠ Unable to connect to plugin registry")
		fmt.Println()
		fmt.Println("The registry may not be accessible or catalog listing may be disabled.")
		fmt.Println("You can still use specific plugins if you know their names:")
		fmt.Println("  - Use 'plugins contract <name>' to view plugin details")
		fmt.Println("  - Use 'plugins install <name>' to install a plugin")

		return
	}

	// Handle empty registry
	if len(data.Available) == 0 {
		fmt.Println("No plugins found in registry")
		return
	}

	// Print plugins table
	fmt.Printf("%-20s %-15s %-40s\n", "NAME", "VERSION", "DESCRIPTION")
	fmt.Println("-------------------------------------------")

	for _, plugin := range data.Available {
		fmt.Printf("%-20s %-15s %-40s\n", plugin.Name, plugin.Version, plugin.Description)
	}

	// Print summary
	fmt.Println()
	fmt.Printf("Total: %d plugin(s) available\n", len(data.Available))

	fmt.Println()
	fmt.Println("Use 'plugins contract <name>' to see detailed information about a plugin")
	fmt.Println("Use 'plugins install <name>' to install a plugin")
}
