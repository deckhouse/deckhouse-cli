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

package plugins

import (
	"fmt"
	"os"
	"path"

	"github.com/spf13/cobra"
)

func (pc *PluginsCommand) pluginsRemoveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove [plugin-name]",
		Aliases: []string{"uninstall", "delete"},
		Short:   "Remove an installed plugin",
		Long:    "Remove a specific plugin from the Deckhouse CLI",
		Args:    cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			pluginName := args[0]
			fmt.Printf("Removing plugin: %s\n", pluginName)

			pluginDir := path.Join(pc.pluginDirectory, "plugins", pluginName)
			fmt.Printf("Removing plugin from: %s\n", pluginDir)

			err := os.RemoveAll(pluginDir)
			if err != nil {
				return fmt.Errorf("failed to remove plugin directory: %w", err)
			}

			fmt.Println("Cleaning up plugin files...")

			os.Remove(path.Join(pc.pluginDirectory, "cache", "contracts", pluginName+".json"))

			fmt.Printf("✓ Plugin '%s' successfully removed!\n", pluginName)

			return nil
		},
	}

	// Add subcommands
	cmd.AddCommand(pc.pluginsRemoveAllCommand())

	return cmd
}

func (pc *PluginsCommand) pluginsRemoveAllCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Remove all installed plugins",
		Long:  "Remove all plugins from the Deckhouse CLI at once",
		RunE: func(_ *cobra.Command, _ []string) error {
			fmt.Println("Removing all installed plugins...")

			plugins, err := os.ReadDir(path.Join(pc.pluginDirectory, "plugins"))
			if err != nil {
				return fmt.Errorf("failed to read plugins directory: %w", err)
			}

			fmt.Println("Found", len(plugins), "plugins to remove:")

			for _, plugin := range plugins {
				pluginDir := path.Join(pc.pluginDirectory, "plugins", plugin.Name())
				fmt.Printf("Removing plugin from: %s\n", pluginDir)

				err := os.RemoveAll(pluginDir)
				if err != nil {
					return fmt.Errorf("failed to remove plugin directory: %w", err)
				}

				fmt.Printf("Cleaning up plugin files for '%s'...\n", plugin.Name())

				os.Remove(path.Join(pc.pluginDirectory, "cache", "contracts", plugin.Name()+".json"))

				fmt.Printf("✓ Plugin '%s' successfully removed!\n", plugin.Name())
			}

			fmt.Println("✓ All plugins successfully removed!")

			return nil
		},
	}

	return cmd
}
