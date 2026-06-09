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

func newRemoveCommand(manager *plugins.Manager) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove <plugin-name>",
		Aliases: []string{"uninstall", "delete"},
		Short:   "Remove an installed plugin",
		Long:    "Remove an installed plugin from disk.",
		Args:    cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			pluginName := args[0]
			if err := plugins.ValidatePluginName(pluginName); err != nil {
				return err
			}

			fmt.Printf("Removing plugin: %s\n", pluginName)

			return manager.Remove(pluginName)
		},
	}

	// Add subcommands
	cmd.AddCommand(newRemoveAllCommand(manager))

	return cmd
}

func newRemoveAllCommand(manager *plugins.Manager) *cobra.Command {
	return &cobra.Command{
		Use:   "all",
		Short: "Remove all installed plugins",
		Long:  "Remove all plugins from the Deckhouse CLI at once",
		RunE: func(_ *cobra.Command, _ []string) error {
			fmt.Println("Removing all installed plugins...")

			if err := manager.RemoveAll(); err != nil {
				return err
			}

			fmt.Println("✓ All plugins successfully removed!")

			return nil
		},
	}
}
