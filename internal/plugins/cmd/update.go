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

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/cmd/layout"
)

func (pc *PluginsCommand) pluginsUpdateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update [plugin-name]",
		Short: "Update an installed plugin",
		Long:  "Update a specific plugin to its latest available version",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			fmt.Printf("Updating plugin: %s\n", pluginName)

			ctx := cmd.Context()

			return pc.InstallPlugin(ctx, pluginName)
		},
	}

	// Add subcommands
	cmd.AddCommand(pc.pluginsUpdateAllCommand())

	return cmd
}

func (pc *PluginsCommand) pluginsUpdateAllCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Update all installed plugins",
		Long:  "Update all installed plugins to their latest available versions",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			fmt.Println("Updating all installed plugins...")

			plugins, err := os.ReadDir(layout.PluginsRoot(pc.pluginDirectory))
			if err != nil {
				return fmt.Errorf("failed to read plugins directory: %w", err)
			}

			for _, plugin := range plugins {
				err := pc.InstallPlugin(ctx, plugin.Name())
				if err != nil {
					return fmt.Errorf("failed to update plugin: %w", err)
				}
			}

			fmt.Println("✓ All plugins updated successfully!")

			return nil
		},
	}

	return cmd
}
