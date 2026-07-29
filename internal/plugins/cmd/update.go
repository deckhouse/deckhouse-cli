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

func newUpdateCommand(manager *plugins.Manager) *cobra.Command {
	var useMajor int

	cmd := &cobra.Command{
		Use:   "update <plugin-name>",
		Short: "Update an installed plugin",
		Long: "Update an installed plugin to the newest version compatible with this cluster,\n" +
			"within its current major version. Plugins it depends on are installed/upgraded\n" +
			"automatically.\n\n" +
			"To cross majors use --use-major N (dependencies may then cross their major too)\n" +
			"or pick an exact version with 'd8 plugins install <name> --version X'.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			fmt.Printf("Updating plugin: %s\n", pluginName)

			return manager.InstallPlugin(cmd.Context(), pluginName, plugins.InstallWithMajorVersion(useMajor))
		},
	}

	cmd.Flags().IntVar(&useMajor, "use-major", -1, "Cross to a specific major version (dependencies may cross theirs too). By default the update stays within the installed major.")

	// Add subcommands
	cmd.AddCommand(newUpdateAllCommand(manager))

	return cmd
}

func newUpdateAllCommand(manager *plugins.Manager) *cobra.Command {
	return &cobra.Command{
		Use:   "all",
		Short: "Update all installed plugins",
		Long:  "Update all installed plugins to their newest cluster-compatible version within each plugin's current major.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Println("Updating all installed plugins...")

			if err := manager.UpdateAll(cmd.Context()); err != nil {
				return err
			}

			fmt.Println("✓ All plugins updated successfully!")

			return nil
		},
	}
}
