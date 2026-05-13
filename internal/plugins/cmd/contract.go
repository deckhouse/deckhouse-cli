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
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

func (pc *PluginsCommand) pluginsContractCommand() *cobra.Command {
	var version string
	var useMajor int

	cmd := &cobra.Command{
		Use:   "contract [plugin-name]",
		Short: "Get the contract for a specific plugin",
		Long:  "Retrieve and display the contract specification for a specific plugin from the registry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pluginName := args[0]
			ctx := cmd.Context()

			latestVersion, err := pc.fetchLatestVersion(ctx, pluginName)
			if err != nil {
				return fmt.Errorf("failed to fetch latest version: %w", err)
			}

			tag := latestVersion.Original()

			pc.logger.Debug("Fetching contract for plugin", slog.String("plugin", pluginName), slog.String("tag", tag))

			// Use service to get plugin contract
			plugin, err := pc.service.GetPluginContract(ctx, pluginName, tag)
			if err != nil {
				pc.logger.Warn("Failed to get plugin contract",
					slog.String("plugin", pluginName),
					slog.String("tag", tag),
					slog.String("error", err.Error()))
				return fmt.Errorf("failed to get plugin contract: %w", err)
			}
			contract := service.DomainToContract(plugin)

			// Display contract
			jsonBytes, err := json.Marshal(contract)
			if err != nil {
				return fmt.Errorf("failed to marshal contract to JSON: %w", err)
			}
			yamlBytes, err := yaml.JSONToYAML(jsonBytes)
			if err != nil {
				return fmt.Errorf("failed to convert JSON to YAML: %w", err)
			}
			fmt.Println(string(yamlBytes))

			return nil
		},
	}

	cmd.Flags().StringVar(&version, "version", "", "Specific version of the plugin contract to retrieve")
	cmd.Flags().IntVar(&useMajor, "use-major", 0, "Use specific major version (e.g., 1, 2)")

	return cmd
}
