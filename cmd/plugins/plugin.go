/*
Copyright 2024 Flant JSC
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
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/cmd/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

const (
	SystemPluginName  = "system"
	PackagePluginName = "package"
)

// TODO: add options pattern
func NewPluginCommand(commandName string, description string, aliases []string, logger *dkplog.Logger) *cobra.Command {
	pc := NewPluginsCommand(logger.Named("plugins-command"))

	pluginContractFilePath := path.Join(flags.DeckhousePluginsDir, "cache", "contracts", "system.json")
	pluginContract, err := service.GetPluginContractFromFile(pluginContractFilePath)
	if err != nil {
		logger.Debug("failed to get plugin contract from cache", slog.String("error", err.Error()))
	}

	if pluginContract != nil {
		description = pluginContract.Description
	}

	systemCmd := &cobra.Command{
		Use:                commandName,
		Short:              description,
		Aliases:            aliases,
		Long:               description,
		DisableFlagParsing: true,
		PreRun: func(_ *cobra.Command, _ []string) {
			// init plugin services for subcommands after flags are parsed
			pc.InitPluginServices()
		},
		Run: func(cmd *cobra.Command, args []string) {
			installed, err := checkInstalled(commandName)
			if err != nil {
				fmt.Println("Error checking installed:", err)
				return
			}
			if !installed {
				fmt.Println("Not installed, installing...")
				err = pc.InstallPlugin(cmd.Context(), commandName, "", -1)
				if err != nil {
					fmt.Println("Error installing:", err)
					return
				}
				fmt.Println("Installed successfully")
			}

			pluginPath := path.Join(flags.DeckhousePluginsDir, "plugins", commandName)
			pluginBinaryPath := path.Join(pluginPath, "current")
			absPath, err := filepath.Abs(pluginBinaryPath)
			if err != nil {
				logger.Warn("failed to compute absolute path", slog.String("error", err.Error()))
				return
			}

			logger.Info("Executing plugin", slog.Any("args", args))

			command := exec.CommandContext(cmd.Context(), absPath, args...)
			command.Stdout = os.Stdout
			command.Stderr = os.Stderr

			err = command.Run()
			if err != nil {
				logger.Warn("Failed to run plugin", slog.String("error", err.Error()))
			}
		},
	}

	return systemCmd
}

func checkInstalled(commandName string) (bool, error) {
	installedFile := path.Join(flags.DeckhousePluginsDir, "plugins", commandName, "current")
	absPath, err := filepath.Abs(installedFile)
	if err != nil {
		return false, fmt.Errorf("failed to compute absolute path: %w", err)
	}

	_, err = os.Stat(absPath)
	if err != nil && os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}
