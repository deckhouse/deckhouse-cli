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

package system

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path"

	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/cmd/plugins"
	"github.com/deckhouse/deckhouse-cli/cmd/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

func NewPluginCommand(logger *dkplog.Logger) *cobra.Command {
	pc := plugins.NewPluginsCommand(logger.Named("plugins-command"))

	description := "Operate system options in DKP"

	pluginContractFilePath := path.Join(flags.DeckhousePluginsDir, "cache", "contracts", "system.json")
	pluginContract, err := service.GetPluginContractFromFile(pluginContractFilePath)
	if err != nil {
		logger.Debug("failed to get plugin contract from cache", slog.String("error", err.Error()))
	}

	if pluginContract != nil {
		description = pluginContract.Description
	}

	systemCmd := &cobra.Command{
		Use:     "system",
		Short:   description,
		Aliases: []string{"s", "p", "platform"},
		Long:    description,
		PreRun: func(_ *cobra.Command, _ []string) {
			// init plugin services for subcommands after flags are parsed
			pc.InitPluginServices()
		},
		Run: func(cmd *cobra.Command, args []string) {
			installed, err := checkInstalled()
			if err != nil {
				fmt.Println("Error checking installed:", err)
				return
			}
			if !installed {
				fmt.Println("Not installed, installing...")
				err = pc.InstallPlugin(cmd.Context(), "system", "", -1)
				if err != nil {
					fmt.Println("Error installing:", err)
					return
				}
				fmt.Println("Installed successfully")
			}

			pluginPath := path.Join(flags.DeckhousePluginsDir, "plugins", "system")
			pluginBinaryPath := path.Join(pluginPath, "current")
			command := exec.CommandContext(cmd.Context(), pluginBinaryPath, args...)
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

func checkInstalled() (bool, error) {
	installedFile := path.Join(flags.DeckhousePluginsDir, "plugins", "system", "current")
	_, err := os.Stat(installedFile)
	if err != nil && os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}
