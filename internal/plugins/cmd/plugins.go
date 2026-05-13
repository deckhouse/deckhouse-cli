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
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	client "github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/cmd/flags"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/cmd/layout"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// PluginsCommand holds shared state for every `d8 plugins ...` subcommand
// and is also reused by the per-plugin wrapper command (see plugin.go).
type PluginsCommand struct {
	service              *service.PluginService
	pluginRegistryClient client.Client
	pluginDirectory      string

	logger *dkplog.Logger
}

func NewPluginsCommand(logger *dkplog.Logger) *PluginsCommand {
	return &PluginsCommand{
		pluginDirectory: flags.DeckhousePluginsDir,
		logger:          logger,
	}
}

// ensureInstallRoot creates <pluginDirectory>/plugins; on permission denied
// falls back to ~/.deckhouse-cli, updates pc.pluginDirectory, and retries.
func (pc *PluginsCommand) ensureInstallRoot() error {
	err := os.MkdirAll(layout.PluginsRoot(pc.pluginDirectory), 0755)
	if !errors.Is(err, os.ErrPermission) {
		return err
	}
	pc.logger.Debug("use homedir instead of default d8 plugins path in '/opt/deckhouse/lib/deckhouse-cli'",
		slog.String("was", pc.pluginDirectory), dkplog.Err(err))
	fallback, ferr := layout.HomeFallbackPath()
	if ferr != nil {
		return fmt.Errorf("home fallback: %w", ferr)
	}
	pc.pluginDirectory = fallback
	return os.MkdirAll(layout.PluginsRoot(pc.pluginDirectory), 0755)
}

// cachedDescription returns the description from the on-disk plugin contract
// cache, or "" if the cache is missing or unreadable.
func (pc *PluginsCommand) cachedDescription(pluginName string) string {
	contract, err := service.GetPluginContractFromFile(layout.ContractFile(pc.pluginDirectory, pluginName))
	if err != nil {
		pc.logger.Debug("failed to get plugin contract from cache", slog.String("error", err.Error()))
		return ""
	}
	if contract == nil {
		return ""
	}
	return contract.Description
}

func NewCommand(logger *dkplog.Logger) *cobra.Command {
	pc := NewPluginsCommand(logger)

	cmd := &cobra.Command{
		Use:    "plugins",
		Short:  "Manage Deckhouse CLI plugins",
		Hidden: true,
		PersistentPreRun: func(_ *cobra.Command, _ []string) {
			// init plugin services for subcommands after flags are parsed
			pc.InitPluginServices()
			if err := pc.ensureInstallRoot(); err != nil {
				pc.logger.Warn("failed to ensure plugin root directory", slog.String("error", err.Error()))
			}
		},
	}

	cmd.AddCommand(pc.pluginsListCommand())
	cmd.AddCommand(pc.pluginsContractCommand())
	cmd.AddCommand(pc.pluginsInstallCommand())
	cmd.AddCommand(pc.pluginsUpdateCommand())
	cmd.AddCommand(pc.pluginsRemoveCommand())

	flags.AddFlags(cmd.PersistentFlags())

	return cmd
}
