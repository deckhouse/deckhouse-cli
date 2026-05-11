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

// Package plugins implements the `d8 plugins ...` command tree
// (list, contract, install, update, remove) along with the wrappers
// for individual plugins (see plugin.go) and the service-init logic
// (see init.go).
//
// The cobra-subcommand implementations are split into domain files:
//   - list.go         -- `d8 plugins list` + display helpers
//   - contract.go     -- `d8 plugins contract <name>`
//   - install.go      -- `d8 plugins install <name>` + install pipeline
//   - update.go       -- `d8 plugins update <name>` / `update all`
//   - remove.go       -- `d8 plugins remove <name>` / `remove all`
//   - validators.go   -- requirement validation, contract cache,
//     version helpers (used by list/install/contract)
//
// This file only wires the root cobra command and constructs the shared
// PluginsCommand state.
package plugins

import (
	"errors"
	"log/slog"
	"os"
	"path"

	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	client "github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/cmd/flags"
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

func NewCommand(logger *dkplog.Logger) *cobra.Command {
	pc := NewPluginsCommand(logger)

	cmd := &cobra.Command{
		Use:    "plugins",
		Short:  "Manage Deckhouse CLI plugins",
		Hidden: true,
		PersistentPreRun: func(_ *cobra.Command, _ []string) {
			// init plugin services for subcommands after flags are parsed
			pc.InitPluginServices()

			err := os.MkdirAll(flags.DeckhousePluginsDir+"/plugins", 0755)
			// if permission failed
			if errors.Is(err, os.ErrPermission) {
				pc.logger.Debug("use homedir instead of default d8 plugins path in '/opt/deckhouse/lib/deckhouse-cli'", slog.String("new_path", flags.DeckhousePluginsDir), dkplog.Err(err))

				homeDir, err := os.UserHomeDir()
				if err != nil {
					logger.Debug("failed to receive home dir to create plugins dir", slog.String("error", err.Error()))
					return
				}

				pc.pluginDirectory = path.Join(homeDir, ".deckhouse-cli")
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
