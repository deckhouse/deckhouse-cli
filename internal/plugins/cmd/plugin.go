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

package pluginscmd

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins"
)

const (
	SystemPluginName = "system"
	// PackagePluginName = "package" TODO(Glitchy-Sheep): will be added later during full plugin system implementation
)

// NewPluginCommand returns the wrapper command that runs an installed plugin
// (e.g. `d8 system`), installing it first when missing.
// TODO: add options pattern
func NewPluginCommand(commandName, description string, aliases []string, logger *dkplog.Logger) *cobra.Command {
	manager := plugins.NewManager(logger.Named("plugins-command"))

	if err := manager.EnsureInstallRoot(); err != nil {
		// Warn but keep building the command: a nil return makes the caller's
		// cobra.AddCommand panic and takes down the whole CLI. RunInstalled
		// surfaces the root error at invocation time.
		logger.Warn("failed to ensure plugin root directory", slog.String("error", err.Error()))
	}

	// Drive the help text from the cached contract (description + declared flags/env).
	// The contract carries only names, so this lists what is available.
	short, long := description, description

	if contract, err := manager.InstalledPluginContract(commandName); err == nil && contract != nil {
		if contract.Description != "" {
			short, long = contract.Description, contract.Description
		}

		long = withContractHelp(long, contract)
	}

	return &cobra.Command{
		Use:     commandName,
		Short:   short,
		Aliases: aliases,
		Long:    long,
		// Flags are forwarded verbatim, so the wrapper parses no d8-level flags.
		// The registry-packages-proxy is configured by env only (KUBECONFIG, D8_RPP_*).
		// The source initializes lazily in RunInstalled: an already-installed
		// plugin needs no cluster access.
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			if err := manager.RunInstalled(cmd.Context(), commandName, args); err != nil {
				// Plain CLI error to stderr (not a structured log line) so the gate's
				// reason and escape hint read like a normal command failure.
				fmt.Fprintln(os.Stderr, "Error:", err)
				os.Exit(1)
			}
		},
	}
}

// withContractHelp augments a plugin's long help with the flags it declares and the
// environment d8 provides, taken from its contract. The contract carries only names
// (no per-flag text or command tree), so this lists what is available.
func withContractHelp(long string, contract *internal.Plugin) string {
	var b strings.Builder

	b.WriteString(long)

	if len(contract.Flags) > 0 {
		b.WriteString("\n\nFlags forwarded to the plugin:")

		for _, flag := range contract.Flags {
			b.WriteString("\n  " + flag.Name)
		}
	}

	if len(contract.Env) > 0 {
		b.WriteString("\n\nEnvironment requested by the plugin:")

		for _, env := range contract.Env {
			if plugins.ProvidesEnv(env.Name) {
				b.WriteString("\n  " + env.Name + " (provided by d8)")
			} else {
				b.WriteString("\n  " + env.Name + " (not provided by d8 yet; passed through if set)")
			}
		}
	}

	return b.String()
}
