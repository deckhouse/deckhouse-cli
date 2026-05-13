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
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/spf13/cobra"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/cmd/layout"
)

const (
	SystemPluginName  = "system"
	PackagePluginName = "package"
)

// TODO: add options pattern
func NewPluginCommand(commandName, description string, aliases []string, logger *dkplog.Logger) *cobra.Command {
	pc := NewPluginsCommand(logger.Named("plugins-command"))

	if err := pc.ensureInstallRoot(); err != nil {
		logger.Warn("failed to ensure plugin root directory", slog.String("error", err.Error()))
		return nil
	}
	if cached := pc.cachedDescription(commandName); cached != "" {
		description = cached
	}

	return &cobra.Command{
		Use:                commandName,
		Short:              description,
		Aliases:            aliases,
		Long:               description,
		DisableFlagParsing: true,
		PreRun:             func(_ *cobra.Command, _ []string) { pc.InitPluginServices() },
		Run: func(cmd *cobra.Command, args []string) {
			if err := pc.runInstalledPlugin(cmd.Context(), commandName, args); err != nil {
				logger.Warn("plugin failed", slog.String("error", err.Error()))
				os.Exit(1)
			}
		},
	}
}

// runInstalledPlugin ensures the plugin is installed and execs its binary with args.
// stdin/stdout/stderr are inherited from the current process.
func (pc *PluginsCommand) runInstalledPlugin(ctx context.Context, pluginName string, args []string) error {
	installed, err := pc.checkInstalled(pluginName)
	if err != nil {
		return fmt.Errorf("check installed: %w", err)
	}
	if !installed {
		fmt.Println("Not installed, installing...")
		if err := pc.InstallPlugin(ctx, pluginName); err != nil {
			return fmt.Errorf("install: %w", err)
		}
		fmt.Println("Installed successfully")
	}

	absPath, err := filepath.Abs(layout.CurrentLinkPath(pc.pluginDirectory, pluginName))
	if err != nil {
		return fmt.Errorf("absolute path: %w", err)
	}

	pc.logger.Debug("Executing plugin", slog.String("plugin", pluginName), slog.Any("args", args))
	cmd := exec.CommandContext(ctx, absPath, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("plugin run: %w", err)
	}
	return nil
}

func (pc *PluginsCommand) checkInstalled(commandName string) (bool, error) {
	absPath, err := filepath.Abs(layout.CurrentLinkPath(pc.pluginDirectory, commandName))
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
