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

package main

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"path"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	helm_v3 "github.com/werf/3p-helm/cmd/helm"
	"github.com/werf/common-go/pkg/graceful"
	"github.com/werf/logboek"
	"github.com/werf/nelm/pkg/action"
	"github.com/werf/werf/v2/cmd/werf/common"
	"github.com/werf/werf/v2/pkg/process_exterminator"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/cmd/commands"
	"github.com/deckhouse/deckhouse-cli/cmd/plugins"
	"github.com/deckhouse/deckhouse-cli/cmd/plugins/flags"
	backup "github.com/deckhouse/deckhouse-cli/internal/backup/cmd"
	data "github.com/deckhouse/deckhouse-cli/internal/data/cmd"
	mirror "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd"
	status "github.com/deckhouse/deckhouse-cli/internal/status/cmd"
	system "github.com/deckhouse/deckhouse-cli/internal/system/cmd"
	"github.com/deckhouse/deckhouse-cli/internal/tools"
	"github.com/deckhouse/deckhouse-cli/internal/version"
)

type RootCommand struct {
	cmd    *cobra.Command
	logger *dkplog.Logger
}

func NewRootCommand() *RootCommand {
	logger := dkplog.NewLogger(
		dkplog.WithLevel(
			slog.Level(
				dkplog.LogLevelFromStr(
					os.Getenv("LOG_LEVEL"),
				),
			),
		),
	)

	rootCmd := &RootCommand{
		logger: logger.Named("d8"),
	}

	rootCmd.cmd = &cobra.Command{
		Use:           "d8",
		Short:         "d8 controls the Deckhouse Kubernetes Platform",
		Version:       version.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			_ = cmd.Help()
		},
	}

	envCliPath := os.Getenv("DECKHOUSE_CLI_PATH")
	if envCliPath != "" {
		flags.DeckhousePluginsDir = envCliPath
	}

	rootCmd.registerCommands()
	rootCmd.cmd.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)

	return rootCmd
}

func (r *RootCommand) registerCommands() {
	deliveryCMD, ctx := commands.NewDeliveryCommand()
	r.cmd.AddCommand(deliveryCMD)
	r.cmd.SetContext(ctx)

	r.cmd.AddCommand(backup.NewCommand())
	r.cmd.AddCommand(data.NewCommand())
	r.cmd.AddCommand(mirror.NewCommand())
	r.cmd.AddCommand(status.NewCommand())
	r.cmd.AddCommand(tools.NewCommand())
	r.cmd.AddCommand(system.NewCommand())
	r.cmd.AddCommand(commands.NewVirtualizationCommand())
	r.cmd.AddCommand(commands.NewKubectlCommand())
	r.cmd.AddCommand(commands.NewLoginCommand())
	r.cmd.AddCommand(commands.NewStrongholdCommand())
	r.cmd.AddCommand(commands.NewHelpJSONCommand(r.cmd))

	r.cmd.AddCommand(plugins.NewPluginsCommand(r.logger.Named("plugins-command")))

	err := os.MkdirAll(flags.DeckhousePluginsDir+"/plugins", 0755)
	if err != nil {
		r.logger.Debug("Failed to create plugins directory", slog.String("error", err.Error()))
	}

	path, err := os.ReadDir(flags.DeckhousePluginsDir + "/plugins")
	if err != nil {
		r.logger.Debug("Failed to read plugins directory", slog.String("error", err.Error()))
	}

	for _, plugin := range path {
		r.cmd.AddCommand(r.addCustomCommands(plugin.Name()))
	}
}

func (r *RootCommand) addCustomCommands(pluginName string) *cobra.Command {
	pluginPath := path.Join(flags.DeckhousePluginsDir, "plugins", pluginName)
	pluginBinaryPath := path.Join(pluginPath, "current")
	cmd := &cobra.Command{
		Use:                pluginName,
		Short:              pluginName,
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			command := exec.CommandContext(cmd.Context(), pluginBinaryPath, args...)
			command.Stdout = os.Stdout
			command.Stderr = os.Stderr

			err := command.Run()
			if err != nil {
				r.logger.Warn("Failed to run plugin", slog.String("error", err.Error()))
			}
		},
	}
	return cmd
}

func (r *RootCommand) Execute() error {
	ctx := r.cmd.Context()

	if shouldTerminate, err := common.ContainerBackendProcessStartupHook(); err != nil {
		graceful.Terminate(ctx, err, 1)
		return err
	} else if shouldTerminate {
		return nil
	}

	log.SetOutput(logboek.OutStream())
	logrus.StandardLogger().SetOutput(logboek.OutStream())

	if err := process_exterminator.Init(); err != nil {
		graceful.Terminate(ctx, fmt.Errorf("process exterminator initialization failed: %w", err), 1)
		return err
	}

	// Do early exit if termination is started
	if graceful.IsTerminating(ctx) {
		return nil
	}

	if err := r.cmd.Execute(); err != nil {
		switch {
		case helm_v3.IsPluginError(err):
			common.ShutdownTelemetry(ctx, helm_v3.PluginErrorCode(err))
			graceful.Terminate(ctx, err, helm_v3.PluginErrorCode(err))
			return err
		case errors.Is(err, action.ErrChangesPlanned):
			common.ShutdownTelemetry(ctx, 2)
			logs.FlushLogs()
			graceful.Terminate(ctx, action.ErrChangesPlanned, 2)
			return err
		}

		common.ShutdownTelemetry(ctx, 1)
		graceful.Terminate(ctx, err, 1)
		return err
	}

	common.ShutdownTelemetry(ctx, 0)
	logs.FlushLogs()
	return nil
}

func execute() {
	rootCmd := NewRootCommand()
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error executing command: %v\n", err)
		os.Exit(1)
	}
}
