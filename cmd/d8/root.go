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
	backup "github.com/deckhouse/deckhouse-cli/internal/backup/cmd"
	cr "github.com/deckhouse/deckhouse-cli/internal/cr/cmd"
	data "github.com/deckhouse/deckhouse-cli/internal/data/cmd"
	iam "github.com/deckhouse/deckhouse-cli/internal/iam/cmd"
	iamuser "github.com/deckhouse/deckhouse-cli/internal/iam/user/cmd"
	mirror "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd"
	network "github.com/deckhouse/deckhouse-cli/internal/network"
	packagecmd "github.com/deckhouse/deckhouse-cli/internal/packagecmd"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/autoupdate"
	pluginscmd "github.com/deckhouse/deckhouse-cli/internal/plugins/cmd"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/selfupdate"
	selfupdatecmd "github.com/deckhouse/deckhouse-cli/internal/selfupdate/cmd"
	status "github.com/deckhouse/deckhouse-cli/internal/status/cmd"
	system "github.com/deckhouse/deckhouse-cli/internal/system/cmd"
	"github.com/deckhouse/deckhouse-cli/internal/tools"
	"github.com/deckhouse/deckhouse-cli/internal/version"
	"github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
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

	envCliPath := os.Getenv(flags.EnvPluginsDir)
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
	r.cmd.AddCommand(cr.NewCommand())
	r.cmd.AddCommand(status.NewCommand())
	r.cmd.AddCommand(iam.NewCommand())
	// Backward-compatibility shim for the four UserOperation commands that
	// used to live at the top level (d8 user lock|unlock|reset-password|reset-2fa)
	// before they moved under d8 iam user. Hidden from help; emits a stderr
	// deprecation banner on each invocation pointing to the new path.
	r.cmd.AddCommand(iamuser.NewDeprecatedTopLevelCommand())
	r.cmd.AddCommand(network.NewCommand())
	r.cmd.AddCommand(tools.NewCommand())
	r.cmd.AddCommand(commands.NewVirtualizationCommand())
	r.cmd.AddCommand(commands.NewKubectlCommand())
	r.cmd.AddCommand(commands.NewLoginCommand())
	r.cmd.AddCommand(commands.NewStrongholdCommand())
	r.cmd.AddCommand(commands.NewHelpJSONCommand(r.cmd))

	if os.Getenv("DECKHOUSE_PLUGINS_ENABLED") != "true" {
		r.cmd.AddCommand(system.NewCommand())
	} else {
		r.cmd.AddCommand(pluginscmd.NewPluginCommand(pluginscmd.SystemPluginName, "Operate system options in DKP", []string{"s", "p", "platform"}, r.logger.Named("system-command")))
	}

	r.cmd.AddCommand(packagecmd.NewCommand())

	r.cmd.AddCommand(pluginscmd.NewCommand(r.logger.Named("plugins-command")))

	r.cmd.AddCommand(selfupdatecmd.NewCommand(r.logger.Named("cli-command")))
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

	executed, err := r.cmd.ExecuteC()
	if err != nil {
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

	// Background update housekeeping, after the command's own output. The whole block
	// is skipped for the commands d8 itself spawns in the background (cli, plugins) and
	// for help/completion: a spawned child must never start further background work, so
	// gating those top-level commands here is the entire recursion guard. Gating is
	// derived from the command cobra actually resolved (not from os.Args), so global
	// persistent flags or aliases added later cannot break it.
	topLevel := topLevelCommandName(executed)
	if !skipUpdateNotify(topLevel) && !isSelfUpdateCommand(topLevel) && !isPluginManagementCommand(topLevel) {
		// Synchronously refresh the cached d8 version (at most once per TTL, bounded by
		// a short timeout), then print the notice. No background process for the CLI.
		selfupdatecmd.RefreshNoticeCache(ctx, r.logger.Named("update-check"))
		selfupdate.NotifyIfUpdateAvailable(os.Stderr, version.Version)

		// Throttled background auto-update of installed plugins, via a detached, visible
		// `d8 plugins update all` that this same gate keeps from recursing.
		autoupdate.ScheduleBackgroundUpdate(r.logger.Named("plugin-update-check"), flags.DeckhousePluginsDir)
	}

	return nil
}

// topLevelCommandName returns the name of the first-level subcommand the executed
// command belongs to ("cli" for `d8 cli update`), or "" when the root itself ran
// (bare `d8`, `d8 --help`, `d8 --version`). It walks the parent chain of the
// command cobra resolved instead of inspecting os.Args, so global persistent
// flags or command aliases added later cannot break the detection.
func topLevelCommandName(executed *cobra.Command) string {
	if executed == nil {
		return ""
	}

	for executed.HasParent() && executed.Parent().HasParent() {
		executed = executed.Parent()
	}

	if !executed.HasParent() {
		return ""
	}

	return executed.Name()
}

// isSelfUpdateCommand reports whether the user is managing the CLI itself
// (`d8 cli ...`). Right after `d8 cli update` the in-process version constant is
// already stale, so the notice would nag about the version just installed, and a
// spawned background child could race the binary swap - skip the whole hook.
func isSelfUpdateCommand(topLevel string) bool {
	return topLevel == "cli"
}

// isPluginManagementCommand is true for `d8 plugins ...`.
// Background auto-update is skipped - the user is already managing plugins.
// Direct plugin calls (`d8 stronghold`) are not management.
func isPluginManagementCommand(topLevel string) bool {
	return topLevel == "plugins"
}

// skipUpdateNotify reports whether the update notice and background refresh must be
// skipped: the root itself (bare `d8`, `--help`, `--version`), shell-completion
// handlers and help queries are expected to be instant and side-effect-free (a
// forked kube-client child from tab-completion or a notice on `d8 --version` would
// be wrong).
func skipUpdateNotify(topLevel string) bool {
	switch topLevel {
	case "", cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd, "completion", "help":
		return true
	default:
		return false
	}
}

func execute() {
	rootCmd := NewRootCommand()
	if err := rootCmd.Execute(); err != nil {
		// If a command returned a HelpfulError, show formatted diagnostic.
		// Commands are responsible for classifying their own errors using
		// domain-specific errdetect packages (e.g. errdetect.Diagnose for mirror).
		var helpErr *diagnostic.HelpfulError
		if errors.As(err, &helpErr) {
			fmt.Fprint(os.Stderr, helpErr.Format())
		} else {
			fmt.Fprintf(os.Stderr, "Error executing command: %v\n", err)
		}

		os.Exit(1)
	}
}
