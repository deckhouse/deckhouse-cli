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
	"context"
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
	distcmd "github.com/deckhouse/deckhouse-cli/internal/dist/cmd"
	iam "github.com/deckhouse/deckhouse-cli/internal/iam/cmd"
	iamuser "github.com/deckhouse/deckhouse-cli/internal/iam/user/cmd"
	mirror "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd"
	network "github.com/deckhouse/deckhouse-cli/internal/network"
	packagecmd "github.com/deckhouse/deckhouse-cli/internal/packagecmd"
	pluginscmd "github.com/deckhouse/deckhouse-cli/internal/plugins/cmd"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
	snapshot "github.com/deckhouse/deckhouse-cli/internal/snapshot/cmd"
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

// overridable is a top-level command that an installed plugin of the same name
// replaces, falling back to the built-in when no such plugin is installed.
//
// builtin is a thunk rather than a ready command: it runs only when the built-in
// wins, so a replaced command's construction cost - the werf and virtualization
// trees are each assembled eagerly - is never paid.
//
// Matching is on the canonical name only, never an alias: a plugin must be named
// exactly like the command it takes over. The built-in's aliases carry over to the
// wrapper, so `d8 dk` and `d8 s` keep working once a plugin serves the command.
type overridable struct {
	name    string
	short   string
	aliases []string

	// satisfiesPluginDep marks a name a plugin contract may depend on while the
	// capability ships as a built-in command instead of a standalone plugin.
	satisfiesPluginDep bool

	builtin func() *cobra.Command
}

// overridableCommands lists the top-level commands a plugin may take over, in
// registration order. short duplicates the built-in's own Short because the thunk
// stays unevaluated when a plugin wins; it is only a fallback, as the wrapper
// prefers the description from the plugin's cached contract.
func (r *RootCommand) overridableCommands(ctx context.Context) []overridable {
	return []overridable{
		{
			name:               commands.DeliveryKitCommandName,
			short:              "A set of tools for building, distributing, and deploying containerized applications",
			aliases:            []string{"dk"},
			satisfiesPluginDep: true,
			builtin:            func() *cobra.Command { return commands.NewDeliveryCommand(ctx) },
		},
		{
			name:    "data",
			short:   "Data operations (export/import)",
			builtin: data.NewCommand,
		},
		{
			name:    "snapshot",
			short:   "Snapshot operations (create, delete, download, restore, upload, get)",
			builtin: snapshot.NewCommand,
		},
		{
			name:    "iam",
			short:   "Manage Deckhouse users, groups, and access grants",
			builtin: iam.NewCommand,
		},
		{
			name:    "network",
			short:   "A group of commands to operate network related tasks in The Deckhouse Ecosystem.",
			aliases: []string{"n"},
			builtin: network.NewCommand,
		},
		{
			name:    "v",
			short:   "Commands to work with Deckhouse Virtualization Platform.",
			aliases: []string{"virtualization"},
			builtin: commands.NewVirtualizationCommand,
		},
		{
			name:    "stronghold",
			short:   "Deckhouse Stronghold commands",
			builtin: commands.NewStrongholdCommand,
		},
		{
			name:               pluginscmd.PackagePluginName,
			short:              "Package build and bootstrap tool for containerized packages",
			satisfiesPluginDep: true,
			builtin:            packagecmd.NewCommand,
		},
		{
			name:    pluginscmd.SystemPluginName,
			short:   "Operate system options in DKP",
			aliases: []string{"s", "p", "platform"},
			builtin: system.NewCommand,
		},
	}
}

func (r *RootCommand) registerCommands() {
	// The termination context is root infrastructure - Execute's graceful.Terminate
	// and telemetry shutdown run on it - so it is established before any command is
	// built, and regardless of whether a plugin ends up serving delivery-kit.
	ctx := commands.NewRootContext()
	r.cmd.SetContext(ctx)

	installRoot, installed := r.installedPlugins()

	// Names still served by a built-in once the override pass is done. A name an
	// installed plugin took over drops off the list: the plugin itself now satisfies
	// that dependency, with the version checking a built-in cannot offer.
	var builtinDeps []string

	for _, o := range r.overridableCommands(ctx) {
		if _, override := installed[o.name]; override {
			r.cmd.AddCommand(pluginscmd.NewPluginCommand(
				o.name,
				o.short,
				o.aliases,
				r.logger.Named(o.name+"-command"),
				pluginscmd.WithInstallRoot(installRoot),
			))

			continue
		}

		// A nil command means the built-in already asked for termination; adding it
		// would panic in cobra before the pending exit runs.
		if cmd := o.builtin(); cmd != nil {
			r.cmd.AddCommand(cmd)
		}

		if o.satisfiesPluginDep {
			builtinDeps = append(builtinDeps, o.name)
		}
	}

	r.cmd.AddCommand(backup.NewCommand())
	r.cmd.AddCommand(mirror.NewCommand())
	r.cmd.AddCommand(cr.NewCommand())
	r.cmd.AddCommand(status.NewCommand())
	// Backward-compatibility shim for the four UserOperation commands that
	// used to live at the top level (d8 user lock|unlock|reset-password|reset-2fa)
	// before they moved under d8 iam user. Hidden from help; emits a stderr
	// deprecation banner on each invocation pointing to the new path.
	r.cmd.AddCommand(iamuser.NewDeprecatedTopLevelCommand())
	r.cmd.AddCommand(tools.NewCommand())
	r.cmd.AddCommand(commands.NewKubectlCommand())
	r.cmd.AddCommand(commands.NewLoginCommand())
	r.cmd.AddCommand(commands.NewHelpJSONCommand(r.cmd))

	r.cmd.AddCommand(distcmd.NewCommand(r.logger.Named("dist-command"), builtinDeps))
}

// installedPlugins returns the plugins root actually holding installs and the set of
// plugin names in it, empty when nothing is installed.
//
// This runs at registration time, before flag parsing, so only the DECKHOUSE_CLI_PATH
// env override (applied in NewRootCommand) can retarget it: --plugins-dir is parsed
// far too late to decide which commands get registered.
func (r *RootCommand) installedPlugins() (string, map[string]struct{}) {
	root, names, ok := layout.ResolveInstalled(flags.DeckhousePluginsDir)
	if !ok {
		return "", nil
	}

	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}

	r.logger.Debug("resolved installed plugins for command override",
		slog.String("root", root), slog.Any("plugins", names))

	return root, set
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
		// If a command returned a HelpfulError, show formatted diagnostic.
		// Commands are responsible for classifying their own errors using
		// domain-specific errdetect packages (e.g. errdetect.Diagnose for mirror).
		var helpErr *diagnostic.HelpfulError
		if errors.As(err, &helpErr) {
			fmt.Fprint(os.Stderr, helpErr.Format())
		} else {
			fmt.Fprintf(os.Stderr, "Error executing command: %v\n", err)
		}

		// Commands may attach an htpasswd-style exit code via an ExitCode()
		// method (see internal/tools/htpasswd); everything else exits 1.
		exitCode := 1

		var coder interface{ ExitCode() int }
		if errors.As(err, &coder) {
			exitCode = coder.ExitCode()
		}

		os.Exit(exitCode)
	}
}
