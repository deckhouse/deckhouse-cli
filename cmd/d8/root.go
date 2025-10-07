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
	"math/rand"
	"os"
	"time"

	"github.com/deckhouse/deckhouse-cli/cmd/commands"
	"github.com/deckhouse/deckhouse-cli/cmd/plugins"
	backup "github.com/deckhouse/deckhouse-cli/internal/backup/cmd"
	dataexport "github.com/deckhouse/deckhouse-cli/internal/dataexport/cmd"
	mirror "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd"
	intplugins "github.com/deckhouse/deckhouse-cli/internal/plugins"
	status "github.com/deckhouse/deckhouse-cli/internal/status/cmd"
	system "github.com/deckhouse/deckhouse-cli/internal/system/cmd"
	"github.com/deckhouse/deckhouse-cli/internal/tools"
	"github.com/deckhouse/deckhouse-cli/internal/version"
	"github.com/deckhouse/deckhouse-cli/pkg/registry"
	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	helm_v3 "github.com/werf/3p-helm/cmd/helm"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"

	"github.com/werf/logboek"
	"github.com/werf/nelm/pkg/resrcchangcalc"
	werfcommon "github.com/werf/werf/v2/cmd/werf/common"
	"github.com/werf/werf/v2/pkg/process_exterminator"
)

func registerCommands(rootCmd *cobra.Command) {
	deliveryCMD, ctx := commands.NewDeliveryCommand()
	rootCmd.AddCommand(deliveryCMD)
	rootCmd.SetContext(ctx)

	rootCmd.AddCommand(backup.NewCommand())
	rootCmd.AddCommand(dataexport.NewCommand())
	rootCmd.AddCommand(mirror.NewCommand())
	rootCmd.AddCommand(status.NewCommand())
	rootCmd.AddCommand(tools.NewCommand())
	rootCmd.AddCommand(system.NewCommand())
	rootCmd.AddCommand(commands.NewVirtualizationCommand())
	rootCmd.AddCommand(commands.NewKubectlCommand())
	rootCmd.AddCommand(commands.NewLoginCommand())
	rootCmd.AddCommand(commands.NewStrongholdCommand())
	rootCmd.AddCommand(commands.NewHelpJsonCommand(rootCmd))

	// plugin service draft
	rootCmd.AddCommand(plugins.NewPluginsCommand(
		intplugins.NewPluginService(
			registry.NewClient(
				"registry.deckhouse.io",
				os.Getenv("D8_REGISTRY_USERNAME"),
				os.Getenv("D8_REGISTRY_PASSWORD"),
				dkplog.NewLogger().Named("registry-client"),
			),
			dkplog.NewLogger().Named("plugin-service"),
		),
	))

}

func execute() {
	rootCmd := &cobra.Command{
		Use:           "d8",
		Short:         "d8 controls the Deckhouse Kubernetes Platform",
		Version:       version.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}

	registerCommands(rootCmd)

	ctx := rootCmd.Context()

	rand.Seed(time.Now().UnixNano())
	defer logs.FlushLogs()

	// It is supposed to be executed against the kubectl command, but we want to use this normalization globally.
	rootCmd.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)

	if shouldTerminate, err := werfcommon.ContainerBackendProcessStartupHook(); err != nil {
		werfcommon.TerminateWithError(err.Error(), 1)
	} else if shouldTerminate {
		return
	}

	werfcommon.EnableTerminationSignalsTrap()
	log.SetOutput(logboek.OutStream())
	logrus.StandardLogger().SetOutput(logboek.OutStream())

	if err := process_exterminator.Init(); err != nil {
		werfcommon.TerminateWithError(fmt.Sprintf("process exterminator initialization failed: %s", err), 1)
	}

	if err := rootCmd.Execute(); err != nil {
		if helm_v3.IsPluginError(err) {
			werfcommon.ShutdownTelemetry(ctx, helm_v3.PluginErrorCode(err))
			werfcommon.TerminateWithError(err.Error(), helm_v3.PluginErrorCode(err))
		} else if errors.Is(err, resrcchangcalc.ErrChangesPlanned) {
			werfcommon.ShutdownTelemetry(ctx, 2)
			os.Exit(2)
		} else {
			werfcommon.ShutdownTelemetry(ctx, 1)
			werfcommon.TerminateWithError(err.Error(), 1)
		}
	}

	werfcommon.ShutdownTelemetry(ctx, 0)
}
