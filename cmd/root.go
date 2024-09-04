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

package cmd

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

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

func ReplaceCommandName(from, to string, c *cobra.Command) *cobra.Command {
	c.Example = strings.Replace(c.Example, from, to, -1)
	// Need some investigation about links
	// c.Long = strings.Replace(c.Long, from, to, -1)
	for _, sub := range c.Commands() {
		ReplaceCommandName(from, to, sub)
	}
	return c
}

var Version string

var rootCmd = &cobra.Command{
	Use:           "d8",
	Short:         "d8 controls the Deckhouse Kubernetes Platform",
	Version:       Version,
	SilenceUsage:  true,
	SilenceErrors: true,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func Execute() {
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
