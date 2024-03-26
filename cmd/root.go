package cmd

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	helm_v3 "helm.sh/helm/v3/cmd/helm"

	"github.com/werf/logboek"
	"github.com/werf/nelm/pkg/resrcchangcalc"
	werfcommon "github.com/werf/werf/cmd/werf/common"
	"github.com/werf/werf/pkg/process_exterminator"
)

var Version string

var rootCmd = &cobra.Command{
	Use:           "d8",
	Short:         "d8 controls the Deckhouse Kubernetes Platform",
	Version:       Version,
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() {
	ctx := rootCmd.Context()

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
