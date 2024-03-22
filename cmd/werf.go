package cmd

import (
	"github.com/samber/lo"
	"github.com/spf13/cobra"

	werfcommon "github.com/werf/werf/cmd/werf/common"
	werfroot "github.com/werf/werf/cmd/werf/root"
)

func init() {
	ctx := werfcommon.GetContextWithLogger()

	werfRootCmd, err := werfroot.ConstructRootCmd(ctx)
	if err != nil {
		werfcommon.ShutdownTelemetry(ctx, 1)
		werfcommon.TerminateWithError(err.Error(), 1)
	}

	werfRootCmd.Use = "delivery"
	removeKubectlCmd(werfRootCmd)

	rootCmd.AddCommand(werfRootCmd)
	rootCmd.SetContext(ctx)
}

func removeKubectlCmd(werfRootCmd *cobra.Command) {
	kubectlCmd, _ := lo.Must2(werfRootCmd.Find([]string{"kubectl"}))
	kubectlCmd.Hidden = true

	for _, cmd := range kubectlCmd.Commands() {
		kubectlCmd.RemoveCommand(cmd)
	}

	werfRootCmd.RemoveCommand(kubectlCmd)
}
