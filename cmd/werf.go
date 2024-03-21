package cmd

import (
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

	rootCmd.AddCommand(werfRootCmd)
	rootCmd.SetContext(ctx)
}
