package cmd

import (
	"k8s.io/component-base/logs"
	"os"

	"github.com/spf13/cobra"
)

var Version string

var rootCmd = &cobra.Command{
	Use:     "d8",
	Short:   "d8 controls the Deckhouse Kubernetes Platform",
	Version: Version,
}

func Execute() {
	logs.InitLogs()
	defer logs.FlushLogs()
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
