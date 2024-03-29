package cmd

import (
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
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
