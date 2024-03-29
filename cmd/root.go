package cmd

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/spf13/cobra"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
)

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
	rand.Seed(time.Now().UnixNano())
	defer logs.FlushLogs()

	// It is supposed to be executed against the kubectl command, but we want to use this normalization globally.
	rootCmd.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
