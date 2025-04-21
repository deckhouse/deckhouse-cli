package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/cmd/debug"
)

func init() {
	defaultImage := "nicolaka/netshoot"
	streams := genericclioptions.IOStreams{
		In:     rootCmd.InOrStdin(),
		Out:    rootCmd.OutOrStdout(),
		ErrOut: rootCmd.ErrOrStderr(),
	}

	configFlags := genericclioptions.NewConfigFlags(true)

	originalDebugCmd := debug.NewCmdDebug(configFlags, streams)
	originalDebugCmd = ReplaceCommandName("kubectl", "d8", originalDebugCmd)

	debugCmd := &cobra.Command{
		Use:                originalDebugCmd.Use,
		Short:              originalDebugCmd.Short,
		Long:               originalDebugCmd.Long,
		Example:            originalDebugCmd.Example,
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			args = append([]string{fmt.Sprintf("--image=%s", defaultImage)}, args...)

			originalDebugCmd.DisableFlagParsing = false
			originalDebugCmd.SetArgs(args)
			if err := originalDebugCmd.Execute(); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "Error: %v\n", err)
			}
		},
	}

	debugCmd.Flags().AddFlagSet(originalDebugCmd.Flags())
	configFlags.AddFlags(debugCmd.Flags())

	if imageFlag := debugCmd.Flags().Lookup("image"); imageFlag != nil {
		imageFlag.Usage = "Container image to use for debug container. If not specified, nicolaka/netshoot will be used."
		imageFlag.DefValue = defaultImage
	}

	rootCmd.AddCommand(debugCmd)
}
