package cmd

import (
	"fmt"
	"strings"

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
		Use:     originalDebugCmd.Use,
		Short:   originalDebugCmd.Short,
		Long:    originalDebugCmd.Long,
		Example: originalDebugCmd.Example,
		RunE: func(cmd *cobra.Command, args []string) error {
			hasImage := false
			hasCopyTo := false
			for _, arg := range args {
				if strings.HasPrefix(arg, "--image=") || arg == "--image" {
					hasImage = true
					break
				}
				if strings.HasPrefix(arg, "--copy-to=") || arg == "--copy-to" {
					hasCopyTo = true
					break
				}
			}

			if !hasImage && !hasCopyTo {
				args = append(args, fmt.Sprintf("--image=%s", defaultImage))
				fmt.Fprintf(cmd.OutOrStdout(), "Using default image: %s\n", defaultImage)
			}

			originalDebugCmd.SetArgs(args)
			return originalDebugCmd.Execute()
		},
	}

	debugCmd.Flags().AddFlagSet(originalDebugCmd.Flags())

	if imageFlag := debugCmd.Flags().Lookup("image"); imageFlag != nil {
		imageFlag.Usage = "Container image to use for debug container. If not specified, nicolaka/netshoot will be used."
		imageFlag.DefValue = defaultImage
	}

	rootCmd.AddCommand(debugCmd)
}
