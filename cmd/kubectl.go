package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	kubecmd "k8s.io/kubectl/pkg/cmd"
	"k8s.io/kubectl/pkg/cmd/debug"
)

func init() {
	kubectlCmd := kubecmd.NewDefaultKubectlCommand()
	kubectlCmd.Use = "k"
	kubectlCmd.Aliases = []string{"kubectl"}
	kubectlCmd = ReplaceCommandName("kubectl", "d8 k", kubectlCmd)

	kubectlCmd.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)
	kubectlCmd.SilenceErrors = true
	logs.AddFlags(kubectlCmd.PersistentFlags())

	switch {
	case kubectlCmd.PersistentPreRun != nil:
		pre := kubectlCmd.PersistentPreRun
		kubectlCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
			logs.InitLogs()
			pre(cmd, args)
		}
	case kubectlCmd.PersistentPreRunE != nil:
		pre := kubectlCmd.PersistentPreRunE
		kubectlCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
			logs.InitLogs()
			return pre(cmd, args)
		}
	default:
		kubectlCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
			logs.InitLogs()
		}
	}

	streams := genericclioptions.IOStreams{
		In:     kubectlCmd.InOrStdin(),
		Out:    kubectlCmd.OutOrStdout(),
		ErrOut: kubectlCmd.ErrOrStderr(),
	}

	configFlags := genericclioptions.NewConfigFlags(true)

	debugCmd := debug.NewCmdDebug(configFlags, streams)
	debugCmd = ReplaceCommandName("kubectl", "d8 k", debugCmd)

	defaultImage := "nicolaka/netshoot"
	if imageFlag := debugCmd.Flags().Lookup("image"); imageFlag != nil {
		imageFlag.Usage = "Container image to use for debug container. If not specified, nicolaka/netshoot will be used."
		imageFlag.DefValue = defaultImage
		if err := imageFlag.Value.Set(defaultImage); err != nil {
			fmt.Fprintf(debugCmd.ErrOrStderr(), "Failed to set default image: %v\n", err)
		}
	} else {
		fmt.Fprintf(debugCmd.ErrOrStderr(), "Image flag not found in debug command\n")
	}

	originalRunE := debugCmd.RunE
	debugCmd.RunE = func(cmd *cobra.Command, args []string) error {
		image, err := cmd.Flags().GetString("image")
		if err != nil {
			return fmt.Errorf("failed to get image flag: %v", err)
		}
		if image == "" {
			if err := cmd.Flags().Set("image", defaultImage); err != nil {
				return fmt.Errorf("failed to set recommended image: %v", err)
			}
			image = defaultImage
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Using image: %s\n", image)

		if originalRunE != nil {
			return originalRunE(cmd, args)
		}
		return fmt.Errorf("original RunE is nil")
	}

	kubectlCmd.AddCommand(debugCmd)
	rootCmd.AddCommand(kubectlCmd)
}
