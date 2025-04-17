package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	kubecmd "k8s.io/kubectl/pkg/cmd"
)

func init() {
	kubectlCmd := kubecmd.NewDefaultKubectlCommand()
	kubectlCmd.Use = "k"
	kubectlCmd.Aliases = []string{"kubectl"}
	kubectlCmd = ReplaceCommandName("kubectl", "d8 k", kubectlCmd)

	kubectlCmd.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)
	kubectlCmd.SilenceErrors = false
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

	for _, subCmd := range kubectlCmd.Commands() {
		if subCmd.Name() == "debug" {
			if imageFlag := subCmd.Flags().Lookup("image"); imageFlag != nil {
				imageFlag.Usage = "Container image to use for debug container. If not specified, the platform's recommended image will be used."
			}

			originalRunE := subCmd.RunE
			subCmd.RunE = func(cmd *cobra.Command, args []string) error {
				image, err := cmd.Flags().GetString("image")
				if err != nil {
					return fmt.Errorf("failed to get image flag: %v", err)
				}
				if image == "" {
					if err := cmd.Flags().Set("image", "busybox"); err != nil {
						return fmt.Errorf("failed to set image flag: %v", err)
					}
				}
				if originalRunE != nil {
					return originalRunE(cmd, args)
				}
				return nil
			}
		}
	}

	rootCmd.AddCommand(kubectlCmd)
}
