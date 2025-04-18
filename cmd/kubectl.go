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

	fmt.Println("Available subcommands in kubectlCmd:")
	for _, subCmd := range kubectlCmd.Commands() {
		fmt.Printf(" - %s\n", subCmd.Name())
	}

	for _, subCmd := range kubectlCmd.Commands() {
		if subCmd.Name() == "debug" {
			fmt.Println("Found debug subcommand, modifying...")

			if imageFlag := subCmd.Flags().Lookup("image"); imageFlag != nil {
				fmt.Println("Updating image flag usage")
				imageFlag.Usage = "Container image to use for debug container. If not specified, the platform's recommended image will be used."
			} else {
				fmt.Println("Image flag not found in debug subcommand")
			}

			originalRunE := subCmd.RunE
			subCmd.RunE = func(cmd *cobra.Command, args []string) error {
				fmt.Println("Entering RunE for d8 k debug")
				fmt.Printf("Args: %v\n", args)
				image, err := cmd.Flags().GetString("image")
				if err != nil {
					fmt.Printf("Failed to get image flag: %v\n", err)
					return err
				}
				fmt.Printf("Current image value: %q\n", image)
				if image == "" {
					fmt.Println("Setting image to busybox")
					if err := cmd.Flags().Set("image", "busybox"); err != nil {
						fmt.Printf("Failed to set image flag: %v\n", err)
						return err
					}
				}
				if originalRunE != nil {
					fmt.Println("Calling original RunE")
					err := originalRunE(cmd, args)
					if err != nil {
						fmt.Printf("original RunE returned error: %v\n", err)
					}
					return err
				}
				fmt.Println("original RunE is nil")
				return nil
			}
		}
	}

	rootCmd.AddCommand(kubectlCmd)
}
