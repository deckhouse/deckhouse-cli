package cmd

import (
	"os"
	"strings"

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

	// Находим команду debug для модификации
	var debugCmd *cobra.Command
	for _, cmd := range kubectlCmd.Commands() {
		if cmd.Name() == "debug" {
			debugCmd = cmd
			break
		}
	}

	if debugCmd != nil {
		originalRun := debugCmd.RunE
		debugCmd.RunE = func(cmd *cobra.Command, args []string) error {
			imageSpecified := false
			for _, arg := range os.Args {
				if strings.HasPrefix(arg, "--image=") || arg == "--image" {
					imageSpecified = true
					break
				}
			}

			if !imageSpecified {
				if imageFlag := cmd.Flags().Lookup("image"); imageFlag != nil {
					cmd.Flags().Set("image", "nicolaka/netshoot")
				}
			}

			return originalRun(cmd, args)
		}
	}

	// Based on https://github.com/kubernetes/kubernetes/blob/v1.29.3/staging/src/k8s.io/component-base/cli/run.go#L88

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

	rootCmd.AddCommand(kubectlCmd)
}
