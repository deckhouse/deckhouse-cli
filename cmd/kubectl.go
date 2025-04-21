package cmd

import (
	"os"
	"os/exec"
	"syscall"

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

	debugCmd := &cobra.Command{
		Use:   "debug",
		Short: "Debug pod with nicolaka/netshoot image by default",
		Run: func(cmd *cobra.Command, args []string) {
			cmdArgs := os.Args

			debugIndex := -1
			for i, arg := range cmdArgs {
				if arg == "debug" {
					debugIndex = i
					break
				}
			}

			if debugIndex == -1 {
				return
			}

			kubectlArgs := []string{"kubectl", "debug"}

			kubectlArgs = append(kubectlArgs, cmdArgs[debugIndex+1:]...)

			hasImageFlag := false
			for _, arg := range kubectlArgs {
				if arg == "--image" || len(arg) > 8 && arg[:8] == "--image=" {
					hasImageFlag = true
					break
				}
			}

			if !hasImageFlag {
				kubectlArgs = append(kubectlArgs, "--image=nicolaka/netshoot")
			}

			kubectlPath, err := exec.LookPath("kubectl")
			if err != nil {
				cmd.PrintErrf("Error finding kubectl: %v\n", err)
				os.Exit(1)
			}

			err = syscall.Exec(kubectlPath, kubectlArgs, os.Environ())
			if err != nil {
				cmd.PrintErrf("Error executing kubectl: %v\n", err)
				os.Exit(1)
			}
		},
	}

	for _, cmd := range kubectlCmd.Commands() {
		if cmd.Name() == "debug" {
			kubectlCmd.RemoveCommand(cmd)
			break
		}
	}

	kubectlCmd.AddCommand(debugCmd)

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
