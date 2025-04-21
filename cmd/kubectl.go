package cmd

import (
	"fmt"
	"strings"

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
	}

	wrappedDebugCmd := &cobra.Command{
		Use:     debugCmd.Use,
		Short:   debugCmd.Short,
		Long:    debugCmd.Long,
		Example: debugCmd.Example,
		Args:    debugCmd.Args,
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

			debugCmd.SetArgs(args)
			return debugCmd.Execute()
		},
	}

	wrappedDebugCmd.Flags().AddFlagSet(debugCmd.Flags())
	wrappedDebugCmd.SetHelpFunc(debugCmd.HelpFunc())
	wrappedDebugCmd.SetUsageFunc(debugCmd.UsageFunc())

	kubectlCmd.AddCommand(wrappedDebugCmd)
	rootCmd.AddCommand(kubectlCmd)
}
