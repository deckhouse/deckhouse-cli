package kubectlalias

import (
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	"k8s.io/klog"
	kubecmd "k8s.io/kubectl/pkg/cmd"
	"k8s.io/kubectl/pkg/cmd/delete"
	"k8s.io/kubectl/pkg/cmd/plugin"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
)

func defaultConfigFlags() *genericclioptions.ConfigFlags {
	return genericclioptions.NewConfigFlags(true).WithDeprecatedPasswordFlag().WithDiscoveryBurst(300).WithDiscoveryQPS(50.0)
}

// NewDefaultKubectlCommand creates the `kubectl` command with default arguments
func NewDefaultKubectlCommand(args []string) *cobra.Command {
	ioStreams := genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}
	return kubecmd.NewDefaultKubectlCommandWithArgs(kubecmd.KubectlOptions{
		PluginHandler: kubecmd.NewDefaultPluginHandler(plugin.ValidPluginFilenamePrefixes),
		Arguments:     args,
		ConfigFlags:   defaultConfigFlags().WithWarningPrinter(ioStreams),
		IOStreams:     ioStreams,
	})
}

const kubectlCmdHeaders = "KUBECTL_COMMAND_HEADERS"

func addCmdHeaderHooks(cmds *cobra.Command, kubeConfigFlags *genericclioptions.ConfigFlags) {
	// If the feature gate env var is set to "false", then do no add kubectl command headers.
	if value, exists := os.LookupEnv(kubectlCmdHeaders); exists {
		if value == "false" || value == "0" {
			klog.V(5).Infoln("kubectl command headers turned off")
			return
		}
	}
	klog.V(5).Infoln("kubectl command headers turned on")
	crt := &genericclioptions.CommandHeaderRoundTripper{}
	existingPreRunE := cmds.PersistentPreRunE
	// Add command parsing to the existing persistent pre-run function.
	cmds.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		crt.ParseCommandHeaders(cmd, args)
		return existingPreRunE(cmd, args)
	}
	wrapConfigFn := kubeConfigFlags.WrapConfigFn
	// Wraps CommandHeaderRoundTripper around standard RoundTripper.
	kubeConfigFlags.WrapConfigFn = func(c *rest.Config) *rest.Config {
		if wrapConfigFn != nil {
			c = wrapConfigFn(c)
		}
		c.Wrap(func(rt http.RoundTripper) http.RoundTripper {
			// Must be separate RoundTripper; not "crt" closure.
			// Fixes: https://github.com/kubernetes/kubectl/issues/1098
			return &genericclioptions.CommandHeaderRoundTripper{
				Delegate: rt,
				Headers:  crt.Headers,
			}
		})
		return c
	}
}

func NewCommand(cmds *cobra.Command, cmdName string) *cobra.Command {
	ioStreams := genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}
	kubeConfigFlags := defaultConfigFlags().WithWarningPrinter(ioStreams)

	//==========================================
	fmt.Printf("kubeConfigFlags.KubeConfig %#v\n\n", *kubeConfigFlags.KubeConfig)
	fmt.Printf("kubeConfigFlags %#v\n\n", *kubeConfigFlags)

	//kubeConfigFlags = cmds.PersistentFlags()
	flags := cmds.PersistentFlags()
	fmt.Printf("flags %#v\n\n", *flags)
	fmt.Printf("flags %#v\n\n", *cmds.Flags())

	fmt.Printf("kubeConfigFlags.KubeConfig %#v\n\n", kubeConfigFlags.KubeConfig)
	//==========================================

	matchVersionKubeConfigFlags := cmdutil.NewMatchVersionFlags(kubeConfigFlags)
	matchVersionKubeConfigFlags.AddFlags(flags)

	f := cmdutil.NewFactory(matchVersionKubeConfigFlags)
	deleteCmd := delete.NewCmdDelete(f, ioStreams)

	origRun := deleteCmd.Run
	deleteCmd.Run = func(cmd *cobra.Command, args []string) {
		fmt.Println("DEBUG --> ", args)
		newArgs := append([]string{"dataexport"}, args...)
		fmt.Println("DEBUG --> ", newArgs)
		origRun(cmd, newArgs)
	}

	return deleteCmd

	newArgs := append(append(os.Args[0:2], cmdName), os.Args[2:]...)
	kubectlCmd := NewDefaultKubectlCommand(newArgs)
	kubectlCmd.Use = cmdName
	kubectlCmd.Aliases = []string{"kubectl " + cmdName}

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

	return kubectlCmd
}
