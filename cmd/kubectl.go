package cmd

import (
	"math/rand"
	"time"

	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	kubecmd "k8s.io/kubectl/pkg/cmd"
)

func init() {
	rand.Seed(time.Now().UnixNano())

	kubectlCmd := kubecmd.NewDefaultKubectlCommand()
	kubectlCmd.Use = "k"
	kubectlCmd.Aliases = []string{"kubectl"}

	kubectlCmd.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)
	logs.AddFlags(kubectlCmd.PersistentFlags())

	rootCmd.AddCommand(kubectlCmd)
}
