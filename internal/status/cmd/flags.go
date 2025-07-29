package status

import (
	"os"

	"github.com/spf13/pflag"
)

func addPersistentFlags(flagSet *pflag.FlagSet) {
	defaultKubeconfigPath := os.ExpandEnv("$HOME/.kube/config")
	if p := os.Getenv("KUBECONFIG"); p != "" {
		defaultKubeconfigPath = p
	}

	flagSet.StringP(
		"kubeconfig", "k",
		defaultKubeconfigPath,
		"KubeConfig of the cluster. (default is $KUBECONFIG when it is set, $HOME/.kube/config otherwise)",
	)

	flagSet.String("context", "", "The name of the kubeconfig context to use")
}
