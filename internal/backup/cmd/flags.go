package backup

import (
	"github.com/spf13/pflag"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func addPersistentFlags(flagSet *pflag.FlagSet) {
	defaultKubeconfigPath := utilk8s.DefaultKubeconfigPath()

	flagSet.StringP(
		"kubeconfig", "k",
		defaultKubeconfigPath,
		"KubeConfig of the cluster. (default is $KUBECONFIG when it is set, $HOME/.kube/config otherwise)",
	)

	flagSet.String("context", "", "The name of the kubeconfig context to use")
}
