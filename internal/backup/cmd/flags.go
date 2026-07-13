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
		"Path to kubeconfig file. (default is $KUBECONFIG when it is set, otherwise the default kubeconfig path for the current OS user)",
	)

	flagSet.String("context", "", "The name of the kubeconfig context to use")
}
