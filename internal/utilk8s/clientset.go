package utilk8s

import (
	"fmt"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const DefaultKubeContext = ""

// SetupK8sClientSet reads kubeconfig file at kubeconfigPath and constructs a kubernetes clientset from it.
// If contextName is not empty, context under that name is used instead of default.
func SetupK8sClientSet(kubeconfigPath, contextName string) (*rest.Config, *kubernetes.Clientset, error) {
	clientConfig := newClientConfig(kubeconfigPath, contextName)

	config, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("reading kubeconfig file: %w", err)
	}

	kubeCl, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("constructing Kubernetes clientset: %w", err)
	}

	return config, kubeCl, nil
}

// KubeconfigNamespace resolves the namespace selected by the given kubeconfig
// path and context, mirroring kubectl: it returns the context's namespace, or
// "default" when the context does not pin one. It uses the same loading rules
// as SetupK8sClientSet so flag handling stays consistent across commands.
func KubeconfigNamespace(kubeconfigPath, contextName string) (string, error) {
	namespace, _, err := newClientConfig(kubeconfigPath, contextName).Namespace()
	if err != nil {
		return "", fmt.Errorf("resolving namespace from kubeconfig: %w", err)
	}

	return namespace, nil
}

// newClientConfig builds the deferred-loading client config shared by
// SetupK8sClientSet and KubeconfigNamespace. Centralising the loading-rules
// logic keeps kubeconfig discovery identical for both rest.Config and
// namespace resolution.
func newClientConfig(kubeconfigPath, contextName string) clientcmd.ClientConfig {
	var configOverrides *clientcmd.ConfigOverrides
	if contextName != DefaultKubeContext {
		configOverrides = &clientcmd.ConfigOverrides{
			CurrentContext: contextName,
		}
	}

	// use splitlist func to use separator from OS specific
	kubeconfigFiles := filepath.SplitList(kubeconfigPath)
	chain := make([]string, 0, len(kubeconfigFiles))
	loadingRules := &clientcmd.ClientConfigLoadingRules{}

	chain = append(chain, deduplicate(kubeconfigFiles)...)

	if len(chain) > 1 {
		loadingRules.Precedence = kubeconfigFiles
		// to make understand about config file lost
		loadingRules.WarnIfAllMissing = true
	} else {
		// we use ExplicitPath to optimize under hood
		loadingRules.ExplicitPath = kubeconfigPath
	}

	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
}

// deduplicate removes any duplicated values and returns a new slice, keeping the order unchanged
func deduplicate(s []string) []string {
	encountered := map[string]bool{}
	ret := make([]string, 0)

	for i := range s {
		if encountered[s[i]] {
			continue
		}

		encountered[s[i]] = true
		ret = append(ret, s[i])
	}

	return ret
}
