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
	var configOverrides *clientcmd.ConfigOverrides
	if contextName != DefaultKubeContext {
		configOverrides = &clientcmd.ConfigOverrides{
			CurrentContext: contextName,
		}
	}

	chain := []string{}
	loadingRules := &clientcmd.ClientConfigLoadingRules{}

	// use splitlist func to use separator from OS specific
	kubeconfigFiles := filepath.SplitList(kubeconfigPath)
	chain = append(chain, deduplicate(kubeconfigFiles)...)

	if len(chain) > 1 {
		loadingRules.Precedence = kubeconfigFiles
		// to make understand about config file lost
		loadingRules.WarnIfAllMissing = true
	} else {
		// we use ExplicitPath to optimize under hood
		loadingRules.ExplicitPath = kubeconfigPath
	}

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("reading kubeconfig file: %w", err)
	}

	kubeCl, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("constructing Kubernetes clientset: %w", err)
	}

	return config, kubeCl, nil
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
