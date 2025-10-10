package utilk8s

import (
	"fmt"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const DefaultKubeContext = ""

// SetupK8sClientSet reads kubeconfig file at kubeconfigPath and constructs a kubernetes clientset from it.
// If contextName is not empty, context under that name is used instead of default.
func SetupK8sClientSet(kubeconfigPath, contextName string) (*rest.Config, *kubernetes.Clientset, error) {
	var configOverrides *clientcmd.ConfigOverrides = nil
	if contextName != DefaultKubeContext {
		configOverrides = &clientcmd.ConfigOverrides{
			CurrentContext: contextName,
		}
	}

	loadingRules := &clientcmd.ClientConfigLoadingRules{}
	kubeconfigFiles := strings.Split(kubeconfigPath, ":")
	if len(kubeconfigFiles) > 1 {
		loadingRules.Precedence = kubeconfigFiles
	} else {
		loadingRules.ExplicitPath = kubeconfigPath
	}

	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("Reading kubeconfig file: %w", err)
	}

	kubeCl, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("Constructing Kubernetes clientset: %w", err)
	}

	return config, kubeCl, nil
}
