package utilk8s

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// SetupK8sClientSet reads kubeconfig file at kubeconfigPath and constructs a kubernetes clientset from it.
func SetupK8sClientSet(kubeconfigPath string) (*rest.Config, *kubernetes.Clientset, error) {
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfigPath}, nil).ClientConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("Reading kubeconfig file: %w", err)
	}

	kubeCl, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, fmt.Errorf("Constructing Kubernetes clientset: %w", err)
	}

	return config, kubeCl, nil
}
