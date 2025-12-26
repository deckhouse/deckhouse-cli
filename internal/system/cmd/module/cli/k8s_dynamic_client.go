package cli

import (
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// GetDynamicClient creates a dynamic Kubernetes client from cobra command flags.
// It reads "kubeconfig" and "context" flags from the command.
// Dynamic client is required to work with Custom Resources like ModuleRelease
// and ModuleConfig, which don't have typed clients in client-go.
func GetDynamicClient(cmd *cobra.Command) (dynamic.Interface, error) {
	kubeconfigPath, _ := cmd.Flags().GetString("kubeconfig")
	contextName, _ := cmd.Flags().GetString("context")

	config, _, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return nil, fmt.Errorf("failed to setup Kubernetes client: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return dynamicClient, nil
}
