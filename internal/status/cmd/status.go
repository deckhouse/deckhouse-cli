package status

import (
    "fmt"

    "github.com/fatih/color"
    "github.com/spf13/cobra"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
    "k8s.io/client-go/dynamic"
    "k8s.io/kubectl/pkg/util/templates"

    "github.com/deckhouse/deckhouse-cli/internal/utilk8s"
    "github.com/deckhouse/deckhouse-cli/internal/status/statusresult"

    "github.com/deckhouse/deckhouse-cli/internal/status/commands/masters"
    "github.com/deckhouse/deckhouse-cli/internal/status/commands/deckhouse_releases"
    "github.com/deckhouse/deckhouse-cli/internal/status/commands/deckhouse_pods"
    "github.com/deckhouse/deckhouse-cli/internal/status/commands/clusteralerts"
    "github.com/deckhouse/deckhouse-cli/internal/status/commands/deckhouse_edition"
    "github.com/deckhouse/deckhouse-cli/internal/status/commands/deckhouse_registry"
    "github.com/deckhouse/deckhouse-cli/internal/status/commands/deckhouse_settings"
    "github.com/deckhouse/deckhouse-cli/internal/status/commands/cni_modules"
    "github.com/deckhouse/deckhouse-cli/internal/status/commands/deckhouse_queue"
)

var statusLong = templates.LongDesc(`
Get status information about Deckhouse Kubernetes Platform

© Flant JSC 2025`)

func NewCommand() *cobra.Command {
    statusCmd := &cobra.Command{
	Use:           "status",
	Short:         "Get cluster status information",
	Long:          statusLong,
	SilenceErrors: true,
	SilenceUsage:  true,
	RunE:          runStatus,
    }

    addPersistentFlags(statusCmd.PersistentFlags())

    return statusCmd
}

func runStatus(cmd *cobra.Command, _ []string) error {
    restConfig, kubeCl, err := setupK8sClients(cmd)
    if err != nil {
	return fmt.Errorf("failed to setup Kubernetes client: %w", err)
    }

    color.Cyan("\n┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
    color.Cyan("┃      Cluster Status Report     ┃")
    color.Cyan("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")

    results := executeAll(restConfig, kubeCl)

    for _, result := range results {
        fmt.Println(result.Output)
    }
    return nil
}

func executeAll(restConfig *rest.Config, kubeCl kubernetes.Interface) []statusresult.StatusResult {
    dynamicClient, err := dynamic.NewForConfig(restConfig)
    if err != nil {
        return []statusresult.StatusResult{
            {Title: "Error", Output: fmt.Sprintf("Error creating dynamic client: %v", err)},
        }
    }

    return []statusresult.StatusResult{
        masters.Status(kubeCl),
        deckhousereleases.Status(dynamicClient),
        deckhousepods.Status(kubeCl),
        clusteralerts.Status(dynamicClient),
        deckhouseedition.Status(kubeCl),
        deckhouseregistry.Status(kubeCl),
        deckhousesettings.Status(dynamicClient),
        cnimodules.Status(dynamicClient),
        deckhousequeue.Status(kubeCl, restConfig),
    }
}

func setupK8sClients(cmd *cobra.Command) (*rest.Config, *kubernetes.Clientset, error) {
    kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
    if err != nil {
	return nil, nil, fmt.Errorf("failed to get kubeconfig: %w", err)
    }

    contextName, err := cmd.Flags().GetString("context")
    if err != nil {
	return nil, nil, fmt.Errorf("failed to get context: %w", err)
    }

    restConfig, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
    if err != nil {
	return nil, nil, fmt.Errorf("failed to setup Kubernetes client: %w", err)
    }

    return restConfig, kubeCl, nil
}