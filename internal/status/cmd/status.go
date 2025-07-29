package status

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"

	"github.com/deckhouse/deckhouse-cli/internal/status/commands/clusteralerts"
	"github.com/deckhouse/deckhouse-cli/internal/status/commands/cni_modules"
	"github.com/deckhouse/deckhouse-cli/internal/status/commands/edition"
	"github.com/deckhouse/deckhouse-cli/internal/status/commands/masters"
	"github.com/deckhouse/deckhouse-cli/internal/status/commands/pods"
	"github.com/deckhouse/deckhouse-cli/internal/status/commands/queue"
	"github.com/deckhouse/deckhouse-cli/internal/status/commands/registry"
	"github.com/deckhouse/deckhouse-cli/internal/status/commands/releases"
	"github.com/deckhouse/deckhouse-cli/internal/status/commands/settings"
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
	ctx := cmd.Context()
	restConfig, kubeCl, err := setupK8sClients(cmd)
	if err != nil {
		return fmt.Errorf("failed to setup Kubernetes client: %w\n", err)
	}
	color.Cyan("\n┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
	color.Cyan("┃      Cluster Status Report     ┃")
	color.Cyan("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n")
	results := executeAll(ctx, restConfig, kubeCl)
	for _, result := range results {
		fmt.Println(result.Output)
	}
	return nil
}

func executeAll(ctx context.Context, restConfig *rest.Config, kubeCl kubernetes.Interface) []statusresult.StatusResult {
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return []statusresult.StatusResult{
			{Title: "Error", Output: fmt.Sprintf("Error creating dynamic client: %v\n", err)},
		}
	}

	return []statusresult.StatusResult{
		masters.Status(ctx, kubeCl),
		deckhousepods.Status(ctx, kubeCl),
		deckhousereleases.Status(ctx, dynamicClient),
		deckhouseedition.Status(ctx, kubeCl),
		deckhousesettings.Status(ctx, dynamicClient),
		deckhouseregistry.Status(ctx, kubeCl),
		clusteralerts.Status(ctx, dynamicClient),
		cnimodules.Status(ctx, dynamicClient),
		deckhousequeue.Status(ctx, kubeCl, restConfig),
	}
}

func setupK8sClients(cmd *cobra.Command) (*rest.Config, *kubernetes.Clientset, error) {
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get kubeconfig: %w\n", err)
	}

	contextName, err := cmd.Flags().GetString("context")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get context: %w\n", err)
	}

	restConfig, kubeCl, err := utilk8s.SetupK8sClientSet(kubeconfigPath, contextName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to setup Kubernetes client: %w\n", err)
	}

	return restConfig, kubeCl, nil
}
