package deckhouseedition

import (
    "context"
    "fmt"
    "strings"

    "github.com/fatih/color"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/kubernetes"
    appsv1 "k8s.io/api/apps/v1"

    "github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
)

func Status(ctx context.Context, kubeCl kubernetes.Interface) statusresult.StatusResult {
    edition, err := getDeckhouseEdition(ctx, kubeCl)
    output := color.RedString("Error getting Deckhouse edition: %v\n", err)
    if err == nil {
        output = formatDeckhouseEdition(edition)
    }
    return statusresult.StatusResult{
        Title:  "Deckhouse Edition",
        Level:  0,
        Output: output,
    }
}

type deckhouseEditionInfo struct {
    Edition string
}

func getDeckhouseEdition(ctx context.Context, kubeCl kubernetes.Interface) (deckhouseEditionInfo, error) {
    deployment, err := kubeCl.AppsV1().Deployments("d8-system").Get(ctx, "deckhouse", metav1.GetOptions{})
    if err != nil {
        return deckhouseEditionInfo{}, fmt.Errorf("failed to get deployment: %w\n", err)
    }
    return deckhouseEditionProcessing(deployment)
}

func deckhouseEditionProcessing(deployment *appsv1.Deployment) (deckhouseEditionInfo, error) {
    edition, found := deployment.Annotations["core.deckhouse.io/edition"]
    if !found {
        return deckhouseEditionInfo{}, fmt.Errorf("annotation 'core.deckhouse.io/edition' not found in deployment\n")
    }
    return deckhouseEditionInfo{Edition: edition}, nil
}

func formatDeckhouseEdition(info deckhouseEditionInfo) string {
    var sb strings.Builder
    yellow := color.New(color.FgYellow).SprintFunc()
    sb.WriteString(yellow("┌ Deckhouse Edition:\n"))
    sb.WriteString(fmt.Sprintf("%s %s\n", yellow("└"), info.Edition))
    return sb.String()
}