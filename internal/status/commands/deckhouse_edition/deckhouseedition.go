package deckhouseedition

import (
    "context"
    "fmt"
    "strings"

    "github.com/fatih/color"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/kubernetes"

    "github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
)

func Status(kubeCl kubernetes.Interface) statusresult.StatusResult {
    edition, err := getDeckhouseEdition(kubeCl)
    var output string
    if err != nil {
        output = color.RedString("Error getting Deckhouse edition: %v", err)
    } else {
        output = formatDeckhouseEdition(edition)
    }
    return statusresult.StatusResult{
        Title:  "Deckhouse Edition",
        Level:  0,
        Output: output,
    }
}



func getDeckhouseEdition(kubeCl kubernetes.Interface) (string, error) {
    deployment, err := kubeCl.AppsV1().Deployments("d8-system").Get(context.TODO(), "deckhouse", metav1.GetOptions{})
    if err != nil {
        return "", fmt.Errorf("failed to get deployment: %w", err)
    }

    edition, found := deployment.Annotations["core.deckhouse.io/edition"]
    if !found {
        return "", fmt.Errorf("annotation 'core.deckhouse.io/edition' not found in deployment")
    }

    return edition, nil
}

func formatDeckhouseEdition(edition string) string {
    var sb strings.Builder
    yellow := color.New(color.FgYellow).SprintFunc()

    sb.WriteString(yellow("┌ Deckhouse Edition\n"))
    sb.WriteString(fmt.Sprintf("%s %s\n", yellow("└"), edition))

    return sb.String()
}