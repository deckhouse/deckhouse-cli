package deckhouseregistry

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
    registry, scheme, err := getDeckhouseRegistry(kubeCl)
    var output string
    if err != nil {
        output = color.RedString("Error getting Deckhouse registry: %v", err)
    } else {
        output = formatDeckhouseRegistry(registry, scheme)
    }
    return statusresult.StatusResult{
        Title:  "Deckhouse Registry",
        Level:  0,
        Output: output,
    }
}



func getDeckhouseRegistry(kubeCl kubernetes.Interface) (string, string, error) {
    secret, err := kubeCl.CoreV1().Secrets("d8-system").Get(context.TODO(), "deckhouse-registry", metav1.GetOptions{})
    if err != nil {
        return "", "", fmt.Errorf("failed to get secret: %w", err)
    }

    registryData, found := secret.Data["imagesRegistry"]
    if !found {
        return "", "", fmt.Errorf("'imagesRegistry' not found in secret")
    }

    schemeData, found := secret.Data["scheme"]
    if !found {
        return "", "", fmt.Errorf("'scheme' not found in secret")
    }

    return string(registryData), string(schemeData), nil
}

func formatDeckhouseRegistry(registry, scheme string) string {
    yellow := color.New(color.FgYellow).SprintFunc()

    if strings.TrimSpace(registry) == "" && strings.TrimSpace(scheme) == "" {
        return yellow("❗ Registry and Scheme data are empty or cannot be found")
    }

    var sb strings.Builder
    sb.WriteString(yellow("┌ Deckhouse Registry Information\n"))
    sb.WriteString(fmt.Sprintf("%s %s %s\n", yellow("├"), yellow("Registry:"), registry))
    sb.WriteString(fmt.Sprintf("%s %s %s\n", yellow("└"), yellow("Scheme:"), scheme))

    return sb.String()
}