package deckhousereleases

import (
    "context"
    "fmt"
    "strings"

    "github.com/fatih/color"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/dynamic"
    "k8s.io/apimachinery/pkg/runtime/schema"
    "time"

    "github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
)

// Status orchestrates retrieval, processing, and formatting of the resource's current status.
func Status(ctx context.Context, dynamicClient dynamic.Interface) statusresult.StatusResult {
    releases, err := getDeckhouseReleases(ctx, dynamicClient)
    output := color.RedString("Error getting Deckhouse releases: %v\n", err)
    if err == nil {
        output = formatDeckhouseReleases(releases)
    }
    return statusresult.StatusResult{
        Title:  "Deckhouse Releases",
        Level:  0,
        Output: output,
    }
}

// Get fetches raw resource data from the Kubernetes API.
type DeckhouseRelease struct {
    Name           string
    Phase          string
    TransitionTime string
    Message        string
}

func getDeckhouseReleases(ctx context.Context, dynamicCl dynamic.Interface) ([]DeckhouseRelease, error) {
    gvr := schema.GroupVersionResource{
        Group:    "deckhouse.io",
        Version:  "v1alpha1",
        Resource: "deckhousereleases",
    }

    releaseList, err := dynamicCl.Resource(gvr).List(ctx, metav1.ListOptions{})
    if err != nil {
        return nil, fmt.Errorf("failed to list deckhouse releases: %w\n", err)
    }

    var releases []DeckhouseRelease
    for _, item := range releaseList.Items {
        release, ok := deckhouseReleaseProcessing(item.Object, item.GetName())
        if !ok {
            continue
        }
        releases = append(releases, release)
    }

    return releases, nil
}

// Processing converts raw resource data into a structured format for easier output and analysis.
func deckhouseReleaseProcessing(item map[string]interface{}, name string) (DeckhouseRelease, bool) {
    statusMap, ok := item["status"].(map[string]interface{})
    if !ok {
        return DeckhouseRelease{}, false
    }
    phase, _          := statusMap["phase"].(string)
    transitionTime, _ := statusMap["transitionTime"].(string)
    message, _        := statusMap["message"].(string)

    return DeckhouseRelease{
        Name:           name,
        Phase:          phase,
        TransitionTime: transitionTime,
        Message:        message,
    }, true
}

// Format returns a readable view of resource status for CLI display.
func formatDeckhouseReleases(releases []DeckhouseRelease) string {
    if len(releases) == 0 {
        return color.YellowString("❗ No Deckhouse releases found\n")
    }
    var sb strings.Builder
    yellow := color.New(color.FgYellow).SprintFunc()
    sb.WriteString(yellow("┌ Deckhouse Releases:\n"))
    sb.WriteString(yellow(fmt.Sprintf("%-15s %-15s %-21s %s\n", "├ NAME", "PHASE", "TRANSITIONTIME", "MESSAGE")))

    for i, release := range releases {
        prefix := "├"
        if i == len(releases)-1 {
            prefix = "└"
        }
        timeAgo := ""
        transitionTimeValue := release.TransitionTime
        if transitionTimeValue != "" {
            if transitionTime, err := time.Parse(time.RFC3339, transitionTimeValue); err == nil {
                duration := time.Since(transitionTime)
                days := int(duration.Hours()) / 24
                hours := int(duration.Hours()) % 24
                if days > 0 {
                    timeAgo = fmt.Sprintf("%dd", days)
                }
                if hours > 0 {
                    if timeAgo != "" {
                        timeAgo += " "
                    }
                    timeAgo += fmt.Sprintf("%dh", hours)
                }
            } else {
                timeAgo = "Parse Error"
            }
        }

        sb.WriteString(fmt.Sprintf("%s %-13s %-15s %-21s %s\n",
            yellow(prefix),
            release.Name,
            release.Phase,
            timeAgo,
            release.Message,
        ))
    }
    return sb.String()
}