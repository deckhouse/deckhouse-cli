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

func Status(dynamicClient dynamic.Interface) statusresult.StatusResult {
    releases, err := getDeckhouseReleases(dynamicClient)
    var output string
    if err != nil {
        output = color.RedString("Error getting Deckhouse releases: %v", err)
    } else {
        output = formatDeckhouseReleases(releases)
    }
    return statusresult.StatusResult{
        Title:  "Deckhouse Releases",
        Level:  0,
        Output: output,
    }
}



func getDeckhouseReleases(dynamicCl dynamic.Interface) ([]map[string]string, error) {
    gvr := schema.GroupVersionResource{
        Group:    "deckhouse.io",
        Version:  "v1alpha1",
        Resource: "deckhousereleases",
    }

    releaseList, err := dynamicCl.Resource(gvr).Namespace("").List(context.TODO(), metav1.ListOptions{})
    if err != nil {
        return nil, fmt.Errorf("failed to list deckhouse releases: %w", err)
    }

    var releases []map[string]string
    for _, item := range releaseList.Items {
        release := map[string]string{
            "name":           item.GetName(),
            "phase":          item.Object["status"].(map[string]interface{})["phase"].(string),
            "transitionTime": item.Object["status"].(map[string]interface{})["transitionTime"].(string),
            "message":        item.Object["status"].(map[string]interface{})["message"].(string),
        }
        releases = append(releases, release)
    }

    return releases, nil
}

func formatDeckhouseReleases(releases []map[string]string) string {
    if len(releases) == 0 {
        return color.YellowString("❗ No Deckhouse releases found")
    }
    var sb strings.Builder
    yellow := color.New(color.FgYellow).SprintFunc()
    sb.WriteString(yellow("┌ Deckhouse Releases\n"))
    sb.WriteString(yellow(fmt.Sprintf("%-15s %-15s %-16s %s\n", "├ NAME", "PHASE", "TRANSITIONTIME", "MESSAGE")))

    for i, release := range releases {
        prefix := "├"
        if i == len(releases)-1 {
            prefix = "└"
        }

        transitionTime, err := time.Parse(time.RFC3339, release["transitionTime"])
        if err != nil {
            sb.WriteString(fmt.Sprintf("%s %s \t %s \t %s \t Parse Error\n",
                yellow(prefix),
                release["name"],
                release["phase"],
                release["message"]))
            continue
        }

        duration := time.Since(transitionTime)
        days := int(duration.Hours()) / 24
        hours := int(duration.Hours()) % 24

        timeAgo := ""
        if days > 0 {
            timeAgo = fmt.Sprintf("%dd", days)
        }
        if hours > 0 {
            if timeAgo != "" {
                timeAgo += " "
            }
            timeAgo += fmt.Sprintf("%dh", hours)
        }

        sb.WriteString(fmt.Sprintf("%s %s\t%s\t%s\t%s\n",
            yellow(prefix),
            release["name"],
            release["phase"],
            timeAgo,
            release["message"]))
    }
    return sb.String()
}