package clusteralerts

import (
    "context"
    "fmt"
    "strings"
    "sort"

    "github.com/fatih/color"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/dynamic"
    "k8s.io/apimachinery/pkg/runtime/schema"

    "github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
)

func Status(dynamicClient dynamic.Interface) statusresult.StatusResult {
    alerts, err := getClusterAlerts(dynamicClient)
    var output string
    if err != nil {
        output = color.RedString("Error getting cluster alerts: %v", err)
    } else {
        output = formatClusterAlerts(alerts)
    }
    return statusresult.StatusResult{
        Title:  "Cluster Alerts",
        Level:  0,
        Output: output,
    }
}



func getClusterAlerts(dynamicCl dynamic.Interface) ([][3]string, error) {
    gvr := schema.GroupVersionResource{
        Group:    "deckhouse.io",
        Version:  "v1alpha1",
        Resource: "clusteralerts",
    }

    alertList, err := dynamicCl.Resource(gvr).Namespace("").List(context.TODO(), metav1.ListOptions{})
    if err != nil {
        return nil, fmt.Errorf("failed to list cluster alerts: %w", err)
    }

    var alerts [][3]string
    for _, item := range alertList.Items {
        alertSpecMap, ok1 := item.Object["alert"].(map[string]interface{})
        statusMap, ok2 := item.Object["status"].(map[string]interface{})
        if !ok1 || !ok2 {
            continue
        }

        name, _ := alertSpecMap["name"].(string)
        severity, _ := alertSpecMap["severityLevel"].(string)
        phase, _ := statusMap["alertStatus"].(string)

        alerts = append(alerts, [3]string{severity, name, phase})
    }

    return alerts, nil
}

func formatClusterAlerts(alerts [][3]string) string {
    if len(alerts) == 0 {
        return color.YellowString("✅ No Cluster Alerts found\n")
    }

    countMap := make(map[[2]string]int)
    for _, alert := range alerts {
        key := [2]string{alert[0], alert[1]}
        countMap[key]++
    }

    var sortedAlerts [][2]string
    for alert := range countMap {
        sortedAlerts = append(sortedAlerts, alert)
    }

    sort.SliceStable(sortedAlerts, func(i, j int) bool {
        return sortedAlerts[i][0] < sortedAlerts[j][0]
    })

    var sb strings.Builder
    yellow := color.New(color.FgYellow).SprintFunc()
    sb.WriteString(yellow("┌ Cluster Alerts\n"))
    sb.WriteString(yellow(fmt.Sprintf("%-12s %-40s %s\n", "├ SEVERITY", "ALERT", "SUM")))
    for i, alert := range sortedAlerts {
        prefix := "├"
        if i == len(sortedAlerts)-1 {
            prefix = "└"
        }
        sb.WriteString(fmt.Sprintf("%s %-10s %-40s %d\n",
            yellow(prefix),
             alert[0],
             alert[1],
             countMap[alert]))
    }
    return sb.String()
}