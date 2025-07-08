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

func Status(ctx context.Context, dynamicClient dynamic.Interface) statusresult.StatusResult {
    alerts, err := getClusterAlerts(ctx, dynamicClient)
    output := color.RedString("Error getting cluster alerts: %v\n", err)
    if err == nil {
        output = formatClusterAlerts(alerts)
    }
    return statusresult.StatusResult{
        Title:  "Cluster Alerts",
        Level:  0,
        Output: output,
    }
}

type ClusterAlert struct {
    Severity string
    Name     string
    Phase    string
}

func getClusterAlerts(ctx context.Context, dynamicCl dynamic.Interface) ([]ClusterAlert, error) {
    gvr := schema.GroupVersionResource{
        Group:    "deckhouse.io",
        Version:  "v1alpha1",
        Resource: "clusteralerts",
    }
    alertList, err := dynamicCl.Resource(gvr).List(ctx, metav1.ListOptions{})
    if err != nil {
        return nil, fmt.Errorf("failed to list cluster alerts: %w\n", err)
    }
    var alerts []ClusterAlert
    for _, item := range alertList.Items {
        alert, ok := ClusterAlertProcessing(item.Object)
        if !ok {
            continue
        }
        alerts = append(alerts, alert)
    }
    return alerts, nil
}

func ClusterAlertProcessing(item map[string]interface{}) (ClusterAlert, bool) {
    alertSpecMap, ok1 := item["alert"].(map[string]interface{})
    statusMap, ok2 := item["status"].(map[string]interface{})
    if !ok1 || !ok2 {
        return ClusterAlert{}, false
    }

    name, _     := alertSpecMap["name"].(string)
    severity, _ := alertSpecMap["severityLevel"].(string)
    phase, _    := statusMap["alertStatus"].(string)
    return ClusterAlert{
        Severity: severity,
        Name:     name,
        Phase:    phase,
    }, true
}

type AlertKey struct {
    Severity string
    Name     string
}

func formatClusterAlerts(alerts []ClusterAlert) string {
    if len(alerts) == 0 {
        return color.YellowString("✅ No Cluster Alerts found\n")
    }
    countMap := make(map[AlertKey]int)
    maxNameLen := len("ALERT")
    for _, alert := range alerts {
        key := AlertKey{Severity: alert.Severity, Name: alert.Name}
        countMap[key]++
        nameLen := len([]rune(alert.Name))
        if nameLen > maxNameLen {
            maxNameLen = nameLen
        }
    }

    nameColWidth := 40
    if maxNameLen > 39 {
        nameColWidth = 51
    }

    if maxNameLen > 50 {
        nameColWidth = 66
    }

    var sortedAlerts []AlertKey
    for alert := range countMap {
        sortedAlerts = append(sortedAlerts, alert)
    }
    sort.SliceStable(sortedAlerts, func(i, j int) bool {
        if sortedAlerts[i].Severity == sortedAlerts[j].Severity {
            return sortedAlerts[i].Name < sortedAlerts[j].Name
        }
        return sortedAlerts[i].Severity < sortedAlerts[j].Severity
    })

    var sb strings.Builder
    yellow := color.New(color.FgYellow).SprintFunc()
    sb.WriteString(yellow("┌ Cluster Alerts:\n"))
    sb.WriteString(yellow(fmt.Sprintf("├ %-10s %-*s %s\n", "SEVERITY", nameColWidth, "ALERT", "SUM")))
    for i, key := range sortedAlerts {
        prefix := "├"
        if i == len(sortedAlerts)-1 {
            prefix = "└"
        }
        severity := key.Severity
        name := key.Name
        nameRunes := []rune(name)
        if len(nameRunes) > nameColWidth {
            name = string(nameRunes[:nameColWidth-3]) + "..."
        }
        count := countMap[key]
        sb.WriteString(fmt.Sprintf("%s %-10s %-*s %d\n",
            yellow(prefix),
            severity,
            nameColWidth, name,
            count,
        ))
    }
    return sb.String()
}