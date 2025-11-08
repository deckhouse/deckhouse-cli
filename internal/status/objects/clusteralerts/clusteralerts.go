/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package clusteralerts

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/color"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"github.com/deckhouse/deckhouse-cli/internal/status/tools/statusresult"
)

// Status orchestrates retrieval, processing, and formatting of the resource's current status.
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

// Get fetches raw resource data from the Kubernetes API.
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
		return nil, fmt.Errorf("failed to list cluster alerts: %w", err)
	}
	alerts := make([]ClusterAlert, 0, len(alertList.Items))
	for _, item := range alertList.Items {
		alert, ok := ClusterAlertProcessing(item.Object)
		if !ok {
			continue
		}
		alerts = append(alerts, alert)
	}
	return alerts, nil
}

// Processing converts raw resource data into a structured format for easier output and analysis.
func ClusterAlertProcessing(item map[string]interface{}) (ClusterAlert, bool) {
	alertSpecMap, ok1 := item["alert"].(map[string]interface{})
	statusMap, ok2 := item["status"].(map[string]interface{})
	if !ok1 || !ok2 {
		return ClusterAlert{}, false
	}

	name, ok3 := alertSpecMap["name"].(string)
	phase, ok5 := statusMap["alertStatus"].(string)
	severity, ok4 := alertSpecMap["severityLevel"].(string)
	if !ok4 {
		severity = "N/A"
	}

	if !ok3 || !ok5 {
		return ClusterAlert{}, false
	}

	return ClusterAlert{
		Severity: severity,
		Name:     name,
		Phase:    phase,
	}, true
}

// Format returns a readable view of resource status for CLI display.
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

	nameColWidth := getNameColWidth(maxNameLen)

	sortedKeys := make([]AlertKey, 0, len(countMap))
	for key := range countMap {
		sortedKeys = append(sortedKeys, key)
	}
	sort.SliceStable(sortedKeys, func(i, j int) bool {
		if sortedKeys[i].Severity == sortedKeys[j].Severity {
			return sortedKeys[i].Name < sortedKeys[j].Name
		}
		return sortedKeys[i].Severity < sortedKeys[j].Severity
	})

	var sb strings.Builder
	yellow := color.New(color.FgYellow).SprintFunc()
	sb.WriteString(yellow("┌ Cluster Alerts:\n"))
	sb.WriteString(yellow(fmt.Sprintf("├ %-10s %-*s %s\n", "SEVERITY", nameColWidth, "ALERT", "SUM")))

	for i, key := range sortedKeys {
		prefix := "├"
		if i == len(sortedKeys)-1 {
			prefix = "└"
		}
		truncName := truncateName(key.Name, nameColWidth)
		count := countMap[key]

		sb.WriteString(fmt.Sprintf("%s %-10s %-*s %d\n",
			yellow(prefix),
			key.Severity,
			nameColWidth, truncName,
			count,
		))
	}

	return sb.String()
}

// getNameColWidth determines the width of the ALERT column based on the maximum name length.
func getNameColWidth(maxNameLen int) int {
	switch {
	case maxNameLen > 50:
		return 66
	case maxNameLen > 39:
		return 51
	default:
		return 40
	}
}

// truncateName truncates a string to a given length.
func truncateName(name string, width int) string {
	nameRunes := []rune(name)
	if len(nameRunes) <= width {
		return name
	}
	if width < 3 {
		return "..."
	}
	return string(nameRunes[:width-3]) + "..."
}
