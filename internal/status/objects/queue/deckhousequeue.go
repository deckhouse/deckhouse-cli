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

package deckhousequeue

import (
	"context"
	"strings"

	"github.com/fatih/color"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/deckhouse/deckhouse-cli/internal/status/tools/queuefetcher"
	"github.com/deckhouse/deckhouse-cli/internal/status/tools/statusresult"
)

// Status orchestrates retrieval, processing, and formatting of the resource's current status.
func Status(ctx context.Context, kubeCl kubernetes.Interface, restConfig *rest.Config) statusresult.StatusResult {
	fetcher := queuefetcher.New(kubeCl, restConfig)
	queue, err := fetcher.GetQueue(ctx)
	if err != nil {
		return statusresult.StatusResult{
			Title:  "Deckhouse Queue",
			Level:  0,
			Output: color.RedString("Error getting Deckhouse queue: %v", err),
		}
	}
	return statusresult.StatusResult{
		Title:  "Deckhouse Queue",
		Level:  0,
		Output: formatDeckhouseQueue(queue),
	}
}

// Format returns a readable view of resource status for CLI display.
func formatDeckhouseQueue(queue queuefetcher.DeckhouseQueue) string {
	yellow := color.New(color.FgYellow).SprintFunc()
	blue := color.New(color.FgCyan).SprintFunc()

	var sb strings.Builder
	sb.WriteString(yellow("┌ Deckhouse Queue:\n"))

	if queue.Header != "" {
		sb.WriteString(yellow("├ ") + blue(queue.Header) + "\n")
	}

	if len(queue.Tasks) > 0 {
		for _, task := range queue.Tasks {
			sb.WriteString(yellow("│ ") + task.Text + "\n")
		}
	}

	for i, sum := range queue.Summary {
		prefix := "├"
		if i == len(queue.Summary)-1 {
			prefix = "└"
		}
		if strings.HasPrefix(sum, "Summary:") {
			sb.WriteString(yellow(prefix+" ") + blue(sum) + "\n")
		} else {
			sb.WriteString(yellow(prefix+" ") + sum + "\n")
		}
	}
	if queue.Header == "" && len(queue.Tasks) == 0 && len(queue.Summary) == 0 {
		sb.WriteString(yellow("│ ") + "no queue information available\n")
	}
	return sb.String()
}
