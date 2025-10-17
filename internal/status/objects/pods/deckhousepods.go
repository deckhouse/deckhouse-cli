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

package deckhousepods

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/deckhouse/deckhouse-cli/internal/status/tools/statusresult"
	"github.com/deckhouse/deckhouse-cli/internal/status/tools/timeutil"
)

// Status orchestrates retrieval, processing, and formatting of the resource's current status.
func Status(ctx context.Context, kubeCl kubernetes.Interface) statusresult.StatusResult {
	pods, err := getDeckhousePods(ctx, kubeCl)
	output := color.RedString("Error getting Deckhouse pods: %v\n", err)
	if err == nil {
		output = formatDeckhousePods(pods)
	}
	return statusresult.StatusResult{
		Title:  "Deckhouse Pods",
		Level:  0,
		Output: output,
	}
}

// Get fetches raw resource data from the Kubernetes API.
type deckhousePod struct {
	Name     string
	Ready    string
	Status   string
	Restarts int
	Age      string
}

func getDeckhousePods(ctx context.Context, kubeCl kubernetes.Interface) ([]deckhousePod, error) {
	pods, err := kubeCl.CoreV1().Pods("d8-system").List(ctx, metav1.ListOptions{
		LabelSelector: "app=deckhouse",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list deckhouse pods: %w\n", err)
	}

	infor := make([]deckhousePod, 0, len(pods.Items))
	for _, pod := range pods.Items {
		infor = append(infor, deckhousePodProcessing(pod))
	}
	return infor, nil
}

// Processing converts raw resource data into a structured format for easier output and analysis.
func deckhousePodProcessing(pod corev1.Pod) deckhousePod {
	totalContainers := len(pod.Status.ContainerStatuses)
	if totalContainers == 0 {
		return deckhousePod{
			Name:     pod.Name,
			Ready:    "0/0",
			Status:   string(pod.Status.Phase),
			Restarts: 0,
			Age:      timeutil.AgeAgo(pod.CreationTimestamp.Time),
		}
	}

	readyCount, restarts := 0, 0
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Ready {
			readyCount++
		}
		restarts += int(cs.RestartCount)
	}

	return deckhousePod{
		Name:     pod.Name,
		Ready:    fmt.Sprintf("%d/%d", readyCount, totalContainers),
		Status:   string(pod.Status.Phase),
		Restarts: restarts,
		Age:      timeutil.AgeAgo(pod.CreationTimestamp.Time),
	}
}

// Format returns a readable view of resource status for CLI display.
func formatDeckhousePods(infor []deckhousePod) string {
	if len(infor) == 0 {
		return color.YellowString("❗ No Deckhouse pods found\n")
	}

	var sb strings.Builder
	yellow := color.New(color.FgYellow).SprintFunc()

	sb.WriteString(yellow("┌ Deckhouse Pods Status:\n"))
	sb.WriteString(yellow(fmt.Sprintf("%-31s %-8s %-12s %-10s %s\n", "├ NAME", "READY", "STATUS", "RESTARTS", "AGE")))

	for i, info := range infor {
		prefix := "├"
		if i == len(infor)-1 {
			prefix = "└"
		}
		sb.WriteString(fmt.Sprintf("%s%-29s %-8s %-12s %-10d %s\n",
			yellow(prefix+" "),
			info.Name,
			info.Ready,
			info.Status,
			info.Restarts,
			info.Age,
		))
	}
	return sb.String()
}
