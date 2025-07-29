package deckhousepods

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"time"

	"github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
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
			Age:      formatAge(pod.CreationTimestamp.Time),
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
		Age:      formatAge(pod.CreationTimestamp.Time),
	}
}

func formatAge(t time.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}

	duration := time.Since(t)
	days := int(duration.Hours()) / 24
	hours := int(duration.Hours()) % 24
	minutes := int(duration.Minutes()) % 60
	seconds := int(duration.Seconds()) % 60

	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, minutes)
	case minutes > 0:
		return fmt.Sprintf("%dm", minutes)
	default:
		return fmt.Sprintf("%ds", seconds)
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
