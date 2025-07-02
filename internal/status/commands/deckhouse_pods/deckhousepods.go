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

func Status(kubeCl kubernetes.Interface) statusresult.StatusResult {
    pods, err := getDeckhousePods(kubeCl)
    var output string
    if err != nil {
        output = color.RedString("Error getting Deckhouse pods: %v", err)
    } else {
        output = formatDeckhousePods(pods)
    }
    return statusresult.StatusResult{
        Title:  "Deckhouse Pods",
        Level:  0,
        Output: output,
    }
}



func getDeckhousePods(kubeCl kubernetes.Interface) ([]corev1.Pod, error) {
    pods, err := kubeCl.CoreV1().Pods("d8-system").List(context.TODO(), metav1.ListOptions{
        LabelSelector: "app=deckhouse",
    })
    if err != nil {
        return nil, fmt.Errorf("failed to list deckhouse pods: %w", err)
    }
    return pods.Items, nil
}

func formatDeckhousePods(pods []corev1.Pod) string {
    if len(pods) == 0 {
        return color.YellowString("❗ No Deckhouse pods found")
    }

    var sb strings.Builder
    yellow := color.New(color.FgYellow).SprintFunc()

    sb.WriteString(yellow("┌ Deckhouse Pods\n"))

    sb.WriteString(yellow(fmt.Sprintf("%-31s %-8s %-12s %-10s %s\n", "├ NAME", "READY", "STATUS", "RESTARTS", "AGE")))

    for i, pod := range pods {
        readyCount := 0
        for _, containerStatus := range pod.Status.ContainerStatuses {
            if containerStatus.Ready {
                readyCount++
            }
        }
        ready := fmt.Sprintf("%d/%d", readyCount, len(pod.Status.ContainerStatuses))
        status := string(pod.Status.Phase)
        restarts := 0
        for _, containerStatus := range pod.Status.ContainerStatuses {
            restarts += int(containerStatus.RestartCount)
        }

        age := "<unknown>"
        if !pod.CreationTimestamp.IsZero() {
            duration := time.Since(pod.CreationTimestamp.Time)
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
            age = timeAgo
        }

        prefix := "├"
        if i == len(pods)-1 {
            prefix = "└"
        }

        sb.WriteString(fmt.Sprintf("%s%-29s %-8s %-12s %-10d %s\n",
            yellow(prefix+" "),
            pod.Name,
            ready,
            status,
            restarts,
            age))
     }
    return sb.String()
}