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

func deckhousePodProcessing(pod corev1.Pod) deckhousePod {
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

    return deckhousePod{
        Name:     pod.Name,
        Ready:    ready,
        Status:   status,
        Restarts: restarts,
        Age:      age,
    }
}

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