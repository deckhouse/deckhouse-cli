package deckhousequeue

import (
    "context"
    "fmt"
    "strings"

    "github.com/fatih/color"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
    "k8s.io/client-go/tools/remotecommand"

    "github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
)

func Status(kubeCl kubernetes.Interface, restConfig *rest.Config) statusresult.StatusResult {
    queueOutput, err := getDeckhouseQueue(kubeCl, restConfig)
    var output string
    if err != nil {
        output = color.RedString("Error getting Deckhouse queue: %v", err)
    } else {
        output = formatDeckhouseQueue(queueOutput)
    }
    return statusresult.StatusResult{
        Title:  "Deckhouse Queue",
        Level:  0,
        Output: output,
    }
}



func getDeckhouseQueue(kubeCl kubernetes.Interface, restConfig *rest.Config) (string, error) {
    labelSelector := "app=deckhouse,leader=true"
    pods, err := kubeCl.CoreV1().Pods("d8-system").List(context.TODO(), metav1.ListOptions{
        LabelSelector: labelSelector,
    })
    if err != nil {
        return "", fmt.Errorf("failed to list pods: %w", err)
    }

    if len(pods.Items) == 0 {
        return "", fmt.Errorf("no pods found for Deckhouse leader with label %s", labelSelector)
    }

    podName := pods.Items[0].Name

    req := kubeCl.CoreV1().RESTClient().Post().
        Resource("pods").
        Name(podName).
        Namespace("d8-system").
        SubResource("exec").
        Param("container", "deckhouse").
        Param("stdin", "false").
        Param("stdout", "true").
        Param("stderr", "true").
        Param("tty", "false").
        Param("command", "deckhouse-controller").
        Param("command", "queue").
        Param("command", "list")

    exec, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
    if err != nil {
        return "", fmt.Errorf("failed to initialize SPDY executor: %w", err)
    }

    var stdout, stderr strings.Builder
    err = exec.Stream(remotecommand.StreamOptions{
        Stdout: &stdout,
        Stderr: &stderr,
    })
    if err != nil {
        return "", fmt.Errorf("failed to execute command: %w", err)
    }

    if stderr.Len() > 0 {
        return "", fmt.Errorf("stderr: %s", stderr.String())
    }

    return stdout.String(), nil
}

func formatDeckhouseQueue(output string) string {
    if strings.TrimSpace(output) == "" {
        return color.YellowString("❗ No Deckhouse queue data found")
    }

    var sb strings.Builder
    yellow := color.New(color.FgYellow).SprintFunc()
    sb.WriteString(yellow("┌ Deckhouse Queue\n"))
    sb.WriteString(output)
    return sb.String()
}