package deckhousequeue

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/fatih/color"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
)

// Status orchestrates retrieval, processing, and formatting of the resource's current status.
func Status(ctx context.Context, kubeCl kubernetes.Interface, restConfig *rest.Config) statusresult.StatusResult {
	queueOutput, err := getDeckhouseQueue(ctx, kubeCl, restConfig)
	output := color.RedString("Error getting Deckhouse queue: %v\n", err)
	if err == nil {
		output = formatDeckhouseQueue(queueOutput)
	}
	return statusresult.StatusResult{
		Title:  "Deckhouse Queue",
		Level:  0,
		Output: output,
	}
}

// Get fetches raw resource data from the Kubernetes API.
type DeckhouseQueue struct {
	Header  string
	Tasks   []DeckhouseQueueTask
	Summary []string
}

type DeckhouseQueueTask struct {
	Index int
	Text  string
}

func getDeckhouseQueue(ctx context.Context, kubeCl kubernetes.Interface, restConfig *rest.Config) (DeckhouseQueue, error) {
	labelSelector := "app=deckhouse,leader=true"
	pods, err := kubeCl.CoreV1().Pods("d8-system").List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return DeckhouseQueue{}, fmt.Errorf("failed to list pods: %w\n", err)
	}

	if len(pods.Items) == 0 {
		return DeckhouseQueue{}, fmt.Errorf("no pods found for Deckhouse leader with label %s\n", labelSelector)
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
		return DeckhouseQueue{}, fmt.Errorf("failed to initialize SPDY executor: %w\n", err)
	}

	var stdout, stderr strings.Builder
	err = exec.Stream(remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return DeckhouseQueue{}, fmt.Errorf("failed to execute command: %w\n", err)
	}

	if stderr.Len() > 0 {
		return DeckhouseQueue{}, fmt.Errorf("stderr: %s\n", stderr.String())
	}

	return deckhouseQueueProcessing(stdout.String()), nil
}

// Processing converts raw resource data into a structured format for easier output and analysis.
func deckhouseQueueProcessing(raw string) DeckhouseQueue {
	lines := strings.Split(strings.TrimRight(raw, "\n"), "\n")

	header := ""
	tasks := []DeckhouseQueueTask{}
	summary := []string{}
	inSummary := false

	taskRegexp := regexp.MustCompile(`^\d+\.\s+(.+)`)

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if i == 0 && strings.HasPrefix(line, "Queue ") {
			header = line
			continue
		}

		if strings.HasPrefix(line, "Summary:") {
			inSummary = true
			summary = append(summary, line)
			continue
		}

		if inSummary {
			summary = append(summary, line)
			continue
		}

		m := taskRegexp.FindStringSubmatch(line)
		if m != nil {
			idx := strings.Index(line, ".")
			index := i
			if idx > 0 {
				n, _ := strconv.Atoi(strings.TrimSpace(line[:idx]))
				index = n
			}
			tasks = append(tasks, DeckhouseQueueTask{
				Index: index,
				Text:  strings.TrimSpace(line),
			})
			continue
		}

		if line != "" && header == "" {
			summary = append(summary, line)
		}
	}

	return DeckhouseQueue{
		Header:  header,
		Tasks:   tasks,
		Summary: summary,
	}
}

// Format returns a readable view of resource status for CLI display.
func formatDeckhouseQueue(queue DeckhouseQueue) string {
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
			continue
		}

		sb.WriteString(yellow(prefix+" ") + sum + "\n")
	}

	return sb.String()
}
