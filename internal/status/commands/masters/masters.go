package masters

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/deckhouse/deckhouse-cli/internal/status/statusresult"
)

// Status orchestrates retrieval, processing, and formatting of the resource's current status.
func Status(ctx context.Context, kubeCl kubernetes.Interface) statusresult.StatusResult {
	nodes, err := getMasterNodes(ctx, kubeCl)
	output := color.RedString("Error getting master nodes: %v\n", err)
	if err == nil {
		output = formatMasterNodes(nodes)
	}
	return statusresult.StatusResult{
		Title:  "Master Nodes",
		Level:  0,
		Output: output,
	}
}

// Get fetches raw resource data from the Kubernetes API.
type MasterNodeStatus struct {
	Name      string
	Status    string
	Version   string
	IPAddress string
}

func getMasterNodes(ctx context.Context, kubeCl kubernetes.Interface) ([]corev1.Node, error) {
	nodes, err := kubeCl.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "node-role.kubernetes.io/master",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list master nodes: %w\n", err)
	}
	return nodes.Items, nil
}

// Processing converts raw resource data into a structured format for easier output and analysis.
func masterNodeStatus(node corev1.Node) MasterNodeStatus {
	return MasterNodeStatus{
		Name:      node.Name,
		Status:    nodeReadyStatus(node),
		Version:   node.Status.NodeInfo.KubeletVersion,
		IPAddress: nodeInternalIP(node),
	}
}

func nodeReadyStatus(node corev1.Node) string {
	status := "NotReady"
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
			status = "Ready"
			break
		}
	}
	return status
}

func nodeInternalIP(node corev1.Node) string {
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address
		}
	}
	return "<none>"
}

// Format returns a readable view of resource status for CLI display.
func formatMasterNodes(nodes []corev1.Node) string {
	if len(nodes) == 0 {
		return color.YellowString("❗ No master nodes found\n")
	}

	statuses := make([]MasterNodeStatus, 0, len(nodes))
	maxNameLen := len("NAME")
	for _, node := range nodes {
		status := masterNodeStatus(node)
		nameRuneLen := len([]rune(status.Name))
		if nameRuneLen > maxNameLen {
			maxNameLen = nameRuneLen
		}
		statuses = append(statuses, status)
	}

	nameColWidth, statusColWidth, versionColWidth := getNameColWidth(maxNameLen)
	yellow := color.New(color.FgYellow).SprintFunc()

	var sb strings.Builder
	sb.WriteString(yellow("┌ Master Nodes Status:\n"))
	sb.WriteString(yellow(fmt.Sprintf(
		"├ %-*s %-*s %-*s %s\n",
		nameColWidth, "NAME",
		statusColWidth, "STATUS",
		versionColWidth, "VERSION",
		"IP-ADDRESS",
	)))

	for i, s := range statuses {
		prefix := "├"
		if i == len(statuses)-1 {
			prefix = "└"
		}
		nodeName := truncateName(s.Name, nameColWidth)
		sb.WriteString(fmt.Sprintf(
			"%s %-*s %-*s %-*s %s\n",
			yellow(prefix),
			nameColWidth, nodeName,
			statusColWidth, s.Status,
			versionColWidth, s.Version,
			s.IPAddress,
		))
	}

	return sb.String()
}

// getNameColWidth returns the formatting width for columns with the given maxNameLen value.
func getNameColWidth(maxNameLen int) (nameCol, statusCol, versionCol int) {
	switch {
	case maxNameLen > 37:
		return 51, 10, 14
	case maxNameLen > 28:
		return 38, 12, 10
	default:
		return 29, 8, 12
	}
}

// truncateName truncates a string to a given length
func truncateName(str string, maxRunes int) string {
	runes := []rune(str)
	if len(runes) <= maxRunes {
		return str
	}
	if maxRunes > 3 {
		return string(runes[:maxRunes-3]) + "..."
	}
	return string(runes[:maxRunes])
}
