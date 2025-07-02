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

func Status(kubeCl kubernetes.Interface) statusresult.StatusResult {
    nodes, err := getMasterNodes(kubeCl)
    var output string
    if err != nil {
        output = color.RedString("Error getting master nodes: %v", err)
    } else {
        output = formatMasterNodes(nodes)
    }
    return statusresult.StatusResult{
        Title:  "Master Nodes",
        Level:  0,
        Output: output,
    }
}



func getMasterNodes(kubeCl kubernetes.Interface) ([]corev1.Node, error) {
    nodes, err := kubeCl.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{
        LabelSelector: "node-role.kubernetes.io/master",
    })
    if err != nil {
        return nil, fmt.Errorf("failed to list master nodes: %w", err)
    }
    return nodes.Items, nil
}

func formatMasterNodes(nodes []corev1.Node) string {
    if len(nodes) == 0 {
    return color.YellowString("❗ No master nodes found")
    }

    var sb strings.Builder
    yellow := color.New(color.FgYellow).SprintFunc()

    sb.WriteString(yellow("┌ Master Nodes Status\n"))
    sb.WriteString(yellow(fmt.Sprintf("%-48s %-15s %-14s %s\n", "├ NAME", "STATUS", "VERSION", "IP-ADRESS")))
    for i, node := range nodes {
    prefix := "├"
    if i == len(nodes)-1 {
        prefix = "└"
    }

    status := "NotReady"
    if isNodeReady(node) {
        status = "Ready"
    }

    sb.WriteString(fmt.Sprintf("%s%-46s %-15s %s\t%s\n",
        yellow(prefix+" "),
        node.Name,
        status,
        node.Status.NodeInfo.KubeletVersion,
        getNodeIP(node)))
    }
    return sb.String()
}

func getNodeIP(node corev1.Node) string {
    for _, addr := range node.Status.Addresses {
    if addr.Type == corev1.NodeInternalIP {
        return addr.Address
    }
    }
    return "<none>"
}

func isNodeReady(node corev1.Node) bool {
    for _, cond := range node.Status.Conditions {
    if cond.Type == corev1.NodeReady {
        return cond.Status == corev1.ConditionTrue
    }
    }
    return false
}