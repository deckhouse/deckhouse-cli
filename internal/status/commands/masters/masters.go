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

type MasterNodeStatus struct {
    Name          string
    Status        string
    Version       string
    IPAddress     string
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

func formatMasterNodes(nodes []corev1.Node) string {
    if len(nodes) == 0 {
        return color.YellowString("❗ No master nodes found\n")
    }

    var statuses []MasterNodeStatus
    maxNameLen := len("NAME")
    for _, node := range nodes {
        s := masterNodeStatus(node)
        if l := len([]rune(s.Name)); l > maxNameLen {
            maxNameLen = l
        }
        statuses = append(statuses, s)
    }

    nameColWidth := 29
    statusColWidth := 8
    versionColWidth := 12

   if maxNameLen > 37 {
       nameColWidth = 51
       statusColWidth = 10
       versionColWidth = 14
   }
   if maxNameLen > 28 && maxNameLen <= 37 {
       nameColWidth = 38
       statusColWidth = 12
       versionColWidth = 10
   }

    yellow := color.New(color.FgYellow).SprintFunc()
    var sb strings.Builder
    sb.WriteString(yellow("┌ Master Nodes Status:\n"))
    sb.WriteString(yellow(fmt.Sprintf("├ %-*s %-*s %-*s %s\n",
        nameColWidth, "NAME", statusColWidth, "STATUS", versionColWidth, "VERSION", "IP-ADDRESS")))

    for i, s := range statuses {
        prefix := "├"
        if i == len(statuses)-1 {
            prefix = "└"
        }
        nodeName := s.Name
        if l := len([]rune(nodeName)); l > nameColWidth {
            nodeName = string([]rune(nodeName)[:nameColWidth-3]) + "..."
        }
        sb.WriteString(fmt.Sprintf("%s %-*s %-*s %-*s %s\n",
            yellow(prefix),
            nameColWidth, nodeName,
            statusColWidth, s.Status,
            versionColWidth, s.Version,
            s.IPAddress,
        ))
    }
    return sb.String()
}