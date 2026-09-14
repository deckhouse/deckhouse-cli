package debugtar

import (
	"context"
	"fmt"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const virtualizationNamespace = "d8-virtualization"

// virtualizationCommands - additional resource-intensive commands collected only in the virtualization archive
var virtualizationCommands = []Command{
	{
		File: "d8-virtualization-pods-wide.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", virtualizationNamespace, "get", "pod", "-o", "wide", "--ignore-not-found=true"},
	},
}

type virtualizationPod struct {
	Name           string
	DaemonSetOwned bool
}

// VirtualizationTarball collects a separate, virtualization-focused debug
// archive: the list of pods in the d8-virtualization namespace
// plus per-pod logs, optionally skipping pods owned by a DaemonSet
// (virt-handler, virtualization-dra, vm-route-forge, ...) since their log
// volume scales with the number of nodes.
//
// The pod list is the entire payload of this archive, so a failure to obtain it
// aborts the collection instead of producing an archive that looks complete.
func VirtualizationTarball(config *rest.Config, kubeCl kubernetes.Interface, commandTimeout, requestInterval time.Duration, skipDsLogs bool) error {
	const (
		namespace     = "d8-system"
		containerName = "deckhouse"
	)

	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return fmt.Errorf("failed to get Deckhouse pod: %w", err)
	}

	pods, err := fetchVirtualizationPods(kubeCl, commandTimeout)
	if err != nil {
		return fmt.Errorf("failed to list pods in namespace %s: %w", virtualizationNamespace, err)
	}

	if len(pods) == 0 {
		return fmt.Errorf("no pods found in namespace %s, is the virtualization module enabled?", virtualizationNamespace)
	}

	return writeArchive(
		config, kubeCl, podName, namespace, containerName,
		buildVirtualizationCommands(pods, skipDsLogs), commandTimeout, requestInterval,
		"Collecting virtualization debug info from Deckhouse...",
		"Virtualization debug archive collection completed.",
	)
}

// fetchVirtualizationPods lists the pods currently running in the
// virtualization namespace and reports which ones are owned by a DaemonSet,
// so the DaemonSet-managed pods can be identified without hardcoding their names.
func fetchVirtualizationPods(kubeCl kubernetes.Interface, timeout time.Duration) ([]virtualizationPod, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	list, err := kubeCl.CoreV1().Pods(virtualizationNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	pods := make([]virtualizationPod, 0, len(list.Items))

	for i := range list.Items {
		owner := metav1.GetControllerOf(&list.Items[i])

		pods = append(pods, virtualizationPod{
			Name:           list.Items[i].Name,
			DaemonSetOwned: owner != nil && owner.Kind == "DaemonSet",
		})
	}

	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })

	return pods, nil
}

// buildVirtualizationCommands transforms the discovered list of pods into a final list.
// first the static commands, then one log collection command for each pod (skipping pods belonging to DaemonSet if skipDsLogs is set).
func buildVirtualizationCommands(pods []virtualizationPod, skipDsLogs bool) []Command {
	commands := make([]Command, 0, len(virtualizationCommands)+len(pods))
	commands = append(commands, virtualizationCommands...)

	for _, pod := range pods {
		if skipDsLogs && pod.DaemonSetOwned {
			continue
		}

		commands = append(commands, Command{
			File: fmt.Sprintf("d8-virtualization-%s-logs.txt", pod.Name),
			Cmd:  "kubectl",
			Args: []string{"-n", virtualizationNamespace, "logs", pod.Name, "--tail=-1", "--ignore-errors=true"},
		})
	}

	return commands
}
