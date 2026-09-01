package debugtar

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

const virtualizationNamespace = "d8-virtualization"

// virtualizationCommands - additional resource-intensive commands collected only in the virtualization archive
var virtualizationCommands = []Command{
	{
		File: "d8-virtualization-pods-wide.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", virtualizationNamespace, "get", "pod", "-o", "wide"},
	},
}

type virtualizationPod struct {
	Name           string
	DaemonSetOwned bool
}

type podList struct {
	Items []struct {
		Metadata struct {
			Name            string `json:"name"`
			OwnerReferences []struct {
				Kind string `json:"kind"`
			} `json:"ownerReferences"`
		} `json:"metadata"`
	} `json:"items"`
}

// VirtualizationTarball collects a separate, virtualization-focused debug
// archive: the list of pods in the d8-virtualization namespace
// plus per-pod logs, optionally skipping pods owned by a DaemonSet
// (virt-handler, virtualization-dra, vm-route-forge, ...) since their log
// volume scales with the number of nodes.
func VirtualizationTarball(config *rest.Config, kubeCl kubernetes.Interface, commandTimeout, requestInterval time.Duration, skipDsLogs bool) error {
	const (
		namespace     = "d8-system"
		containerName = "deckhouse"
	)

	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return fmt.Errorf("failed to get Deckhouse pod: %w", err)
	}

	pods, err := fetchVirtualizationPods(config, kubeCl, podName, namespace, containerName, commandTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: could not list pods in %s: %v\n", virtualizationNamespace, err)
	} else if len(pods) == 0 {
		fmt.Fprintf(os.Stderr, "WARNING: no pods found in namespace %s, is the virtualization module enabled?\n", virtualizationNamespace)
	}

	commands := buildVirtualizationCommands(pods, skipDsLogs)

	gzipWriter := gzip.NewWriter(os.Stdout)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	fmt.Fprintf(os.Stderr, "Collecting virtualization debug info from Deckhouse...\n")

	if err = runCommands(tarWriter, config, kubeCl, podName, namespace, containerName, commands, nil, commandTimeout, requestInterval); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Virtualization debug archive collection completed.\n")

	return nil
}

// fetchVirtualizationPods lists the pods currently running in the
// virtualization namespace and reports which ones are owned by a DaemonSet,
// so the DaemonSet-managed pods can be identified without hardcoding their names.
func fetchVirtualizationPods(
	config *rest.Config,
	kubeCl kubernetes.Interface,
	podName, namespace, containerName string,
	timeout time.Duration,
) ([]virtualizationPod, error) {
	cmdLine := []string{"kubectl", "-n", virtualizationNamespace, "get", "pods", "-o", "json", "--ignore-not-found=true"}

	executor, err := utilk8s.ExecInPod(config, kubeCl, cmdLine, podName, namespace, containerName)
	if err != nil {
		return nil, fmt.Errorf("create executor: %w", err)
	}

	var stdout, stderr bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		return nil, fmt.Errorf("stream kubectl get pods: %w (stderr: %s)", err, stderr.String())
	}

	if stdout.Len() == 0 {
		return nil, nil
	}

	var list podList
	if err = json.Unmarshal(stdout.Bytes(), &list); err != nil {
		return nil, fmt.Errorf("parse pod list: %w", err)
	}

	pods := make([]virtualizationPod, 0, len(list.Items))
	for _, item := range list.Items {
		daemonSetOwned := false
		for _, owner := range item.Metadata.OwnerReferences {
			if owner.Kind == "DaemonSet" {
				daemonSetOwned = true
				break
			}
		}

		pods = append(pods, virtualizationPod{
			Name:           item.Metadata.Name,
			DaemonSetOwned: daemonSetOwned,
		})
	}

	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })

	return pods, nil
}

// buildVirtualizationCommands turns the discovered pod list into the final
// the static commands first, then one log-collection command per pod (skipping DaemonSet-owned pods when skipDsLogs is set).
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
