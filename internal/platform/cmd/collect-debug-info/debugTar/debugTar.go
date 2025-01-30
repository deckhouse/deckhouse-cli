package debugTar

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/platform/cmd/operatepod"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"os"
	"strings"
)

type Command struct {
	Cmd  string
	Args []string
	File string
}

func Tarball(config *rest.Config, kubeCl kubernetes.Interface) error {
	const (
		labelSelector = "leader=true"
		namespace     = "d8-system"
		containerName = "deckhouse"
	)

	debugCommands := []Command{
		{
			File: "queue.txt",
			Cmd:  "deckhouse-controller",
			Args: []string{"queue", "list"},
		},
		{
			File: "global-values.json",
			Cmd:  "bash",
			Args: []string{"-c", `deckhouse-controller global values -o json | jq '.internal.modules.kubeRBACProxyCA = "REDACTED" | .modulesImages.registry.dockercfg = "REDACTED"'`},
		},
		{
			File: "deckhouse-enabled-modules.json",
			Cmd:  "bash",
			Args: []string{"-c", "kubectl get modules -o json | jq '.items[]'"},
		},
		{
			File: "events.json",
			Cmd:  "kubectl",
			Args: []string{"get", "events", "--sort-by=.metadata.creationTimestamp", "-A", "-o", "json"},
		},
		{
			File: "d8-all.json",
			Cmd:  "bash",
			Args: []string{"-c", `for ns in $(kubectl get ns -o go-template='{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}{{"kube-system"}}' -l heritage=deckhouse); do kubectl -n $ns get all -o json; done | jq -s '[.[].items[]]'`},
		},
		{
			File: "node-groups.json",
			Cmd:  "kubectl",
			Args: []string{"get", "nodegroups", "-A", "-o", "json"},
		},
		{
			File: "nodes.json",
			Cmd:  "kubectl",
			Args: []string{"get", "nodes", "-A", "-o", "json"},
		},
		{
			File: "machines.json",
			Cmd:  "kubectl",
			Args: []string{"get", "machines.machine.sapcloud.io", "-A", "-o", "json"},
		},
		{
			File: "instances.json",
			Cmd:  "kubectl",
			Args: []string{"get", "instances.deckhouse.io", "-o", "json"},
		},
		{
			File: "staticinstances.json",
			Cmd:  "kubectl",
			Args: []string{"get", "staticinstances.deckhouse.io", "-o", "json"},
		},
		{
			File: "deckhouse-version.json",
			Cmd:  "bash",
			Args: []string{"-c", "jq -s add <(kubectl -n d8-system get deployment deckhouse -o json | jq -r '.metadata.annotations | {\"core.deckhouse.io/edition\",\"core.deckhouse.io/version\"}') <(kubectl -n d8-system get deployment deckhouse -o json | jq -r '.spec.template.spec.containers[] | select(.name == \"deckhouse\") | {image}')"},
		},
		{
			File: "deckhouse-releases.json",
			Cmd:  "kubectl",
			Args: []string{"get", "deckhousereleases", "-o", "json"},
		},
		{
			File: "deckhouse-logs.json",
			Cmd:  "kubectl",
			Args: []string{"logs", "deploy/deckhouse", "--tail", "3000"},
		},
		{
			File: "mcm-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "d8-cloud-instance-manager", "logs", "-l", "app=machine-controller-manager", "--tail", "3000", "-c", "controller"},
		},
		{
			File: "ccm-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "d8-cloud-provider", "logs", "-l", "app=cloud-controller-manager", "--tail", "3000"},
		},
		{
			File: "cluster-autoscaler-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "d8-cloud-instance-manager", "logs", "-l", "app=cluster-autoscaler", "--tail", "3000", "-c", "cluster-autoscaler"},
		},
		{
			File: "vpa-admission-controller-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "kube-system", "logs", "-l", "app=vpa-admission-controller", "--tail", "3000", "-c", "admission-controller"},
		},
		{
			File: "vpa-recommender-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "kube-system", "logs", "-l", "app=vpa-recommender", "--tail", "3000", "-c", "recommender"},
		},
		{
			File: "vpa-updater-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "kube-system", "logs", "-l", "app=vpa-updater", "--tail", "3000", "-c", "updater"},
		},
		{
			File: "prometheus-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "d8-monitoring", "logs", "-l", "prometheus=main", "--tail", "3000", "-c", "prometheus"},
		},
		{
			File: "terraform-check.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl exec deploy/terraform-state-exporter -- dhctl terraform check --logger-type json -o json | jq -c '.terraform_plan[]?.variables.providerClusterConfiguration.value.provider = "REDACTED"'`},
		},
		{
			File: "alerts.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get clusteralerts.deckhouse.io -o json | jq '.items[]'`},
		},
		{
			File: "pods.txt",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get pod -A -owide | grep -Pv '\s+([1-9]+[\d]*)\/\1\s+' | grep -v 'Completed\|Evicted' | grep -E "^(d8-|kube-system)"`},
		},
		{
			File: "cluster-authorization-rules.json",
			Cmd:  "kubectl",
			Args: []string{"get", "clusterauthorizationrules", "-o", "json"},
		},
		{
			File: "authorization-rules.json",
			Cmd:  "kubectl",
			Args: []string{"get", "authorizationrules", "-o", "json"},
		},
		{
			File: "module-configs.json",
			Cmd:  "kubectl",
			Args: []string{"get", "moduleconfig", "-o", "json"},
		},
	}

	podName, err := operatepod.GetDeckhousePod(kubeCl, namespace, labelSelector)

	var stdout, stderr bytes.Buffer
	gzipWriter := gzip.NewWriter(os.Stdout)
	defer gzipWriter.Close()
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	for _, cmd := range debugCommands {
		fullCommand := append([]string{cmd.Cmd}, cmd.Args...)
		executor, err := operatepod.ExecInPod(config, kubeCl, fullCommand, podName, namespace, containerName)
		if err = executor.StreamWithContext(
			context.Background(),
			remotecommand.StreamOptions{
				Stdout: &stdout,
				Stderr: &stderr,
			}); err != nil {
			fmt.Fprintf(os.Stderr, strings.Join(fullCommand, " "))
			fmt.Fprintf(os.Stderr, stderr.String())
		}

		err = cmd.Writer(tarWriter, stdout.Bytes())
		if err != nil {
			return fmt.Errorf("failed to update the %w", err)
		}
		stdout.Reset()

	}
	return err
}

func (c *Command) Writer(tarWriter *tar.Writer, fileContent []byte) error {
	header := &tar.Header{
		Name: c.File,
		Mode: 0o600,
		Size: int64(len(fileContent)),
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("write tar header: %v", err)
	}

	if _, err := tarWriter.Write(fileContent); err != nil {
		return fmt.Errorf("copy content: %v", err)
	}

	return nil
}
