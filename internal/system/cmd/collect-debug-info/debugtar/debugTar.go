package debugtar

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

type Command struct {
	Cmd  string
	Args []string
	File string
}

func Tarball(config *rest.Config, kubeCl kubernetes.Interface, excludeFiles []string) error {
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
			File: "deckhouse-module-sources.json",
			Cmd:  "bash",
			Args: []string{"-c", "kubectl get modulesources -o json | jq '.items[]'"},
		},
		{
			File: "deckhouse-module-pull-overrides.json",
			Cmd:  "bash",
			Args: []string{"-c", "kubectl get modulepulloverrides -o json | jq '.items[]'"},
		},
		{
			File: "deckhouse-maintenance-modules.txt",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get moduleconfig -ojson | jq -r '.items[] | select(.spec.maintenance == "NoResourceReconciliation") | .metadata.name'`},
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
			File: "node-group-configuration.json",
			Cmd:  "kubectl",
			Args: []string{"get", "nodegroupconfiguration", "-A", "-o", "json"},
		},
		{
			File: "nodes.json",
			Cmd:  "kubectl",
			Args: []string{"get", "nodes", "-A", "-o", "json"},
		},
		{
			File: "machines.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get machines.machine.sapcloud.io -A -o json | jq '.items[]'`},
		},
		{
			File: "instances.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get instances.deckhouse.io -o json | jq '.items[]'`},
		},
		{
			File: "staticinstances.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get staticinstances.deckhouse.io -o json | jq '.items[]'`},
		},
		{
			File: "cloud-machine-deployment.txt",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get modules -o json | jq -r '.items[] | select(.status.phase == "Ready" and (.metadata.name | test("^cloud-provider"))) | "kubectl -n d8-cloud-instance-manager get machinedeployments.machine.sapcloud.io -o json | jq '.items[]'"' | bash`},
		},
		{
			File: "static-machine-deployment.txt",
			Cmd:  "bash",
			Args: []string{"-c", "kubectl -n d8-cloud-instance-manager get machinedeployments.cluster.x-k8s.io -o json --ignore-not-found | jq '.items[]'"},
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
			Args: []string{"-n", "d8-system", "logs", "-l", "app=deckhouse", "--tail", "3000"},
		},
		{
			File: "capi-controller-manager.json",
			Cmd:  "kubectl",
			Args: []string{"-n", "d8-cloud-instance-manager", "get", "pods", "-l", "app=capi-controller-manager", "-o", "json", "--ignore-not-found=true"},
		},
		{
			File: "caps-controller-manager.json",
			Cmd:  "kubectl",
			Args: []string{"-n", "d8-cloud-instance-manager", "get", "pods", "-l", "app=caps-controller-manager", "-o", "json", "--ignore-not-found=true"},
		},
		{
			File: "machine-controller-manager.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get modules -o json | jq -r '.items[] | select(.status.phase == "Ready" and (.metadata.name | test("^cloud-provider"))) | "kubectl -n d8-cloud-instance-manager get pods -l app=machine-controller-manager -o json | jq '.items[]'"' | bash`},
		},
		{
			File: "mcm-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "d8-cloud-instance-manager", "logs", "-l", "app=machine-controller-manager", "--tail=3000", "-c", "controller", "--ignore-errors=true"},
		},
		{
			File: "ccm-logs.txt",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get modules -o json | jq -r '.items[] | select(.status.phase == "Ready" and (.metadata.name | test("^cloud-provider"))) | "kubectl -n d8-"+.metadata.name+" logs -l app=cloud-controller-manager --tail=3000"' | bash`},
		},
		{
			File: "csi-controller-logs.txt",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get modules -o json | jq -r '.items[] | select(.status.phase == "Ready" and (.metadata.name | test("^cloud-provider"))) | "kubectl -n d8-"+.metadata.name+" logs -l app=csi-controller --tail=3000"' | bash`},
		},
		{
			File: "cluster-autoscaler-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "d8-cloud-instance-manager", "logs", "-l", "app=cluster-autoscaler", "--tail=5000", "-c", "cluster-autoscaler", "--ignore-errors=true"},
		},
		{
			File: "vpa-admission-controller-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "kube-system", "logs", "-l", "app=vpa-admission-controller", "--tail=3000", "-c", "admission-controller", "--ignore-errors=true"},
		},
		{
			File: "vpa-recommender-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "kube-system", "logs", "-l", "app=vpa-recommender", "--tail=3000", "-c", "recommender", "--ignore-errors=true"},
		},
		{
			File: "vpa-updater-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "kube-system", "logs", "-l", "app=vpa-updater", "--tail=3000", "-c", "updater", "--ignore-errors=true"},
		},
		{
			File: "prometheus-logs.txt",
			Cmd:  "kubectl",
			Args: []string{"-n", "d8-monitoring", "logs", "-l", "prometheus=main", "--tail=3000", "-c", "prometheus", "--ignore-errors=true"},
		},
		{
			File: "alerts.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get clusteralerts.deckhouse.io -o json | jq '.items[]'`},
		},
		{
			File: "bad-pods.txt",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get pod -A -owide | grep -Pv '\s+([1-9]+[\d]*)\/\1\s+' | grep -v 'Completed\|Evicted' | grep -E "^(d8-|kube-system)" || true`},
		},
		{
			File: "cluster-authorization-rules.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get clusterauthorizationrules.deckhouse.io -A -o json | jq '.items[]'`},
		},
		{
			File: "authorization-rules.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get authorizationrules.deckhouse.io -A -o json | jq '.items[]'`},
		},
		{
			File: "module-configs.json",
			Cmd:  "kubectl",
			Args: []string{"get", "moduleconfig", "-o", "json"},
		},
		{
			File: "d8-istio-resources.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl -n d8-istio get all -o json | jq '.items[]'`},
		},
		{
			File: "d8-istio-custom-resources.json",
			Cmd:  "bash",
			Args: []string{"-c", `for crd in $(kubectl get crds | grep -E 'istio.io|gateway.networking.k8s.io' | awk '{print $1}'); do echo "Listing resources for CRD: $crd" && kubectl get $crd -A -o json; done`},
		},
		{
			File: "d8-istio-envoy-config.json",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl port-forward daemonset/ingressgateway -n d8-istio 15000:15000 & sleep 5; (curl http://localhost:15000/config_dump?include_eds=true | jq 'del(.configs[6].dynamic_active_secrets)' && kill $!) || { kill $!; exit 0; }`},
		},
		{
			File: "d8-istio-system-logs.txt",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl -n d8-istio logs -l app=istiod || true`},
		},
		{
			File: "d8-istio-ingress-logs.txt",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl -n d8-istio logs daemonset/ingressgateway || true`},
		},
		{
			File: "d8-istio-users-logs.txt",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get pods --all-namespaces -o jsonpath='{range .items[?(@.metadata.annotations.istio\.io/rev)]}{.metadata.namespace}{" "}{.metadata.name}{" "}{.spec.containers[*].name}{"\n"}{end}' | awk '/istio-proxy/ {print $0}' | shuf -n 1 | while read namespace pod_name containers; do echo "Collecting logs from istio-proxy in Pod $pod_name (Namespace: $namespace)"; kubectl logs "$pod_name" -n "$namespace" -c istio-proxy; done`},
		},
		{
			File: "cilium-health-status.txt",
			Cmd:  "bash",
			Args: []string{"-c", `kubectl get modules -o json | jq -r '.items[] | select(.status.phase == "Ready" and .metadata.name == "cni-cilium") | "kubectl -n d8-cni-cilium exec -it $(kubectl -n d8-cni-cilium get pod -o name | grep agent | head -n 1) -c cilium-agent -- cilium-health status"' | bash`},
		},
		{
			File: "audit-policy.json",
			Cmd:  "kubectl",
			Args: []string{"-n", "kube-system", "get", "secrets", "audit-policy", "-o", "json", "--ignore-not-found=true"},
		},
	}

	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return fmt.Errorf("failed to get Deckhouse pod: %w", err)
	}

	var stdout, stderr bytes.Buffer
	gzipWriter := gzip.NewWriter(os.Stdout)
	defer gzipWriter.Close()
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	excludeMap := make(map[string]bool, len(excludeFiles))
	for _, file := range excludeFiles {
		excludeMap[file] = true
	}

	for _, cmd := range debugCommands {
		if isFileExcluded(cmd.File, excludeMap) {
			continue
		}

		fullCommand := append([]string{cmd.Cmd}, cmd.Args...)
		executor, err := utilk8s.ExecInPod(config, kubeCl, fullCommand, podName, namespace, containerName)
		if err != nil {
			return fmt.Errorf("failed to create executor for command %s: %w", cmd.File, err)
		}

		if err = executor.StreamWithContext(
			context.Background(),
			remotecommand.StreamOptions{
				Stdout: &stdout,
				Stderr: &stderr,
			}); err != nil {
			fmt.Fprint(os.Stderr, strings.Join(fullCommand, " "))
			fmt.Fprint(os.Stderr, stderr.String())
		}

		if err = cmd.Writer(tarWriter, stdout.Bytes()); err != nil {
			return fmt.Errorf("failed to write tar file %s: %w", cmd.File, err)
		}
		stdout.Reset()
	}
	return nil
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

func isFileExcluded(fileName string, excludeMap map[string]bool) bool {
	if excludeMap[fileName] {
		return true
	}

	fileNameWithoutExt := strings.TrimSuffix(fileName, ".json")
	fileNameWithoutExt = strings.TrimSuffix(fileNameWithoutExt, ".txt")

	return excludeMap[fileNameWithoutExt]
}

// GetExcludableFiles returns list of files that can be excluded in alphabetical order
func GetExcludableFiles() []string {
	return []string{
		"alerts",
		"audit-policy",
		"authorization-rules",
		"bad-pods",
		"capi-controller-manager",
		"caps-controller-manager",
		"ccm-logs",
		"cilium-health-status",
		"cloud-machine-deployment",
		"cluster-authorization-rules",
		"cluster-autoscaler-logs",
		"csi-controller-logs",
		"d8-all",
		"d8-istio-custom-resources",
		"d8-istio-envoy-config",
		"d8-istio-ingress-logs",
		"d8-istio-resources",
		"d8-istio-system-logs",
		"d8-istio-users-logs",
		"deckhouse-enabled-modules",
		"deckhouse-logs",
		"deckhouse-maintenance-modules",
		"deckhouse-module-pull-overrides",
		"deckhouse-module-sources",
		"deckhouse-releases",
		"deckhouse-version",
		"events",
		"global-values",
		"instances",
		"machine-controller-manager",
		"machines",
		"mcm-logs",
		"module-configs",
		"node-group-configuration",
		"node-groups",
		"nodes",
		"prometheus-logs",
		"queue",
		"static-machine-deployment",
		"staticinstances",
		"vpa-admission-controller-logs",
		"vpa-recommender-logs",
		"vpa-updater-logs",
	}
}
