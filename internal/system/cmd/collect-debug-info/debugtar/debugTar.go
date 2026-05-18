package debugtar

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

type Command struct {
	Cmd  string
	Args []string
	File string

	// RequiredModule is the module prefix (status.phase == "Ready"). If the module is enabled, data from it will be collected. An empty string means always run.
	RequiredModule string
	// ExpandPerModule — If true, the command is duplicated for each active module matching RequiredModule. The {module-name} placeholder is accepted in File and Args, and is replaced with the actual module name.
	ExpandPerModule bool
}

type moduleList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
}

// debugCommands - a complete list of commands for collecting debug information.
var debugCommands = []Command{
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
		File: "namespace.json",
		Cmd:  "kubectl",
		Args: []string{"get", "namespaces", "-o", "json"},
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
		File:           "cloud-machine-deployment.txt",
		Cmd:            "bash",
		Args:           []string{"-c", `kubectl -n d8-cloud-instance-manager get machinedeployments.machine.sapcloud.io -o json | jq '.items[]'`},
		RequiredModule: "cloud-provider",
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
		File:           "machine-controller-manager.json",
		Cmd:            "bash",
		Args:           []string{"-c", `kubectl -n d8-cloud-instance-manager get pods -l app=machine-controller-manager -o json | jq '.items[]'`},
		RequiredModule: "cloud-provider",
	},
	{
		File: "mcm-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "d8-cloud-instance-manager", "logs", "-l", "app=machine-controller-manager", "--tail=3000", "-c", "controller", "--ignore-errors=true"},
	},
	{
		File:            "ccm-logs-{module-name}.txt",
		Cmd:             "kubectl",
		Args:            []string{"-n", "d8-{module-name}", "logs", "-l", "app=cloud-controller-manager", "--tail=3000"},
		RequiredModule:  "cloud-provider",
		ExpandPerModule: true,
	},
	{
		File:            "csi-controller-logs-{module-name}.txt",
		Cmd:             "kubectl",
		Args:            []string{"-n", "d8-{module-name}", "logs", "-l", "app=csi-controller", "--tail=3000"},
		RequiredModule:  "cloud-provider",
		ExpandPerModule: true,
	},
	{
		File: "cluster-autoscaler-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "d8-cloud-instance-manager", "logs", "-l", "app=cluster-autoscaler", "--tail=5000", "-c", "cluster-autoscaler", "--ignore-errors=true"},
	},
	{
		File:            "cert-manager-logs.txt",
		Cmd:             "kubectl",
		Args:            []string{"-n", "d8-cert-manager", "logs", "-l", "app=cert-manager", "--tail=3000"},
		RequiredModule:  "cert-manager",
		ExpandPerModule: false,
	},
	{
		File:            "certificate-cert-manager.json",
		Cmd:             "kubectl",
		Args:            []string{"get", "certificate", "-A", "-o", "json", "--ignore-not-found=true"},
		RequiredModule:  "cert-manager",
		ExpandPerModule: false,
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
		File:            "d8-istio-resources.json",
		Cmd:             "bash",
		Args:            []string{"-c", `kubectl -n d8-istio get all -o json | jq '.items[]'`},
		RequiredModule:  "istio",
		ExpandPerModule: false,
	},
	{
		File:            "d8-istio-custom-resources.json",
		Cmd:             "bash",
		Args:            []string{"-c", `for crd in $(kubectl get crds | grep -E 'istio.io|gateway.networking.k8s.io' | awk '{print $1}'); do echo "Listing resources for CRD: $crd" && kubectl get $crd -A -o json; done`},
		RequiredModule:  "istio",
		ExpandPerModule: false,
	},
	{
		File:            "d8-istio-envoy-config.json",
		Cmd:             "bash",
		Args:            []string{"-c", `kubectl port-forward daemonset/ingressgateway -n d8-istio 15000:15000 & sleep 5; (curl http://localhost:15000/config_dump?include_eds=true | jq 'del(.configs[6].dynamic_active_secrets)' && kill $!) || { kill $!; exit 0; }`},
		RequiredModule:  "istio",
		ExpandPerModule: false,
	},
	{
		File:            "d8-istio-system-logs.txt",
		Cmd:             "bash",
		Args:            []string{"-c", `kubectl -n d8-istio logs -l app=istiod || true`},
		RequiredModule:  "istio",
		ExpandPerModule: false,
	},
	{
		File:            "d8-istio-ingress-logs.txt",
		Cmd:             "bash",
		Args:            []string{"-c", `kubectl -n d8-istio logs daemonset/ingressgateway || true`},
		RequiredModule:  "istio",
		ExpandPerModule: false,
	},
	{
		File:            "d8-istio-users-logs.txt",
		Cmd:             "bash",
		Args:            []string{"-c", `kubectl get pods --all-namespaces -o jsonpath='{range .items[?(@.metadata.annotations.istio\.io/rev)]}{.metadata.namespace}{" "}{.metadata.name}{" "}{.spec.containers[*].name}{"\n"}{end}' | awk '/istio-proxy/ {print $0}' | shuf -n 1 | while read namespace pod_name containers; do echo "Collecting logs from istio-proxy in Pod $pod_name (Namespace: $namespace)"; kubectl logs "$pod_name" -n "$namespace" -c istio-proxy; done`},
		RequiredModule:  "istio",
		ExpandPerModule: false,
	},
	{
		File:            "cni-cilium-health-status.txt",
		Cmd:             "bash",
		Args:            []string{"-c", `kubectl -n d8-cni-cilium exec -it $(kubectl -n d8-cni-cilium get pod -o name | grep agent | head -n 1) -c cilium-agent -- cilium-health status`},
		RequiredModule:  "cni-cilium",
		ExpandPerModule: false,
	},
	{
		File: "audit-policy.json",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "get", "secrets", "audit-policy", "-o", "json", "--ignore-not-found=true"},
	},
	{
		File: "kube-system-control-plane-manager-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "app=d8-control-plane-manager", "--tail=3000"},
	},
	{
		File: "kube-system-etcd-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "component=etcd", "--tail=3000"},
	},
	{
		File: "kube-system-kube-apiserver-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "component=kube-apiserver", "--tail=3000"},
	},
	{
		File: "kube-system-kube-controller-manager-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "component=kube-controller-manager", "--tail=3000"},
	},
	{
		File: "kube-system-kube-scheduler-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "component=kube-scheduler", "--tail=3000"},
	},
	{
		File: "kube-system-kube-dns-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "k8s-app=kube-dns", "--tail=3000"},
	},
	{
		File: "prometheusremotewrites.json",
		Cmd:  "kubectl",
		Args: []string{"get", "prometheusremotewrites", "-A", "-o", "json", "--ignore-not-found=true"},
	},
	{
		File: "mutatingwebhookconfigurations.json",
		Cmd:  "kubectl",
		Args: []string{"get", "mutatingwebhookconfigurations.admissionregistration.k8s.io", "-o", "json"},
	},
	{
		File: "validatingwebhookconfigurations.json",
		Cmd:  "kubectl",
		Args: []string{"get", "validatingwebhookconfigurations.admissionregistration.k8s.io", "-o", "json"},
	},
	{
		File: "storage-deckhouse-io-terminating.txt",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl get $(kubectl api-resources --api-group=storage.deckhouse.io --verbs=list -o name | paste -sd, -) --ignore-not-found -A --chunk-size=200 -o json | jq -r '.items[] | select(.apiVersion == "storage.deckhouse.io/v1alpha1") | select(.metadata.deletionTimestamp != null) | "[\(.kind)] \(.metadata.namespace // "-")/\(.metadata.name)"'`},
	},
}

func Tarball(config *rest.Config, kubeCl kubernetes.Interface, excludeFiles []string, commandTimeout time.Duration, requestInterval time.Duration) error {
	const (
		namespace     = "d8-system"
		containerName = "deckhouse"
	)

	podName, err := utilk8s.GetDeckhousePod(kubeCl)
	if err != nil {
		return fmt.Errorf("failed to get Deckhouse pod: %w", err)
	}

	activeModules, err := fetchActiveModules(config, kubeCl, podName, namespace, containerName, commandTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: could not fetch active modules, module-dependent commands will be skipped: %v\n", err)
	}

	commands := filterAndExpandCommands(debugCommands, activeModules)

	excludeMap := make(map[string]bool, len(excludeFiles))
	for _, file := range excludeFiles {
		excludeMap[file] = true
	}

	var tickCh <-chan time.Time
	if requestInterval > 0 {
		ticker := time.NewTicker(requestInterval)
		defer ticker.Stop()
		tickCh = ticker.C
	}

	var stdout, stderr bytes.Buffer
	gzipWriter := gzip.NewWriter(os.Stdout)
	defer gzipWriter.Close()
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	for _, cmd := range commands {
		if isFileExcluded(cmd.File, excludeMap) {
			continue
		}

		if tickCh != nil {
			<-tickCh
		}

		fullCommand := append([]string{cmd.Cmd}, cmd.Args...)
		executor, err := utilk8s.ExecInPod(config, kubeCl, fullCommand, podName, namespace, containerName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: failed to create executor for %s: %v\n", cmd.File, err)
			continue
		}

		cmdCtx, cancel := context.WithTimeout(context.Background(), commandTimeout)
		streamErr := executor.StreamWithContext(cmdCtx, remotecommand.StreamOptions{
			Stdout: &stdout,
			Stderr: &stderr,
		})
		cancel()

		if streamErr != nil {
			if errors.Is(streamErr, context.DeadlineExceeded) {
				fmt.Fprintf(os.Stderr, "WARNING: timed out collecting %s after %s\n", cmd.File, commandTimeout)
			} else {
				fmt.Fprintf(os.Stderr, "ERROR: collecting %s: %s\n%s\n", cmd.File, strings.Join(fullCommand, " "), stderr.String())
			}
		}

		if err = cmd.writeToTar(tarWriter, stdout.Bytes()); err != nil {
			return fmt.Errorf("failed to write tar file %s: %w", cmd.File, err)
		}
		stdout.Reset()
		stderr.Reset()
	}
	return nil
}

// fetchActiveModules returns a map with the names of modules that are in the Ready phase.
func fetchActiveModules(
	config *rest.Config,
	kubeCl kubernetes.Interface,
	podName, namespace, containerName string,
	timeout time.Duration,
) (map[string]bool, error) {
	cmdLine := []string{"kubectl", "get", "module", "-o", "json"}
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
		return nil, fmt.Errorf("stream kubectl get module: %w (stderr: %s)", err, stderr.String())
	}

	var list moduleList
	if err = json.Unmarshal(stdout.Bytes(), &list); err != nil {
		return nil, fmt.Errorf("parse module list: %w", err)
	}

	active := make(map[string]bool, len(list.Items))
	for _, item := range list.Items {
		if item.Status.Phase == "Ready" {
			active[item.Metadata.Name] = true
		}
	}
	return active, nil
}

func filterAndExpandCommands(commands []Command, activeModules map[string]bool) []Command {
	result := make([]Command, 0, len(commands))
	for _, cmd := range commands {
		if cmd.RequiredModule == "" {
			result = append(result, cmd)
			continue
		}
		if len(activeModules) == 0 {
			continue
		}
		if cmd.ExpandPerModule {
			matchedModules := matchingModules(activeModules, cmd.RequiredModule)
			for _, moduleName := range matchedModules {
				result = append(result, Command{
					Cmd:  cmd.Cmd,
					File: strings.ReplaceAll(cmd.File, "{module-name}", moduleName),
					Args: replaceModuleName(cmd.Args, moduleName),
				})
			}
		} else {
			for moduleName := range activeModules {
				if isModuleMatch(moduleName, cmd.RequiredModule) {
					result = append(result, cmd)
					break
				}
			}
		}
	}
	return result
}

func matchingModules(activeModules map[string]bool, required string) []string {
	var matched []string
	for name := range activeModules {
		if isModuleMatch(name, required) {
			matched = append(matched, name)
		}
	}
	sort.Strings(matched)
	return matched
}

func isModuleMatch(moduleName, required string) bool {
	return moduleName == required || strings.HasPrefix(moduleName, required)
}

func replaceModuleName(args []string, moduleName string) []string {
	expanded := make([]string, len(args))
	for i, arg := range args {
		expanded[i] = strings.ReplaceAll(arg, "{module-name}", moduleName)
	}
	return expanded
}

func (c *Command) writeToTar(tarWriter *tar.Writer, fileContent []byte) error {
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

func excludeBaseName(file string) string {
	name := strings.TrimSuffix(file, ".json")
	name = strings.TrimSuffix(name, ".txt")
	name = strings.TrimSuffix(name, "-{module-name}")
	return name
}

func isFileExcluded(fileName string, excludeMap map[string]bool) bool {
	if excludeMap[fileName] {
		return true
	}

	base := strings.TrimSuffix(fileName, ".json")
	base = strings.TrimSuffix(base, ".txt")
	if excludeMap[base] {
		return true
	}

	for excluded := range excludeMap {
		if strings.HasPrefix(base, excluded+"-") {
			return true
		}
	}
	return false
}

func GetExcludableFiles() []string {
	seen := make(map[string]bool, len(debugCommands))
	files := make([]string, 0, len(debugCommands))
	for _, cmd := range debugCommands {
		name := excludeBaseName(cmd.File)
		if !seen[name] {
			seen[name] = true
			files = append(files, name)
		}
	}
	sort.Strings(files)
	return files
}
