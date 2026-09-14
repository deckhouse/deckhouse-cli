package debugtar

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

type Command struct {
	Cmd  string
	Args []string
	File string

	// RequiredModule gates the command on a module being Ready (status.phase ==
	// "Ready"): the command runs only when the name of some Ready module starts
	// with this string, so "cloud-provider" matches cloud-provider-aws. An empty
	// string means always run.
	//
	// Together with the {module-name} placeholder it also means "once per
	// matching module": when RequiredModule is set and File or any Args element
	// contains the placeholder (see needsModuleExpansion), the command is
	// duplicated for every matching Ready module, with the placeholder
	// substituted in both File and Args. Put the placeholder in File whenever it
	// appears in Args — copies that differ only in Args all end up under the same
	// archive entry name, and only the last one survives extraction.
	//
	// Leaving RequiredModule empty while the placeholder is present means it is
	// never resolved and stays literal in the output.
	//
	// When the module list cannot be fetched at all, gating is impossible and
	// the fallback differs by shape: commands without the placeholder run anyway
	// (they either produce data or an empty file), commands with it are skipped,
	// since their archive entry name cannot be resolved.
	RequiredModule string
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
		File: "deckhouse-queue.txt",
		Cmd:  "deckhouse-controller",
		Args: []string{"queue", "list"},
	},
	{
		File: "cluster-global-values.json",
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
		File: "deckhouse-module-update-policies.json",
		Cmd:  "bash",
		Args: []string{"-c", "kubectl get moduleupdatepolicies -o json | jq '.items[]'"},
	},
	{
		File: "deckhouse-maintenance-modules.txt",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl get moduleconfig -ojson | jq -r '.items[] | select(.spec.maintenance == "NoResourceReconciliation") | .metadata.name'`},
	},
	{
		File: "cluster-events.json",
		Cmd:  "kubectl",
		Args: []string{"get", "events", "--sort-by=.metadata.creationTimestamp", "-A", "-o", "json"},
	},
	{
		File: "d8-all.json",
		Cmd:  "bash",
		Args: []string{"-c", `for ns in $(kubectl get ns -o go-template='{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}{{"kube-system"}}' -l heritage=deckhouse); do kubectl -n $ns get all -o json; done | jq -s '[.[].items[]]'`},
	},
	{
		File: "cluster-node-groups.json",
		Cmd:  "kubectl",
		Args: []string{"get", "nodegroups", "-A", "-o", "json"},
	},
	{
		File: "cluster-node-group-configuration.json",
		Cmd:  "kubectl",
		Args: []string{"get", "nodegroupconfiguration", "-A", "-o", "json"},
	},
	{
		File: "cluster-nodes.json",
		Cmd:  "kubectl",
		Args: []string{"get", "nodes", "-A", "-o", "json"},
	},
	{
		File: "cluster-namespace.json",
		Cmd:  "kubectl",
		Args: []string{"get", "namespaces", "-o", "json"},
	},
	{
		File: "instance-manager-capi-machines.json",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl -n d8-cloud-instance-manager get machines.cluster.x-k8s.io -o json | jq '.items[]'`},
	},
	{
		File: "instance-manager-instances.json",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl get instances.deckhouse.io -o json | jq '.items[]'`},
	},
	{
		File: "instance-manager-staticinstances.json",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl get staticinstances.deckhouse.io -o json | jq '.items[]'`},
	},
	{
		File:           "instance-manager-cloud-machine-deployment.txt",
		Cmd:            "bash",
		Args:           []string{"-c", `kubectl -n d8-cloud-instance-manager get machinedeployments.machine.sapcloud.io -o json | jq '.items[]'`},
		RequiredModule: "cloud-provider",
	},
	{
		File: "instance-manager-static-machine-deployment.txt",
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
		File: "instance-manager-capi-controller-manager-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "d8-cloud-instance-manager", "logs", "-l", "app=capi-controller-manager", "--tail", "3000", "--ignore-errors=true"},
	},
	{
		File: "instance-manager-caps-controller-manager-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "d8-cloud-instance-manager", "logs", "-l", "app=caps-controller-manager", "--tail", "3000", "--ignore-errors=true"},
	},
	{
		File: "instance-manager-machine-controller-manager.json",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl -n d8-cloud-instance-manager get pods -l app=machine-controller-manager -o json | jq '.items[]'`},
	},
	{
		File: "instance-manager-mcm-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "d8-cloud-instance-manager", "logs", "-l", "app=machine-controller-manager", "--tail=3000", "-c", "controller", "--ignore-errors=true"},
	},
	{
		File: "instance-manager-mcm-cloud-machines.json",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl -n d8-cloud-instance-manager get machines.machine.sapcloud.io -o json | jq '.items[]'`},
	},
	{
		File:           "d8-{module-name}-ccm-logs.txt",
		Cmd:            "kubectl",
		Args:           []string{"-n", "d8-{module-name}", "logs", "-l", "app=cloud-controller-manager", "--tail=3000"},
		RequiredModule: "cloud-provider",
	},
	{
		File:           "d8-{module-name}-csi-controller-logs.txt",
		Cmd:            "kubectl",
		Args:           []string{"-n", "d8-{module-name}", "logs", "-l", "app=csi-controller", "--tail=3000"},
		RequiredModule: "cloud-provider",
	},
	{
		File: "instance-manager-autoscaler-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "d8-cloud-instance-manager", "logs", "-l", "app=cluster-autoscaler", "--tail=5000", "-c", "cluster-autoscaler", "--ignore-errors=true"},
	},
	{
		File:           "d8-cert-manager-logs.txt",
		Cmd:            "kubectl",
		Args:           []string{"-n", "d8-cert-manager", "logs", "-l", "app=cert-manager", "--tail=3000", "--ignore-errors=true"},
		RequiredModule: "cert-manager",
	},
	{
		File:           "d8-cert-manager-all-certificate.json",
		Cmd:            "kubectl",
		Args:           []string{"get", "certificate", "-A", "-o", "json", "--ignore-not-found=true"},
		RequiredModule: "cert-manager",
	},
	{
		File: "kube-system-vpa-admission-controller-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "app=vpa-admission-controller", "--tail=3000", "-c", "admission-controller", "--ignore-errors=true"},
	},
	{
		File: "kube-system-vpa-recommender-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "app=vpa-recommender", "--tail=3000", "-c", "recommender", "--ignore-errors=true"},
	},
	{
		File: "kube-system-vpa-updater-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "app=vpa-updater", "--tail=3000", "-c", "updater", "--ignore-errors=true"},
	},
	{
		File: "monitoring-prometheus-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "d8-monitoring", "logs", "-l", "prometheus=main", "--tail=3000", "-c", "prometheus", "--ignore-errors=true"},
	},
	{
		File: "cluster-alerts.json",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl get clusteralerts.deckhouse.io -o json | jq '.items[]'`},
	},
	{
		File: "cluster-bad-pods.txt",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl get pod -A -owide | grep -Pv '\s+([1-9]+[\d]*)\/\1\s+' | grep -v 'Completed\|Evicted' | grep -E "^(d8-|kube-system)" || true`},
	},
	{
		File: "security-cluster-authorization-rules.json",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl get clusterauthorizationrules.deckhouse.io -A -o json | jq '.items[]'`},
	},
	{
		File: "security-authorization-rules.json",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl get authorizationrules.deckhouse.io -A -o json | jq '.items[]'`},
	},
	{
		File: "deckhouse-module-configs.json",
		Cmd:  "kubectl",
		Args: []string{"get", "moduleconfig", "-o", "json"},
	},
	{
		File:           "d8-istio-resources.json",
		Cmd:            "bash",
		Args:           []string{"-c", `kubectl -n d8-istio get all -o json | jq '.items[]'`},
		RequiredModule: "istio",
	},
	{
		File:           "d8-istio-custom-resources.json",
		Cmd:            "bash",
		Args:           []string{"-c", `for crd in $(kubectl get crds | grep -E 'istio.io|gateway.networking.k8s.io' | awk '{print $1}'); do echo "Listing resources for CRD: $crd" && kubectl get $crd -A -o json; done`},
		RequiredModule: "istio",
	},
	{
		File:           "d8-istio-envoy-config.json",
		Cmd:            "bash",
		Args:           []string{"-c", `kubectl port-forward daemonset/ingressgateway -n d8-istio 15000:15000 & sleep 5; (curl http://localhost:15000/config_dump?include_eds=true | jq 'del(.configs[6].dynamic_active_secrets)' && kill $!) || { kill $!; exit 0; }`},
		RequiredModule: "istio",
	},
	{
		File:           "d8-istio-system-logs.txt",
		Cmd:            "bash",
		Args:           []string{"-c", `kubectl -n d8-istio logs -l app=istiod || true`},
		RequiredModule: "istio",
	},
	{
		File:           "d8-istio-ingress-logs.txt",
		Cmd:            "bash",
		Args:           []string{"-c", `kubectl -n d8-istio logs daemonset/ingressgateway || true`},
		RequiredModule: "istio",
	},
	{
		File:           "d8-istio-users-logs.txt",
		Cmd:            "bash",
		Args:           []string{"-c", `kubectl get pods --all-namespaces -o jsonpath='{range .items[?(@.metadata.annotations.istio\.io/rev)]}{.metadata.namespace}{" "}{.metadata.name}{" "}{.spec.containers[*].name}{"\n"}{end}' | awk '/istio-proxy/ {print $0}' | shuf -n 1 | while read namespace pod_name containers; do echo "Collecting logs from istio-proxy in Pod $pod_name (Namespace: $namespace)"; kubectl logs "$pod_name" -n "$namespace" -c istio-proxy; done`},
		RequiredModule: "istio",
	},
	{
		File:           "network-cni-cilium-health-status.txt",
		Cmd:            "bash",
		Args:           []string{"-c", `kubectl -n d8-cni-cilium exec -it $(kubectl -n d8-cni-cilium get pod -o name | grep agent | head -n 1) -c cilium-agent -- cilium-health status`},
		RequiredModule: "cni-cilium",
	},
	{
		File: "kube-system-audit-policy.json",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "get", "secrets", "audit-policy", "-o", "json", "--ignore-not-found=true"},
	},
	{
		File: "kube-system-control-plane-manager-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "app=d8-control-plane-manager", "--tail=3000", "--ignore-errors=true"},
	},
	{
		File: "kube-system-etcd-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "component=etcd", "--tail=3000", "--ignore-errors=true"},
	},
	{
		File: "kube-system-kube-apiserver-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "component=kube-apiserver", "--tail=3000", "--ignore-errors=true"},
	},
	{
		File: "kube-system-kube-controller-manager-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "component=kube-controller-manager", "--tail=3000", "--ignore-errors=true"},
	},
	{
		File: "kube-system-kube-scheduler-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "component=kube-scheduler", "--tail=3000", "--ignore-errors=true"},
	},
	{
		File: "kube-system-kube-dns-logs.txt",
		Cmd:  "kubectl",
		Args: []string{"-n", "kube-system", "logs", "-l", "k8s-app=kube-dns", "--tail=3000", "--ignore-errors=true"},
	},
	{
		File: "monitoring-prometheusremotewrites.json",
		Cmd:  "kubectl",
		Args: []string{"get", "prometheusremotewrites", "-A", "-o", "json", "--ignore-not-found=true"},
	},
	{
		File: "other-mutatingwebhookconfigurations.json",
		Cmd:  "kubectl",
		Args: []string{"get", "mutatingwebhookconfigurations.admissionregistration.k8s.io", "-o", "json"},
	},
	{
		File: "other-validatingwebhookconfigurations.json",
		Cmd:  "kubectl",
		Args: []string{"get", "validatingwebhookconfigurations.admissionregistration.k8s.io", "-o", "json"},
	},
	{
		File: "other-storage-deckhouse-io-terminating.txt",
		Cmd:  "bash",
		Args: []string{"-c", `kubectl get $(kubectl api-resources --api-group=storage.deckhouse.io --verbs=list -o name | paste -sd, -) --ignore-not-found -A --chunk-size=200 -o json | jq -r '.items[] | select(.apiVersion == "storage.deckhouse.io/v1alpha1") | select(.metadata.deletionTimestamp != null) | "[\(.kind)] \(.metadata.namespace // "-")/\(.metadata.name)"'`},
	},
	{
		File: "network-ingressnginxcontrollers.json",
		Cmd:  "kubectl",
		Args: []string{"get", "ingressnginxcontrollers.deckhouse.io", "-o", "json", "--ignore-not-found=true"},
	},
	{
		File: "cluster-crd.json",
		Cmd:  "bash",
		// The OpenAPI schemas dominate the size of a full CRD dump (tens of MB on
		// a cluster with virtualization/istio/cilium/storage) without adding
		// diagnostic value, so they are dropped here instead of being buffered,
		// transferred and stored.
		Args: []string{"-c", `set -o pipefail; kubectl get customresourcedefinitions -o json | jq 'del(.items[].spec.versions[].schema)'`},
	},
	{
		File:           "d8-virtualization-dvcr-logs.txt",
		Cmd:            "kubectl",
		Args:           []string{"-n", "d8-virtualization", "logs", "-l", "app=dvcr", "--tail=3000", "--ignore-errors=true"},
		RequiredModule: "virtualization",
	},
	{
		File:           "d8-virtualization-virt-controller-logs.txt",
		Cmd:            "kubectl",
		Args:           []string{"-n", "d8-virtualization", "logs", "-l", "kubevirt.internal.virtualization.deckhouse.io=virt-controller", "--tail=3000", "--ignore-errors=true"},
		RequiredModule: "virtualization",
	},
	{
		File:           "d8-virtualization-controller-logs.txt",
		Cmd:            "kubectl",
		Args:           []string{"-n", "d8-virtualization", "logs", "-l", "app=virtualization-controller", "--tail=3000", "--ignore-errors=true"},
		RequiredModule: "virtualization",
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

	activeModules, modulesErr := fetchActiveModules(config, kubeCl, podName, namespace, containerName, commandTimeout)
	if modulesErr != nil {
		fmt.Fprintf(os.Stderr, "ERROR: could not fetch the list of active modules: %v\n", modulesErr)
		fmt.Fprintf(os.Stderr, "  collection continues without module filtering: module-gated commands run anyway and may produce empty files; per-module commands are skipped because their file names cannot be resolved (%s)\n",
			strings.Join(moduleScopedFiles(debugCommands), ", "))
	}

	commands, acceptedNames := filterAndExpandCommands(debugCommands, activeModules, modulesErr == nil, newExcludeSet(excludeFiles))

	if err := validateExcludeNames(excludeFiles, acceptedNames); err != nil {
		return err
	}

	return writeArchive(
		config, kubeCl, podName, namespace, containerName,
		commands, commandTimeout, requestInterval,
		"Collecting debug info from Deckhouse...",
		"Debug archive collection completed.",
	)
}

// writeArchive streams a gzipped tar of the given commands' output to stdout.
// It is the shared body of the debug archives: only the command set and the
// progress banners differ between them.
func writeArchive(
	config *rest.Config,
	kubeCl kubernetes.Interface,
	podName, namespace, containerName string,
	commands []Command,
	commandTimeout, requestInterval time.Duration,
	startBanner, doneBanner string,
) (err error) {
	gzipWriter := gzip.NewWriter(os.Stdout)
	tarWriter := tar.NewWriter(gzipWriter)

	defer func() {
		if closeErr := tarWriter.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to finalize tar archive: %w", closeErr)
		}

		if closeErr := gzipWriter.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to finalize gzip stream: %w", closeErr)
		}
	}()

	fmt.Fprintf(os.Stderr, "%s\n", startBanner)

	if err = runCommands(tarWriter, config, kubeCl, podName, namespace, containerName, commands, commandTimeout, requestInterval); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "%s\n", doneBanner)

	return nil
}

// moduleScopedFiles lists the File templates that can only be resolved with a
// known module list, for the warning printed when that list is unavailable.
func moduleScopedFiles(commands []Command) []string {
	var files []string

	for _, cmd := range commands {
		if cmd.RequiredModule != "" && needsModuleExpansion(cmd) {
			files = append(files, cmd.File)
		}
	}

	return files
}

// runCommands executes each command inside the Deckhouse pod and streams its
// output into the tar archive, honoring the optional rate limit between command
// executions. The command list is already filtered by the caller.
func runCommands(
	tarWriter *tar.Writer,
	config *rest.Config,
	kubeCl kubernetes.Interface,
	podName, namespace, containerName string,
	commands []Command,
	commandTimeout, requestInterval time.Duration,
) error {
	var tickCh <-chan time.Time

	if requestInterval > 0 {
		ticker := time.NewTicker(requestInterval)
		defer ticker.Stop()

		tickCh = ticker.C
	}

	for _, cmd := range commands {
		if tickCh != nil {
			<-tickCh
		}

		fullCommand := append([]string{cmd.Cmd}, cmd.Args...)

		cmdCtx, cancel := context.WithTimeout(context.Background(), commandTimeout)
		output, stderrOutput, streamErr := utilk8s.ExecCommandInPod(cmdCtx, config, kubeCl, fullCommand, podName, namespace, containerName)

		cancel()

		if streamErr != nil {
			if errors.Is(streamErr, context.DeadlineExceeded) {
				fmt.Fprintf(os.Stderr, "  WARNING: timed out collecting %s after %s\n", cmd.File, commandTimeout)
			} else {
				fmt.Fprintf(os.Stderr, "  ERROR: collecting %s: %s\n%s\n", cmd.File, strings.Join(fullCommand, " "), stderrOutput)
			}
		}

		if notice := defaultedContainerNotice(stderrOutput); notice != "" {
			output = append([]byte(notice), output...)
		}

		if err := cmd.writeToTar(tarWriter, output); err != nil {
			return fmt.Errorf("failed to write tar file %s: %w", cmd.File, err)
		}
	}

	return nil
}

// defaultedContainerNotice extracts kubectl's client-side "Defaulted container
// ... out of: ..." notice(s) from a command's stderr, so they can be prepended
// to the collected log output. Without this, the discarded stderr would take
// with it the only record of which container a `logs` command without
// -c/--all-containers actually collected from a multi-container pod.
func defaultedContainerNotice(stderrOutput string) string {
	var notice strings.Builder

	for _, line := range strings.Split(stderrOutput, "\n") {
		if strings.Contains(line, "Defaulted container") {
			notice.WriteString(line)
			notice.WriteString("\n")
		}
	}

	return notice.String()
}

// fetchActiveModules returns a map with the names of modules that are in the Ready phase.
func fetchActiveModules(
	config *rest.Config,
	kubeCl kubernetes.Interface,
	podName, namespace, containerName string,
	timeout time.Duration,
) (map[string]bool, error) {
	cmdLine := []string{"kubectl", "get", "module", "-o", "json"}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	stdout, stderr, err := utilk8s.ExecCommandInPod(ctx, config, kubeCl, cmdLine, podName, namespace, containerName)
	if err != nil {
		return nil, fmt.Errorf("stream kubectl get module: %w (stderr: %s)", err, stderr)
	}

	if len(stdout) == 0 {
		return nil, fmt.Errorf("kubectl get module returned no output (stderr: %s)", stderr)
	}

	var list moduleList
	if err = json.Unmarshal(stdout, &list); err != nil {
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

// filterAndExpandCommands selects the commands to run: it resolves the
// {module-name} placeholder against the active modules and drops the entries
// excluded on the command line. It also returns every name --exclude accepts
// for this run, including the names of entries these very excludes dropped, so
// a valid name is never reported as unknown.
//
// Exclusion happens here, and not further down, because this is the only place
// where both spellings of an entry are known at once: the resolved archive name
// (d8-cloud-provider-aws-ccm-logs.txt) and the module-independent token printed
// by --list-exclude (ccm-logs). The resolved name alone does not reveal which
// of its segments is the module.
//
// modulesKnown reports whether activeModules actually describes the cluster. It
// is false when the module list could not be fetched: module-gated commands are
// then collected anyway (an empty file beats a silently missing one), except
// those whose File carries the {module-name} placeholder — their archive entry
// name cannot be resolved, so they are skipped rather than stored under a
// literal placeholder name.
func filterAndExpandCommands(commands []Command, activeModules map[string]bool, modulesKnown bool, excludeSet map[string]bool) (selected []Command, acceptedNames []string) {
	selected = make([]Command, 0, len(commands))
	acceptedNames = make([]string, 0, len(commands))

	for _, cmd := range commands {
		// The token stays accepted even when the command is gated out below:
		// --exclude ccm-logs must not fail on a cluster without a cloud provider.
		token := excludeBaseName(cmd)
		acceptedNames = append(acceptedNames, token)

		if excludedByName(excludeSet, cmd.File, token) {
			continue
		}

		if cmd.RequiredModule == "" {
			selected = append(selected, cmd)
			continue
		}

		if !modulesKnown {
			if !needsModuleExpansion(cmd) {
				selected = append(selected, cmd)
			}

			continue
		}

		// No explicit guard for an empty activeModules is needed: both branches
		// below iterate the matching modules, of which there are none.
		if needsModuleExpansion(cmd) {
			matchedModules := matchingModules(activeModules, cmd.RequiredModule)
			for _, moduleName := range matchedModules {
				// Copy the command and overwrite only what is substituted, so a
				// field added to Command later cannot be silently dropped here.
				expanded := cmd
				expanded.File = strings.ReplaceAll(cmd.File, "{module-name}", moduleName)
				expanded.Args = replaceModuleName(cmd.Args, moduleName)

				acceptedNames = append(acceptedNames, expanded.File)

				if excludedByName(excludeSet, expanded.File) {
					continue
				}

				selected = append(selected, expanded)
			}
		} else {
			for moduleName := range activeModules {
				if isModuleMatch(moduleName, cmd.RequiredModule) {
					selected = append(selected, cmd)
					break
				}
			}
		}
	}

	return selected, acceptedNames
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

// needsModuleExpansion reports whether cmd must be duplicated once per active
// module matching RequiredModule (with {module-name} substituted into File
// and Args), rather than run once as-is.
func needsModuleExpansion(cmd Command) bool {
	if strings.Contains(cmd.File, "{module-name}") {
		return true
	}

	return slices.ContainsFunc(cmd.Args, func(arg string) bool {
		return strings.Contains(arg, "{module-name}")
	})
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

// trimArchiveExt drops the archive entry extension, so --exclude accepts a name
// with or without it.
func trimArchiveExt(name string) string {
	return strings.TrimSuffix(strings.TrimSuffix(name, ".json"), ".txt")
}

// excludeBaseName returns the --exclude token printed by --list-exclude for a
// command template: the archive entry name as written in debugCommands, or —
// when that name is per-module and therefore cluster-specific — the
// module-independent remainder (d8-{module-name}-ccm-logs.txt -> ccm-logs).
//
// The token is always derived from File, so a new per-module command needs no
// extra per-command data and cannot disagree with its own file name.
func excludeBaseName(cmd Command) string {
	if !strings.Contains(cmd.File, "{module-name}") {
		return cmd.File
	}

	name := strings.ReplaceAll(cmd.File, "d8-{module-name}-", "")
	name = strings.ReplaceAll(name, "-{module-name}-", "-")
	name = strings.ReplaceAll(name, "-{module-name}", "")
	name = strings.ReplaceAll(name, "{module-name}-", "")
	name = strings.ReplaceAll(name, "{module-name}", "")

	return trimArchiveExt(name)
}

// newExcludeSet normalizes the raw --exclude values into the form matched
// against command names: surrounding spaces and the extension are irrelevant.
func newExcludeSet(excludeFiles []string) map[string]bool {
	set := make(map[string]bool, len(excludeFiles))

	for _, name := range excludeFiles {
		name = trimArchiveExt(strings.TrimSpace(name))
		if name != "" {
			set[name] = true
		}
	}

	return set
}

// excludedByName reports whether any of the spellings of one archive entry was
// excluded on the command line. A name matches only that entry: there is no
// prefix or group matching, so --exclude d8 cannot silently drop every d8-* file.
func excludedByName(excludeSet map[string]bool, names ...string) bool {
	if len(excludeSet) == 0 {
		return false
	}

	for _, name := range names {
		if name != "" && excludeSet[trimArchiveExt(name)] {
			return true
		}
	}

	return false
}

// validateExcludeNames rejects --exclude values that cannot match any archive
// entry, so a typo is reported instead of quietly collecting the full archive.
// acceptedNames comes from filterAndExpandCommands and already covers both the
// resolved entry names of this run and the module-independent tokens.
func validateExcludeNames(excludeFiles, acceptedNames []string) error {
	known := make(map[string]bool, len(acceptedNames))
	accepted := make([]string, 0, len(acceptedNames))

	for _, name := range acceptedNames {
		key := trimArchiveExt(name)
		if key == "" || known[key] {
			continue
		}

		known[key] = true

		accepted = append(accepted, name)
	}

	var unknown []string

	for _, name := range excludeFiles {
		name = trimArchiveExt(strings.TrimSpace(name))
		if name != "" && !known[name] {
			unknown = append(unknown, name)
		}
	}

	if len(unknown) == 0 {
		return nil
	}

	return fmt.Errorf("unknown --exclude name(s): %s%s\nrun \"d8 system collect-debug-info --list-exclude\" to see the accepted names",
		strings.Join(unknown, ", "), suggestExcludeNames(unknown, accepted))
}

// suggestExcludeNames offers the accepted names that contain (or are contained
// in) an unknown one, which covers both typos and the group prefixes that used
// to match implicitly.
func suggestExcludeNames(unknown, accepted []string) string {
	const maxSuggestions = 5

	seen := make(map[string]bool, maxSuggestions)

	var matches []string

	for _, name := range unknown {
		for _, candidate := range accepted {
			key := trimArchiveExt(candidate)
			if seen[key] || !strings.Contains(key, name) && !strings.Contains(name, key) {
				continue
			}

			seen[key] = true

			matches = append(matches, candidate)
		}
	}

	if len(matches) == 0 {
		return ""
	}

	sort.Strings(matches)

	if len(matches) > maxSuggestions {
		return fmt.Sprintf("; did you mean one of: %s, ... (%d more)", strings.Join(matches[:maxSuggestions], ", "), len(matches)-maxSuggestions)
	}

	return fmt.Sprintf("; did you mean: %s", strings.Join(matches, ", "))
}

// GetExcludableFiles returns the tokens accepted by --exclude, one per archive
// entry, as printed by --list-exclude.
func GetExcludableFiles() []string {
	seen := make(map[string]bool, len(debugCommands))

	files := make([]string, 0, len(debugCommands))
	for _, cmd := range debugCommands {
		name := excludeBaseName(cmd)
		if !seen[name] {
			seen[name] = true
			files = append(files, name)
		}
	}

	sort.Strings(files)

	return files
}
