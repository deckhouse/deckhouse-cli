package debugtar

// This file holds the command table of the cluster-wide debug archive and
// nothing else: which command produces which archive entry. The table of the
// separate virtualization archive lives in virtualization.go, the shape of a
// single entry in commandtype.go, and the mechanics of selecting, excluding and
// running these commands in selection.go, exclude.go and archive.go.

// debugCommands - a complete list of commands for collecting debug information.
var debugCommands = []command{
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
