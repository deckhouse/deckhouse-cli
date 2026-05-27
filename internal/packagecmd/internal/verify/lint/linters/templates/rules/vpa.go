package rules

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/templates/rules/models"
)

// Rule purpose: require every pod controller to be covered by a VerticalPodAutoscaler with valid update mode and resource policy bounds.

// VPARuleID is the stable identifier used to reference this rule in configuration.
const VPARuleID = "vpa"

// VPARule asserts every pod controller has a matching VerticalPodAutoscaler
// with a valid update mode and resource policy bounds for every container.
type VPARule struct {
	collector *diag.Collector
	objects   []render.Object
}

// NewVPARule constructs a VPARule scoped to its rule identifier.
func NewVPARule(objects []render.Object, collector *diag.Collector) *VPARule {
	return &VPARule{
		collector: collector.With(diag.RuleID(VPARuleID)),
		objects:   objects,
	}
}

// vpaTarget uniquely identifies the controller a VPA targets within a namespace.
type vpaTarget struct {
	Namespace string
	Kind      string
	Name      string
}

// Check verifies VPA presence and configuration for every pod controller.
func (r *VPARule) Check(_ context.Context) {
	targets, containers, modes := r.collectVPAs()

	for _, obj := range r.objects {
		kind := obj.GetKind()
		if !isVPATargetKind(kind) {
			continue
		}

		target := vpaTarget{
			Namespace: obj.GetNamespace(),
			Kind:      kind,
			Name:      obj.GetName(),
		}

		collector := r.collector.With(
			diag.ObjectID(obj.ObjectID()),
			diag.Path(obj.FilePath),
		)

		if _, ok := targets[target]; !ok {
			collector.Error("no VPA is found for controller")
			continue
		}

		if modes[target] == models.UpdateModeOff {
			continue
		}

		ensureVPAContainersMatch(obj, containers[target], collector)
	}
}

// collectVPAs returns the set of VPA-covered targets along with their covered
// container names and configured update modes.
func (r *VPARule) collectVPAs() (
	map[vpaTarget]struct{},
	map[vpaTarget]map[string]struct{},
	map[vpaTarget]models.UpdateMode,
) {
	targets := make(map[vpaTarget]struct{})
	containers := make(map[vpaTarget]map[string]struct{})
	modes := make(map[vpaTarget]models.UpdateMode)

	for _, obj := range r.objects {
		if obj.GetKind() != "VerticalPodAutoscaler" {
			continue
		}

		collector := r.collector.With(
			diag.ObjectID(obj.ObjectID()),
			diag.Path(obj.FilePath),
		)

		vpa, ok := parseVPA(obj.Unstructured, collector)
		if !ok {
			continue
		}

		if vpa.Spec.TargetRef == nil || vpa.Spec.TargetRef.Kind == "" || vpa.Spec.TargetRef.Name == "" {
			collector.Error("no VPA spec.targetRef is found for object")
			continue
		}

		target := vpaTarget{
			Namespace: obj.GetNamespace(),
			Kind:      vpa.Spec.TargetRef.Kind,
			Name:      vpa.Spec.TargetRef.Name,
		}
		targets[target] = struct{}{}

		mode := vpaUpdateMode(vpa)
		modes[target] = mode

		if mode == models.UpdateModeOff {
			continue
		}

		names, ok := validateVPAContainers(vpa, mode, collector)
		if !ok {
			continue
		}

		containers[target] = names
	}

	return targets, containers, modes
}

// parseVPA converts an unstructured VPA object into a typed VerticalPodAutoscaler.
func parseVPA(u *unstructured.Unstructured, collector *diag.Collector) (models.VerticalPodAutoscaler, bool) {
	var vpa models.VerticalPodAutoscaler
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.UnstructuredContent(), &vpa); err != nil {
		collector.Error("cannot unmarshal VPA object: %v", err)
		return models.VerticalPodAutoscaler{}, false
	}

	return vpa, true
}

// vpaUpdateMode returns the configured update mode, defaulting to Auto when omitted (matches upstream VPA default).
func vpaUpdateMode(vpa models.VerticalPodAutoscaler) models.UpdateMode {
	if vpa.Spec.UpdatePolicy == nil || vpa.Spec.UpdatePolicy.UpdateMode == nil {
		return models.UpdateModeAuto
	}

	return *vpa.Spec.UpdatePolicy.UpdateMode
}

// validateVPAContainers checks each container policy's CPU/memory bounds and returns the covered container names.
func validateVPAContainers(vpa models.VerticalPodAutoscaler, mode models.UpdateMode, collector *diag.Collector) (map[string]struct{}, bool) {
	if vpa.Spec.ResourcePolicy == nil || len(vpa.Spec.ResourcePolicy.ContainerPolicies) == 0 {
		collector.Error("no VPA spec.resourcePolicy.containerPolicies is found for object")
		return nil, false
	}

	if mode == models.UpdateModeAuto {
		collector.Error("VPA updateMode cannot be 'Auto' as it is deprecated; use 'InPlaceOrRecreate' instead")
	}

	if !isValidUpdateMode(mode) {
		collector.Error("invalid updateMode %q; allowed values are: Off, Initial, Recreate, InPlaceOrRecreate", mode)
	}

	names := make(map[string]struct{}, len(vpa.Spec.ResourcePolicy.ContainerPolicies))
	for _, cp := range vpa.Spec.ResourcePolicy.ContainerPolicies {
		validateContainerPolicy(cp, collector)
		names[cp.ContainerName] = struct{}{}
	}

	return names, true
}

// validateContainerPolicy emits diagnostics for missing or contradictory CPU/memory bounds on a single container policy.
func validateContainerPolicy(cp models.ContainerResourcePolicy, collector *diag.Collector) {
	minCPU := cp.MinAllowed[corev1.ResourceCPU]
	minMem := cp.MinAllowed[corev1.ResourceMemory]
	maxCPU := cp.MaxAllowed[corev1.ResourceCPU]
	maxMem := cp.MaxAllowed[corev1.ResourceMemory]

	if minCPU.IsZero() {
		collector.Error("no VPA spec minAllowed.cpu is found for container %s", cp.ContainerName)
	}

	if minMem.IsZero() {
		collector.Error("no VPA spec minAllowed.memory is found for container %s", cp.ContainerName)
	}

	if maxCPU.IsZero() {
		collector.Error("no VPA spec maxAllowed.cpu is found for container %s", cp.ContainerName)
	}

	if maxMem.IsZero() {
		collector.Error("no VPA spec maxAllowed.memory is found for container %s", cp.ContainerName)
	}

	if minCPU.Cmp(maxCPU) > 0 {
		collector.Error("minAllowed.cpu for container %s should be less than maxAllowed.cpu", cp.ContainerName)
	}

	if minMem.Cmp(maxMem) > 0 {
		collector.Error("minAllowed.memory for container %s should be less than maxAllowed.memory", cp.ContainerName)
	}
}

// isValidUpdateMode reports whether mode is one of the accepted VPA update modes.
func isValidUpdateMode(mode models.UpdateMode) bool {
	switch mode {
	case models.UpdateModeOff,
		models.UpdateModeInitial,
		models.UpdateModeRecreate,
		models.UpdateModeInPlaceOrReacreate,
		models.UpdateModeAuto:
		return true
	default:
		return false
	}
}

// ensureVPAContainersMatch reports controller containers missing from the VPA, and vice versa.
func ensureVPAContainersMatch(controller render.Object, vpaContainers map[string]struct{}, collector *diag.Collector) {
	if vpaContainers == nil {
		collector.Error("getting VPA container name list for the object failed")
		return
	}

	names, err := podControllerContainerNames(controller.Unstructured)
	if err != nil {
		collector.Error("getting containers list for the object failed: %v", err)
		return
	}

	id := controller.ObjectID()

	for name := range names {
		if _, ok := vpaContainers[name]; !ok {
			collector.With(diag.ObjectID(fmt.Sprintf("%s ; container = %s", id, name))).
				Error("the container should have corresponding VPA resourcePolicy entry")
		}
	}

	for name := range vpaContainers {
		if _, ok := names[name]; !ok {
			collector.Error("VPA has resourcePolicy for container %s, but the controller does not have corresponding container resource entry", name)
		}
	}
}

// podControllerContainerNames extracts container names from a controller pod template.
func podControllerContainerNames(u *unstructured.Unstructured) (map[string]struct{}, error) {
	containers, found, err := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
	if err != nil {
		return nil, fmt.Errorf("read containers: %w", err)
	}

	if !found {
		return map[string]struct{}{}, nil
	}

	names := make(map[string]struct{}, len(containers))
	for _, item := range containers {
		cm, ok := item.(map[string]any)
		if !ok {
			continue
		}

		name, _ := cm["name"].(string)
		names[name] = struct{}{}
	}

	return names, nil
}

// isVPATargetKind reports whether kind is a workload kind that requires a VPA.
func isVPATargetKind(kind string) bool {
	return kind == "Deployment" || kind == "DaemonSet" || kind == "StatefulSet"
}
