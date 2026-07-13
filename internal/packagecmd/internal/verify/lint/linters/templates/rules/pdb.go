package rules

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require every pod controller (except DaemonSets) to be covered by a PodDisruptionBudget in the same namespace.

// PDBRuleID is the stable identifier used to reference this rule in configuration.
const PDBRuleID = "pdb"

// PDBRule asserts every pod controller (Deployment, StatefulSet) is covered by
// a PodDisruptionBudget whose selector matches the controller's pod template labels.
type PDBRule struct {
	collector *diag.Collector
	objects   []render.Object
}

// NewPDBRule constructs a PDBRule scoped to its rule identifier.
func NewPDBRule(objects []render.Object, collector *diag.Collector) *PDBRule {
	return &PDBRule{
		collector: collector.With(diag.RuleID(PDBRuleID)),
		objects:   objects,
	}
}

// pdbSelector pairs a parsed PDB label selector with its namespace.
type pdbSelector struct {
	namespace string
	selector  labels.Selector
}

// Check verifies every pod controller has a matching PDB.
func (r *PDBRule) Check(_ context.Context) {
	selectors := r.collectPDBSelectors()

	for _, obj := range r.objects {
		if !isPDBTargetKind(obj.GetKind()) {
			continue
		}

		collector := r.collector.With(
			diag.ObjectID(obj.ObjectID()),
			diag.Path(obj.FilePath),
		)

		if len(selectors) == 0 {
			collector.Error("no PodDisruptionBudget found for controller")
			return
		}

		ensurePDBMatches(obj, selectors, collector)
	}
}

// collectPDBSelectors parses every PDB object into a namespace-scoped selector.
func (r *PDBRule) collectPDBSelectors() []pdbSelector {
	var selectors []pdbSelector

	for _, obj := range r.objects {
		if obj.GetKind() != "PodDisruptionBudget" {
			continue
		}

		collector := r.collector.With(
			diag.ObjectID(obj.ObjectID()),
			diag.Path(obj.FilePath),
		)

		sel, ok := parsePDBSelector(obj.Unstructured, collector)
		if !ok {
			continue
		}

		selectors = append(selectors, pdbSelector{
			namespace: obj.GetNamespace(),
			selector:  sel,
		})
	}

	return selectors
}

// ensurePDBMatches emits an error when no PDB selector matches the controller pod template labels.
func ensurePDBMatches(controller render.Object, selectors []pdbSelector, collector *diag.Collector) {
	podLabels, err := podTemplateLabels(controller.Unstructured)
	if err != nil {
		collector.Error("cannot parse pod controller: %v", err)
		return
	}

	namespace := controller.GetNamespace()
	labelSet := labels.Set(podLabels)

	for _, sel := range selectors {
		if sel.namespace == namespace && sel.selector.Matches(labelSet) {
			return
		}
	}

	collector.With(diag.Value(labelSet)).
		Error("no PodDisruptionBudget matches pod labels of the controller")
}

// parsePDBSelector reads the PDB spec.selector into a labels.Selector and rejects PDBs carrying helm hook annotations.
func parsePDBSelector(u *unstructured.Unstructured, collector *diag.Collector) (labels.Selector, bool) {
	annotations := u.GetAnnotations()
	if annotations["helm.sh/hook"] != "" || annotations["helm.sh/hook-delete-policy"] != "" {
		collector.Error("PDB must have no helm hook annotations")
		return nil, false
	}

	rawSelector, found, err := unstructured.NestedMap(u.Object, "spec", "selector")
	if err != nil {
		collector.Error("cannot read PDB selector: %v", err)
		return nil, false
	}

	if !found {
		collector.Error("PDB spec.selector is missing")
		return nil, false
	}

	var labelSelector metav1.LabelSelector
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(rawSelector, &labelSelector); err != nil {
		collector.Error("cannot parse PDB selector: %v", err)
		return nil, false
	}

	sel, err := metav1.LabelSelectorAsSelector(&labelSelector)
	if err != nil {
		collector.Error("cannot parse label selector: %v", err)
		return nil, false
	}

	return sel, true
}

// podTemplateLabels returns spec.template.metadata.labels for a pod controller.
func podTemplateLabels(u *unstructured.Unstructured) (map[string]string, error) {
	lbls, _, err := unstructured.NestedStringMap(u.Object, "spec", "template", "metadata", "labels")
	if err != nil {
		return nil, fmt.Errorf("read template labels: %w", err)
	}

	return lbls, nil
}

// isPDBTargetKind reports whether kind must be covered by a PDB.
func isPDBTargetKind(kind string) bool {
	return kind == "Deployment" || kind == "StatefulSet"
}
