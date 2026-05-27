package rules

import (
	"context"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: forbid templates from hardcoding metadata.namespace so the runtime can place rendered resources in the instance namespace.

// InstanceNamespaceRuleID is the stable identifier used to reference this rule in configuration.
const InstanceNamespaceRuleID = "instance-namespace"

// InstanceNamespaceRule asserts no rendered object hardcodes metadata.namespace.
type InstanceNamespaceRule struct {
	collector *diag.Collector
	objects   []render.Object
}

// NewInstanceNamespaceRule constructs an InstanceNamespaceRule scoped to its rule identifier.
func NewInstanceNamespaceRule(objects []render.Object, collector *diag.Collector) *InstanceNamespaceRule {
	return &InstanceNamespaceRule{
		collector: collector.With(diag.RuleID(InstanceNamespaceRuleID)),
		objects:   objects,
	}
}

// Check verifies no rendered object hardcodes metadata.namespace.
func (r *InstanceNamespaceRule) Check(_ context.Context) {
	for _, obj := range r.objects {
		namespace := obj.GetNamespace()
		if namespace == "" {
			continue
		}

		r.collector.With(
			diag.ObjectID(obj.ObjectID()),
			diag.Path(obj.FilePath),
			diag.Value(namespace),
		).Error("object must not hardcode metadata.namespace; the runtime injects the instance namespace")
	}
}
