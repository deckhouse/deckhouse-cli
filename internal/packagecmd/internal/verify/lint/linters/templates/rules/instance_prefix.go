package rules

import (
	"context"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require every rendered object's name to start with the application instance prefix so cluster resources are scoped to the instance.

// InstancePrefixRuleID is the stable identifier used to reference this rule in configuration.
const InstancePrefixRuleID = "instance-prefix"

// instancePrefix is the prefix every rendered object's name must start with.
// It tracks the hardcoded verify-time instance name from internal/packages.Render.
const instancePrefix = "test-"

// InstancePrefixRule asserts every rendered object's name starts with the instance prefix.
type InstancePrefixRule struct {
	collector *diag.Collector
	objects   []render.Object
}

// NewInstancePrefixRule constructs an InstancePrefixRule scoped to its rule identifier.
func NewInstancePrefixRule(objects []render.Object, collector *diag.Collector) *InstancePrefixRule {
	return &InstancePrefixRule{
		collector: collector.With(diag.RuleID(InstancePrefixRuleID)),
		objects:   objects,
	}
}

// Check verifies every rendered object's name starts with the instance prefix.
func (r *InstancePrefixRule) Check(_ context.Context) {
	for _, obj := range r.objects {
		name := obj.GetName()
		if strings.HasPrefix(name, instancePrefix) {
			continue
		}

		r.collector.With(
			diag.ObjectID(obj.ObjectID()),
			diag.Path(obj.FilePath),
		).Error("object name does not start with the instance prefix")
	}
}
