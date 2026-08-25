package rules

import (
	"context"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages/render"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: require every rendered object's name to start with the d8a- application
// marker followed by the instance name, so cluster resources are recognizable as
// Deckhouse-application-owned and scoped to the instance.

// InstancePrefixRuleID is the stable identifier used to reference this rule in configuration.
const InstancePrefixRuleID = "instance-prefix"

// d8aPrefix is the fixed application marker every rendered object's name must start with.
const d8aPrefix = "d8a-"

// instancePrefix is the hardcoded verify-time instance name from internal/packages.Render.
const instancePrefix = "test-"

// objectNameTemplatePrefix is the required template prefix shown in diagnostics.
const objectNameTemplatePrefix = "d8a-{{ .Application.Instance.Name }}-"

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
		name, ok := strings.CutPrefix(obj.GetName(), d8aPrefix)
		expectedName := objectNameTemplatePrefix + strings.TrimPrefix(name, instancePrefix)

		if !ok {
			r.collector.With(
				diag.ObjectID(obj.ObjectID()),
				diag.Path(obj.FilePath),
			).Error("object name does not start with the required d8a- prefix; it should be %q", expectedName)
		} else if !strings.HasPrefix(name, instancePrefix) {
			r.collector.With(
				diag.ObjectID(obj.ObjectID()),
				diag.Path(obj.FilePath),
			).Error("object name does not start with the required instance prefix; it should be %q", expectedName)
		}
	}

	r.collector.Commit()
}
