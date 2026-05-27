package rules

import (
	"context"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/linters/oss/model"
)

// FieldsRuleID is the stable identifier used to reference this rule in configuration.
const FieldsRuleID = "fields"

// FieldsRule validates required non-empty component fields in oss.yaml.
type FieldsRule struct {
	collector  *diag.Collector
	components []model.Component
}

// NewFieldsRule constructs a FieldsRule over parsed oss components.
func NewFieldsRule(components []model.Component, collector *diag.Collector) *FieldsRule {
	return &FieldsRule{
		collector:  collector.With(diag.RuleID(FieldsRuleID)),
		components: components,
	}
}

// Check validates required component fields.
func (r *FieldsRule) Check(_ context.Context) {
	for idx, component := range r.components {
		collector := r.collector.With(diag.ObjectID(model.ComponentObjectID(idx, component)))

		r.checkField(collector, "id", component.ID)
		r.checkField(collector, "name", component.Name)
		r.checkField(collector, "description", component.Description)
		r.checkField(collector, "link", component.Link)
		r.checkField(collector, "license", component.License)
	}
}

// checkField emits an error when value is empty after trimming whitespace.
func (r *FieldsRule) checkField(collector *diag.Collector, name, value string) {
	if strings.TrimSpace(value) != "" {
		return
	}

	collector.Error("%s field is required and must not be empty", name)
}
