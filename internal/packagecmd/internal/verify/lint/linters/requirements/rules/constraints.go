package rules

import (
	"context"
	"fmt"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: reject unparseable version constraints before the package is shipped.
// Constraints inside anyOf/noneOf groups are covered by the module-groups rule.

// ConstraintsRuleID is the stable identifier used to reference this rule in configuration.
const ConstraintsRuleID = "constraints"

// constraintsRule checks that the platform and flat module constraints are valid semver.
type constraintsRule struct {
	collector    *diag.Collector
	requirements packages.Requirements
}

// NewConstraintsRule constructs a rule that validates the declared version constraints.
func NewConstraintsRule(requirements packages.Requirements, collector *diag.Collector) *constraintsRule {
	return &constraintsRule{
		requirements: requirements,
		collector:    collector.With(diag.RuleID(ConstraintsRuleID), diag.Path(packages.DefinitionFile)),
	}
}

// Check validates the kubernetes, deckhouse, mandatory and conditional constraints.
func (r *constraintsRule) Check(_ context.Context) {
	r.checkConstraint("requirements.kubernetes.constraint", r.requirements.Kubernetes.Constraint)
	r.checkConstraint("requirements.deckhouse.constraint", r.requirements.Deckhouse.Constraint)

	for _, dep := range r.requirements.Modules.Mandatory {
		r.checkConstraint(fmt.Sprintf("mandatory module %q", dep.Name), dep.Constraint)
	}

	for _, dep := range r.requirements.Modules.Conditional {
		r.checkConstraint(fmt.Sprintf("conditional module %q", dep.Name), dep.Constraint)
	}
}

// checkConstraint reports a finding when constraint is set but is not valid semver.
// An empty constraint is allowed and means "any version".
func (r *constraintsRule) checkConstraint(subject, constraint string) {
	if constraint == "" {
		return
	}

	if _, err := semver.NewConstraint(constraint); err != nil {
		r.collector.With(diag.Value(constraint)).Error("%s: invalid version constraint", subject)
	}
}
