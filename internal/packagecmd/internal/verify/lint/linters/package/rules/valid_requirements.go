package rules

import (
	"context"
	"fmt"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: reject ill-formed requirements before the package is shipped.

// ValidRequirementsRuleID is the stable identifier used to reference this rule in diagnostics.
const ValidRequirementsRuleID = "valid_requirements"

// ValidRequirementsRule checks that package.yaml requirements are valid and consistent.
type ValidRequirementsRule struct {
	collector    *diag.Collector
	requirements packages.Requirements
}

// NewValidRequirementsRule constructs a rule that validates the requirements block.
func NewValidRequirementsRule(requirements packages.Requirements, collector *diag.Collector) *ValidRequirementsRule {
	return &ValidRequirementsRule{
		requirements: requirements,
		collector:    collector.With(diag.RuleID(ValidRequirementsRuleID), diag.Path(packages.DefinitionFile)),
	}
}

// Check validates platform constraints, module constraints and module groups.
func (r *ValidRequirementsRule) Check(_ context.Context) {
	r.checkConstraint("requirements.kubernetes.constraint", r.requirements.Kubernetes.Constraint)
	r.checkConstraint("requirements.deckhouse.constraint", r.requirements.Deckhouse.Constraint)

	for _, dep := range r.requirements.Modules.Mandatory {
		r.checkConstraint(fmt.Sprintf("mandatory module %q", dep.Name), dep.Constraint)
	}

	for _, dep := range r.requirements.Modules.Conditional {
		r.checkConstraint(fmt.Sprintf("conditional module %q", dep.Name), dep.Constraint)
	}

	anyOf := r.checkBucket(r.requirements.Modules.AnyOf, "anyOf")
	noneOf := r.checkBucket(r.requirements.Modules.NoneOf, "noneOf")

	r.checkCollisions(anyOf, noneOf)
}

// checkConstraint reports a finding when constraint is set but is not valid semver.
func (r *ValidRequirementsRule) checkConstraint(subject, constraint string) {
	if constraint == "" {
		return
	}

	if _, err := semver.NewConstraint(constraint); err != nil {
		r.collector.With(diag.Value(constraint)).Error("%s: invalid version constraint", subject)
	}
}

// checkBucket validates one bucket of groups and returns its members.
func (r *ValidRequirementsRule) checkBucket(groups []packages.ModuleGroup, bucket string) map[string]string {
	members := make(map[string]string)
	seenGroups := make(map[string]struct{}, len(groups))

	for i, group := range groups {
		if group.Name == "" {
			r.collector.Error("%s group [%d]: name is required", bucket, i)
			continue
		}

		if _, dup := seenGroups[group.Name]; dup {
			r.collector.With(diag.Value(group.Name)).
				Error("%s group %q: duplicate group name", bucket, group.Name)

			continue
		}

		seenGroups[group.Name] = struct{}{}

		if len(group.Modules) == 0 {
			r.collector.With(diag.Value(group.Name)).
				Error("%s group %q: at least one module is required", bucket, group.Name)

			continue
		}

		r.checkGroupModules(group, bucket, members)
	}

	return members
}

// checkGroupModules validates the modules of one group and records them in members.
func (r *ValidRequirementsRule) checkGroupModules(group packages.ModuleGroup, bucket string, members map[string]string) {
	seen := make(map[string]struct{}, len(group.Modules))

	for _, module := range group.Modules {
		if module.Name == "" {
			r.collector.With(diag.Value(group.Name)).
				Error("%s group %q: module name is required", bucket, group.Name)

			continue
		}

		if _, dup := seen[module.Name]; dup {
			r.collector.With(diag.Value(module.Name)).
				Error("%s group %q: duplicate module %q", bucket, group.Name, module.Name)

			continue
		}

		seen[module.Name] = struct{}{}

		r.checkConstraint(fmt.Sprintf("%s group %q module %q", bucket, group.Name, module.Name), module.Constraint)

		members[module.Name] = group.Name
	}
}

// checkCollisions reports modules declared in contradictory buckets.
func (r *ValidRequirementsRule) checkCollisions(anyOf, noneOf map[string]string) {
	mandatory := moduleNames(r.requirements.Modules.Mandatory)
	conditional := moduleNames(r.requirements.Modules.Conditional)

	for name := range conditional {
		if _, dup := mandatory[name]; dup {
			r.collector.With(diag.Value(name)).Error("module %q is both mandatory and conditional", name)
		}
	}

	for name, group := range anyOf {
		if _, dup := mandatory[name]; dup {
			r.collector.With(diag.Value(name)).
				Error("module %q is both mandatory and a member of anyOf group %q", name, group)
		}

		if _, dup := conditional[name]; dup {
			r.collector.With(diag.Value(name)).
				Error("module %q is both conditional and a member of anyOf group %q", name, group)
		}
	}

	for name, group := range noneOf {
		if _, dup := mandatory[name]; dup {
			r.collector.With(diag.Value(name)).
				Error("module %q is both mandatory and a member of noneOf group %q", name, group)
		}

		if _, dup := conditional[name]; dup {
			r.collector.With(diag.Value(name)).
				Error("module %q is both conditional and a member of noneOf group %q", name, group)
		}

		if anyGroup, dup := anyOf[name]; dup {
			r.collector.With(diag.Value(name)).
				Error("module %q is a member of both anyOf group %q and noneOf group %q", name, anyGroup, group)
		}
	}
}

// moduleNames collects the declared module names of a flat bucket.
func moduleNames(deps []packages.ModuleDependency) map[string]struct{} {
	out := make(map[string]struct{}, len(deps))
	for _, dep := range deps {
		out[dep.Name] = struct{}{}
	}

	return out
}
