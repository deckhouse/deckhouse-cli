package rules

import (
	"context"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/diag"
)

// Rule purpose: reject ill-formed anyOf/noneOf module groups before the package is
// shipped, so the author sees the problem here rather than when the controller
// rejects the package at install time.

// ModuleGroupsRuleID is the stable identifier used to reference this rule in configuration.
const ModuleGroupsRuleID = "module-groups"

// moduleGroupsRule checks that anyOf/noneOf groups are well-formed and that no module
// is declared in contradictory buckets.
type moduleGroupsRule struct {
	collector *diag.Collector
	modules   packages.ModulesRequirements
}

// NewModuleGroupsRule constructs a rule that validates the anyOf/noneOf module groups.
func NewModuleGroupsRule(modules packages.ModulesRequirements, collector *diag.Collector) *moduleGroupsRule {
	return &moduleGroupsRule{
		modules:   modules,
		collector: collector.With(diag.RuleID(ModuleGroupsRuleID), diag.Path(packages.DefinitionFile)),
	}
}

// Check validates both group buckets, then the collisions between all buckets.
func (r *moduleGroupsRule) Check(_ context.Context) {
	anyOf := r.checkBucket(r.modules.AnyOf, "anyOf")
	noneOf := r.checkBucket(r.modules.NoneOf, "noneOf")

	r.checkCollisions(anyOf, noneOf)
}

// checkBucket validates one bucket of groups and returns its members as
// module name -> declaring group name, for the cross-bucket checks. The same
// module in two distinct groups of one bucket is allowed.
func (r *moduleGroupsRule) checkBucket(groups []packages.ModuleGroup, bucket string) map[string]string {
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
func (r *moduleGroupsRule) checkGroupModules(group packages.ModuleGroup, bucket string, members map[string]string) {
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

		if module.Constraint != "" {
			if _, err := semver.NewConstraint(module.Constraint); err != nil {
				r.collector.With(diag.Value(module.Constraint)).
					Error("%s group %q module %q: invalid version constraint", bucket, group.Name, module.Name)
			}
		}

		members[module.Name] = group.Name
	}
}

// checkCollisions reports modules declared in contradictory buckets: a module cannot be
// both required and forbidden, and a group member that is already an unconditional
// dependency makes the group dead weight.
func (r *moduleGroupsRule) checkCollisions(anyOf, noneOf map[string]string) {
	mandatory := moduleNames(r.modules.Mandatory)
	conditional := moduleNames(r.modules.Conditional)

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
