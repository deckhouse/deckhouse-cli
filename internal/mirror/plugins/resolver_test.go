/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plugins

import (
	"context"
	"fmt"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse/pkg/log"
	dkpclient "github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
)

// catalogStub is an in-memory Catalog for resolver tests. The invariant of a
// real catalog holds: every listed tag has a contract behind it.
type catalogStub struct {
	names      []string
	namesErr   error
	tags       map[string][]string
	contracts  map[string]*internal.Plugin // "name@tag"
	invalid    map[string]bool             // "name@tag" -> broken published contract
	transport  map[string]bool             // "name@tag" -> transport error on contract fetch
	namesCalls int
}

func newStub() *catalogStub {
	return &catalogStub{
		tags:      make(map[string][]string),
		contracts: make(map[string]*internal.Plugin),
		invalid:   make(map[string]bool),
		transport: make(map[string]bool),
	}
}

func (s *catalogStub) add(name, tag string, contract *internal.Plugin) *catalogStub {
	s.registerName(name)
	s.tags[name] = append(s.tags[name], tag)
	s.contracts[name+"@"+tag] = contract

	return s
}

func (s *catalogStub) markInvalid(name, tag string) *catalogStub {
	s.registerName(name)
	s.tags[name] = append(s.tags[name], tag)
	s.invalid[name+"@"+tag] = true

	return s
}

func (s *catalogStub) failTransport(name, tag string) *catalogStub {
	s.registerName(name)
	s.tags[name] = append(s.tags[name], tag)
	s.transport[name+"@"+tag] = true

	return s
}

func (s *catalogStub) registerName(name string) {
	for _, existing := range s.names {
		if existing == name {
			return
		}
	}

	s.names = append(s.names, name)
}

func (s *catalogStub) PluginNames(_ context.Context) ([]string, error) {
	s.namesCalls++

	if s.namesErr != nil {
		return nil, s.namesErr
	}

	return s.names, nil
}

func (s *catalogStub) PluginVersions(_ context.Context, name string) ([]*semver.Version, error) {
	tags, ok := s.tags[name]
	if !ok {
		return nil, fmt.Errorf("list versions of plugin %q: %w", name, dkpclient.ErrImageNotFound)
	}

	return stableVersions(sortedSemverDesc(tags)), nil
}

func (s *catalogStub) Contract(_ context.Context, name string, version *semver.Version) (*internal.Plugin, error) {
	ref := name + "@" + version.Original()

	if s.transport[ref] {
		return nil, fmt.Errorf("get contract of plugin %q %s: connection reset", name, version.Original())
	}

	if s.invalid[ref] {
		return nil, invalidContract(name, version.Original(), fmt.Errorf("boom"))
	}

	contract, ok := s.contracts[ref]
	if !ok {
		return nil, fmt.Errorf("get contract of plugin %q %s: %w", name, version.Original(), dkpclient.ErrImageNotFound)
	}

	return contract, nil
}

// ---- contract builders ----

func plug(name, version string) *internal.Plugin {
	return &internal.Plugin{Name: name, Version: version}
}

func needsModule(p *internal.Plugin, module, constraint string) *internal.Plugin {
	p.Requirements.Modules.Mandatory = append(p.Requirements.Modules.Mandatory,
		internal.ModuleRequirement{Name: module, Constraint: constraint})

	return p
}

func condModule(p *internal.Plugin, module, constraint string) *internal.Plugin {
	p.Requirements.Modules.Conditional = append(p.Requirements.Modules.Conditional,
		internal.ModuleRequirement{Name: module, Constraint: constraint})

	return p
}

func needsAnyOf(p *internal.Plugin, group string, members ...internal.ModuleRequirement) *internal.Plugin {
	p.Requirements.Modules.AnyOf = append(p.Requirements.Modules.AnyOf,
		internal.ModuleGroup{Name: group, Modules: members})

	return p
}

func needsPlugin(p *internal.Plugin, name, constraint string) *internal.Plugin {
	p.Requirements.Plugins.Mandatory = append(p.Requirements.Plugins.Mandatory,
		internal.PluginRequirement{Name: name, Constraint: constraint})

	return p
}

func needsDeckhouse(p *internal.Plugin, constraint string) *internal.Plugin {
	p.Requirements.Deckhouse.Constraint = constraint

	return p
}

// ---- input/output helpers ----

func mod(name string, versions ...string) ModuleInBundle {
	return ModuleInBundle{Name: name, Versions: semvers(versions...)}
}

func semvers(versions ...string) []*semver.Version {
	out := make([]*semver.Version, 0, len(versions))
	for _, v := range versions {
		out = append(out, semver.MustParse(v))
	}

	return out
}

func mustFilter(t *testing.T, expressions ...string) *modules.Filter {
	t.Helper()

	filter, err := modules.NewFilter(expressions, modules.FilterTypeWhitelist)
	require.NoError(t, err)

	return filter
}

func resolve(t *testing.T, stub *catalogStub, in ResolveInput) *Resolution {
	t.Helper()

	res, err := NewResolver(stub, log.NewNop()).Resolve(context.Background(), in)
	require.NoError(t, err)

	return res
}

func selectedVersions(res *Resolution, name string) []string {
	for _, p := range res.Plugins {
		if p.Name != name {
			continue
		}

		out := make([]string, 0, len(p.Versions))
		for _, sv := range p.Versions {
			out = append(out, sv.Version.Original())
		}

		return out
	}

	return nil
}

func selectedVersion(t *testing.T, res *Resolution, name, version string) SelectedVersion {
	t.Helper()

	for _, p := range res.Plugins {
		if p.Name != name {
			continue
		}

		for _, sv := range p.Versions {
			if sv.Version.Original() == version {
				return sv
			}
		}
	}

	t.Fatalf("plugin %s@%s is not selected; resolution: %+v", name, version, res.Plugins)

	return SelectedVersion{}
}

// ---- tests ----

// TestResolve_PerModuleVersionSelection is the requirement's core example:
// for EACH bundled module version the newest compatible plugin version is
// picked, and picks are deduplicated. Module postgresql {1.0.0, 1.5.0,
// 1.10.0}: plugin v1.1.0 covers 1.0.0, v1.2.0 covers both 1.5.0 and 1.10.0 -
// the bundle gets exactly {v1.1.0, v1.2.0}.
func TestResolve_PerModuleVersionSelection(t *testing.T) {
	stub := newStub().
		add("postgresql-mgr", "v1.1.0", needsModule(plug("postgresql-mgr", "v1.1.0"), "postgresql", ">=1.0.0 <1.5.0")).
		add("postgresql-mgr", "v1.2.0", needsModule(plug("postgresql-mgr", "v1.2.0"), "postgresql", ">=1.5.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.0.0", "v1.5.0", "v1.10.0")},
	})

	assert.Equal(t, []string{"v1.2.0", "v1.1.0"}, selectedVersions(res, "postgresql-mgr"),
		"each bundled module version gets its newest compatible plugin version, deduplicated")
	assert.Empty(t, res.Skipped)

	sv := selectedVersion(t, res, "postgresql-mgr", "v1.2.0")
	assert.Contains(t, sv.Reasons, Reason{Kind: ReasonModule, Subject: "postgresql", Constraint: ">=1.5.0"})
}

// TestResolve_TransitiveDependency: a selected plugin drags its mandatory
// plugin dependency into the bundle, newest satisfying version.
func TestResolve_TransitiveDependency(t *testing.T) {
	stub := newStub().
		add("postgresql-mgr", "v1.2.0",
			needsPlugin(needsModule(plug("postgresql-mgr", "v1.2.0"), "postgresql", ">=1.0.0"), "db-connector", ">=0.9.0")).
		add("db-connector", "v0.8.0", plug("db-connector", "v0.8.0")).
		add("db-connector", "v0.9.1", plug("db-connector", "v0.9.1"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})

	assert.Equal(t, []string{"v1.2.0"}, selectedVersions(res, "postgresql-mgr"))
	assert.Equal(t, []string{"v0.9.1"}, selectedVersions(res, "db-connector"))

	dep := selectedVersion(t, res, "db-connector", "v0.9.1")
	assert.Contains(t, dep.Reasons, Reason{Kind: ReasonDependency, Subject: "postgresql-mgr@v1.2.0", Constraint: ">=0.9.0"})
}

// TestResolve_DependencyUnionReuse: two dependents with overlapping
// constraints share one version of the dependency instead of pulling two.
func TestResolve_DependencyUnionReuse(t *testing.T) {
	stub := newStub().
		add("alpha", "v1.0.0", needsPlugin(needsModule(plug("alpha", "v1.0.0"), "m1", ""), "shared", ">=1.0.0")).
		add("beta", "v1.0.0", needsPlugin(needsModule(plug("beta", "v1.0.0"), "m1", ""), "shared", ">=1.2.0")).
		add("shared", "v1.3.0", plug("shared", "v1.3.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("m1", "v1.0.0")},
	})

	assert.Equal(t, []string{"v1.3.0"}, selectedVersions(res, "shared"),
		"one shared version must satisfy both dependents")

	sv := selectedVersion(t, res, "shared", "v1.3.0")
	assert.Contains(t, sv.Reasons, Reason{Kind: ReasonDependency, Subject: "alpha@v1.0.0", Constraint: ">=1.0.0"})
	assert.Contains(t, sv.Reasons, Reason{Kind: ReasonDependency, Subject: "beta@v1.0.0", Constraint: ">=1.2.0"})
}

// TestResolve_DisjointDependencyConstraints: dependents whose constraints
// cannot be satisfied by one version get two versions of the dependency -
// each dependent stays installable on the air-gapped side.
func TestResolve_DisjointDependencyConstraints(t *testing.T) {
	stub := newStub().
		add("alpha", "v1.0.0", needsPlugin(needsModule(plug("alpha", "v1.0.0"), "m1", ""), "shared", "<=1.0.0")).
		add("beta", "v1.0.0", needsPlugin(needsModule(plug("beta", "v1.0.0"), "m1", ""), "shared", ">=2.0.0")).
		add("shared", "v1.0.0", plug("shared", "v1.0.0")).
		add("shared", "v2.1.0", plug("shared", "v2.1.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("m1", "v1.0.0")},
	})

	assert.Equal(t, []string{"v2.1.0", "v1.0.0"}, selectedVersions(res, "shared"),
		"disjoint constraints require two bundled versions of the dependency")
}

// TestResolve_BuiltinDependency: a dependency on a built-in d8 command is
// satisfied by presence - nothing is pulled and the catalog is not asked. The
// stub has no "package" entry, so any lookup would fail the dependent.
func TestResolve_BuiltinDependency(t *testing.T) {
	stub := newStub().
		add("packer", "v1.0.0", needsPlugin(needsModule(plug("packer", "v1.0.0"), "m1", ""), "package", ""))

	res := resolve(t, stub, ResolveInput{
		Modules:  []ModuleInBundle{mod("m1", "v1.0.0")},
		Builtins: map[string]struct{}{"package": {}},
	})

	assert.Equal(t, []string{"v1.0.0"}, selectedVersions(res, "packer"))
	assert.Nil(t, selectedVersions(res, "package"), "built-ins are never pulled")
	assert.Empty(t, res.Skipped)
}

// TestResolve_DependencyCycleSkips: a cycle in mandatory dependencies rejects
// the candidate; with no other candidates the plugin is skipped with the
// cycle spelled out.
func TestResolve_DependencyCycleSkips(t *testing.T) {
	stub := newStub().
		add("alpha", "v1.0.0", needsPlugin(needsModule(plug("alpha", "v1.0.0"), "m1", ""), "beta", ">=1.0.0")).
		add("beta", "v1.0.0", needsPlugin(plug("beta", "v1.0.0"), "alpha", ">=1.0.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("m1", "v1.0.0")},
	})

	assert.Nil(t, selectedVersions(res, "alpha"))
	require.Len(t, res.Skipped, 1)
	assert.Equal(t, "alpha", res.Skipped[0].Name)
	assert.Contains(t, res.Skipped[0].Reason, "cycle")
}

// TestResolve_DependencyNotPublishedSkips: an auto-selected plugin whose
// dependency is missing from the catalog is skipped with the reason recorded,
// and the pull goes on.
func TestResolve_DependencyNotPublishedSkips(t *testing.T) {
	stub := newStub().
		add("alpha", "v1.0.0", needsPlugin(needsModule(plug("alpha", "v1.0.0"), "m1", ""), "ghost", ">=1.0.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("m1", "v1.0.0")},
	})

	assert.Empty(t, res.Plugins)
	require.Len(t, res.Skipped, 1)
	assert.Contains(t, res.Skipped[0].Reason, `"ghost" is not published`)
}

// TestResolve_UnrelatedPluginsNotSelected: a plugin with no module
// requirements and a plugin with only a conditional module mention are not
// auto-selected - nothing extra enters the bundle.
func TestResolve_UnrelatedPluginsNotSelected(t *testing.T) {
	stub := newStub().
		add("standalone", "v1.0.0", plug("standalone", "v1.0.0")).
		add("condonly", "v1.0.0", condModule(plug("condonly", "v1.0.0"), "m1", ">=1.0.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("m1", "v1.0.0")},
	})

	assert.Empty(t, res.Plugins, "standalone and conditional-only plugins must not be auto-selected")
	assert.Empty(t, res.Skipped)
}

// TestResolve_AnyOfTriggers: a mirrored module that is a member of an anyOf
// group triggers selection, with the member's constraint on the reason edge.
func TestResolve_AnyOfTriggers(t *testing.T) {
	stub := newStub().
		add("cni-tool", "v1.0.0", needsAnyOf(plug("cni-tool", "v1.0.0"), "cni",
			internal.ModuleRequirement{Name: "cni-flannel", Constraint: ">=1.0.0"},
			internal.ModuleRequirement{Name: "cni-cilium", Constraint: ">=1.0.0"},
		))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("cni-cilium", "v1.2.0")},
	})

	assert.Equal(t, []string{"v1.0.0"}, selectedVersions(res, "cni-tool"))

	sv := selectedVersion(t, res, "cni-tool", "v1.0.0")
	assert.Contains(t, sv.Reasons, Reason{Kind: ReasonModule, Subject: "cni-cilium", Constraint: ">=1.0.0"})
}

// TestResolve_NoCompatibleVersionSkips: every candidate requires a newer
// module than the bundle has - the plugin is skipped and the reason quotes
// the constraint, so the operator knows what to bump.
func TestResolve_NoCompatibleVersionSkips(t *testing.T) {
	stub := newStub().
		add("backup-tool", "v2.0.0", needsModule(plug("backup-tool", "v2.0.0"), "postgresql", ">=3.0.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.10.0")},
	})

	assert.Empty(t, res.Plugins)
	require.Len(t, res.Skipped, 1)
	assert.Equal(t, "backup-tool", res.Skipped[0].Name)
	assert.Contains(t, res.Skipped[0].Reason, ">=3.0.0")
	assert.Contains(t, res.Skipped[0].Reason, "for module postgresql v1.10.0")
}

// TestResolve_BrokenNewestContractFallsBack: a broken published contract on
// the newest version must not hide the plugin - triage and selection fall
// back to the next readable version.
func TestResolve_BrokenNewestContractFallsBack(t *testing.T) {
	stub := newStub().
		markInvalid("postgresql-mgr", "v1.3.0").
		add("postgresql-mgr", "v1.2.0", needsModule(plug("postgresql-mgr", "v1.2.0"), "postgresql", ">=1.0.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})

	assert.Equal(t, []string{"v1.2.0"}, selectedVersions(res, "postgresql-mgr"))
}

// TestResolve_DeckhouseConstraintGates: a candidate demanding a newer
// Deckhouse than the bundle carries is passed over for an older candidate.
func TestResolve_DeckhouseConstraintGates(t *testing.T) {
	stub := newStub().
		add("postgresql-mgr", "v1.1.0", needsModule(plug("postgresql-mgr", "v1.1.0"), "postgresql", ">=1.0.0")).
		add("postgresql-mgr", "v1.2.0",
			needsDeckhouse(needsModule(plug("postgresql-mgr", "v1.2.0"), "postgresql", ">=1.0.0"), ">=1.80.0"))

	res := resolve(t, stub, ResolveInput{
		Modules:          []ModuleInBundle{mod("postgresql", "v1.5.0")},
		PlatformVersions: semvers("v1.71.3"),
	})

	assert.Equal(t, []string{"v1.1.0"}, selectedVersions(res, "postgresql-mgr"),
		"the deckhouse constraint must demote to an older compatible version")
}

// TestResolve_ExplicitInclude: --include-plugin pulls a plugin unrelated to
// the bundle's modules; an unmet module requirement warns but does not block
// (the user's explicit choice wins, modules are never auto-added).
func TestResolve_ExplicitInclude(t *testing.T) {
	stub := newStub().
		add("velero-helper", "v0.3.0", needsModule(plug("velero-helper", "v0.3.0"), "velero", ">=1.0.0"))

	res := resolve(t, stub, ResolveInput{
		Filter: mustFilter(t, "velero-helper"),
	})

	assert.Equal(t, []string{"v0.3.0"}, selectedVersions(res, "velero-helper"))

	sv := selectedVersion(t, res, "velero-helper", "v0.3.0")
	assert.Contains(t, sv.Reasons, Reason{Kind: ReasonExplicit, Subject: "--include-plugin velero-helper"})

	require.Len(t, res.Warnings, 1)
	assert.Contains(t, res.Warnings[0], `requires module "velero"`)
	assert.Contains(t, res.Warnings[0], "explicitly included")
}

// TestResolve_ExplicitIncludeNotPublished: an explicit request that cannot be
// met is an error, never a silent skip.
func TestResolve_ExplicitIncludeNotPublished(t *testing.T) {
	_, err := NewResolver(newStub(), log.NewNop()).Resolve(context.Background(), ResolveInput{
		Filter: mustFilter(t, "ghost"),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ghost")
	assert.Contains(t, err.Error(), "not published")
}

// TestResolve_ExplicitExactPinReachesPrerelease: an exact pin bypasses the
// stable-version list, so pre-releases stay reachable - the mirror analog of
// `d8 plugins install --version`.
func TestResolve_ExplicitExactPinReachesPrerelease(t *testing.T) {
	stub := newStub().
		add("experimental", "v2.0.0-rc.1", plug("experimental", "v2.0.0-rc.1"))

	res := resolve(t, stub, ResolveInput{
		Filter: mustFilter(t, "experimental@=v2.0.0-rc.1"),
	})

	assert.Equal(t, []string{"v2.0.0-rc.1"}, selectedVersions(res, "experimental"))

	sv := selectedVersion(t, res, "experimental", "v2.0.0-rc.1")
	assert.Contains(t, sv.Reasons, Reason{Kind: ReasonExplicit, Subject: "--include-plugin experimental", Constraint: "=v2.0.0-rc.1"})
}

// TestResolve_EmptyInput: no modules and no explicit includes - the resolver
// does nothing and does not even list the catalog.
func TestResolve_EmptyInput(t *testing.T) {
	stub := newStub().add("anything", "v1.0.0", plug("anything", "v1.0.0"))

	res := resolve(t, stub, ResolveInput{})

	assert.Empty(t, res.Plugins)
	assert.Empty(t, res.Skipped)
	assert.Zero(t, stub.namesCalls, "with nothing to resolve the catalog must not be listed")
}

// TestResolve_NewestContractDecidesRelevance: relevance is judged by the
// newest readable contract only. A plugin whose CURRENT version dropped the
// module integration is not resurrected by its older contracts.
func TestResolve_NewestContractDecidesRelevance(t *testing.T) {
	stub := newStub().
		add("postgresql-mgr", "v2.0.0", plug("postgresql-mgr", "v2.0.0")). // integration dropped
		add("postgresql-mgr", "v1.2.0", needsModule(plug("postgresql-mgr", "v1.2.0"), "postgresql", ">=1.0.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})

	assert.Empty(t, res.Plugins, "an abandoned integration must not be resurrected from old contracts")
	assert.Empty(t, res.Skipped)
}

// TestResolve_ExplicitRangedConstraint: a semver range on --include-plugin
// picks the newest version inside the range, not the newest overall.
func TestResolve_ExplicitRangedConstraint(t *testing.T) {
	stub := newStub().
		add("foo", "v2.0.0", plug("foo", "v2.0.0")).
		add("foo", "v1.5.0", plug("foo", "v1.5.0")).
		add("foo", "v1.4.0", plug("foo", "v1.4.0"))

	res := resolve(t, stub, ResolveInput{
		Filter: mustFilter(t, "foo@^1.2"),
	})

	assert.Equal(t, []string{"v1.5.0"}, selectedVersions(res, "foo"))
}

// TestResolve_ExplicitMultiHonorsEveryRange is the regression for the
// exact-pin shadowing bug: combining an exact pin with a semver range must
// satisfy BOTH - the exact pick must not swallow the ranged one.
func TestResolve_ExplicitMultiHonorsEveryRange(t *testing.T) {
	stub := newStub().
		add("foo", "v3.0.0", plug("foo", "v3.0.0")).
		add("foo", "v1.5.0", plug("foo", "v1.5.0"))

	res := resolve(t, stub, ResolveInput{
		Filter: mustFilter(t, "foo@=v3.0.0", "foo@^1.0"),
	})

	assert.Equal(t, []string{"v3.0.0", "v1.5.0"}, selectedVersions(res, "foo"),
		"the exact pin and the semver range must each get their version")
}

// TestResolve_SecondMandatoryModuleGates: a candidate needing another module
// the bundle lacks is passed over for an older self-sufficient version.
func TestResolve_SecondMandatoryModuleGates(t *testing.T) {
	stub := newStub().
		add("postgresql-mgr", "v1.1.0", needsModule(plug("postgresql-mgr", "v1.1.0"), "postgresql", ">=1.0.0")).
		add("postgresql-mgr", "v1.2.0",
			needsModule(needsModule(plug("postgresql-mgr", "v1.2.0"), "postgresql", ">=1.0.0"), "redis", ">=1.0.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})

	assert.Equal(t, []string{"v1.1.0"}, selectedVersions(res, "postgresql-mgr"))
}

// TestResolve_AnyOfGroupGatesBesidePairing is the regression for the anyOf
// hole: pairing on a weak mandatory constraint must not silence a stricter
// anyOf group containing the same module.
func TestResolve_AnyOfGroupGatesBesidePairing(t *testing.T) {
	contract := needsModule(plug("pg-tool", "v1.0.0"), "postgresql", ">=1.0.0")
	contract = needsAnyOf(contract, "storage",
		internal.ModuleRequirement{Name: "postgresql", Constraint: ">=2.0.0"},
		internal.ModuleRequirement{Name: "ceph", Constraint: ">=1.0.0"},
	)

	stub := newStub().add("pg-tool", "v1.0.0", contract)

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})

	assert.Empty(t, res.Plugins, "the unsatisfiable anyOf group must gate the candidate")
	require.Len(t, res.Skipped, 1)
	assert.Contains(t, res.Skipped[0].Reason, `"storage"`)
}

// TestResolve_PartialFailureWarnsNotSkips is the regression for the
// both-selected-and-skipped bug: a plugin that covered some module versions
// is selected, and the uncovered ones become a warning, not a skip.
func TestResolve_PartialFailureWarnsNotSkips(t *testing.T) {
	stub := newStub().
		add("postgresql-mgr", "v1.1.0", needsModule(plug("postgresql-mgr", "v1.1.0"), "postgresql", ">=2.0.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.0.0", "v2.0.0")},
	})

	assert.Equal(t, []string{"v1.1.0"}, selectedVersions(res, "postgresql-mgr"))
	assert.Empty(t, res.Skipped, "a partially covered plugin is not skipped")
	require.Len(t, res.Warnings, 1)
	assert.Contains(t, res.Warnings[0], "for module postgresql v1.0.0")
}

// TestResolve_CIMarkerModuleVersionSatisfies is the regression for the
// normalization divergence: a module pinned to a CI-marked version (-dev)
// must satisfy a plain floor constraint, same as install-time checks do.
func TestResolve_CIMarkerModuleVersionSatisfies(t *testing.T) {
	stub := newStub().
		add("postgresql-mgr", "v1.2.0", needsModule(plug("postgresql-mgr", "v1.2.0"), "postgresql", ">=1.0.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.2.3-dev")},
	})

	assert.Equal(t, []string{"v1.2.0"}, selectedVersions(res, "postgresql-mgr"))
}

// TestResolve_TransportErrorFailsResolution: a registry error that is neither
// not-found nor a broken contract must fail the whole resolution - it is
// never laundered into "no compatible version".
func TestResolve_TransportErrorFailsResolution(t *testing.T) {
	stub := newStub().
		failTransport("postgresql-mgr", "v1.2.0")

	_, err := NewResolver(stub, log.NewNop()).Resolve(context.Background(), ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection reset")
}

// TestResolve_DepthCapRejects: a dependency chain deeper than the cap rejects
// the root candidate instead of recursing forever.
func TestResolve_DepthCapRejects(t *testing.T) {
	stub := newStub().
		add("p0", "v1.0.0", needsPlugin(needsModule(plug("p0", "v1.0.0"), "m1", ""), "p1", ""))

	for i := 1; i <= 20; i++ {
		contract := plug(fmt.Sprintf("p%d", i), "v1.0.0")
		if i < 20 {
			contract = needsPlugin(contract, fmt.Sprintf("p%d", i+1), "")
		}

		stub.add(fmt.Sprintf("p%d", i), "v1.0.0", contract)
	}

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("m1", "v1.0.0")},
	})

	assert.Nil(t, selectedVersions(res, "p0"))
	require.Len(t, res.Skipped, 1)
	assert.Contains(t, res.Skipped[0].Reason, "deeper than")
}

// TestResolve_KubernetesAndNoneOfIgnored: kubernetes and noneOf requirements
// are cluster-side; mirror must not gate on them.
func TestResolve_KubernetesAndNoneOfIgnored(t *testing.T) {
	contract := needsModule(plug("pg-tool", "v1.0.0"), "postgresql", ">=1.0.0")
	contract.Requirements.Kubernetes.Constraint = ">=1.99.0"
	contract.Requirements.Modules.NoneOf = []internal.ModuleGroup{{
		Name:    "legacy",
		Modules: []internal.ModuleRequirement{{Name: "postgresql"}},
	}}

	stub := newStub().add("pg-tool", "v1.0.0", contract)

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})

	assert.Equal(t, []string{"v1.0.0"}, selectedVersions(res, "pg-tool"))
}

// TestResolve_ExplicitDependencyGateRelaxed: dependencies of an explicitly
// included plugin are pulled even when the bundle lacks their modules - with
// a warning, matching the plugin's own relaxed gate. A plugins-only bundle
// stays possible.
func TestResolve_ExplicitDependencyGateRelaxed(t *testing.T) {
	stub := newStub().
		add("foo", "v1.0.0", needsPlugin(plug("foo", "v1.0.0"), "bar", ">=1.0.0")).
		add("bar", "v1.0.0", needsModule(plug("bar", "v1.0.0"), "m1", ">=1.0.0"))

	res := resolve(t, stub, ResolveInput{
		Filter: mustFilter(t, "foo"),
	})

	assert.Equal(t, []string{"v1.0.0"}, selectedVersions(res, "foo"))
	assert.Equal(t, []string{"v1.0.0"}, selectedVersions(res, "bar"))
	require.NotEmpty(t, res.Warnings)
	assert.Contains(t, res.Warnings[0], `requires module "m1"`)
}

// TestResolve_AllContractsBrokenSkips: a plugin whose every published
// contract is broken is surfaced in Skipped, not silently dropped.
func TestResolve_AllContractsBrokenSkips(t *testing.T) {
	stub := newStub().
		markInvalid("broken", "v1.0.0").
		markInvalid("broken", "v1.1.0")

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("m1", "v1.0.0")},
	})

	assert.Empty(t, res.Plugins)
	require.Len(t, res.Skipped, 1)
	assert.Equal(t, "broken", res.Skipped[0].Name)
	assert.Contains(t, res.Skipped[0].Reason, "no readable contract")
}

// TestResolve_RollbackDiscardsPartialDeps: when a candidate resolves part of
// its dependencies and then fails, the fallback to an older candidate must
// not leak the partial picks into the result.
func TestResolve_RollbackDiscardsPartialDeps(t *testing.T) {
	v2 := needsModule(plug("pg-tool", "v2.0.0"), "postgresql", ">=1.0.0")
	v2 = needsPlugin(v2, "dep-a", "") // resolvable
	v2 = needsPlugin(v2, "ghost", "") // not published -> candidate fails

	stub := newStub().
		add("pg-tool", "v2.0.0", v2).
		add("pg-tool", "v1.0.0", needsModule(plug("pg-tool", "v1.0.0"), "postgresql", ">=1.0.0")).
		add("dep-a", "v1.0.0", plug("dep-a", "v1.0.0"))

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
	})

	assert.Equal(t, []string{"v1.0.0"}, selectedVersions(res, "pg-tool"),
		"the failed v2.0.0 candidate must fall back to v1.0.0")
	assert.Nil(t, selectedVersions(res, "dep-a"),
		"partially resolved deps of the failed candidate must not leak into the result")
}

// TestResolve_MissingCatalogSkipsAuto: a registry without a plugins catalog
// yields an empty auto-selection, not an error - and explicit includes still
// resolve against their own repositories.
func TestResolve_MissingCatalogSkipsAuto(t *testing.T) {
	stub := newStub().
		add("velero-helper", "v0.3.0", plug("velero-helper", "v0.3.0"))
	stub.namesErr = fmt.Errorf("list plugins catalog: %w", dkpclient.ErrImageNotFound)

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{mod("postgresql", "v1.5.0")},
		Filter:  mustFilter(t, "velero-helper"),
	})

	assert.Equal(t, []string{"v0.3.0"}, selectedVersions(res, "velero-helper"),
		"explicit includes must survive a missing catalog")
	assert.Len(t, res.Plugins, 1, "nothing must be auto-selected without a catalog")
}

// TestResolve_ConditionalModuleAdvisory: a conditional module constraint the
// bundle cannot satisfy warns about the co-installation hazard but does not
// gate the selection.
func TestResolve_ConditionalModuleAdvisory(t *testing.T) {
	contract := needsModule(plug("pg-tool", "v1.0.0"), "postgresql", ">=1.0.0")
	contract = condModule(contract, "observability", ">=5.0.0")

	stub := newStub().add("pg-tool", "v1.0.0", contract)

	res := resolve(t, stub, ResolveInput{
		Modules: []ModuleInBundle{
			mod("postgresql", "v1.5.0"),
			mod("observability", "v1.0.0"),
		},
	})

	assert.Equal(t, []string{"v1.0.0"}, selectedVersions(res, "pg-tool"),
		"a conditional constraint never gates selection")
	require.Len(t, res.Warnings, 1)
	assert.Contains(t, res.Warnings[0], "conditional")
	assert.Contains(t, res.Warnings[0], "observability")
}

// TestResolve_MalformedNamesRejected: a plugin name is a filesystem path and
// a registry route, so every source is held to the single-component grammar.
// A malformed catalog entry is dropped silently (registry data), a malformed
// --include-plugin is the user's error, and a malformed dependency name from
// a published contract rejects the dependent.
func TestResolve_MalformedNamesRejected(t *testing.T) {
	t.Run("catalog entry is dropped", func(t *testing.T) {
		stub := newStub().
			add("../../outside", "v1.0.0", needsModule(plug("../../outside", "v1.0.0"), "m1", "")).
			add("alpha", "v1.0.0", needsModule(plug("alpha", "v1.0.0"), "m1", ""))

		res := resolve(t, stub, ResolveInput{
			Modules: []ModuleInBundle{mod("m1", "v1.0.0")},
		})

		assert.Equal(t, []string{"v1.0.0"}, selectedVersions(res, "alpha"))
		assert.Nil(t, selectedVersions(res, "../../outside"))
		assert.Empty(t, res.Skipped, "a malformed catalog entry is not a plugin, so it is not reported as skipped")
	})

	t.Run("explicit include is an error", func(t *testing.T) {
		_, err := NewResolver(newStub(), log.NewNop()).Resolve(context.Background(), ResolveInput{
			Filter: mustFilter(t, "../../outside@=v1.0.0"),
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "--include-plugin ../../outside")
		assert.Contains(t, err.Error(), "invalid plugin name")
	})

	t.Run("dependency name rejects the dependent", func(t *testing.T) {
		stub := newStub().
			add("alpha", "v1.0.0", needsPlugin(needsModule(plug("alpha", "v1.0.0"), "m1", ""), "../../outside", ">=1.0.0"))

		res := resolve(t, stub, ResolveInput{
			Modules: []ModuleInBundle{mod("m1", "v1.0.0")},
		})

		assert.Empty(t, res.Plugins)
		require.Len(t, res.Skipped, 1)
		assert.Equal(t, "alpha", res.Skipped[0].Name)
		assert.Contains(t, res.Skipped[0].Reason, `invalid plugin name "../../outside"`)
	})
}
