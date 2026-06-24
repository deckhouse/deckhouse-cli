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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/requirements"
	"github.com/deckhouse/deckhouse-cli/internal/rpp"
	"github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
)

// multiPluginSource is a pluginSource serving several plugins, for planner and
// resolution tests. ExtractPlugin writes a script binary that echoes the tag, so
// getInstalledPluginVersion reports the installed version correctly.
type multiPluginSource struct {
	tags      map[string][]string
	contracts map[string]map[string]*internal.Plugin
	// unpublished names return an rpp.ErrNotFound from ListPluginTags, emulating a
	// dependency that is not published in the registry (a 404 from the proxy).
	unpublished map[string]bool
	// tagErrors returns a specific error for a name, for non-404 failures
	// (auth, proxy, transport) that must hard-stop rather than skip.
	tagErrors map[string]error
}

func (s *multiPluginSource) ListPluginTags(_ context.Context, name string) ([]string, error) {
	if err, ok := s.tagErrors[name]; ok {
		return nil, err
	}

	if s.unpublished[name] {
		return nil, fmt.Errorf("%s: %w", name, rpp.ErrNotFound)
	}

	tags, ok := s.tags[name]
	if !ok {
		return nil, fmt.Errorf("no such plugin %q", name)
	}

	return tags, nil
}

func (s *multiPluginSource) GetPluginContract(_ context.Context, name, tag string) (*internal.Plugin, error) {
	byTag, ok := s.contracts[name]
	if !ok {
		return nil, fmt.Errorf("no such plugin %q", name)
	}

	contract, ok := byTag[tag]
	if !ok {
		return nil, fmt.Errorf("no contract for %s@%s", name, tag)
	}

	return contract, nil
}

func (s *multiPluginSource) ExtractPlugin(_ context.Context, _, tag, dest string) error {
	return os.WriteFile(dest, []byte("#!/bin/sh\necho '"+tag+"'\n"), 0o755)
}

// requires builds a contract for name@version that mandatorily depends on dep at
// constraint.
func requires(name, version, dep, constraint string) *internal.Plugin {
	return &internal.Plugin{
		Name:    name,
		Version: version,
		Requirements: internal.Requirements{
			Plugins: internal.PluginRequirementsGroup{
				Mandatory: []internal.PluginRequirement{{Name: dep, Constraint: constraint}},
			},
		},
	}
}

func plannerManager(t *testing.T, src *multiPluginSource) *Manager {
	t.Helper()

	m := testManager()
	m.pluginDirectory = t.TempDir()
	m.service = src
	// A non-nil snapshot so cluster checks (when a contract declares them) read it
	// instead of dialing a real API server.
	m.clusterStateCache = &requirements.ClusterState{Kubernetes: semver.MustParse("v1.28.3")}

	return m
}

// installVersionFixture installs name at version (major derived from version) with
// a version-reporting binary and the current symlink.
func installVersionFixture(t *testing.T, root, name, version string) {
	t.Helper()

	major := int(semver.MustParse(version).Major())
	dir := filepath.Dir(layout.BinaryPath(root, name, major))
	require.NoError(t, os.MkdirAll(dir, 0o755))

	bin := writeScriptBinary(t, dir, name, version, 0)
	abs, err := filepath.Abs(bin)
	require.NoError(t, err)
	require.NoError(t, os.Symlink(abs, layout.CurrentLinkPath(root, name)))
}

func planStepVersions(plan *resolutionPlan) map[string]string {
	out := make(map[string]string, len(plan.steps))
	for _, step := range plan.steps {
		out[step.pluginName] = step.version.Original()
	}

	return out
}

func TestPlannerInstallsMissingMandatoryDep(t *testing.T) {
	src := &multiPluginSource{
		tags: map[string][]string{"foo": {"v1.0.0", "v1.3.0"}},
		contracts: map[string]map[string]*internal.Plugin{
			"foo": {"v1.0.0": {Name: "foo", Version: "v1.0.0"}, "v1.3.0": {Name: "foo", Version: "v1.3.0"}},
		},
	}
	m := plannerManager(t, src)

	top := requires("p", "v1.0.0", "foo", ">= 1.0.0")

	plan, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err)
	require.Nil(t, reason, "the missing dependency is installable")
	assert.Equal(t, "v1.3.0", planStepVersions(plan)["foo"], "newest satisfying version is planned")
}

func TestPlannerUnpublishedDepIsReasonWithChain(t *testing.T) {
	src := &multiPluginSource{
		unpublished: map[string]bool{"foo": true},
		contracts:   map[string]map[string]*internal.Plugin{},
	}
	m := plannerManager(t, src)

	top := requires("p", "v1.0.0", "foo", "")

	plan, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err, "an unpublished dependency is not an operational error")
	require.Nil(t, plan)
	require.NotNil(t, reason, "the candidate is unsatisfiable, not a hard stop")
	assert.Equal(t, `dependency "foo" (via p -> foo): not published as deckhouse-cli/plugins/foo`, reason.summary())
}

func TestPlannerSkipsDepVersionWithUnpublishedSubdep(t *testing.T) {
	// dk v2 needs the unpublished "x"; dk v1 has no deps. Resolution falls back to v1.
	src := &multiPluginSource{
		tags:        map[string][]string{"dk": {"v2.0.0", "v1.0.0"}},
		unpublished: map[string]bool{"x": true},
		contracts: map[string]map[string]*internal.Plugin{
			"dk": {
				"v2.0.0": requires("dk", "v2.0.0", "x", ""),
				"v1.0.0": {Name: "dk", Version: "v1.0.0"},
			},
		},
	}
	m := plannerManager(t, src)

	top := requires("p", "v1.0.0", "dk", "")

	plan, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err)
	require.Nil(t, reason, "dk v1 satisfies the requirement")
	assert.Equal(t, "v1.0.0", planStepVersions(plan)["dk"], "skips dk v2 whose subdep is unpublished")
}

func TestPlannerDeepUnpublishedDepNamesTheChain(t *testing.T) {
	// p -> dk (only v2) -> x (unpublished). The failure must name x and the full chain.
	src := &multiPluginSource{
		tags:        map[string][]string{"dk": {"v2.0.0"}},
		unpublished: map[string]bool{"x": true},
		contracts: map[string]map[string]*internal.Plugin{
			"dk": {"v2.0.0": requires("dk", "v2.0.0", "x", "")},
		},
	}
	m := plannerManager(t, src)

	top := requires("p", "v1.0.0", "dk", "")

	_, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err)
	require.NotNil(t, reason)
	assert.Contains(t, reason.summary(), "not published as deckhouse-cli/plugins/x")
	assert.Contains(t, reason.summary(), "via p -> dk -> x")
}

func TestPlannerNonNotFoundDepErrorHardStops(t *testing.T) {
	// A non-404 dependency error (auth, proxy, transport) must hard-stop the whole
	// install, not skip to an older version, and must preserve the sentinel.
	src := &multiPluginSource{
		tagErrors: map[string]error{"dk": fmt.Errorf("dk: %w", rpp.ErrUnauthorized)},
		contracts: map[string]map[string]*internal.Plugin{},
	}
	m := plannerManager(t, src)

	top := requires("p", "v1.0.0", "dk", "")

	plan, reason, err := m.planFor(context.Background(), top, false)
	require.Error(t, err)
	require.Nil(t, plan)
	require.Nil(t, reason, "an operational error is not a skippable reason")
	assert.ErrorIs(t, err, rpp.ErrUnauthorized, "the sentinel is preserved for errdetect")
}

func TestSelectTopWithPlanUnpublishedDepIsHelpful(t *testing.T) {
	// When the only version is rejected for a missing dependency, the install
	// failure is a structured HelpfulError that names the dependency.
	src := &multiPluginSource{
		tags:        map[string][]string{"package": {"v0.0.21"}},
		unpublished: map[string]bool{"delivery-kit": true},
		contracts: map[string]map[string]*internal.Plugin{
			"package": {"v0.0.21": requires("package", "v0.0.21", "delivery-kit", "")},
		},
	}
	m := plannerManager(t, src)

	_, _, err := m.selectTopWithPlan(context.Background(), "package", []string{"v0.0.21"}, false)
	var he *diagnostic.HelpfulError
	require.ErrorAs(t, err, &he, "a no-installable-version failure is a HelpfulError")
	assert.Contains(t, he.Category, `no installable version of plugin "package"`)
	assert.Contains(t, he.Format(), "not published as deckhouse-cli/plugins/delivery-kit",
		"the rendered message names the missing dependency")
}

func TestPlannerUpgradesInstalledDepWithinMajor(t *testing.T) {
	src := &multiPluginSource{
		tags: map[string][]string{"foo": {"v1.0.0", "v1.2.0", "v1.5.0", "v2.0.0"}},
		contracts: map[string]map[string]*internal.Plugin{
			"foo": {
				"v1.0.0": {Name: "foo", Version: "v1.0.0"},
				"v1.2.0": {Name: "foo", Version: "v1.2.0"},
				"v1.5.0": {Name: "foo", Version: "v1.5.0"},
				"v2.0.0": {Name: "foo", Version: "v2.0.0"},
			},
		},
	}
	m := plannerManager(t, src)
	installVersionFixture(t, m.pluginDirectory, "foo", "v1.0.0")

	top := requires("p", "v1.0.0", "foo", ">= 1.2.0")

	plan, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err)
	require.Nil(t, reason)
	assert.Equal(t, "v1.5.0", planStepVersions(plan)["foo"],
		"newest in the installed major (not v2.0.0) is chosen")
}

func TestPlannerDepMajorCrossNeedsCascade(t *testing.T) {
	src := &multiPluginSource{
		tags: map[string][]string{"foo": {"v1.0.0", "v2.0.0"}},
		contracts: map[string]map[string]*internal.Plugin{
			"foo": {"v1.0.0": {Name: "foo", Version: "v1.0.0"}, "v2.0.0": {Name: "foo", Version: "v2.0.0"}},
		},
	}
	m := plannerManager(t, src)
	installVersionFixture(t, m.pluginDirectory, "foo", "v1.0.0")

	top := requires("p", "v1.0.0", "foo", ">= 2.0.0")

	// Without cascade the installed dep is bound to its major -> unsatisfiable.
	_, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err)
	require.NotNil(t, reason, "dep cannot reach >= 2.0.0 within major 1")

	// With cascade (allowMajorCross) it may cross to v2.0.0.
	plan, reason, err := m.planFor(context.Background(), top, true)
	require.NoError(t, err)
	require.Nil(t, reason)
	assert.Equal(t, "v2.0.0", planStepVersions(plan)["foo"])
}

func TestPlannerSkipsClusterIncompatibleDepVersion(t *testing.T) {
	src := &multiPluginSource{
		tags: map[string][]string{"foo": {"v1.0.0", "v2.0.0"}},
		contracts: map[string]map[string]*internal.Plugin{
			"foo": {
				"v1.0.0": {Name: "foo", Version: "v1.0.0"},
				"v2.0.0": {
					Name: "foo", Version: "v2.0.0",
					Requirements: internal.Requirements{Kubernetes: internal.KubernetesRequirement{Constraint: ">= 99.0"}},
				},
			},
		},
	}
	m := plannerManager(t, src)

	top := requires("p", "v1.0.0", "foo", ">= 1.0.0")

	plan, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err)
	require.Nil(t, reason)
	assert.Equal(t, "v1.0.0", planStepVersions(plan)["foo"],
		"the cluster-incompatible newest dep version is skipped for an older one")
}

func TestPlannerCycleIsUnsatisfiable(t *testing.T) {
	src := &multiPluginSource{
		tags: map[string][]string{"a": {"v1.0.0"}, "b": {"v1.0.0"}},
		contracts: map[string]map[string]*internal.Plugin{
			"a": {"v1.0.0": requires("a", "v1.0.0", "b", ">= 1.0.0")},
			"b": {"v1.0.0": requires("b", "v1.0.0", "a", ">= 1.0.0")},
		},
	}
	m := plannerManager(t, src)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, reason, err := m.planFor(ctx, src.contracts["a"]["v1.0.0"], false)
	require.NoError(t, err)
	require.NotNil(t, reason, "a self-referential dependency chain is unsatisfiable, not an infinite loop")
	assert.Contains(t, reason.summary(), "dependency cycle")
	assert.Contains(t, reason.summary(), "via a -> b -> a", "the cycle is shown as a chain")
}

func TestPlannerConditionalDepWrongVersionUnsatisfiable(t *testing.T) {
	src := &multiPluginSource{tags: map[string][]string{}, contracts: map[string]map[string]*internal.Plugin{}}
	m := plannerManager(t, src)
	installVersionFixture(t, m.pluginDirectory, "foo", "v1.0.0")

	top := &internal.Plugin{
		Name:    "p",
		Version: "v1.0.0",
		Requirements: internal.Requirements{
			Plugins: internal.PluginRequirementsGroup{
				Conditional: []internal.PluginRequirement{{Name: "foo", Constraint: ">= 2.0.0"}},
			},
		},
	}

	_, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err)
	require.NotNil(t, reason, "an installed conditional dep at the wrong version is not auto-upgraded")
}

func TestPlannerDependencyFirstOrdering(t *testing.T) {
	src := &multiPluginSource{
		tags: map[string][]string{"a": {"v1.0.0"}, "b": {"v1.0.0"}, "c": {"v1.0.0"}},
		contracts: map[string]map[string]*internal.Plugin{
			"a": {"v1.0.0": requires("a", "v1.0.0", "b", ">= 1.0.0")},
			"b": {"v1.0.0": requires("b", "v1.0.0", "c", ">= 1.0.0")},
			"c": {"v1.0.0": {Name: "c", Version: "v1.0.0"}},
		},
	}
	m := plannerManager(t, src)

	plan, reason, err := m.planFor(context.Background(), src.contracts["a"]["v1.0.0"], false)
	require.NoError(t, err)
	require.Nil(t, reason)

	order := make([]string, 0, len(plan.steps))
	for _, step := range plan.steps {
		order = append(order, step.pluginName)
	}

	assert.Equal(t, []string{"c", "b"}, order, "dependencies precede dependents")
}

func TestPlannerDryRunWritesNothing(t *testing.T) {
	src := &multiPluginSource{
		tags: map[string][]string{"foo": {"v1.0.0"}},
		contracts: map[string]map[string]*internal.Plugin{
			"foo": {"v1.0.0": {Name: "foo", Version: "v1.0.0"}},
		},
	}
	m := plannerManager(t, src)

	before, err := os.ReadDir(m.pluginDirectory)
	require.NoError(t, err)

	_, _, err = m.planFor(context.Background(), requires("p", "v1.0.0", "foo", ">= 1.0.0"), false)
	require.NoError(t, err)

	after, err := os.ReadDir(m.pluginDirectory)
	require.NoError(t, err)
	assert.Equal(t, len(before), len(after), "planning is read-only: it installs nothing")
}

func TestInstallPluginResolvesDepByDefault(t *testing.T) {
	src := &multiPluginSource{
		tags: map[string][]string{"p": {"v1.0.0"}, "foo": {"v1.0.0"}},
		contracts: map[string]map[string]*internal.Plugin{
			"p":   {"v1.0.0": requires("p", "v1.0.0", "foo", ">= 1.0.0")},
			"foo": {"v1.0.0": {Name: "foo", Version: "v1.0.0"}},
		},
	}
	m := plannerManager(t, src)

	require.NoError(t, m.InstallPlugin(context.Background(), "p"),
		"a missing mandatory dependency is installed automatically")

	installed, err := m.checkInstalled("foo")
	require.NoError(t, err)
	assert.True(t, installed, "the dependency was pulled in")

	installed, err = m.checkInstalled("p")
	require.NoError(t, err)
	assert.True(t, installed, "the plugin itself is installed after its deps")
}
