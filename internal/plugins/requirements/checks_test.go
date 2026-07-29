/*
Copyright 2025 Flant JSC

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

package requirements

import (
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
)

func testChecker() *Checker {
	return NewChecker(dkplog.NewNop())
}

func enabled(version string) ModuleState {
	m := ModuleState{Enabled: true}
	if version != "" {
		m.Version = semver.MustParse(version)
	}

	return m
}

func TestNormalizedForConstraint(t *testing.T) {
	// CI / build markers are stripped to the release version.
	assert.Equal(t, "1.77.0", normalizedForConstraint(semver.MustParse("v1.77.0-main+abc")).String())
	assert.Equal(t, "1.28.3", normalizedForConstraint(semver.MustParse("v1.28.3-eks-1-30")).String())
	// Genuine pre-releases are kept (only build metadata is dropped).
	assert.Equal(t, "1.30.0-rc.1", normalizedForConstraint(semver.MustParse("v1.30.0-rc.1+build")).String())
	assert.Equal(t, "1.30.0-alpha.2", normalizedForConstraint(semver.MustParse("v1.30.0-alpha.2")).String())
}

func TestHasClusterRequirements(t *testing.T) {
	assert.False(t, HasClusterRequirements(&internal.Plugin{}))
	assert.True(t, HasClusterRequirements(&internal.Plugin{
		Requirements: internal.Requirements{Kubernetes: internal.KubernetesRequirement{Constraint: ">= 1.27"}},
	}))
	assert.True(t, HasClusterRequirements(&internal.Plugin{
		Requirements: internal.Requirements{Modules: internal.ModuleRequirementsGroup{
			Mandatory: []internal.ModuleRequirement{{Name: "x"}},
		}},
	}))
	assert.True(t, HasClusterRequirements(&internal.Plugin{
		Requirements: internal.Requirements{Modules: internal.ModuleRequirementsGroup{
			AnyOf: []internal.ModuleGroup{{Name: "g", Modules: []internal.ModuleRequirement{{Name: "x"}}}},
		}},
	}))
	assert.True(t, HasClusterRequirements(&internal.Plugin{
		Requirements: internal.Requirements{Modules: internal.ModuleRequirementsGroup{
			NoneOf: []internal.ModuleGroup{{Name: "g", Modules: []internal.ModuleRequirement{{Name: "x"}}}},
		}},
	}))
}

func TestIsUnmet(t *testing.T) {
	assert.True(t, IsUnmet(unmetf("requirement not met")))
	assert.False(t, IsUnmet(assert.AnError), "an operational error is not an unmet requirement")
}

func TestValidateKubernetesRequirement(t *testing.T) {
	c := testChecker()
	state := &ClusterState{Kubernetes: semver.MustParse("v1.28.3")}

	require.NoError(t, c.validateKubernetesRequirement(&internal.Plugin{}, state), "empty constraint passes")
	require.NoError(t, c.validateKubernetesRequirement(reqK8s(">= 1.27"), state), "satisfied")
	require.Error(t, c.validateKubernetesRequirement(reqK8s(">= 1.30"), state), "violated")
}

func TestValidateKubernetesRequirementUnknownVersion(t *testing.T) {
	c := testChecker()
	state := &ClusterState{} // Kubernetes == nil (unparseable cluster version)

	require.Error(t, c.validateKubernetesRequirement(reqK8s(">= 1.27"), state), "declared requirement cannot be verified")
	require.NoError(t, c.validateKubernetesRequirement(&internal.Plugin{}, state), "no constraint → no error")
}

func TestValidateDeckhouseRequirement(t *testing.T) {
	c := testChecker()
	state := &ClusterState{Deckhouse: semver.MustParse("v1.65.3")}

	require.NoError(t, c.validateDeckhouseRequirement(reqDeckhouse(">= 1.60"), state), "satisfied")
	require.Error(t, c.validateDeckhouseRequirement(reqDeckhouse(">= 1.70"), state), "violated")

	// dev cluster (nil version) is skipped, not blocked
	dev := &ClusterState{}
	require.NoError(t, c.validateDeckhouseRequirement(reqDeckhouse(">= 1.70"), dev), "dev cluster skips")
}

func TestValidateModuleRequirementMandatory(t *testing.T) {
	c := testChecker()

	state := func(mods map[string]ModuleState) *ClusterState { return &ClusterState{Modules: mods} }

	// enabled + version satisfies
	require.NoError(t, c.validateModuleRequirement(
		reqModules(mandatory("stronghold", ">= 1.0")),
		state(map[string]ModuleState{"stronghold": enabled("v1.2.0")})))

	// absent → error
	require.Error(t, c.validateModuleRequirement(
		reqModules(mandatory("stronghold", "")),
		state(map[string]ModuleState{})))

	// present but disabled → error
	require.Error(t, c.validateModuleRequirement(
		reqModules(mandatory("stronghold", "")),
		state(map[string]ModuleState{"stronghold": {Enabled: false}})))

	// enabled but version mismatch → error
	require.Error(t, c.validateModuleRequirement(
		reqModules(mandatory("stronghold", ">= 2.0")),
		state(map[string]ModuleState{"stronghold": enabled("v1.2.0")})))

	// enabled, version unknown → presence satisfied, version skipped
	require.NoError(t, c.validateModuleRequirement(
		reqModules(mandatory("stronghold", ">= 2.0")),
		state(map[string]ModuleState{"stronghold": {Enabled: true}})))

	// dev module version (pre-release) still satisfies a plain constraint via coreVersion
	require.NoError(t, c.validateModuleRequirement(
		reqModules(mandatory("stronghold", ">= 1.0.0")),
		state(map[string]ModuleState{"stronghold": enabled("v1.77.0-main+abc")})))
}

func TestValidateModuleRequirementConditional(t *testing.T) {
	c := testChecker()
	reqs := reqModules(internal.ModuleRequirementsGroup{
		Conditional: []internal.ModuleRequirement{{Name: "stronghold", Constraint: ">= 2.0"}},
	})

	// not enabled → skipped
	require.NoError(t, c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{}}))

	// enabled but fails → error
	require.Error(t, c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{"stronghold": enabled("v1.0.0")}}))
}

func TestValidateModuleRequirementAnyOf(t *testing.T) {
	c := testChecker()
	reqs := reqModules(internal.ModuleRequirementsGroup{
		AnyOf: []internal.ModuleGroup{{
			Description: "ingress",
			Modules: []internal.ModuleRequirement{
				{Name: "ingress-nginx", Constraint: ">= 1.0"},
				{Name: "ingress-alb", Constraint: ">= 1.0"},
			},
		}},
	})

	// one satisfied → ok
	require.NoError(t, c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{
		"ingress-alb": enabled("v1.5.0"),
	}}))

	// none enabled → error
	require.Error(t, c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{}}))

	// enabled but all fail constraint → error
	require.Error(t, c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{
		"ingress-nginx": enabled("v0.9.0"),
	}}))
}

func TestValidateModuleRequirementAnyOfUnversionedNotSatisfied(t *testing.T) {
	c := testChecker()
	reqs := reqModules(internal.ModuleRequirementsGroup{
		AnyOf: []internal.ModuleGroup{{Modules: []internal.ModuleRequirement{{Name: "m", Constraint: ">= 1.0"}}}},
	})

	// enabled but no version → does NOT satisfy a versioned anyOf alternative
	require.Error(t, c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{
		"m": {Enabled: true},
	}}))
}

func TestValidateModuleRequirementMalformedConstraintPropagates(t *testing.T) {
	c := testChecker()
	reqs := reqModules(internal.ModuleRequirementsGroup{
		AnyOf: []internal.ModuleGroup{{Modules: []internal.ModuleRequirement{{Name: "m", Constraint: "abc"}}}},
	})

	// a malformed constraint is operational - it propagates, not swallowed as "none satisfied"
	err := c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{
		"m": enabled("v1.0.0"),
	}})
	require.Error(t, err)
	assert.False(t, IsUnmet(err), "operational errors are not reported as unmet requirements")
}

func TestValidateModuleRequirementNoneOf(t *testing.T) {
	c := testChecker()
	reqs := reqModules(internal.ModuleRequirementsGroup{
		NoneOf: []internal.ModuleGroup{{
			Description: "legacy",
			Modules:     []internal.ModuleRequirement{{Name: "legacy-cni", Constraint: "< 1.0"}},
		}},
	})

	// forbidden module not enabled → ok
	require.NoError(t, c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{}}))

	// enabled, version outside the forbidden range → ok
	require.NoError(t, c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{
		"legacy-cni": enabled("v1.5.0"),
	}}))

	// enabled, version inside the forbidden range → unmet
	err := c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{
		"legacy-cni": enabled("v0.9.0"),
	}})
	require.Error(t, err)
	assert.True(t, IsUnmet(err))
}

func TestValidateModuleRequirementNoneOfEmptyConstraintForbidsAnyVersion(t *testing.T) {
	c := testChecker()
	reqs := reqModules(internal.ModuleRequirementsGroup{
		NoneOf: []internal.ModuleGroup{{Modules: []internal.ModuleRequirement{{Name: "banned"}}}},
	})

	// an empty constraint forbids the module at any version
	require.Error(t, c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{
		"banned": enabled("v3.0.0"),
	}}))
}

func TestValidateModuleRequirementNoneOfMalformedConstraintPropagates(t *testing.T) {
	c := testChecker()
	reqs := reqModules(internal.ModuleRequirementsGroup{
		NoneOf: []internal.ModuleGroup{{Modules: []internal.ModuleRequirement{{Name: "m", Constraint: "abc"}}}},
	})

	// a malformed constraint is operational - it propagates, not swallowed as "not forbidden"
	err := c.validateModuleRequirement(reqs, &ClusterState{Modules: map[string]ModuleState{
		"m": enabled("v1.0.0"),
	}})
	require.Error(t, err)
	assert.False(t, IsUnmet(err), "operational errors are not reported as unmet requirements")
}

// --- helpers to build plugins with specific requirements ---

func reqK8s(constraint string) *internal.Plugin {
	return &internal.Plugin{Name: "p", Requirements: internal.Requirements{
		Kubernetes: internal.KubernetesRequirement{Constraint: constraint},
	}}
}

func reqDeckhouse(constraint string) *internal.Plugin {
	return &internal.Plugin{Name: "p", Requirements: internal.Requirements{
		Deckhouse: internal.DeckhouseRequirement{Constraint: constraint},
	}}
}

func reqModules(group internal.ModuleRequirementsGroup) *internal.Plugin {
	return &internal.Plugin{Name: "p", Requirements: internal.Requirements{Modules: group}}
}

func mandatory(name, constraint string) internal.ModuleRequirementsGroup {
	return internal.ModuleRequirementsGroup{
		Mandatory: []internal.ModuleRequirement{{Name: name, Constraint: constraint}},
	}
}
