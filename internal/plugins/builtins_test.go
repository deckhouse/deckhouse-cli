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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal"
)

// A built-in command satisfies a mandatory plugin dependency by presence. The
// dependency name is marked unpublished, so if resolution ever reached the
// registry it would fail with reasonDepNotPublished; the built-in short-circuit
// must avoid that lookup entirely. This is the delivery-kit bridge: it ships as a
// built-in command, not a plugin.
func TestPlannerBuiltinSatisfiesMandatoryDep(t *testing.T) {
	src := &multiPluginSource{
		unpublished: map[string]bool{"delivery-kit": true},
		contracts:   map[string]map[string]*internal.Plugin{},
	}
	m := plannerManager(t, src)
	m.SetBuiltinCommands([]string{"delivery-kit"})

	top := requires("package", "v1.0.0", "delivery-kit", ">= 1.0.0")

	plan, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err)
	require.Nil(t, reason, "a built-in command satisfies the dependency")
	require.NotNil(t, plan)
	assert.NotContains(t, planStepVersions(plan), "delivery-kit",
		"a built-in is not installed, so it is never planned")
}

// presence-only: a version constraint on a built-in dependency is not enforced,
// since a built-in cannot be upgraded.
func TestPlannerBuiltinIgnoresVersionConstraint(t *testing.T) {
	src := &multiPluginSource{
		unpublished: map[string]bool{"delivery-kit": true},
		contracts:   map[string]map[string]*internal.Plugin{},
	}
	m := plannerManager(t, src)
	m.SetBuiltinCommands([]string{"delivery-kit"})

	// An unsatisfiable constraint still passes - presence is enough.
	top := requires("package", "v1.0.0", "delivery-kit", ">= 999.0.0")

	_, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err)
	assert.Nil(t, reason, "presence satisfies the dependency regardless of the constraint")
}

// Without the built-in registered, the same contract is unsatisfiable: the
// dependency is looked up in the registry and reported unpublished. This is the
// contrast that proves the built-in short-circuit is what changes the outcome.
func TestPlannerUnregisteredBuiltinStillUnresolved(t *testing.T) {
	src := &multiPluginSource{
		unpublished: map[string]bool{"delivery-kit": true},
		contracts:   map[string]map[string]*internal.Plugin{},
	}
	m := plannerManager(t, src) // no SetBuiltinCommands

	top := requires("package", "v1.0.0", "delivery-kit", "")

	plan, reason, err := m.planFor(context.Background(), top, false)
	require.NoError(t, err)
	require.Nil(t, plan)
	require.NotNil(t, reason, "an unregistered, unpublished dependency is unsatisfiable")
	assert.Equal(t, reasonDepNotPublished, reason.kind)
}

// The final pre-switch guard also treats a built-in as satisfying a mandatory
// requirement: no soft failure is recorded, so the install is not rejected after
// the binary swap.
func TestValidateMandatoryRequirementSatisfiedByBuiltin(t *testing.T) {
	m := testManager()
	m.pluginDirectory = t.TempDir()
	m.SetBuiltinCommands([]string{"delivery-kit"})

	plugin := &internal.Plugin{
		Name:    "package",
		Version: "v1.0.0",
		Requirements: internal.Requirements{
			Plugins: internal.PluginRequirementsGroup{
				Mandatory: []internal.PluginRequirement{{Name: "delivery-kit", Constraint: ">= 1.0.0"}},
			},
		},
	}

	failed, err := m.validatePluginRequirementMandatory(plugin)
	require.NoError(t, err)
	assert.Empty(t, failed, "a built-in command satisfies the mandatory requirement")
}

// Without the built-in registered, the same mandatory requirement is recorded as
// missing - the regression guard for the validator change.
func TestValidateMandatoryRequirementMissingWithoutBuiltin(t *testing.T) {
	m := testManager()
	m.pluginDirectory = t.TempDir() // empty install root: nothing installed

	plugin := &internal.Plugin{
		Name:    "package",
		Version: "v1.0.0",
		Requirements: internal.Requirements{
			Plugins: internal.PluginRequirementsGroup{
				Mandatory: []internal.PluginRequirement{{Name: "delivery-kit", Constraint: ""}},
			},
		},
	}

	failed, err := m.validatePluginRequirementMandatory(plugin)
	require.NoError(t, err)
	assert.Contains(t, failed, "delivery-kit", "an unregistered, uninstalled dependency is missing")
}

// A conditional requirement on a built-in is satisfied (a no-op), never a hard
// error - even with a version constraint that is not enforced for a built-in.
func TestValidateConditionalRequirementSatisfiedByBuiltin(t *testing.T) {
	m := testManager()
	m.pluginDirectory = t.TempDir()
	m.SetBuiltinCommands([]string{"delivery-kit"})

	plugin := &internal.Plugin{
		Name:    "package",
		Version: "v1.0.0",
		Requirements: internal.Requirements{
			Plugins: internal.PluginRequirementsGroup{
				Conditional: []internal.PluginRequirement{{Name: "delivery-kit", Constraint: ">= 1.0.0"}},
			},
		},
	}

	require.NoError(t, m.validatePluginRequirementConditional(plugin))
}
