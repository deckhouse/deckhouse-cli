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

package plugins

import (
	"context"
	"strings"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal"
	d8flags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
)

// findSuggestion returns the suggestion whose Cause matches exactly. Shared by the
// install- and run-time tests that assert on the requirement diagnostic.
func findSuggestion(he *diagnostic.HelpfulError, cause string) (diagnostic.Suggestion, bool) {
	for _, s := range he.Suggestions {
		if s.Cause == cause {
			return s, true
		}
	}

	return diagnostic.Suggestion{}, false
}

func TestFailedConstraintsHelpfulError(t *testing.T) {
	wrongVersion, err := semver.NewConstraint(">=2.0.0")
	require.NoError(t, err)

	fc := failedConstraints{
		"zeta":  nil,          // missing entirely
		"alpha": wrongVersion, // installed but incompatible
	}

	he := fc.helpfulError("header text", true)
	assert.Equal(t, "header text", he.Category)
	require.Len(t, he.Suggestions, 2)

	// Suggestions are sorted by dependency name: alpha before zeta.
	assert.Equal(t, "alpha must satisfy >=2.0.0", he.Suggestions[0].Cause)
	assert.Contains(t, strings.Join(he.Suggestions[0].Solutions, " "), "--version",
		"a version mismatch suggests installing a matching version")

	assert.Equal(t, "zeta is not installed", he.Suggestions[1].Cause)
	assert.Contains(t, strings.Join(he.Suggestions[1].Solutions, " "), "--resolve-plugins-conflicts",
		"a missing dep offers the auto-install flag when resolvable")

	// resolvable=false drops the --resolve-plugins-conflicts hint.
	plain := fc.helpfulError("header text", false)
	missing, ok := findSuggestion(plain, "zeta is not installed")
	require.True(t, ok)
	assert.NotContains(t, strings.Join(missing.Solutions, " "), "--resolve-plugins-conflicts")
}

func TestSkipClusterChecksDowngradesEnforcement(t *testing.T) {
	prev := d8flags.SkipClusterChecks
	t.Cleanup(func() { d8flags.SkipClusterChecks = prev })
	d8flags.SkipClusterChecks = true

	plugin := &internal.Plugin{Name: "p", Requirements: internal.Requirements{
		Kubernetes: internal.KubernetesRequirement{Constraint: ">= 99.0"},
	}}

	// With the escape hatch set, a plugin with a cluster requirement is not blocked
	// and no cluster is contacted (validateClusterRequirements returns before clusterState).
	require.NoError(t, testManager().validateClusterRequirements(context.Background(), plugin))
}

// TestValidatePluginConflict_BugRegression covers the historical bug where the
// reverse conflict check compared the *installed* plugin's version against its
// own constraint on the new plugin (a tautology). With the fix the constraint
// is checked against the NEW plugin's version, which is the only thing that
// matters when deciding whether the install is compatible.
//
// Pre-fix scenario the bug silently passed:
//   - plugin-A v2.5.0 installed; requires "foo ^2.0.0"
//   - installing foo v5.0.0
//   - bug: check A's 2.5.0 vs "^2.0.0" → satisfied → no error (WRONG)
//   - fix: check foo's 5.0.0 vs "^2.0.0" → not satisfied → error (CORRECT)
func TestValidatePluginConflict_BugRegression(t *testing.T) {
	installedA := &internal.Plugin{
		Name:    "plugin-a",
		Version: "v2.5.0",
		Requirements: internal.Requirements{
			Plugins: internal.PluginRequirementsGroup{
				Mandatory: []internal.PluginRequirement{
					{Name: "foo", Constraint: "^2.0.0"},
				},
			},
		},
	}
	newFoo := &internal.Plugin{Name: "foo", Version: "v5.0.0"}

	if err := validatePluginConflict(newFoo, installedA); err == nil {
		t.Fatal("expected conflict error (bug regression): installing foo v5.0.0 violates plugin-a's '^2.0.0' constraint, but pre-fix code missed it")
	}
}

// TestValidatePluginConflict_NoConflictWhenSatisfies covers the happy path:
// when the new plugin's version satisfies the existing requirement, no error.
func TestValidatePluginConflict_NoConflictWhenSatisfies(t *testing.T) {
	installedA := &internal.Plugin{
		Name:    "plugin-a",
		Version: "v1.0.0",
		Requirements: internal.Requirements{
			Plugins: internal.PluginRequirementsGroup{
				Mandatory: []internal.PluginRequirement{
					{Name: "foo", Constraint: "^2.0.0"},
				},
			},
		},
	}
	newFoo := &internal.Plugin{Name: "foo", Version: "v2.5.0"}

	if err := validatePluginConflict(newFoo, installedA); err != nil {
		t.Errorf("expected no conflict (v2.5.0 satisfies ^2.0.0), got: %v", err)
	}
}

// TestValidatePluginConflict_DetectsConditional ensures that constraints
// declared under .Conditional also trigger the conflict check - the section
// describes intent (mandatory vs conditional from the installer's perspective)
// but for backwards compatibility on the conflict side both must be honoured.
func TestValidatePluginConflict_DetectsConditional(t *testing.T) {
	installedA := &internal.Plugin{
		Name:    "plugin-a",
		Version: "v1.0.0",
		Requirements: internal.Requirements{
			Plugins: internal.PluginRequirementsGroup{
				Conditional: []internal.PluginRequirement{
					{Name: "foo", Constraint: "^2.0.0"},
				},
			},
		},
	}
	newFoo := &internal.Plugin{Name: "foo", Version: "v5.0.0"}

	if err := validatePluginConflict(newFoo, installedA); err == nil {
		t.Fatal("expected conflict error for .Conditional section, got nil")
	}
}
