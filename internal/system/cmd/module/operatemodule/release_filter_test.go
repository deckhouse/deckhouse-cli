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

package operatemodule

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/v1alpha1"
)

func TestCanBeApproved(t *testing.T) {
	tests := []struct {
		name     string
		release  ModuleReleaseInfo
		expected bool
	}{
		{
			name: "pending and not approved - can be approved",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhasePending,
				IsApproved: false,
			},
			expected: true,
		},
		{
			name: "pending but already approved - cannot be approved",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhasePending,
				IsApproved: true,
			},
			expected: false,
		},
		{
			name: "deployed and not approved - cannot be approved",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhaseDeployed,
				IsApproved: false,
			},
			expected: false,
		},
		{
			name: "deployed and approved - cannot be approved",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhaseDeployed,
				IsApproved: true,
			},
			expected: false,
		},
		{
			name: "superseded phase - cannot be approved",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhaseSuperseded,
				IsApproved: false,
			},
			expected: false,
		},
		{
			name: "suspended phase - cannot be approved",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhaseSuspended,
				IsApproved: false,
			},
			expected: false,
		},
		{
			name: "skipped phase - cannot be approved",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhaseSkipped,
				IsApproved: false,
			},
			expected: false,
		},
		{
			name: "empty phase - cannot be approved",
			release: ModuleReleaseInfo{
				Phase:      "",
				IsApproved: false,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CanBeApproved(tt.release)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCanBeAppliedNow(t *testing.T) {
	tests := []struct {
		name     string
		release  ModuleReleaseInfo
		expected bool
	}{
		{
			name: "pending and no apply-now - can be applied now",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhasePending,
				IsApplyNow: false,
			},
			expected: true,
		},
		{
			name: "pending but already has apply-now - cannot be applied now",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhasePending,
				IsApplyNow: true,
			},
			expected: false,
		},
		{
			name: "deployed and no apply-now - cannot be applied now",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhaseDeployed,
				IsApplyNow: false,
			},
			expected: false,
		},
		{
			name: "deployed and has apply-now - cannot be applied now",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhaseDeployed,
				IsApplyNow: true,
			},
			expected: false,
		},
		{
			name: "superseded phase - cannot be applied now",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhaseSuperseded,
				IsApplyNow: false,
			},
			expected: false,
		},
		{
			name: "suspended phase - cannot be applied now",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhaseSuspended,
				IsApplyNow: false,
			},
			expected: false,
		},
		{
			name: "skipped phase - cannot be applied now",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhaseSkipped,
				IsApplyNow: false,
			},
			expected: false,
		},
		{
			name: "empty phase - cannot be applied now",
			release: ModuleReleaseInfo{
				Phase:      "",
				IsApplyNow: false,
			},
			expected: false,
		},
		{
			name: "pending with both flags",
			release: ModuleReleaseInfo{
				Phase:      v1alpha1.ModuleReleasePhasePending,
				IsApproved: true,
				IsApplyNow: false,
			},
			expected: true, // IsApproved doesn't affect CanBeAppliedNow
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CanBeAppliedNow(tt.release)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestReleaseMatchFuncAsFilter(t *testing.T) {
	// Test that ReleaseMatchFunc works correctly as a filter function
	releases := []ModuleReleaseInfo{
		{Name: "mod-v1.0.0", Version: "v1.0.0", Phase: v1alpha1.ModuleReleasePhasePending, IsApproved: false},
		{Name: "mod-v1.1.0", Version: "v1.1.0", Phase: v1alpha1.ModuleReleasePhasePending, IsApproved: true},
		{Name: "mod-v1.2.0", Version: "v1.2.0", Phase: v1alpha1.ModuleReleasePhaseDeployed, IsApproved: true},
		{Name: "mod-v1.3.0", Version: "v1.3.0", Phase: v1alpha1.ModuleReleasePhasePending, IsApproved: false, IsApplyNow: true},
	}

	t.Run("filter for approval", func(t *testing.T) {
		var filtered []ModuleReleaseInfo
		for _, r := range releases {
			if CanBeApproved(r) {
				filtered = append(filtered, r)
			}
		}
		// v1.0.0 and v1.3.0 should match (pending and not approved)
		// Note: CanBeApproved doesn't check IsApplyNow, only phase and IsApproved
		assert.Len(t, filtered, 2)
		versions := []string{filtered[0].Version, filtered[1].Version}
		assert.Contains(t, versions, "v1.0.0")
		assert.Contains(t, versions, "v1.3.0")
	})

	t.Run("filter for apply-now", func(t *testing.T) {
		var filtered []ModuleReleaseInfo
		for _, r := range releases {
			if CanBeAppliedNow(r) {
				filtered = append(filtered, r)
			}
		}
		// v1.0.0 and v1.1.0 should match (pending and no apply-now)
		assert.Len(t, filtered, 2)
		versions := []string{filtered[0].Version, filtered[1].Version}
		assert.Contains(t, versions, "v1.0.0")
		assert.Contains(t, versions, "v1.1.0")
	})
}
