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

package modulereleases

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"

	"github.com/deckhouse/deckhouse-cli/internal/system/cmd/module/api/v1alpha1"
)

func TestNewReleaseInfo(t *testing.T) {
	tests := []struct {
		name     string
		release  *v1alpha1.ModuleRelease
		expected ModuleReleaseInfo
	}{
		{
			name: "full release info",
			release: &v1alpha1.ModuleRelease{
				ObjectMeta: metav1.ObjectMeta{
					Name: "csi-hpe-v0.3.10",
					Annotations: map[string]string{
						v1alpha1.ModuleReleaseApprovedAnnotation: "true",
					},
				},
				Spec: v1alpha1.ModuleReleaseSpec{
					ModuleName: "csi-hpe",
					Version:    "v0.3.10",
				},
				Status: v1alpha1.ModuleReleaseStatus{
					Phase:   "Pending",
					Message: "Waiting for approval",
				},
			},
			expected: ModuleReleaseInfo{
				Name:       "csi-hpe-v0.3.10",
				ModuleName: "csi-hpe",
				Version:    "v0.3.10",
				Phase:      "Pending",
				Message:    "Waiting for approval",
				IsApproved: true,
				IsApplyNow: false,
			},
		},
		{
			name: "release with apply-now annotation",
			release: &v1alpha1.ModuleRelease{
				ObjectMeta: metav1.ObjectMeta{
					Name: "mymodule-v1.0.0",
					Annotations: map[string]string{
						v1alpha1.ModuleReleaseApplyNowAnnotation: "true",
					},
				},
				Spec: v1alpha1.ModuleReleaseSpec{
					ModuleName: "mymodule",
					Version:    "v1.0.0",
				},
				Status: v1alpha1.ModuleReleaseStatus{
					Phase: "Pending",
				},
			},
			expected: ModuleReleaseInfo{
				Name:       "mymodule-v1.0.0",
				ModuleName: "mymodule",
				Version:    "v1.0.0",
				Phase:      "Pending",
				IsApproved: false,
				IsApplyNow: true,
			},
		},
		{
			name: "release without version in spec - extracted from name",
			release: &v1alpha1.ModuleRelease{
				ObjectMeta: metav1.ObjectMeta{
					Name: "mymodule-v2.0.0",
				},
				Spec: v1alpha1.ModuleReleaseSpec{
					ModuleName: "mymodule",
					Version:    "", // empty version
				},
				Status: v1alpha1.ModuleReleaseStatus{
					Phase: "Deployed",
				},
			},
			expected: ModuleReleaseInfo{
				Name:       "mymodule-v2.0.0",
				ModuleName: "mymodule",
				Version:    "v2.0.0", // extracted from name
				Phase:      "Deployed",
				IsApproved: false,
				IsApplyNow: false,
			},
		},
		{
			name: "release with no annotations",
			release: &v1alpha1.ModuleRelease{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-v1.0.0",
				},
				Spec: v1alpha1.ModuleReleaseSpec{
					ModuleName: "test",
					Version:    "v1.0.0",
				},
				Status: v1alpha1.ModuleReleaseStatus{
					Phase: "Superseded",
				},
			},
			expected: ModuleReleaseInfo{
				Name:       "test-v1.0.0",
				ModuleName: "test",
				Version:    "v1.0.0",
				Phase:      "Superseded",
				IsApproved: false,
				IsApplyNow: false,
			},
		},
		{
			name: "release with both annotations",
			release: &v1alpha1.ModuleRelease{
				ObjectMeta: metav1.ObjectMeta{
					Name: "dual-v1.0.0",
					Annotations: map[string]string{
						v1alpha1.ModuleReleaseApprovedAnnotation: "true",
						v1alpha1.ModuleReleaseApplyNowAnnotation: "true",
					},
				},
				Spec: v1alpha1.ModuleReleaseSpec{
					ModuleName: "dual",
					Version:    "v1.0.0",
				},
				Status: v1alpha1.ModuleReleaseStatus{
					Phase: "Pending",
				},
			},
			expected: ModuleReleaseInfo{
				Name:       "dual-v1.0.0",
				ModuleName: "dual",
				Version:    "v1.0.0",
				Phase:      "Pending",
				IsApproved: true,
				IsApplyNow: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := newReleaseInfo(tt.release)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestListModuleReleases(t *testing.T) {
	// Create test releases
	releases := []runtime.Object{
		createUnstructuredRelease("csi-hpe-v0.3.10", "csi-hpe", "v0.3.10", "Pending", false, false),
		createUnstructuredRelease("csi-hpe-v0.3.11", "csi-hpe", "v0.3.11", "Deployed", true, false),
		createUnstructuredRelease("prometheus-v1.0.0", "prometheus", "v1.0.0", "Pending", false, false),
	}

	scheme := runtime.NewScheme()
	dynamicClient := fake.NewSimpleDynamicClient(scheme, releases...)

	t.Run("list all releases", func(t *testing.T) {
		result, err := ListModuleReleases(dynamicClient, "")
		require.NoError(t, err)
		assert.Len(t, result, 3)
	})

	t.Run("list releases for specific module", func(t *testing.T) {
		result, err := ListModuleReleases(dynamicClient, "csi-hpe")
		require.NoError(t, err)
		assert.Len(t, result, 2)
		for _, r := range result {
			assert.Equal(t, "csi-hpe", r.ModuleName)
		}
	})

	t.Run("list releases for non-existent module", func(t *testing.T) {
		result, err := ListModuleReleases(dynamicClient, "non-existent")
		require.NoError(t, err)
		assert.Empty(t, result)
	})
}

func TestGetModuleRelease(t *testing.T) {
	releases := []runtime.Object{
		createUnstructuredRelease("csi-hpe-v0.3.10", "csi-hpe", "v0.3.10", "Pending", false, false),
	}

	scheme := runtime.NewScheme()
	dynamicClient := fake.NewSimpleDynamicClient(scheme, releases...)

	t.Run("get existing release", func(t *testing.T) {
		result, err := GetModuleRelease(dynamicClient, "csi-hpe", "v0.3.10")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "csi-hpe-v0.3.10", result.Name)
		assert.Equal(t, "csi-hpe", result.ModuleName)
		assert.Equal(t, "v0.3.10", result.Version)
	})

	t.Run("get release without v prefix", func(t *testing.T) {
		result, err := GetModuleRelease(dynamicClient, "csi-hpe", "0.3.10")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "csi-hpe-v0.3.10", result.Name)
	})

	t.Run("get non-existent release", func(t *testing.T) {
		_, err := GetModuleRelease(dynamicClient, "csi-hpe", "v9.9.9")
		assert.Error(t, err)
	})
}

func TestApproveModuleRelease(t *testing.T) {
	releases := []runtime.Object{
		createUnstructuredRelease("csi-hpe-v0.3.10", "csi-hpe", "v0.3.10", "Pending", false, false),
	}

	scheme := runtime.NewScheme()
	dynamicClient := fake.NewSimpleDynamicClient(scheme, releases...)

	t.Run("approve existing release", func(t *testing.T) {
		err := ApproveModuleRelease(dynamicClient, "csi-hpe-v0.3.10")
		require.NoError(t, err)
	})

	t.Run("approve non-existent release", func(t *testing.T) {
		err := ApproveModuleRelease(dynamicClient, "non-existent-v1.0.0")
		assert.Error(t, err)
	})
}

func TestApplyNowModuleRelease(t *testing.T) {
	releases := []runtime.Object{
		createUnstructuredRelease("mymodule-v1.0.0", "mymodule", "v1.0.0", "Pending", false, false),
	}

	scheme := runtime.NewScheme()
	dynamicClient := fake.NewSimpleDynamicClient(scheme, releases...)

	t.Run("apply-now existing release", func(t *testing.T) {
		err := ApplyNowModuleRelease(dynamicClient, "mymodule-v1.0.0")
		require.NoError(t, err)
	})

	t.Run("apply-now non-existent release", func(t *testing.T) {
		err := ApplyNowModuleRelease(dynamicClient, "non-existent-v1.0.0")
		assert.Error(t, err)
	})
}

func TestFindReleases(t *testing.T) {
	releases := []runtime.Object{
		createUnstructuredRelease("mod-v1.0.0", "mod", "v1.0.0", "Pending", false, false),
		createUnstructuredRelease("mod-v1.1.0", "mod", "v1.1.0", "Pending", true, false),  // approved
		createUnstructuredRelease("mod-v1.2.0", "mod", "v1.2.0", "Deployed", true, false), // deployed
		createUnstructuredRelease("mod-v1.3.0", "mod", "v1.3.0", "Pending", false, true),  // apply-now
	}

	scheme := runtime.NewScheme()
	dynamicClient := fake.NewSimpleDynamicClient(scheme, releases...)

	t.Run("find releases that can be approved", func(t *testing.T) {
		result, err := FindReleases(dynamicClient, "mod", CanBeApproved)
		require.NoError(t, err)
		// v1.0.0 and v1.3.0 are pending and not approved
		// (CanBeApproved only checks phase and IsApproved, not IsApplyNow)
		assert.Len(t, result, 2)
		versions := []string{result[0].Version, result[1].Version}
		assert.Contains(t, versions, "v1.0.0")
		assert.Contains(t, versions, "v1.3.0")
	})

	t.Run("find releases that can be applied now", func(t *testing.T) {
		result, err := FindReleases(dynamicClient, "mod", CanBeAppliedNow)
		require.NoError(t, err)
		// v1.0.0 and v1.1.0 are pending without apply-now
		assert.Len(t, result, 2)
	})

	t.Run("find releases with custom predicate", func(t *testing.T) {
		// Custom predicate: only deployed releases
		isDeployed := func(r ModuleReleaseInfo) bool {
			return r.Phase == v1alpha1.ModuleReleasePhaseDeployed
		}
		result, err := FindReleases(dynamicClient, "mod", isDeployed)
		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, "v1.2.0", result[0].Version)
	})

	t.Run("results are sorted by version", func(t *testing.T) {
		allPending := func(r ModuleReleaseInfo) bool {
			return r.Phase == v1alpha1.ModuleReleasePhasePending
		}
		result, err := FindReleases(dynamicClient, "mod", allPending)
		require.NoError(t, err)
		assert.Len(t, result, 3)
		// Should be sorted: v1.0.0, v1.1.0, v1.3.0
		assert.Equal(t, "v1.0.0", result[0].Version)
		assert.Equal(t, "v1.1.0", result[1].Version)
		assert.Equal(t, "v1.3.0", result[2].Version)
	})
}

func TestFindVersions(t *testing.T) {
	releases := []runtime.Object{
		createUnstructuredRelease("mod-v1.0.0", "mod", "v1.0.0", "Pending", false, false),
		createUnstructuredRelease("mod-v1.1.0", "mod", "v1.1.0", "Pending", false, false),
		createUnstructuredRelease("mod-v2.0.0", "mod", "v2.0.0", "Deployed", false, false),
	}

	scheme := runtime.NewScheme()
	dynamicClient := fake.NewSimpleDynamicClient(scheme, releases...)

	t.Run("find versions for pending releases", func(t *testing.T) {
		isPending := func(r ModuleReleaseInfo) bool {
			return r.Phase == v1alpha1.ModuleReleasePhasePending
		}
		result, err := FindVersions(dynamicClient, "mod", isPending)
		require.NoError(t, err)
		assert.Equal(t, []string{"v1.0.0", "v1.1.0"}, result)
	})

	t.Run("versions are normalized with v prefix", func(t *testing.T) {
		all := func(r ModuleReleaseInfo) bool { return true }
		result, err := FindVersions(dynamicClient, "mod", all)
		require.NoError(t, err)
		for _, v := range result {
			assert.True(t, len(v) > 0 && v[0] == 'v', "version should have v prefix: %s", v)
		}
	})
}

func TestListModuleNames(t *testing.T) {
	releases := []runtime.Object{
		createUnstructuredRelease("csi-hpe-v0.3.10", "csi-hpe", "v0.3.10", "Pending", false, false),
		createUnstructuredRelease("csi-hpe-v0.3.11", "csi-hpe", "v0.3.11", "Deployed", false, false),
		createUnstructuredRelease("prometheus-v1.0.0", "prometheus", "v1.0.0", "Pending", false, false),
		createUnstructuredRelease("grafana-v2.0.0", "grafana", "v2.0.0", "Pending", false, false),
	}

	scheme := runtime.NewScheme()
	dynamicClient := fake.NewSimpleDynamicClient(scheme, releases...)

	t.Run("list unique module names", func(t *testing.T) {
		result, err := ListModuleNames(dynamicClient)
		require.NoError(t, err)
		assert.Len(t, result, 3) // csi-hpe, prometheus, grafana (no duplicates)
		assert.Contains(t, result, "csi-hpe")
		assert.Contains(t, result, "prometheus")
		assert.Contains(t, result, "grafana")
	})

	t.Run("module names are sorted", func(t *testing.T) {
		result, err := ListModuleNames(dynamicClient)
		require.NoError(t, err)
		// Should be sorted alphabetically
		assert.Equal(t, []string{"csi-hpe", "grafana", "prometheus"}, result)
	})
}

// Helper function to create unstructured module release for testing
func createUnstructuredRelease(name, moduleName, version, phase string, approved, applyNow bool) *unstructured.Unstructured {
	annotations := make(map[string]interface{})
	if approved {
		annotations[v1alpha1.ModuleReleaseApprovedAnnotation] = "true"
	}
	if applyNow {
		annotations[v1alpha1.ModuleReleaseApplyNowAnnotation] = "true"
	}

	metadata := map[string]interface{}{
		"name": name,
	}
	if len(annotations) > 0 {
		metadata["annotations"] = annotations
	}

	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "deckhouse.io/v1alpha1",
			"kind":       "ModuleRelease",
			"metadata":   metadata,
			"spec": map[string]interface{}{
				"moduleName": moduleName,
				"version":    version,
			},
			"status": map[string]interface{}{
				"phase":    phase,
				"approved": approved,
			},
		},
	}
}
