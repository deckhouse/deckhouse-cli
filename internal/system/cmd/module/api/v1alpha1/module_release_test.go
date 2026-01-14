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

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestModuleReleaseFromUnstructured(t *testing.T) {
	tests := []struct {
		name        string
		obj         *unstructured.Unstructured
		expectError bool
		validate    func(t *testing.T, mr *ModuleRelease)
	}{
		{
			name: "valid module release",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "deckhouse.io/v1alpha1",
					"kind":       "ModuleRelease",
					"metadata": map[string]interface{}{
						"name": "csi-hpe-v0.3.10",
						"annotations": map[string]interface{}{
							ModuleReleaseApprovedAnnotation: "true",
						},
					},
					"spec": map[string]interface{}{
						"moduleName": "csi-hpe",
						"version":    "v0.3.10",
						"weight":     float64(900),
					},
					"status": map[string]interface{}{
						"phase":    "Pending",
						"approved": true,
						"message":  "Waiting for approval",
					},
				},
			},
			expectError: false,
			validate: func(t *testing.T, mr *ModuleRelease) {
				assert.Equal(t, "csi-hpe-v0.3.10", mr.Name)
				assert.Equal(t, "csi-hpe", mr.Spec.ModuleName)
				assert.Equal(t, "v0.3.10", mr.Spec.Version)
				assert.Equal(t, uint32(900), mr.Spec.Weight)
				assert.Equal(t, "Pending", mr.Status.Phase)
				assert.True(t, mr.Status.Approved)
				assert.Equal(t, "Waiting for approval", mr.Status.Message)
			},
		},
		{
			name: "module release without annotations",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "deckhouse.io/v1alpha1",
					"kind":       "ModuleRelease",
					"metadata": map[string]interface{}{
						"name": "mymodule-v1.0.0",
					},
					"spec": map[string]interface{}{
						"moduleName": "mymodule",
						"version":    "v1.0.0",
					},
					"status": map[string]interface{}{
						"phase": "Deployed",
					},
				},
			},
			expectError: false,
			validate: func(t *testing.T, mr *ModuleRelease) {
				assert.Equal(t, "mymodule-v1.0.0", mr.Name)
				assert.Equal(t, "mymodule", mr.Spec.ModuleName)
				assert.Nil(t, mr.Annotations)
				assert.Equal(t, "Deployed", mr.Status.Phase)
			},
		},
		{
			name: "module release with empty spec",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "deckhouse.io/v1alpha1",
					"kind":       "ModuleRelease",
					"metadata": map[string]interface{}{
						"name": "test-release",
					},
					"spec":   map[string]interface{}{},
					"status": map[string]interface{}{},
				},
			},
			expectError: false,
			validate: func(t *testing.T, mr *ModuleRelease) {
				assert.Equal(t, "test-release", mr.Name)
				assert.Empty(t, mr.Spec.ModuleName)
				assert.Empty(t, mr.Spec.Version)
			},
		},
		{
			name: "module release with apply-now annotation",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "deckhouse.io/v1alpha1",
					"kind":       "ModuleRelease",
					"metadata": map[string]interface{}{
						"name": "mymodule-v2.0.0",
						"annotations": map[string]interface{}{
							ModuleReleaseApplyNowAnnotation: "true",
						},
					},
					"spec": map[string]interface{}{
						"moduleName": "mymodule",
						"version":    "v2.0.0",
					},
					"status": map[string]interface{}{
						"phase": "Pending",
					},
				},
			},
			expectError: false,
			validate: func(t *testing.T, mr *ModuleRelease) {
				assert.True(t, mr.IsApplyNow())
				assert.False(t, mr.IsApproved())
			},
		},
		{
			name: "module release with both annotations",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "deckhouse.io/v1alpha1",
					"kind":       "ModuleRelease",
					"metadata": map[string]interface{}{
						"name": "mymodule-v3.0.0",
						"annotations": map[string]interface{}{
							ModuleReleaseApprovedAnnotation: "true",
							ModuleReleaseApplyNowAnnotation: "true",
						},
					},
					"spec": map[string]interface{}{
						"moduleName": "mymodule",
						"version":    "v3.0.0",
					},
					"status": map[string]interface{}{
						"phase": "Pending",
					},
				},
			},
			expectError: false,
			validate: func(t *testing.T, mr *ModuleRelease) {
				assert.True(t, mr.IsApproved())
				assert.True(t, mr.IsApplyNow())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr, err := ModuleReleaseFromUnstructured(tt.obj)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, mr)
			tt.validate(t, mr)
		})
	}
}

func TestModuleRelease_IsApproved(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    bool
	}{
		{
			name:        "nil annotations",
			annotations: nil,
			expected:    false,
		},
		{
			name:        "empty annotations",
			annotations: map[string]string{},
			expected:    false,
		},
		{
			name: "approved annotation set to true",
			annotations: map[string]string{
				ModuleReleaseApprovedAnnotation: "true",
			},
			expected: true,
		},
		{
			name: "approved annotation set to false",
			annotations: map[string]string{
				ModuleReleaseApprovedAnnotation: "false",
			},
			expected: false,
		},
		{
			name: "approved annotation set to empty string",
			annotations: map[string]string{
				ModuleReleaseApprovedAnnotation: "",
			},
			expected: false,
		},
		{
			name: "approved annotation set to TRUE (uppercase)",
			annotations: map[string]string{
				ModuleReleaseApprovedAnnotation: "TRUE",
			},
			expected: false, // only exact "true" matches
		},
		{
			name: "approved annotation set to 1",
			annotations: map[string]string{
				ModuleReleaseApprovedAnnotation: "1",
			},
			expected: false, // only exact "true" matches
		},
		{
			name: "other annotation present",
			annotations: map[string]string{
				"other-annotation": "true",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := &ModuleRelease{}
			mr.Annotations = tt.annotations

			result := mr.IsApproved()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestModuleRelease_IsApplyNow(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    bool
	}{
		{
			name:        "nil annotations",
			annotations: nil,
			expected:    false,
		},
		{
			name:        "empty annotations",
			annotations: map[string]string{},
			expected:    false,
		},
		{
			name: "apply-now annotation set to true",
			annotations: map[string]string{
				ModuleReleaseApplyNowAnnotation: "true",
			},
			expected: true,
		},
		{
			name: "apply-now annotation set to false",
			annotations: map[string]string{
				ModuleReleaseApplyNowAnnotation: "false",
			},
			expected: false,
		},
		{
			name: "apply-now annotation set to empty string",
			annotations: map[string]string{
				ModuleReleaseApplyNowAnnotation: "",
			},
			expected: false,
		},
		{
			name: "apply-now annotation set to TRUE (uppercase)",
			annotations: map[string]string{
				ModuleReleaseApplyNowAnnotation: "TRUE",
			},
			expected: false, // only exact "true" matches
		},
		{
			name: "apply-now annotation set to 1",
			annotations: map[string]string{
				ModuleReleaseApplyNowAnnotation: "1",
			},
			expected: false, // only exact "true" matches
		},
		{
			name: "other annotation present",
			annotations: map[string]string{
				"other-annotation": "true",
			},
			expected: false,
		},
		{
			name: "both approved and apply-now",
			annotations: map[string]string{
				ModuleReleaseApprovedAnnotation: "true",
				ModuleReleaseApplyNowAnnotation: "true",
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := &ModuleRelease{}
			mr.Annotations = tt.annotations

			result := mr.IsApplyNow()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestModuleReleaseGVR(t *testing.T) {
	assert.Equal(t, "deckhouse.io", ModuleReleaseGVR.Group)
	assert.Equal(t, "v1alpha1", ModuleReleaseGVR.Version)
	assert.Equal(t, "modulereleases", ModuleReleaseGVR.Resource)
}

func TestModuleReleasePhaseConstants(t *testing.T) {
	// Verify phase constants are correct
	assert.Equal(t, "Pending", ModuleReleasePhasePending)
	assert.Equal(t, "Deployed", ModuleReleasePhaseDeployed)
	assert.Equal(t, "Superseded", ModuleReleasePhaseSuperseded)
	assert.Equal(t, "Suspended", ModuleReleasePhaseSuspended)
	assert.Equal(t, "Skipped", ModuleReleasePhaseSkipped)
}

func TestModuleReleaseAnnotationConstants(t *testing.T) {
	// Verify annotation key constants
	assert.Equal(t, "modules.deckhouse.io/approved", ModuleReleaseApprovedAnnotation)
	assert.Equal(t, "modules.deckhouse.io/apply-now", ModuleReleaseApplyNowAnnotation)
}
