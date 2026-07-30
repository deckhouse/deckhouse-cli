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

package v1alpha1

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newCreatePVCTemplate builds a fully populated PVC template so that every branch of the
// hand-written deepcopy (slice, map, both pointers) has something to copy.
func newCreatePVCTemplate() *PersistentVolumeClaimTemplateSpec {
	storageClass := "linstor-thin-r1"
	volumeMode := PersistentVolumeFilesystem

	return &PersistentVolumeClaimTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Name: "restored-pvc"},
		PersistentVolumeClaimSpec: PersistentVolumeClaimSpec{
			AccessModes: []PersistentVolumeAccessMode{ReadWriteOnce},
			Resources: VolumeResourceRequirements{
				Requests: ResourceList{ResourceStorage: resource.MustParse("10Gi")},
			},
			StorageClassName: &storageClass,
			VolumeMode:       &volumeMode,
		},
	}
}

// TestDataImportSpec_JSONWireShape pins the exact set of spec keys this type puts on the wire,
// because the CRD's behaviour differs per key and the difference is invisible in Go: the schema is
// non-preserving (an unknown key such as the removed spec.targetRef is pruned without an error),
// and waitForFirstConsumer is defaulted to true (so an absent key is NOT the same as false).
// A stray omitempty on the wrong field therefore changes what the server does while every
// struct-level assertion keeps passing.
func TestDataImportSpec_JSONWireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		spec                     DataImportSpec
		wantKeys                 []string
		wantWaitForFirstConsumer string
	}{
		{
			name: "success: fully populated CreatePVC spec emits every key",
			spec: DataImportSpec{
				TTL:                  "15m",
				Publish:              true,
				WaitForFirstConsumer: true,
				Mode:                 DataImportModeCreatePVC,
				PvcTemplate:          newCreatePVCTemplate(),
			},
			wantKeys:                 []string{"mode", "publish", "pvcTemplate", "ttl", "waitForFirstConsumer"},
			wantWaitForFirstConsumer: "true",
		},
		{
			// Regression guard: with omitempty this key disappeared and the server applied its
			// default of true, making --wffc=false unexpressible.
			name: "success: waitForFirstConsumer=false still reaches the wire",
			spec: DataImportSpec{
				TTL:                  "15m",
				Publish:              true,
				WaitForFirstConsumer: false,
				Mode:                 DataImportModeCreatePVC,
				PvcTemplate:          newCreatePVCTemplate(),
			},
			wantKeys:                 []string{"mode", "publish", "pvcTemplate", "ttl", "waitForFirstConsumer"},
			wantWaitForFirstConsumer: "false",
		},
		{
			// publish keeps its omitempty on purpose: the CRD does not default it to true, so an
			// absent key and false are equivalent there.
			name: "success: publish=false is omitted",
			spec: DataImportSpec{
				TTL:                  "15m",
				Publish:              false,
				WaitForFirstConsumer: true,
				Mode:                 DataImportModeCreatePVC,
				PvcTemplate:          newCreatePVCTemplate(),
			},
			wantKeys:                 []string{"mode", "pvcTemplate", "ttl", "waitForFirstConsumer"},
			wantWaitForFirstConsumer: "true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := json.Marshal(tt.spec)
			require.NoError(t, err)

			var decoded map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &decoded))

			gotKeys := make([]string, 0, len(decoded))
			for key := range decoded {
				gotKeys = append(gotKeys, key)
			}

			assert.ElementsMatch(t, tt.wantKeys, gotKeys, "exact set of spec keys sent to the apiserver")
			assert.Equal(t, tt.wantWaitForFirstConsumer, string(decoded["waitForFirstConsumer"]))

			// The discriminator is spec.mode; spec.targetRef was removed from the CRD and would be
			// pruned silently, so it must never reappear here under any input.
			assert.NotContains(t, decoded, "targetRef")
			assert.Equal(t, `"CreatePVC"`, string(decoded["mode"]))
		})
	}
}

// TestDataImportSpec_DeepCopy_DoesNotAliasPvcTemplate is not redundant boilerplate: the deepcopy
// in this package is maintained by hand (there is no controller-gen or //go:generate wired up), so
// a field added to PersistentVolumeClaimSpec without a matching line in DeepCopyInto produces a
// copy that shares memory with its original — the exact mistake a generator would have prevented.
func TestDataImportSpec_DeepCopy_DoesNotAliasPvcTemplate(t *testing.T) {
	t.Parallel()

	original := DataImportSpec{
		TTL:                  "15m",
		Publish:              true,
		WaitForFirstConsumer: true,
		Mode:                 DataImportModeCreatePVC,
		PvcTemplate:          newCreatePVCTemplate(),
	}

	copied := original.DeepCopy()
	require.NotNil(t, copied)
	require.NotNil(t, copied.PvcTemplate)
	require.NotSame(t, original.PvcTemplate, copied.PvcTemplate, "pvcTemplate pointer must not be shared")

	copied.PvcTemplate.Name = "mutated-pvc"
	copied.PvcTemplate.AccessModes[0] = ReadWriteMany
	copied.PvcTemplate.Resources.Requests[ResourceStorage] = resource.MustParse("99Gi")
	*copied.PvcTemplate.StorageClassName = "mutated-class"
	*copied.PvcTemplate.VolumeMode = PersistentVolumeBlock

	assert.Equal(t, "restored-pvc", original.PvcTemplate.Name)
	assert.Equal(t, []PersistentVolumeAccessMode{ReadWriteOnce}, original.PvcTemplate.AccessModes)

	originalSize := original.PvcTemplate.Resources.Requests[ResourceStorage]
	assert.Equal(t, "10Gi", originalSize.String())

	require.NotNil(t, original.PvcTemplate.StorageClassName)
	assert.Equal(t, "linstor-thin-r1", *original.PvcTemplate.StorageClassName)
	require.NotNil(t, original.PvcTemplate.VolumeMode)
	assert.Equal(t, PersistentVolumeFilesystem, *original.PvcTemplate.VolumeMode)
}
