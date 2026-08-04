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

// TestDataImportSpec_PvcTemplateJSONWireShape pins the wire shape of spec.pvcTemplate itself, one
// level deeper than TestDataImportSpec_JSONWireShape, which only checks that the key exists.
// Two distinct CRD behaviours depend on this subtree and neither is visible in Go:
//
//   - metadata.name is the one field a CEL rule demands ("pvcTemplate requires metadata.name"), so a
//     tag change that hoists the name out of metadata turns every import into a hard admission
//     rejection;
//   - every spec.* key here is pruned when it does not match the CRD schema, without an error. A
//     drifted accessModes/resources/storageClassName/volumeMode key therefore produces a PVC that
//     is created successfully but with the wrong access mode, size or volume mode.
func TestDataImportSpec_PvcTemplateJSONWireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		pvcTemplate *PersistentVolumeClaimTemplateSpec
		want        map[string]any
	}{
		{
			// Boundary case, and the shape `d8 data import` sends most often: the smallest
			// template the server will accept is metadata.name alone (everything else is
			// defaulted from the StorageClass), so it must not lose the name and must not grow
			// stray keys that the schema would prune.
			name:        "success: minimal template carries metadata.name and nothing unexpected",
			pvcTemplate: &PersistentVolumeClaimTemplateSpec{ObjectMeta: metav1.ObjectMeta{Name: "restored-pvc"}},
			want: map[string]any{
				"metadata": map[string]any{"name": "restored-pvc"},
				// An always-present empty spec/resources is accepted by the schema (both are plain
				// objects with no required members); it is pinned here so that a future change
				// which starts emitting something else inside them is noticed.
				"spec": map[string]any{"resources": map[string]any{}},
			},
		},
		{
			name:        "success: fully populated template uses the CRD schema key names",
			pvcTemplate: newCreatePVCTemplate(),
			want: map[string]any{
				"metadata": map[string]any{"name": "restored-pvc"},
				"spec": map[string]any{
					"accessModes":      []any{"ReadWriteOnce"},
					"resources":        map[string]any{"requests": map[string]any{"storage": "10Gi"}},
					"storageClassName": "linstor-thin-r1",
					"volumeMode":       "Filesystem",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := json.Marshal(DataImportSpec{
				TTL:                  "15m",
				WaitForFirstConsumer: true,
				Mode:                 DataImportModeCreatePVC,
				PvcTemplate:          tt.pvcTemplate,
			})
			require.NoError(t, err)

			var decoded struct {
				PvcTemplate map[string]any `json:"pvcTemplate"`
			}
			require.NoError(t, json.Unmarshal(raw, &decoded))

			assert.Equal(t, tt.want, decoded.PvcTemplate, "exact pvcTemplate subtree sent to the apiserver")
		})
	}
}

// TestDataImportSpec_UnmarshalServerObject covers the server → CLI direction, which every
// marshal-side test leaves untested. It is not academic: GetDataImportWithRestart rebuilds a fresh
// spec out of a DataImport it decoded from the apiserver, so a spec field this type fails to decode
// is silently dropped from the recreated object (a nil pvcTemplate there aborts the recreate
// outright). The fixtures are shaped the way the apiserver returns objects after defaulting:
// mode is always populated, waitForFirstConsumer is always present.
func TestDataImportSpec_UnmarshalServerObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                     string
		payload                  string
		wantMode                 DataImportMode
		wantWaitForFirstConsumer bool
		wantPvcTemplateName      string // empty means pvcTemplate must decode as nil
		wantStorageClassName     string
	}{
		{
			name: "success: server-defaulted CreatePVC object decodes into mode and top-level pvcTemplate",
			payload: `{
				"ttl": "15m",
				"publish": true,
				"waitForFirstConsumer": true,
				"mode": "CreatePVC",
				"pvcTemplate": {
					"metadata": {"name": "restored-pvc"},
					"spec": {
						"accessModes": ["ReadWriteOnce"],
						"resources": {"requests": {"storage": "10Gi"}},
						"storageClassName": "linstor-thin-r1",
						"volumeMode": "Filesystem"
					}
				}
			}`,
			wantMode:                 DataImportModeCreatePVC,
			wantWaitForFirstConsumer: true,
			wantPvcTemplateName:      "restored-pvc",
			wantStorageClassName:     "linstor-thin-r1",
		},
		{
			// The mirror image of the omitempty defect: a false the CLI managed to store must also
			// read back as false, otherwise the recreate path would flip it to true on its own.
			name:                     "success: waitForFirstConsumer=false from the server decodes as false",
			payload:                  `{"ttl":"15m","waitForFirstConsumer":false,"mode":"CreatePVC","pvcTemplate":{"metadata":{"name":"restored-pvc"}}}`,
			wantMode:                 DataImportModeCreatePVC,
			wantWaitForFirstConsumer: false,
			wantPvcTemplateName:      "restored-pvc",
		},
		{
			// A PopulateData object (created by `d8 snapshot upload`, which builds its own
			// unstructured spec) is readable through this type: the CLI lists/gets DataImports it
			// did not create, and the PopulateData-only fields it does not model must be ignored
			// rather than break the decode.
			name: "success: PopulateData object with fields unknown to this type still decodes",
			payload: `{
				"ttl": "30m",
				"waitForFirstConsumer": true,
				"mode": "PopulateData",
				"snapshotRef": {"apiVersion":"virtualization.deckhouse.io/v1alpha2","kind":"VirtualDiskSnapshot","name":"vds-1"},
				"storageParams": {"storageClassName":"linstor-thin-r1","size":"10Gi","volumeMode":"Filesystem"}
			}`,
			wantMode:                 "PopulateData",
			wantWaitForFirstConsumer: true,
		},
		{
			// Guards the direction the mode/pvcTemplate move did NOT make backwards compatible: an
			// object still carrying the obsolete nested spec.targetRef yields no pvcTemplate at
			// all, so the recreate path fails loudly instead of silently building a PVC-less
			// import. Asserted so that anyone tempted to re-add a targetRef fallback sees the
			// deliberate contract here first.
			name:                     "success: obsolete targetRef-nested spec yields no pvcTemplate",
			payload:                  `{"ttl":"15m","waitForFirstConsumer":true,"targetRef":{"kind":"PersistentVolumeClaim","pvcTemplate":{"metadata":{"name":"restored-pvc"}}}}`,
			wantMode:                 "",
			wantWaitForFirstConsumer: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var spec DataImportSpec
			require.NoError(t, json.Unmarshal([]byte(tt.payload), &spec))

			assert.Equal(t, tt.wantMode, spec.Mode)
			assert.Equal(t, tt.wantWaitForFirstConsumer, spec.WaitForFirstConsumer)

			if tt.wantPvcTemplateName == "" {
				assert.Nil(t, spec.PvcTemplate, "pvcTemplate must not be invented from an absent or obsolete key")

				return
			}

			require.NotNil(t, spec.PvcTemplate)
			assert.Equal(t, tt.wantPvcTemplateName, spec.PvcTemplate.Name)

			if tt.wantStorageClassName != "" {
				assert.Equal(t, []PersistentVolumeAccessMode{ReadWriteOnce}, spec.PvcTemplate.AccessModes)

				size := spec.PvcTemplate.Resources.Requests[ResourceStorage]
				assert.Equal(t, "10Gi", size.String())

				require.NotNil(t, spec.PvcTemplate.StorageClassName)
				assert.Equal(t, tt.wantStorageClassName, *spec.PvcTemplate.StorageClassName)
				require.NotNil(t, spec.PvcTemplate.VolumeMode)
				assert.Equal(t, PersistentVolumeFilesystem, *spec.PvcTemplate.VolumeMode)
			}
		})
	}
}

// TestDataImport_DeepCopy_DoesNotAliasStatusOrMeta completes the deepcopy coverage of the
// hand-maintained zz_generated file: TestDataImportSpec_DeepCopy_DoesNotAliasPvcTemplate takes the
// spec leg, this one takes the whole object (metadata labels + the status.conditions slice).
// EnsureDataImportPublish depends on it — it builds a MergeFrom patch from diObj.DeepCopy() and
// then mutates diObj, so a copy that aliases its source produces an empty patch, i.e. a silently
// lost update rather than a crash.
func TestDataImport_DeepCopy_DoesNotAliasStatusOrMeta(t *testing.T) {
	t.Parallel()

	original := &DataImport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-di",
			Namespace: "test-ns",
			Labels:    map[string]string{"owner": "d8-cli"},
		},
		Spec: DataImportSpec{
			TTL:                  "15m",
			WaitForFirstConsumer: true,
			Mode:                 DataImportModeCreatePVC,
			PvcTemplate:          newCreatePVCTemplate(),
		},
		Status: DataExportImportStatus{
			URL:        "https://10.0.0.1:8085/",
			VolumeMode: "Filesystem",
			Conditions: []metav1.Condition{{
				Type:    "Ready",
				Status:  metav1.ConditionTrue,
				Reason:  "PodReady",
				Message: "Pod is ready",
			}},
		},
	}

	copied := original.DeepCopy()
	require.NotNil(t, copied)
	require.NotSame(t, original, copied)
	require.Len(t, copied.Status.Conditions, 1)

	copied.Labels["owner"] = "someone-else"
	copied.Status.Conditions[0].Status = metav1.ConditionFalse
	copied.Status.Conditions[0].Reason = "Expired"
	copied.Status.URL = "https://127.0.0.1:9999/"

	assert.Equal(t, "d8-cli", original.Labels["owner"], "metadata.labels map must not be shared")
	assert.Equal(t, metav1.ConditionTrue, original.Status.Conditions[0].Status, "status.conditions slice must not be shared")
	assert.Equal(t, "PodReady", original.Status.Conditions[0].Reason)
	assert.Equal(t, "https://10.0.0.1:8085/", original.Status.URL)
}
