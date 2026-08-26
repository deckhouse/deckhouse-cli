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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataapi"
)

// TestAPIGroup_DefaultsToStorageFoundation pins the DataExport default API group as a literal
// rather than through the constant it is defined from, which would only restate itself.
//
// This is the group used whenever the caller does not resolve one — every caller outside
// `d8 data`, notably `d8 snapshot download`, which needs storage-foundation and nothing else.
func TestAPIGroup_DefaultsToStorageFoundation(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "storage-foundation.deckhouse.io", APIGroup)
	assert.Equal(t, "storage-foundation.deckhouse.io/v1alpha1", SchemeGroupVersion.String())
	assert.NotEqual(t, "storage.deckhouse.io", APIGroup)
}

// TestAddToSchemeFor_RegistersUnderTheRequestedGroup covers the runtime-selected registration: the
// scheme has to map the Go types to whichever group was resolved, and to exactly one of them.
//
// Both halves matter. Registering the requested group is what makes the older producer reachable
// at all; registering only it is what keeps the client able to address the object — a type mapped
// to two GroupVersionKinds makes controller-runtime refuse the request as ambiguous, which no test
// asserting the happy group alone would catch.
func TestAddToSchemeFor_RegistersUnderTheRequestedGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		gv        schema.GroupVersion
		wantGroup string
	}{
		{
			name:      "storage-foundation group",
			gv:        dataapi.FoundationGroupVersion,
			wantGroup: "storage-foundation.deckhouse.io",
		},
		{
			name:      "storage-volume-data-manager group",
			gv:        dataapi.LegacyGroupVersion,
			wantGroup: "storage.deckhouse.io",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			require.NoError(t, AddToSchemeFor(tt.gv)(scheme))

			for _, obj := range []runtime.Object{&DataExport{}, &DataExportList{}} {
				gvks, _, err := scheme.ObjectKinds(obj)
				require.NoError(t, err)
				require.Len(t, gvks, 1, "the type must map to exactly one GroupVersionKind")
				assert.Equal(t, tt.wantGroup, gvks[0].Group)
				assert.Equal(t, "v1alpha1", gvks[0].Version)
			}
		})
	}
}

// TestAddToScheme_RegistersUnderTheDefaultGroup guards the drift between the APIGroup constant and
// what the default scheme builder actually registers, which an assertion on the constant alone
// would miss.
func TestAddToScheme_RegistersUnderTheDefaultGroup(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, AddToScheme(scheme))

	gvks, _, err := scheme.ObjectKinds(&DataExport{})
	require.NoError(t, err)
	require.Len(t, gvks, 1)
	assert.Equal(t, "storage-foundation.deckhouse.io", gvks[0].Group)
	assert.Equal(t, "DataExport", gvks[0].Kind)
}

// TestTargetRefSpec_OmitsGroupWhenEmpty pins the serialised shape of the one field the two
// producers disagree about.
//
// A core-group target (PersistentVolumeClaim) has an empty group, and the key must then be absent
// rather than sent as "": the older producer's schema declares no group property and prunes it
// either way, but storage-foundation reads it, and an explicit empty string there is a claim about
// the target rather than the absence of one.
func TestTargetRefSpec_OmitsGroupWhenEmpty(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(TargetRefSpec{Kind: "PersistentVolumeClaim", Name: "my-pvc"})
	require.NoError(t, err)

	assert.NotContains(t, string(raw), `"group"`)
	assert.Contains(t, string(raw), `"kind":"PersistentVolumeClaim"`)
	assert.Contains(t, string(raw), `"name":"my-pvc"`)

	raw, err = json.Marshal(TargetRefSpec{Group: "snapshot.storage.k8s.io", Kind: "VolumeSnapshot", Name: "my-vs"})
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"group":"snapshot.storage.k8s.io"`)
}
