/*
Copyright 2023 Flant JSC

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

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataapi"
)

// TestAPIGroup_IsStorageFoundationGroup pins the DataImport default API group to
// storage-foundation.deckhouse.io, spelled out as a literal rather than through the constant it is
// defined from — otherwise the assertion would restate whatever the constant became.
//
// This is the group used whenever the caller does not resolve one, which is every caller outside
// `d8 data` (notably `d8 snapshot upload`). Reaching the other producer is a deliberate act, via
// AddToSchemeFor; drifting into it by default is not.
func TestAPIGroup_IsStorageFoundationGroup(t *testing.T) {
	t.Parallel()

	require.Equal(t, "storage-foundation.deckhouse.io", APIGroup)
	require.Equal(t, "storage-foundation.deckhouse.io/v1alpha1", SchemeGroupVersion.String())
	require.NotEqual(t, "storage.deckhouse.io", APIGroup)
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

			for _, obj := range []runtime.Object{&DataImport{}, &DataImportList{}} {
				gvks, _, err := scheme.ObjectKinds(obj)
				require.NoError(t, err)
				require.Len(t, gvks, 1, "the type must map to exactly one GroupVersionKind")
				require.Equal(t, tt.wantGroup, gvks[0].Group)
				require.Equal(t, "v1alpha1", gvks[0].Version)
			}
		})
	}
}

// TestAddToScheme_RegistersUnderStorageFoundationGroup verifies that AddKnownTypes registers
// DataImport/DataImportList under the current APIGroup, catching a drift between the constant
// and the scheme registration that unit tests on APIGroup alone would miss.
func TestAddToScheme_RegistersUnderStorageFoundationGroup(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, AddToScheme(scheme))

	tests := []struct {
		name     string
		object   runtime.Object
		wantKind string
	}{
		{
			name:     "success: DataImport registered under storage-foundation group",
			object:   &DataImport{},
			wantKind: "DataImport",
		},
		{
			name:     "success: DataImportList registered under storage-foundation group",
			object:   &DataImportList{},
			wantKind: "DataImportList",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gvks, _, err := scheme.ObjectKinds(tt.object)
			require.NoError(t, err)
			require.Len(t, gvks, 1)
			require.Equal(t, "storage-foundation.deckhouse.io", gvks[0].Group)
			require.Equal(t, tt.wantKind, gvks[0].Kind)
		})
	}
}
