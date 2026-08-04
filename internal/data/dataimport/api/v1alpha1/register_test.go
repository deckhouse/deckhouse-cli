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
)

// TestAPIGroup_IsStorageFoundationGroup pins the DataImport API group to
// storage-foundation.deckhouse.io. The legacy storage.deckhouse.io group is removed by the
// storage-foundation 025-migrate-legacy-crds migration hook, so a regression here would take
// down every `d8 data import`/`d8 snapshot upload` client silently.
func TestAPIGroup_IsStorageFoundationGroup(t *testing.T) {
	t.Parallel()

	require.Equal(t, "storage-foundation.deckhouse.io", APIGroup)
	require.Equal(t, "storage-foundation.deckhouse.io/v1alpha1", SchemeGroupVersion.String())

	// Explicit negative assertion: the legacy group is being deleted by the migration
	// hook, so silently drifting back to it must fail loudly rather than compile clean.
	require.NotEqual(t, "storage.deckhouse.io", APIGroup)
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
