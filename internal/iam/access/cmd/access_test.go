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

package access

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

func TestParseSubjectKind(t *testing.T) {
	tests := []struct {
		input   string
		want    iamtypes.SubjectKind
		wantErr string
	}{
		{input: "user", want: iamtypes.KindUser},
		{input: "User", want: iamtypes.KindUser},
		{input: "group", want: iamtypes.KindGroup},
		{input: "Group", want: iamtypes.KindGroup},
		{input: "serviceaccount", wantErr: "invalid subject kind"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseSubjectKind(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParseScopeFlags(t *testing.T) {
	tests := []struct {
		name       string
		namespaces []string
		cluster    bool
		allNS      bool
		want       iamtypes.Scope
		wantErr    string
	}{
		{name: "namespace", namespaces: []string{"dev"}, want: iamtypes.ScopeNamespace},
		{name: "cluster", cluster: true, want: iamtypes.ScopeCluster},
		{name: "all-namespaces", allNS: true, want: iamtypes.ScopeAllNamespaces},
		{name: "none", wantErr: "one of"},
		{name: "namespace+cluster", namespaces: []string{"dev"}, cluster: true, wantErr: "mutually exclusive"},
		{name: "cluster+allNS", cluster: true, allNS: true, wantErr: "mutually exclusive"},
		{name: "all three", namespaces: []string{"dev"}, cluster: true, allNS: true, wantErr: "mutually exclusive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseScopeFlags(tt.namespaces, tt.cluster, tt.allNS)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidateAccessLevel(t *testing.T) {
	tests := []struct {
		name       string
		level      string
		namespaced bool
		wantErr    string
	}{
		{name: "User namespaced", level: "User", namespaced: true},
		{name: "Admin namespaced", level: "Admin", namespaced: true},
		{name: "ClusterAdmin namespaced", level: "ClusterAdmin", namespaced: true, wantErr: "not valid for namespaced scope"},
		{name: "SuperAdmin namespaced", level: "SuperAdmin", namespaced: true, wantErr: "not valid for namespaced scope"},
		{name: "User cluster", level: "User", namespaced: false},
		{name: "ClusterAdmin cluster", level: "ClusterAdmin", namespaced: false},
		{name: "SuperAdmin cluster", level: "SuperAdmin", namespaced: false},
		{name: "Invalid", level: "NotALevel", namespaced: false, wantErr: "invalid access level"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAccessLevel(tt.level, tt.namespaced)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMaxAccessLevel(t *testing.T) {
	tests := []struct {
		name   string
		levels []string
		want   string
	}{
		{name: "single", levels: []string{"Admin"}, want: "Admin"},
		{name: "multiple ascending", levels: []string{"User", "Admin"}, want: "Admin"},
		{name: "with cluster levels", levels: []string{"Admin", "ClusterAdmin", "Editor"}, want: "ClusterAdmin"},
		{name: "super admin", levels: []string{"SuperAdmin", "User"}, want: "SuperAdmin"},
		{name: "empty", levels: nil, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, maxAccessLevel(tt.levels))
		})
	}
}

func TestCanonicalGrantSpec_JSON_Deterministic(t *testing.T) {
	spec := &canonicalGrantSpec{
		Model:            "current",
		SubjectKind:      "User",
		SubjectRef:       "anton",
		SubjectPrincipal: "anton@abc.com",
		AccessLevel:      "Admin",
		ScopeType:        "namespace",
		Namespaces:       []string{"stage", "dev"},
	}

	j1, err := spec.JSON()
	require.NoError(t, err)

	// Namespaces in different order should produce same JSON (sorted)
	spec2 := &canonicalGrantSpec{
		Model:            "current",
		SubjectKind:      "User",
		SubjectRef:       "anton",
		SubjectPrincipal: "anton@abc.com",
		AccessLevel:      "Admin",
		ScopeType:        "namespace",
		Namespaces:       []string{"dev", "stage"},
	}
	j2, err := spec2.JSON()
	require.NoError(t, err)

	assert.Equal(t, j1, j2, "namespace order should not affect canonical JSON")
}

func TestRemoveSubject(t *testing.T) {
	t.Run("subject found", func(t *testing.T) {
		obj := buildTestObj([]map[string]any{
			{"kind": "User", "name": "anton@abc.com"},
			{"kind": "Group", "name": "admins"},
		})
		newSubjects, removed := removeSubject(obj, iamtypes.KindUser, "anton@abc.com")
		assert.True(t, removed)
		assert.Len(t, newSubjects, 1)
	})

	t.Run("subject not found", func(t *testing.T) {
		obj := buildTestObj([]map[string]any{
			{"kind": "Group", "name": "admins"},
		})
		newSubjects, removed := removeSubject(obj, iamtypes.KindUser, "anton@abc.com")
		assert.False(t, removed)
		assert.Len(t, newSubjects, 1)
	})
}

func buildTestObj(subjects []map[string]any) *unstructured.Unstructured {
	var rawSubjects []any
	for _, s := range subjects {
		rawSubjects = append(rawSubjects, s)
	}
	return &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{
				"subjects": rawSubjects,
			},
		},
	}
}
