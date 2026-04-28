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

func TestParseScope(t *testing.T) {
	tests := []struct {
		name       string
		scope      string
		namespaces []string
		want       iamtypes.Scope
		wantNS     []string
		wantLabels map[string]string
		wantErr    string
	}{
		{name: "namespace", namespaces: []string{"dev"}, want: iamtypes.ScopeNamespace, wantNS: []string{"dev"}},
		{name: "namespace multi", namespaces: []string{"dev", "stage"}, want: iamtypes.ScopeNamespace, wantNS: []string{"dev", "stage"}},
		{name: "scope cluster", scope: "cluster", want: iamtypes.ScopeCluster},
		{name: "scope all-namespaces", scope: "all-namespaces", want: iamtypes.ScopeAllNamespaces},
		{name: "scope all alias", scope: "all", want: iamtypes.ScopeAllNamespaces},
		{name: "scope labels single", scope: "labels=team=platform", want: iamtypes.ScopeLabels, wantLabels: map[string]string{"team": "platform"}},
		{name: "scope labels multi", scope: "labels=team=platform,tier=prod", want: iamtypes.ScopeLabels, wantLabels: map[string]string{"team": "platform", "tier": "prod"}},
		{name: "none", wantErr: "one of"},
		{name: "namespace + scope mutually exclusive", scope: "cluster", namespaces: []string{"dev"}, wantErr: "mutually exclusive"},
		{name: "invalid scope", scope: "global", wantErr: "invalid --scope"},
		{name: "labels empty", scope: "labels=", wantErr: "labels=... must contain"},
		{name: "labels malformed pair", scope: "labels=team", wantErr: "expected key=value"},
		{name: "labels empty key", scope: "labels==prod", wantErr: "key and value must be non-empty"},
		{name: "labels duplicate key", scope: "labels=team=a,team=b", wantErr: "duplicate label key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ns, labels, err := parseScope(tt.scope, tt.namespaces)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantNS, ns)
			assert.Equal(t, tt.wantLabels, labels)
		})
	}
}

func TestParseLabelMatch(t *testing.T) {
	got, err := parseLabelMatch("team=platform,tier=prod")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "platform", "tier": "prod"}, got)

	got, err = parseLabelMatch("k=v")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, got)
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

// TestCanonicalGrantSpec_OldScopesJSONStable freezes the on-disk shape of
// the canonical-spec annotation for the three pre-labels scopes. The field
// is what `createOrUpdateGrant` uses to detect "same spec → no-op", and a
// silent shape change (e.g. losing omitempty on LabelMatch, or changing the
// field order Go's encoding/json emits for these exact fields) would treat
// every previously-applied grant as needing an Update. That would break
// idempotency on upgrade.
func TestCanonicalGrantSpec_OldScopesJSONStable(t *testing.T) {
	tests := []struct {
		name string
		spec *canonicalGrantSpec
		want string
	}{
		{
			name: "namespaced",
			spec: &canonicalGrantSpec{
				Model:            "current",
				SubjectKind:      "User",
				SubjectRef:       "anton",
				SubjectPrincipal: "anton@abc.com",
				AccessLevel:      "Admin",
				ScopeType:        "namespace",
				Namespaces:       []string{"dev"},
			},
			want: `{"model":"current","subjectKind":"User","subjectRef":"anton","subjectPrincipal":"anton@abc.com","accessLevel":"Admin","scopeType":"namespace","namespaces":["dev"],"allowScale":false,"portForwarding":false}`,
		},
		{
			name: "cluster",
			spec: &canonicalGrantSpec{
				Model:            "current",
				SubjectKind:      "Group",
				SubjectRef:       "admins",
				SubjectPrincipal: "admins",
				AccessLevel:      "ClusterAdmin",
				ScopeType:        "cluster",
			},
			want: `{"model":"current","subjectKind":"Group","subjectRef":"admins","subjectPrincipal":"admins","accessLevel":"ClusterAdmin","scopeType":"cluster","allowScale":false,"portForwarding":false}`,
		},
		{
			name: "all-namespaces",
			spec: &canonicalGrantSpec{
				Model:            "current",
				SubjectKind:      "User",
				SubjectRef:       "anton",
				SubjectPrincipal: "anton@abc.com",
				AccessLevel:      "SuperAdmin",
				ScopeType:        "all-namespaces",
			},
			want: `{"model":"current","subjectKind":"User","subjectRef":"anton","subjectPrincipal":"anton@abc.com","accessLevel":"SuperAdmin","scopeType":"all-namespaces","allowScale":false,"portForwarding":false}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.spec.JSON()
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
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
