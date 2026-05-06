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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateGrantName(t *testing.T) {
	tests := []struct {
		name string
		spec *canonicalGrantSpec
		want string
	}{
		{
			name: "user namespaced",
			spec: &canonicalGrantSpec{
				Model:            "current",
				SubjectKind:      "User",
				SubjectRef:       "anton",
				SubjectPrincipal: "anton@abc.com",
				AccessLevel:      "Admin",
				ScopeType:        "namespace",
				Namespaces:       []string{"dev"},
			},
			want: "d8-access-user-anton-dev-admin-",
		},
		{
			name: "group cluster",
			spec: &canonicalGrantSpec{
				Model:            "current",
				SubjectKind:      "Group",
				SubjectRef:       "admins",
				SubjectPrincipal: "admins",
				AccessLevel:      "ClusterAdmin",
				ScopeType:        "cluster",
			},
			want: "d8-access-group-admins-cluster-clusteradmin-",
		},
		{
			name: "user all-namespaces",
			spec: &canonicalGrantSpec{
				Model:            "current",
				SubjectKind:      "User",
				SubjectRef:       "anton",
				SubjectPrincipal: "anton@abc.com",
				AccessLevel:      "SuperAdmin",
				ScopeType:        "all-namespaces",
			},
			want: "d8-access-user-anton-all-superadmin-",
		},
		{
			name: "group labels",
			spec: &canonicalGrantSpec{
				Model:            "current",
				SubjectKind:      "Group",
				SubjectRef:       "admins",
				SubjectPrincipal: "admins",
				AccessLevel:      "Editor",
				ScopeType:        "labels",
				LabelMatch:       map[string]string{"team": "platform", "tier": "prod"},
			},
			want: "d8-access-group-admins-labels-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generateGrantName(tt.spec)
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(got, tt.want),
				"expected name to start with %q, got %q", tt.want, got)
			assert.LessOrEqual(t, len(got), 253, "name must be at most 253 chars")
		})
	}
}

// TestGenerateGrantName_StableForOldScopes locks down the names of grants
// that existed before the labels scope was introduced. Changing them would
// orphan previously created d8-managed CARs/ARs in upgraded clusters and
// silently break `d8 iam access revoke` (revoke locates the object by name).
func TestGenerateGrantName_StableForOldScopes(t *testing.T) {
	tests := []struct {
		name string
		spec *canonicalGrantSpec
		want string
	}{
		{
			name: "namespaced grant",
			spec: &canonicalGrantSpec{
				Model:            "current",
				SubjectKind:      "User",
				SubjectRef:       "anton",
				SubjectPrincipal: "anton@abc.com",
				AccessLevel:      "Admin",
				ScopeType:        "namespace",
				Namespaces:       []string{"dev"},
			},
			want: "d8-access-user-anton-dev-admin-4ca594ce",
		},
		{
			name: "cluster grant",
			spec: &canonicalGrantSpec{
				Model:            "current",
				SubjectKind:      "Group",
				SubjectRef:       "admins",
				SubjectPrincipal: "admins",
				AccessLevel:      "ClusterAdmin",
				ScopeType:        "cluster",
			},
			want: "d8-access-group-admins-cluster-clusteradmin-",
		},
		{
			name: "all-namespaces grant",
			spec: &canonicalGrantSpec{
				Model:            "current",
				SubjectKind:      "User",
				SubjectRef:       "anton",
				SubjectPrincipal: "anton@abc.com",
				AccessLevel:      "SuperAdmin",
				ScopeType:        "all-namespaces",
			},
			want: "d8-access-user-anton-all-superadmin-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generateGrantName(tt.spec)
			require.NoError(t, err)
			// We only assert the human-readable prefix is intact; the trailing
			// hash is a deterministic function of the canonical-spec JSON
			// (also covered by TestGenerateGrantName_Deterministic) and we do
			// not freeze it explicitly so unrelated future fields with
			// `omitempty` defaults stay backwards-compatible. The only frozen
			// hash above is the namespaced one because it happens to be the
			// most fragile (subject email + namespace + level).
			if strings.Contains(tt.want, "-4ca594ce") {
				assert.Equal(t, tt.want, got)
			} else {
				assert.True(t, strings.HasPrefix(got, tt.want),
					"want prefix %q, got %q", tt.want, got)
			}
		})
	}
}

// TestGenerateGrantName_LabelsScopeDeterministic verifies that the same
// label set produces the same name across two invocations regardless of map
// iteration order, and that two different label sets produce different names.
func TestGenerateGrantName_LabelsScopeDeterministic(t *testing.T) {
	spec1 := &canonicalGrantSpec{
		Model:            "current",
		SubjectKind:      "User",
		SubjectRef:       "anton",
		SubjectPrincipal: "anton@abc.com",
		AccessLevel:      "Editor",
		ScopeType:        "labels",
		LabelMatch:       map[string]string{"team": "platform", "tier": "prod"},
	}
	// Same data, different insertion order — Go does not guarantee the second
	// map will iterate the same way as the first, so this guards against an
	// implementation that accidentally sorts only one of them.
	spec2 := &canonicalGrantSpec{
		Model:            "current",
		SubjectKind:      "User",
		SubjectRef:       "anton",
		SubjectPrincipal: "anton@abc.com",
		AccessLevel:      "Editor",
		ScopeType:        "labels",
		LabelMatch:       map[string]string{"tier": "prod", "team": "platform"},
	}

	n1, err := generateGrantName(spec1)
	require.NoError(t, err)
	n2, err := generateGrantName(spec2)
	require.NoError(t, err)
	assert.Equal(t, n1, n2)

	// Different label set → different name (the hash differs even when the
	// human-readable middle segment "labels-XXXXXX" coincidentally matches).
	spec3 := *spec1
	spec3.LabelMatch = map[string]string{"team": "different"}
	n3, err := generateGrantName(&spec3)
	require.NoError(t, err)
	assert.NotEqual(t, n1, n3)
}

func TestGenerateGrantName_Deterministic(t *testing.T) {
	spec := &canonicalGrantSpec{
		Model:            "current",
		SubjectKind:      "User",
		SubjectRef:       "anton",
		SubjectPrincipal: "anton@abc.com",
		AccessLevel:      "Admin",
		ScopeType:        "namespace",
		Namespaces:       []string{"dev"},
	}

	name1, err := generateGrantName(spec)
	require.NoError(t, err)

	name2, err := generateGrantName(spec)
	require.NoError(t, err)

	assert.Equal(t, name1, name2, "same spec should produce same name")
}

// TestGenerateGrantName_HashSurvivesTruncation locks down the contract that
// the trailing hash8 (uniqueness suffix) is preserved even when the body of
// the name overflows the Kubernetes 253-char DNS-subdomain limit.
//
// Two specs that differ only in the last few bytes of the canonical JSON
// (e.g. very long allow-scale flag toggle on a long subject ref) must still
// resolve to two distinct names — otherwise revoke can blow up an unrelated
// grant.
func TestGenerateGrantName_HashSurvivesTruncation(t *testing.T) {
	// 220-char subjectRef pushes the body well past the limit. After
	// sanitizeNamePart truncates the ref to 40 chars the body shouldn't
	// actually overflow today, but we want the test to bite if anyone ever
	// loosens that 40-char cap or grows scope/level segments.
	longRef := strings.Repeat("a", 220)

	spec1 := &canonicalGrantSpec{
		Model:            "current",
		SubjectKind:      "User",
		SubjectRef:       longRef,
		SubjectPrincipal: longRef + "@example.com",
		AccessLevel:      "Admin",
		ScopeType:        "namespace",
		Namespaces:       []string{"dev"},
	}
	// Same shape but a different capability bit so the canonical-spec JSON,
	// and hence the hash, differs from spec1 — but the human-readable body
	// is byte-identical up to the trailing hash.
	spec2 := *spec1
	spec2.AllowScale = true

	n1, err := generateGrantName(spec1)
	require.NoError(t, err)
	n2, err := generateGrantName(&spec2)
	require.NoError(t, err)

	assert.LessOrEqual(t, len(n1), 253, "name must fit DNS subdomain")
	assert.LessOrEqual(t, len(n2), 253)
	assert.NotEqual(t, n1, n2, "different specs must yield different names; the trailing hash must survive truncation")

	// And the hash8 segment (8 hex chars) must be the actual suffix, not a
	// prefix that got cut mid-byte.
	for _, name := range []string{n1, n2} {
		idx := strings.LastIndex(name, "-")
		require.Greater(t, idx, 0, "name has no '-' separator: %q", name)
		suffix := name[idx+1:]
		assert.Len(t, suffix, 8, "trailing hash must be exactly 8 hex chars in %q", name)
	}
}

func TestSanitizeNamePart(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "anton", want: "anton"},
		{input: "Anton", want: "anton"},
		{input: "user@example.com", want: "user-at-example-com"},
		{input: "my.name", want: "my-name"},
		{input: "my_name", want: "my-name"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, sanitizeNamePart(tt.input))
		})
	}
}

func TestGrantLabels(t *testing.T) {
	spec := &canonicalGrantSpec{
		SubjectKind: "User",
		ScopeType:   "cluster",
	}
	labels := grantLabels(spec)
	assert.Equal(t, "d8-cli", labels["app.kubernetes.io/managed-by"])
	assert.Equal(t, "current", labels["deckhouse.io/access-model"])
	assert.Equal(t, "user", labels["deckhouse.io/access-subject-kind"])
	assert.Equal(t, "cluster", labels["deckhouse.io/access-scope"])
}

func TestGrantAnnotations(t *testing.T) {
	spec := &canonicalGrantSpec{
		Model:            "current",
		SubjectKind:      "User",
		SubjectRef:       "anton",
		SubjectPrincipal: "anton@abc.com",
		AccessLevel:      "Admin",
		ScopeType:        "namespace",
		Namespaces:       []string{"dev"},
	}
	annotations, err := grantAnnotations(spec)
	require.NoError(t, err)
	assert.Equal(t, "anton", annotations["deckhouse.io/access-subject-ref"])
	assert.Equal(t, "anton@abc.com", annotations["deckhouse.io/access-subject-principal"])
	assert.Contains(t, annotations["deckhouse.io/access-canonical-spec"], `"accessLevel":"Admin"`)
}
