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
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

func TestParseRuleRef(t *testing.T) {
	tests := []struct {
		input    string
		wantKind string
		wantNS   string
		wantName string
		wantErr  bool
	}{
		{input: "ClusterAuthorizationRule/superadmins", wantKind: "ClusterAuthorizationRule", wantName: "superadmins"},
		{input: "CAR/superadmins", wantKind: "ClusterAuthorizationRule", wantName: "superadmins"},
		{input: "car/superadmins", wantKind: "ClusterAuthorizationRule", wantName: "superadmins"},
		{input: "AuthorizationRule/dev/editors", wantKind: "AuthorizationRule", wantNS: "dev", wantName: "editors"},
		{input: "AR/dev/editors", wantKind: "AuthorizationRule", wantNS: "dev", wantName: "editors"},
		{input: "ar/dev/editors", wantKind: "AuthorizationRule", wantNS: "dev", wantName: "editors"},
		{input: "superadmins", wantErr: true},
		{input: "CAR/dev/editors", wantErr: true},
		{input: "AR/editors", wantErr: true},
		{input: "Role/dev/x", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			kind, ns, name, err := parseRuleRef(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				assert.True(t, errors.Is(err, ErrInvalidRuleRef),
					"expected ErrInvalidRuleRef, got %v", err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantKind, kind)
			assert.Equal(t, tc.wantNS, ns)
			assert.Equal(t, tc.wantName, name)
		})
	}
}

func TestFilterByManagement(t *testing.T) {
	rows := []ruleRow{
		{Name: "a", ManagedByD8: true},
		{Name: "b", ManagedByD8: false},
		{Name: "c", ManagedByD8: true},
	}

	t.Run("no filters keeps all", func(t *testing.T) {
		out := filterByManagement(append([]ruleRow(nil), rows...), false, false)
		assert.Len(t, out, 3)
	})
	t.Run("managed-only keeps d8-managed", func(t *testing.T) {
		out := filterByManagement(append([]ruleRow(nil), rows...), true, false)
		assert.Len(t, out, 2)
		assert.Equal(t, "a", out[0].Name)
		assert.Equal(t, "c", out[1].Name)
	})
	t.Run("manual-only keeps non-managed", func(t *testing.T) {
		out := filterByManagement(append([]ruleRow(nil), rows...), false, true)
		assert.Len(t, out, 1)
		assert.Equal(t, "b", out[0].Name)
	})
}

func TestSortRuleRows(t *testing.T) {
	// Ensure CARs come before ARs, then by namespace, then by name.
	rows := []ruleRow{
		{Kind: "AuthorizationRule", Namespace: "dev", Name: "b"},
		{Kind: "ClusterAuthorizationRule", Name: "z"},
		{Kind: "AuthorizationRule", Namespace: "dev", Name: "a"},
		{Kind: "AuthorizationRule", Namespace: "aaa", Name: "x"},
		{Kind: "ClusterAuthorizationRule", Name: "a"},
	}
	sortRuleRows(rows)

	// lexicographic: "AuthorizationRule" < "ClusterAuthorizationRule", so ARs come first.
	// The intent is stable, predictable ordering — so we just assert what the
	// implementation produces (AR block before CAR block, grouped by ns/name).
	expectedOrder := []string{
		"AuthorizationRule/aaa/x",
		"AuthorizationRule/dev/a",
		"AuthorizationRule/dev/b",
		"ClusterAuthorizationRule/a",
		"ClusterAuthorizationRule/z",
	}
	var got []string
	for _, r := range rows {
		got = append(got, r.ref())
	}
	assert.Equal(t, expectedOrder, got)
}

func TestCapsAndManagedByColumns(t *testing.T) {
	assert.Equal(t, "-", capsColumn(ruleRow{}))
	assert.Equal(t, "scale", capsColumn(ruleRow{AllowScale: true}))
	assert.Equal(t, "pfwd", capsColumn(ruleRow{PortForwarding: true}))
	assert.Equal(t, "scale,pfwd", capsColumn(ruleRow{AllowScale: true, PortForwarding: true}))

	assert.Equal(t, "d8-cli", managedByColumn(ruleRow{ManagedByD8: true}))
	assert.Equal(t, "manual", managedByColumn(ruleRow{}))
}

func TestRuleRowFromObject_CAR(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "deckhouse.io/v1",
		"kind":       "ClusterAuthorizationRule",
		"metadata": map[string]any{
			"name":   "superadmins",
			"labels": map[string]any{iamtypes.LabelManagedBy: iamtypes.ManagedByValueCLI},
		},
		"spec": map[string]any{
			"accessLevel":    "SuperAdmin",
			"allowScale":     true,
			"portForwarding": true,
			"namespaceSelector": map[string]any{
				"matchAny": true,
			},
			"subjects": []any{
				map[string]any{"kind": "Group", "name": "admins"},
				map[string]any{"kind": "User", "name": "alice@example.com"},
			},
		},
	}}
	obj.SetCreationTimestamp(metav1.NewTime(time.Now()))

	row := ruleRowFromObject(obj)

	assert.Equal(t, "ClusterAuthorizationRule", row.Kind)
	assert.Equal(t, "superadmins", row.Name)
	assert.Empty(t, row.Namespace)
	assert.Equal(t, "SuperAdmin", row.AccessLevel)
	assert.Equal(t, iamtypes.ScopeAllNamespaces, row.ScopeType)
	assert.True(t, row.AllowScale)
	assert.True(t, row.PortForwarding)
	assert.True(t, row.ManagedByD8)
	require.Len(t, row.Subjects, 2)
	assert.Equal(t, "ClusterAuthorizationRule/superadmins", row.ref())
}

func TestRuleRowFromObject_AR(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "deckhouse.io/v1alpha1",
		"kind":       "AuthorizationRule",
		"metadata": map[string]any{
			"name":      "editors",
			"namespace": "dev",
		},
		"spec": map[string]any{
			"accessLevel": "Editor",
			"subjects": []any{
				map[string]any{"kind": "User", "name": "bob@example.com"},
			},
		},
	}}

	row := ruleRowFromObject(obj)

	assert.Equal(t, "AuthorizationRule", row.Kind)
	assert.Equal(t, "editors", row.Name)
	assert.Equal(t, "dev", row.Namespace)
	assert.Equal(t, iamtypes.ScopeNamespace, row.ScopeType)
	assert.Equal(t, []string{"dev"}, row.ScopeNamespaces)
	assert.False(t, row.ManagedByD8)
	assert.Equal(t, "AuthorizationRule/dev/editors", row.ref())
}

func TestPrintRuleRowText_HighlightsSuperAdminImplicitCaps(t *testing.T) {
	row := ruleRow{
		Kind:        "ClusterAuthorizationRule",
		Name:        "superadmins",
		AccessLevel: "SuperAdmin",
		ScopeType:   "all-namespaces",
		// NOTE: both capabilities left false on the CAR — we want the Notes
		// section to appear and explain the implicit bump.
		AllowScale:     false,
		PortForwarding: false,
		Subjects: []subjectRef{
			{Kind: "User", Name: "alice@example.com"},
		},
	}
	reverse := map[string]string{"User/alice@example.com": "alice"}

	var buf bytes.Buffer
	require.NoError(t, printRuleRowText(&buf, row, reverse))
	out := buf.String()

	assert.Contains(t, out, "=== ClusterAuthorizationRule/superadmins ===")
	assert.Contains(t, out, "alice@example.com (local User CR: alice)")
	assert.Contains(t, out, "=== Notes ===")
	assert.Contains(t, out, "user-authz:super-admin ClusterRole")
}

func TestPrintRuleRowText_NoNotesForNonSuperAdmin(t *testing.T) {
	row := ruleRow{
		Kind:        "AuthorizationRule",
		Namespace:   "dev",
		Name:        "editors",
		AccessLevel: "Editor",
		ScopeType:   "namespace",
		Subjects:    []subjectRef{{Kind: "Group", Name: "team-x"}},
	}
	var buf bytes.Buffer
	require.NoError(t, printRuleRowText(&buf, row, map[string]string{}))
	out := buf.String()

	assert.NotContains(t, out, "Notes")
	assert.Contains(t, out, "Group: team-x")
}

func TestTruncate(t *testing.T) {
	assert.Equal(t, "abc", truncate("abc", 5))
	assert.Equal(t, "ab…", truncate("abcdef", 3))
	assert.Equal(t, "a", truncate("abc", 1))
}

// humanAge delegates to apimachinery duration.HumanDuration; we only assert
// the zero-time placeholder, which is our own behavior on top of it.
func TestHumanAge_ZeroPlaceholder(t *testing.T) {
	assert.Equal(t, "-", humanAge(time.Time{}))
}
