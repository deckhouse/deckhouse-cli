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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestResolveUserGroups(t *testing.T) {
	inv := &accessInventory{
		Users:  map[string]string{"anton": "anton@abc.com"},
		Emails: map[string]string{"anton@abc.com": "anton"},
		GroupMembers: map[string][]memberRef{
			"admins": {
				{Kind: "User", Name: "anton"},
			},
			"devs": {
				{Kind: "User", Name: "anton"},
			},
			"platform": {
				{Kind: "Group", Name: "admins"},
			},
		},
	}

	direct, transitive := inv.ResolveUserGroups("anton")
	assert.ElementsMatch(t, []string{"admins", "devs"}, direct)
	assert.ElementsMatch(t, []string{"admins", "devs", "platform"}, transitive)
}

func TestResolveUserGroups_NoGroups(t *testing.T) {
	inv := &accessInventory{
		Users:        map[string]string{"anton": "anton@abc.com"},
		Emails:       map[string]string{"anton@abc.com": "anton"},
		GroupMembers: map[string][]memberRef{},
	}

	direct, transitive := inv.ResolveUserGroups("anton")
	assert.Empty(t, direct)
	assert.Empty(t, transitive)
}

func TestUserGrants(t *testing.T) {
	inv := &accessInventory{
		Users:  map[string]string{"anton": "anton@abc.com"},
		Emails: map[string]string{"anton@abc.com": "anton"},
		GroupMembers: map[string][]memberRef{
			"admins": {
				{Kind: "User", Name: "anton"},
			},
		},
		Grants: []normalizedGrant{
			{
				SourceKind:       "AuthorizationRule",
				SourceName:       "direct-grant",
				SourceNamespace:  "dev",
				SubjectKind:      "User",
				SubjectPrincipal: "anton@abc.com",
				AccessLevel:      "Admin",
				ScopeType:        "namespace",
				ScopeNamespaces:  []string{"dev"},
			},
			{
				SourceKind:       "ClusterAuthorizationRule",
				SourceName:       "group-grant",
				SubjectKind:      "Group",
				SubjectPrincipal: "admins",
				AccessLevel:      "ClusterAdmin",
				ScopeType:        "cluster",
			},
			{
				SourceKind:       "AuthorizationRule",
				SourceName:       "unrelated",
				SourceNamespace:  "prod",
				SubjectKind:      "User",
				SubjectPrincipal: "other@abc.com",
				AccessLevel:      "Editor",
				ScopeType:        "namespace",
				ScopeNamespaces:  []string{"prod"},
			},
		},
	}

	directGrants, inheritedGrants := inv.UserGrants("anton")
	assert.Len(t, directGrants, 1)
	assert.Equal(t, "Admin", directGrants[0].AccessLevel)
	assert.Len(t, inheritedGrants, 1)
	assert.Equal(t, "ClusterAdmin", inheritedGrants[0].AccessLevel)
}

func TestComputeEffectiveSummary(t *testing.T) {
	grants := []normalizedGrant{
		{AccessLevel: "Admin", ScopeType: "namespace", ScopeNamespaces: []string{"dev"}},
		{AccessLevel: "Editor", ScopeType: "namespace", ScopeNamespaces: []string{"dev"}},
		{AccessLevel: "ClusterAdmin", ScopeType: "cluster"},
		{AccessLevel: "User", ScopeType: "namespace", ScopeNamespaces: []string{"prod"}, PortForwarding: true},
	}

	summary := computeEffectiveSummary(grants)
	assert.Equal(t, "ClusterAdmin", summary.ClusterLevel)
	assert.Equal(t, "Admin", summary.Namespaced["dev"])
	assert.Equal(t, "User", summary.Namespaced["prod"])
	assert.True(t, summary.PortForwarding)
	assert.False(t, summary.AllowScale)
}

func TestComputeEffectiveSummary_SuperAdminImplicitCapabilities(t *testing.T) {
	// SuperAdmin binds user-authz:super-admin ClusterRole which carries */*/* rules;
	// port-forward and scale are therefore always allowed regardless of CAR flags.
	grants := []normalizedGrant{
		{AccessLevel: "SuperAdmin", ScopeType: "all-namespaces"},
	}
	summary := computeEffectiveSummary(grants)

	assert.Equal(t, "SuperAdmin", summary.ClusterLevel)
	assert.True(t, summary.PortForwarding)
	assert.True(t, summary.AllowScale)
	assert.True(t, summary.PortForwardingImplicit)
	assert.True(t, summary.AllowScaleImplicit)
}

func TestComputeEffectiveSummary_SuperAdminExplicitFlagsNotMarkedImplicit(t *testing.T) {
	// When the CAR already sets the flags explicitly, the summary should not
	// mark the capabilities as implicit — the explicit source wins.
	grants := []normalizedGrant{
		{AccessLevel: "SuperAdmin", ScopeType: "cluster", PortForwarding: true, AllowScale: true},
	}
	summary := computeEffectiveSummary(grants)

	assert.True(t, summary.PortForwarding)
	assert.True(t, summary.AllowScale)
	assert.False(t, summary.PortForwardingImplicit)
	assert.False(t, summary.AllowScaleImplicit)
}

func TestComputeEffectiveSummary_ClusterAdminDoesNotImplyPortForwardOrScale(t *testing.T) {
	// ClusterAdmin is composed of scoped rules; it must NOT be treated as
	// implicitly granting port-forwarding or scale.
	grants := []normalizedGrant{
		{AccessLevel: "ClusterAdmin", ScopeType: "cluster"},
	}
	summary := computeEffectiveSummary(grants)

	assert.Equal(t, "ClusterAdmin", summary.ClusterLevel)
	assert.False(t, summary.PortForwarding)
	assert.False(t, summary.AllowScale)
	assert.False(t, summary.PortForwardingImplicit)
	assert.False(t, summary.AllowScaleImplicit)
}

func TestComputeEffectiveSummary_Empty(t *testing.T) {
	summary := computeEffectiveSummary(nil)
	assert.Equal(t, "", summary.ClusterLevel)
	assert.Empty(t, summary.Namespaced)
	assert.False(t, summary.PortForwarding)
	assert.False(t, summary.AllowScale)
}

func TestEffectiveSummary_String(t *testing.T) {
	summary := &effectiveSummary{
		ClusterLevel:   "ClusterAdmin",
		Namespaced:     map[string]string{"dev": "Admin"},
		PortForwarding: true,
	}
	s := summary.String()
	assert.Contains(t, s, "ClusterAdmin[*]")
	assert.Contains(t, s, "Admin[dev]")
}

func TestEffectiveSummary_String_None(t *testing.T) {
	summary := &effectiveSummary{
		Namespaced: map[string]string{},
	}
	assert.Equal(t, "<none>", summary.String())
}

func TestDetectGroupCycles(t *testing.T) {
	t.Run("no cycles", func(t *testing.T) {
		inv := &accessInventory{
			GroupMembers: map[string][]memberRef{
				"a": {{Kind: "Group", Name: "b"}},
				"b": {{Kind: "User", Name: "u1"}},
			},
		}
		cycles := inv.DetectGroupCycles()
		assert.Empty(t, cycles)
	})

	t.Run("simple cycle", func(t *testing.T) {
		inv := &accessInventory{
			GroupMembers: map[string][]memberRef{
				"a": {{Kind: "Group", Name: "b"}},
				"b": {{Kind: "Group", Name: "a"}},
			},
		}
		cycles := inv.DetectGroupCycles()
		assert.NotEmpty(t, cycles)
	})
}

func TestNormalizeClusterAuthRule(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name":   "test-rule",
				"labels": map[string]any{"app.kubernetes.io/managed-by": "d8-cli"},
			},
			"spec": map[string]any{
				"accessLevel":    "ClusterAdmin",
				"allowScale":     true,
				"portForwarding": false,
				"subjects": []any{
					map[string]any{"kind": "User", "name": "anton@abc.com"},
					map[string]any{"kind": "Group", "name": "admins"},
					map[string]any{"kind": "ServiceAccount", "name": "sa1"},
				},
			},
		},
	}

	grants := normalizeClusterAuthRule(obj)
	assert.Len(t, grants, 2) // ServiceAccount filtered out
	assert.Equal(t, "User", grants[0].SubjectKind)
	assert.Equal(t, "anton@abc.com", grants[0].SubjectPrincipal)
	assert.True(t, grants[0].ManagedByD8)
	assert.Equal(t, "Group", grants[1].SubjectKind)
}

func TestNormalizeClusterAuthRule_AllNamespaces(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{
				"name": "all-ns-rule",
			},
			"spec": map[string]any{
				"accessLevel": "Admin",
				"namespaceSelector": map[string]any{
					"matchAny": true,
				},
				"subjects": []any{
					map[string]any{"kind": "User", "name": "test@test.com"},
				},
			},
		},
	}

	grants := normalizeClusterAuthRule(obj)
	assert.Len(t, grants, 1)
	assert.Equal(t, "all-namespaces", grants[0].ScopeType)
}
