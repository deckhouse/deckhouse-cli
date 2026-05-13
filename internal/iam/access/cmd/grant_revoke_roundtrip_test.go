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
	"errors"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

// TestGrantRevokeRoundTrip exercises the public contract of
// applyGrants → revokeManagedGrants against a fake dynamic client:
//   - grant creates exactly the d8-managed object derived from the canonical
//     spec, with the right kind/namespace and the LabelManagedBy label;
//   - revoke with the same flags finds and deletes it;
//   - re-running revoke after a successful one yields NotFound.
//
// Each scope (namespaced, cluster, labels) is its own subtest because the
// concrete object kind, namespace key, and selector shape differ.
func TestGrantRevokeRoundTrip(t *testing.T) {
	cases := []struct {
		name       string
		grantOpts  grantOpts
		revokeOpts revokeOpts
		// expectKind/Namespace let us assert the right resource lineage was
		// touched; revoke uses the same canonical-spec hash so the names
		// match by construction.
		expectKind      string
		expectNamespace string
	}{
		{
			name: "namespaced grant: AR in dev",
			grantOpts: grantOpts{
				subjectKind:      iamtypes.KindUser,
				subjectRef:       "anton",
				subjectPrincipal: "anton@example.com",
				accessLevel:      "Admin",
				scopeType:        iamtypes.ScopeNamespace,
				namespaces:       []string{"dev"},
				outputFmt:        "name",
			},
			revokeOpts: revokeOpts{
				subjectKind:      iamtypes.KindUser,
				subjectRef:       "anton",
				subjectPrincipal: "anton@example.com",
				accessLevel:      "Admin",
				scopeType:        iamtypes.ScopeNamespace,
				namespaces:       []string{"dev"},
			},
			expectKind:      iamtypes.KindAuthorizationRule,
			expectNamespace: "dev",
		},
		{
			name: "cluster-scoped grant: CAR",
			grantOpts: grantOpts{
				subjectKind:      iamtypes.KindUser,
				subjectRef:       "anton",
				subjectPrincipal: "anton@example.com",
				accessLevel:      "ClusterAdmin",
				scopeType:        iamtypes.ScopeCluster,
				outputFmt:        "name",
			},
			revokeOpts: revokeOpts{
				subjectKind:      iamtypes.KindUser,
				subjectRef:       "anton",
				subjectPrincipal: "anton@example.com",
				accessLevel:      "ClusterAdmin",
				scopeType:        iamtypes.ScopeCluster,
			},
			expectKind: iamtypes.KindClusterAuthorizationRule,
		},
		{
			name: "labels-scoped grant: CAR with namespaceSelector",
			grantOpts: grantOpts{
				subjectKind:      iamtypes.KindGroup,
				subjectRef:       "admins",
				subjectPrincipal: "admins",
				accessLevel:      "Editor",
				scopeType:        iamtypes.ScopeLabels,
				labelMatch:       map[string]string{"team": "platform", "tier": "prod"},
				outputFmt:        "name",
			},
			revokeOpts: revokeOpts{
				subjectKind:      iamtypes.KindGroup,
				subjectRef:       "admins",
				subjectPrincipal: "admins",
				accessLevel:      "Editor",
				scopeType:        iamtypes.ScopeLabels,
				labelMatch:       map[string]string{"team": "platform", "tier": "prod"},
			},
			expectKind: iamtypes.KindClusterAuthorizationRule,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())
			cmd := &cobra.Command{}
			cmd.SetContext(t.Context())

			// 1. Grant
			require.NoError(t, applyGrants(cmd, dyn, tc.grantOpts))

			// 2. The expected object must exist with the d8-managed label.
			spec := mustCanonicalSpecFromGrantOpts(t, tc.grantOpts)
			expectedName, err := generateGrantName(spec)
			require.NoError(t, err)

			var stored *unstructured.Unstructured
			switch tc.expectKind {
			case iamtypes.KindAuthorizationRule:
				stored, err = dyn.Resource(iamtypes.AuthorizationRuleGVR).
					Namespace(tc.expectNamespace).
					Get(t.Context(), expectedName, metav1.GetOptions{})
			case iamtypes.KindClusterAuthorizationRule:
				stored, err = dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR).
					Get(t.Context(), expectedName, metav1.GetOptions{})
			}
			require.NoErrorf(t, err, "grant did not create the expected %s/%s",
				tc.expectKind, expectedName)
			assert.Equal(t, iamtypes.ManagedByValueCLI,
				stored.GetLabels()[iamtypes.LabelManagedBy],
				"grant must label the object as d8-cli managed")

			// 3. Revoke removes it.
			require.NoError(t, revokeManagedGrants(cmd, dyn, tc.revokeOpts))

			switch tc.expectKind {
			case iamtypes.KindAuthorizationRule:
				_, err = dyn.Resource(iamtypes.AuthorizationRuleGVR).
					Namespace(tc.expectNamespace).
					Get(t.Context(), expectedName, metav1.GetOptions{})
			case iamtypes.KindClusterAuthorizationRule:
				_, err = dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR).
					Get(t.Context(), expectedName, metav1.GetOptions{})
			}
			require.Error(t, err, "object must be gone after revoke")
			assert.True(t, apierrors.IsNotFound(err),
				"expected NotFound after revoke, got: %v", err)

			// 4. Re-running revoke is now a NotFound — surfaced as a typed
			//    error so callers can detect the "already revoked" case.
			err = revokeManagedGrants(cmd, dyn, tc.revokeOpts)
			require.Error(t, err)
			assert.True(t, isNotFoundDeep(err),
				"second revoke must wrap a NotFound, got: %v", err)
		})
	}
}

// TestRevokeManagedGrants_RefusesUnmanagedObject locks down the safety
// invariant: revoke must NEVER delete a CAR/AR that lacks the d8-cli
// managed-by label, even if its name happens to collide with the
// canonical hash. This is what protects shared/manual rules.
func TestRevokeManagedGrants_RefusesUnmanagedObject(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	opts := revokeOpts{
		subjectKind:      iamtypes.KindUser,
		subjectRef:       "anton",
		subjectPrincipal: "anton@example.com",
		accessLevel:      "ClusterAdmin",
		scopeType:        iamtypes.ScopeCluster,
	}
	spec := mustCanonicalSpecFromRevokeOpts(t, opts)
	name, err := generateGrantName(spec)
	require.NoError(t, err)

	// Pre-create a CAR with the same name but NO managed-by label.
	manual := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": iamtypes.APIVersionDeckhouseV1,
			"kind":       iamtypes.KindClusterAuthorizationRule,
			"metadata": map[string]any{
				"name": name,
			},
			"spec": map[string]any{
				"accessLevel": "ClusterAdmin",
				"subjects": []any{
					map[string]any{"kind": "User", "name": "anton@example.com"},
				},
			},
		},
	}
	_, err = dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR).
		Create(t.Context(), manual, metav1.CreateOptions{})
	require.NoError(t, err)

	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())

	err = revokeManagedGrants(cmd, dyn, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not managed by d8-cli")

	// And the object must still be there.
	_, err = dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR).
		Get(t.Context(), name, metav1.GetOptions{})
	require.NoError(t, err, "revoke must not delete unmanaged objects")
}

// TestRevokeManagedGrants_DryRunDoesNotDelete verifies the new --dry-run
// flag plumbed into deleteManagedGrant: the object remains, even though
// the canonical spec resolves to it and the managed-by label is set.
func TestRevokeManagedGrants_DryRunDoesNotDelete(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())
	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())

	gOpts := grantOpts{
		subjectKind:      iamtypes.KindUser,
		subjectRef:       "anton",
		subjectPrincipal: "anton@example.com",
		accessLevel:      "ClusterAdmin",
		scopeType:        iamtypes.ScopeCluster,
		outputFmt:        "name",
	}
	require.NoError(t, applyGrants(cmd, dyn, gOpts))

	rOpts := revokeOpts{
		subjectKind:      iamtypes.KindUser,
		subjectRef:       "anton",
		subjectPrincipal: "anton@example.com",
		accessLevel:      "ClusterAdmin",
		scopeType:        iamtypes.ScopeCluster,
		dryRun:           true,
		outputFmt:        "name",
	}
	require.NoError(t, revokeManagedGrants(cmd, dyn, rOpts))

	spec := mustCanonicalSpecFromRevokeOpts(t, rOpts)
	name, err := generateGrantName(spec)
	require.NoError(t, err)
	_, err = dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR).
		Get(t.Context(), name, metav1.GetOptions{})
	require.NoError(t, err, "dry-run must not delete the object")
}

// --- helpers ---

func mustCanonicalSpecFromGrantOpts(t *testing.T, opts grantOpts) *canonicalGrantSpec {
	t.Helper()
	specs, err := canonicalGrantSpecs(canonicalGrantInput{
		SubjectKind:      opts.subjectKind,
		SubjectRef:       opts.subjectRef,
		SubjectPrincipal: opts.subjectPrincipal,
		AccessLevel:      opts.accessLevel,
		ScopeType:        opts.scopeType,
		Namespaces:       opts.namespaces,
		LabelMatch:       opts.labelMatch,
		AllowScale:       opts.allowScale,
		PortForwarding:   opts.portForwarding,
	})
	require.NoError(t, err)
	require.Lenf(t, specs, 1, "round-trip cases assume exactly one canonical spec")
	return specs[0]
}

func mustCanonicalSpecFromRevokeOpts(t *testing.T, opts revokeOpts) *canonicalGrantSpec {
	t.Helper()
	specs, err := canonicalGrantSpecs(canonicalGrantInput{
		SubjectKind:      opts.subjectKind,
		SubjectRef:       opts.subjectRef,
		SubjectPrincipal: opts.subjectPrincipal,
		AccessLevel:      opts.accessLevel,
		ScopeType:        opts.scopeType,
		Namespaces:       opts.namespaces,
		LabelMatch:       opts.labelMatch,
		AllowScale:       opts.allowScale,
		PortForwarding:   opts.portForwarding,
	})
	require.NoError(t, err)
	require.Lenf(t, specs, 1, "round-trip cases assume exactly one canonical spec")
	return specs[0]
}

// isNotFoundDeep checks whether err (or anything it wraps) is a typed k8s
// NotFound. revokeManagedGrants wraps with %w via multierror, so we have to
// walk the chain manually.
func isNotFoundDeep(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsNotFound(err) {
		return true
	}
	type unwrapAll interface{ Unwrap() []error }
	if u, ok := err.(unwrapAll); ok {
		for _, e := range u.Unwrap() {
			if isNotFoundDeep(e) {
				return true
			}
		}
	}
	if u := errors.Unwrap(err); u != nil {
		return isNotFoundDeep(u)
	}
	return false
}
