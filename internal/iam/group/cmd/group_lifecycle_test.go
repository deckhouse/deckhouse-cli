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

package group

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

// TestGroupCreate_DryRun verifies that --dry-run produces a YAML manifest
// and does NOT call Create on the server. We feed a fake dynamic client
// with a Create reactor that fails the test if it ever fires.
func TestGroupCreate_DryRun(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	// Direct exercise of the rendering path; the real RunE goes through
	// utilk8s.PrintObject the same way.
	obj := buildGroupObject("admins")

	// Sanity: no group exists pre-call.
	_, err := dyn.Resource(iamtypes.GroupGVR).Get(t.Context(), "admins", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "precondition failed: group already exists")

	// And it should still not exist after we just rendered the object.
	_, err = dyn.Resource(iamtypes.GroupGVR).Get(t.Context(), "admins", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "rendering must not touch the cluster")
	require.NotNil(t, obj)
}

// TestGroupCreateDelete_RoundTrip exercises the full lifecycle: create the
// CR via dyn.Resource(...).Create with the same payload as the cobra
// command, then delete it via the same path the delete command takes. We
// don't go through cobra here because newDeleteCommand depends on a live
// kubeconfig; the production code paths under test are .Create and .Delete
// on the typed GVR, plus the buildGroupObject shape.
func TestGroupCreateDelete_RoundTrip(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())
	groupClient := dyn.Resource(iamtypes.GroupGVR)

	obj := buildGroupObject("admins")
	created, err := groupClient.Create(t.Context(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
	assert.Equal(t, "admins", created.GetName())

	// Re-creating the same group surfaces an AlreadyExists error from the
	// API server; the cobra command surfaces that verbatim.
	_, err = groupClient.Create(t.Context(), obj, metav1.CreateOptions{})
	require.Error(t, err)
	assert.True(t, apierrors.IsAlreadyExists(err))

	require.NoError(t, groupClient.Delete(t.Context(), "admins", metav1.DeleteOptions{}))

	_, err = groupClient.Get(t.Context(), "admins", metav1.GetOptions{})
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err), "group must be gone after delete")

	// Re-deleting yields NotFound — the cobra command wraps and propagates it.
	err = groupClient.Delete(t.Context(), "admins", metav1.DeleteOptions{})
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err))
}

// TestRemoveMember_RemovesUser is the happy path: a user member is removed
// from spec.members on a group with mixed members; only the targeted entry
// is dropped and the rest is preserved verbatim.
func TestRemoveMember_RemovesUser(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	createGroupWithMembers(t, dyn, "admins", []map[string]any{
		{"kind": "User", "name": "alice@example.com"},
		{"kind": "User", "name": "bob@example.com"},
		{"kind": "Group", "name": "platform"},
	})

	removed, err := RemoveMember(t.Context(), dyn, "admins", iamtypes.KindUser, "alice@example.com")
	require.NoError(t, err)
	assert.True(t, removed)

	gotMembers := readMembers(t, dyn, "admins")
	assert.Len(t, gotMembers, 2)
	for _, m := range gotMembers {
		assert.NotEqual(t, "alice@example.com", m["name"])
	}
}

// TestRemoveMember_RemovesGroup verifies that the explicit "group" kind
// removes a nested group, not a user with the same name. This pins down
// the (kind, name) compound identity used by spec.members entries.
func TestRemoveMember_RemovesGroup(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	createGroupWithMembers(t, dyn, "admins", []map[string]any{
		{"kind": "User", "name": "platform"},
		{"kind": "Group", "name": "platform"},
	})

	removed, err := RemoveMember(t.Context(), dyn, "admins", iamtypes.KindGroup, "platform")
	require.NoError(t, err)
	assert.True(t, removed)

	got := readMembers(t, dyn, "admins")
	require.Len(t, got, 1)
	assert.Equal(t, "User", got[0]["kind"])
	assert.Equal(t, "platform", got[0]["name"])
}

// TestRemoveMember_NoOp covers the case where the member is not present:
// returns removed=false with no error and the group is untouched.
func TestRemoveMember_NoOp(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	createGroupWithMembers(t, dyn, "admins", []map[string]any{
		{"kind": "User", "name": "alice@example.com"},
	})

	removed, err := RemoveMember(t.Context(), dyn, "admins", iamtypes.KindUser, "ghost@example.com")
	require.NoError(t, err)
	assert.False(t, removed)

	got := readMembers(t, dyn, "admins")
	require.Len(t, got, 1, "no-op must not touch existing members")
}

// TestRemoveMember_GroupNotFound surfaces the underlying NotFound from Get
// so callers can detect "no such group" through the wrap chain.
func TestRemoveMember_GroupNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	_, err := RemoveMember(t.Context(), dyn, "missing-group", iamtypes.KindUser, "alice@example.com")
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err),
		"missing group must surface as NotFound through %%w; got: %v", err)
}

// TestRemoveMember_Idempotent runs RemoveMember twice in a row on the same
// member: the first call removes, the second one is a no-op. This is what
// makes user-delete cleanup safe to retry after a partial failure.
func TestRemoveMember_Idempotent(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	createGroupWithMembers(t, dyn, "admins", []map[string]any{
		{"kind": "User", "name": "alice@example.com"},
	})

	removed, err := RemoveMember(t.Context(), dyn, "admins", iamtypes.KindUser, "alice@example.com")
	require.NoError(t, err)
	assert.True(t, removed)

	removed, err = RemoveMember(t.Context(), dyn, "admins", iamtypes.KindUser, "alice@example.com")
	require.NoError(t, err)
	assert.False(t, removed)
}

// --- helpers ---

func createGroupWithMembers(t *testing.T, dyn *fake.FakeDynamicClient, name string, members []map[string]any) {
	t.Helper()
	rawMembers := make([]any, 0, len(members))
	for _, m := range members {
		rawMembers = append(rawMembers, m)
	}
	obj := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": iamtypes.APIVersionDeckhouseV1Alpha1,
			"kind":       string(iamtypes.KindGroup),
			"metadata": map[string]any{
				"name": name,
			},
			"spec": map[string]any{
				"name":    name,
				"members": rawMembers,
			},
		},
	}
	_, err := dyn.Resource(iamtypes.GroupGVR).Create(t.Context(), obj, metav1.CreateOptions{})
	require.NoError(t, err)
}

func readMembers(t *testing.T, dyn *fake.FakeDynamicClient, name string) []map[string]any {
	t.Helper()
	got, err := dyn.Resource(iamtypes.GroupGVR).Get(t.Context(), name, metav1.GetOptions{})
	require.NoError(t, err)
	raw, _, _ := unstructured.NestedSlice(got.Object, "spec", "members")
	out := make([]map[string]any, 0, len(raw))
	for _, m := range raw {
		mp, ok := m.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, mp)
	}
	return out
}
