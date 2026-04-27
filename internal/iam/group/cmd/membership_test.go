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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

func fakeListKinds() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		iamtypes.GroupGVR: "GroupList",
	}
}

// TestEnsureMember_MissingGroupNotFoundIsDetectable guarantees that a caller
// can still recognise the "group not found" failure through the wrapped
// error returned by EnsureMember. Several callers (e.g. add-member, user
// create --member-of) decide whether to retry / create lazily based on
// apierrors.IsNotFound; if EnsureMember ever loses that signal under
// fmt.Errorf wrapping, those branches go silently dead.
func TestEnsureMember_MissingGroupNotFoundIsDetectable(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	_, err := EnsureMember(t.Context(), dyn,
		"does-not-exist", iamtypes.KindUser, "alice@example.com",
		EnsureMemberOpts{CreateGroupIfMissing: false})
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err),
		"missing-group error must satisfy apierrors.IsNotFound; got %v", err)
}

// TestEnsureMember_CreatesGroupWhenMissing verifies the lazy-create branch
// (used by `d8 iam user create --member-of`). It also doubles as a sanity
// check that the wrapped NotFound error path doesn't run in this case.
func TestEnsureMember_CreatesGroupWhenMissing(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	res, err := EnsureMember(t.Context(), dyn,
		"new-group", iamtypes.KindUser, "alice@example.com",
		EnsureMemberOpts{CreateGroupIfMissing: true})
	require.NoError(t, err)
	assert.True(t, res.GroupCreated)
	assert.True(t, res.Added)

	got, err := dyn.Resource(iamtypes.GroupGVR).Get(t.Context(), "new-group", metav1.GetOptions{})
	require.NoError(t, err)
	members, _, _ := unstructured.NestedSlice(got.Object, "spec", "members")
	assert.Len(t, members, 1)
}

// TestEnsureMember_IsIdempotent verifies that adding the same member twice is
// a no-op and is reported as such. We reuse the typed SubjectKind enum to
// also assert that internal comparisons survive a round-trip through the
// unstructured map.
func TestEnsureMember_IsIdempotent(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	const groupName = "admins"
	const memberName = "alice@example.com"

	_, err := EnsureMember(t.Context(), dyn, groupName, iamtypes.KindUser, memberName,
		EnsureMemberOpts{CreateGroupIfMissing: true})
	require.NoError(t, err)

	res, err := EnsureMember(t.Context(), dyn, groupName, iamtypes.KindUser, memberName,
		EnsureMemberOpts{CreateGroupIfMissing: false})
	require.NoError(t, err)
	assert.True(t, res.AlreadyMember)
	assert.False(t, res.Added)
}
