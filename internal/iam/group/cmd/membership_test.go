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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

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

// TestEnsureMember_NonNotFoundGetErrorPropagates is the symmetric guard for
// the same class of bug we previously fixed in createOrUpdateGrant: a
// non-NotFound Get failure (Forbidden, Timeout, transient API error) must
// not fall through to Create. Otherwise we would either overwrite an
// existing group we just couldn't read, or create one for a caller who
// lacks visibility to it. The error must surface verbatim.
func TestEnsureMember_NonNotFoundGetErrorPropagates(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	sentinel := errors.New("simulated etcd timeout")
	dyn.PrependReactor("get", "groups", func(_ clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, sentinel
	})
	// A Create reactor would let us assert "Create was never called" by
	// failing loudly if it ever fires. The Get reactor above short-circuits
	// the codepath, so this should never trigger.
	createCalled := false
	dyn.PrependReactor("create", "groups", func(_ clienttesting.Action) (bool, runtime.Object, error) {
		createCalled = true
		return false, nil, nil
	})

	_, err := EnsureMember(t.Context(), dyn,
		"some-group", iamtypes.KindUser, "alice@example.com",
		EnsureMemberOpts{CreateGroupIfMissing: true})
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel,
		"non-NotFound Get error must propagate to the caller")
	assert.False(t, apierrors.IsNotFound(err),
		"non-NotFound error must not be misclassified as NotFound; got %v", err)
	assert.False(t, createCalled,
		"Create must not be called when Get returned a non-NotFound error")
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
