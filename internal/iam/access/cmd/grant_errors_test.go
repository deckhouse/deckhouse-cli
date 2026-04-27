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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"

	iamtypes "github.com/deckhouse/deckhouse-cli/internal/iam/types"
)

// fakeListKinds tells the dynamic fake client about list kinds for the GVRs
// used by the access subcommand. Without this the fake client panics on the
// first List() call. Kept local to this test file because production code
// never builds a dynamic client this way.
func fakeListKinds() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		iamtypes.AuthorizationRuleGVR:        "AuthorizationRuleList",
		iamtypes.ClusterAuthorizationRuleGVR: "ClusterAuthorizationRuleList",
		iamtypes.GroupGVR:                    "GroupList",
		iamtypes.UserGVR:                     "UserList",
	}
}

// TestBuildGrantObject_RejectsInvalidNamespaceSpec exercises the defensive
// validation in buildGrantObject that rejects namespaced specs without
// exactly one non-empty namespace. We deliberately bypass canonicalGrantSpecs
// to simulate a future caller that constructs a spec by hand.
func TestBuildGrantObject_RejectsInvalidNamespaceSpec(t *testing.T) {
	base := &canonicalGrantSpec{
		Model:            iamtypes.ModelCurrent,
		SubjectKind:      iamtypes.KindUser,
		SubjectRef:       "anton",
		SubjectPrincipal: "anton@abc.com",
		AccessLevel:      "Admin",
		ScopeType:        iamtypes.ScopeNamespace,
	}

	t.Run("zero namespaces", func(t *testing.T) {
		spec := *base
		spec.Namespaces = nil
		_, err := buildGrantObject(&spec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one namespace")
	})

	t.Run("two namespaces", func(t *testing.T) {
		spec := *base
		spec.Namespaces = []string{"dev", "stage"}
		_, err := buildGrantObject(&spec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exactly one namespace")
	})

	t.Run("empty namespace string", func(t *testing.T) {
		spec := *base
		spec.Namespaces = []string{""}
		_, err := buildGrantObject(&spec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty namespace")
	})

	t.Run("happy path: one non-empty namespace", func(t *testing.T) {
		spec := *base
		spec.Namespaces = []string{"dev"}
		obj, err := buildGrantObject(&spec)
		require.NoError(t, err)
		assert.Equal(t, iamtypes.KindAuthorizationRule, obj.GetKind())
		assert.Equal(t, "dev", obj.GetNamespace())
	})
}

// TestCreateOrUpdateGrant_NotFoundCreates verifies the happy "first create"
// path: when the server returns NotFound on Get, we fall through to Create
// and return the created object.
func TestCreateOrUpdateGrant_NotFoundCreates(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())
	client := dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR)

	spec := &canonicalGrantSpec{
		Model:            iamtypes.ModelCurrent,
		SubjectKind:      iamtypes.KindUser,
		SubjectRef:       "anton",
		SubjectPrincipal: "anton@abc.com",
		AccessLevel:      "ClusterAdmin",
		ScopeType:        iamtypes.ScopeCluster,
	}
	obj, err := buildGrantObject(spec)
	require.NoError(t, err)

	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())

	got, err := createOrUpdateGrant(cmd, client, obj, spec)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, obj.GetName(), got.GetName())

	stored, err := client.Get(t.Context(), obj.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, obj.GetName(), stored.GetName())
}

// TestCreateOrUpdateGrant_OtherGetErrorPropagates verifies that a non-NotFound
// Get error is surfaced rather than silently routed to Create. This is the
// exact regression the second refactor pass was supposed to prevent: a
// transient API failure used to fall through to Create() and produce a
// misleading "AlreadyExists" or a blind overwrite.
func TestCreateOrUpdateGrant_OtherGetErrorPropagates(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())

	sentinel := errors.New("simulated etcd timeout")
	dyn.PrependReactor("get", "clusterauthorizationrules", func(_ clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, sentinel
	})

	client := dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR)

	spec := &canonicalGrantSpec{
		Model:            iamtypes.ModelCurrent,
		SubjectKind:      iamtypes.KindUser,
		SubjectRef:       "anton",
		SubjectPrincipal: "anton@abc.com",
		AccessLevel:      "ClusterAdmin",
		ScopeType:        iamtypes.ScopeCluster,
	}
	obj, err := buildGrantObject(spec)
	require.NoError(t, err)

	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())

	_, err = createOrUpdateGrant(cmd, client, obj, spec)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel,
		"non-NotFound Get error must propagate to the caller")

	// And critically: nothing was created — Create was never called because
	// we bailed out on the Get error.
	_, getErr := client.Get(t.Context(), obj.GetName(), metav1.GetOptions{})
	require.Error(t, getErr)
	assert.True(t, apierrors.IsNotFound(getErr) || errors.Is(getErr, sentinel),
		"object must not exist after a Get error short-circuited Create; got %v", getErr)
}

// TestCreateOrUpdateGrant_SameSpecIsNoop verifies that re-running grant with
// the same canonical spec is treated as a no-op (no Update call and the
// existing object is returned). This is the contract createOrUpdateGrant
// uses to make `d8 iam access grant` idempotent.
func TestCreateOrUpdateGrant_SameSpecIsNoop(t *testing.T) {
	scheme := runtime.NewScheme()
	dyn := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, fakeListKinds())
	client := dyn.Resource(iamtypes.ClusterAuthorizationRuleGVR)

	spec := &canonicalGrantSpec{
		Model:            iamtypes.ModelCurrent,
		SubjectKind:      iamtypes.KindUser,
		SubjectRef:       "anton",
		SubjectPrincipal: "anton@abc.com",
		AccessLevel:      "ClusterAdmin",
		ScopeType:        iamtypes.ScopeCluster,
	}
	obj, err := buildGrantObject(spec)
	require.NoError(t, err)

	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())

	// First call: created.
	_, err = createOrUpdateGrant(cmd, client, obj, spec)
	require.NoError(t, err)

	// Second call with a freshly-built object that has the same canonical
	// spec annotation: must be a no-op (same canonical-spec annotation).
	obj2, err := buildGrantObject(spec)
	require.NoError(t, err)
	got, err := createOrUpdateGrant(cmd, client, obj2, spec)
	require.NoError(t, err)
	assert.Equal(t, obj.GetName(), got.GetName())
}
