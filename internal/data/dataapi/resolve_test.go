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

package dataapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// errDiscoveryBroken stands in for a discovery endpoint that fails for a reason other than "this
// group does not exist".
var errDiscoveryBroken = errors.New("discovery unreachable")

// fakeLister answers discovery from a fixed table. A group absent from served is reported the way
// a real API server reports an unknown group: 404, not an empty listing.
type fakeLister struct {
	served map[string][]string
	err    error
	// errFor fails discovery for one group/version only, the way a single broken endpoint does.
	errFor map[string]error
	asked  []string
}

func (f *fakeLister) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	f.asked = append(f.asked, groupVersion)

	if f.err != nil {
		return nil, f.err
	}

	if err, ok := f.errFor[groupVersion]; ok {
		return nil, err
	}

	resources, ok := f.served[groupVersion]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "groupversion"}, groupVersion)
	}

	list := &metav1.APIResourceList{GroupVersion: groupVersion}
	for _, name := range resources {
		list.APIResources = append(list.APIResources, metav1.APIResource{Name: name})
	}

	return list, nil
}

// fakeReviewer answers SelfSubjectAccessReview from a fixed table keyed by API group, and records
// what it was asked so the tests can pin the question as well as the answer.
type fakeReviewer struct {
	allow    map[string]bool
	evalErr  map[string]string
	err      error
	asked    []authv1.ResourceAttributes
	askCount int
}

func (f *fakeReviewer) Create(
	_ context.Context,
	ssar *authv1.SelfSubjectAccessReview,
	_ metav1.CreateOptions,
) (*authv1.SelfSubjectAccessReview, error) {
	f.askCount++

	if ssar.Spec.ResourceAttributes != nil {
		f.asked = append(f.asked, *ssar.Spec.ResourceAttributes)
	}

	if f.err != nil {
		return nil, f.err
	}

	group := ""
	if ssar.Spec.ResourceAttributes != nil {
		group = ssar.Spec.ResourceAttributes.Group
	}

	out := ssar.DeepCopy()
	out.Status.Allowed = f.allow[group]
	out.Status.EvaluationError = f.evalErr[group]

	return out, nil
}

func bothGroupsServing(resource string) map[string][]string {
	return map[string][]string{
		FoundationGroupVersion.String(): {resource},
		LegacyGroupVersion.String():     {resource},
	}
}

// TestResolve_DecisionTable pins the full cross product of the two questions resolution asks.
// Each row states a cluster the CLI has to work against, and neither question alone separates the
// rows: "served but forbidden" and "authorized but not served" differ only in which of the two
// answers is negative, and they demand opposite fixes from the operator.
func TestResolve_DecisionTable(t *testing.T) {
	t.Parallel()

	const resource = ResourceDataExports

	tests := []struct {
		name      string
		served    map[string][]string
		allow     map[string]bool
		wantGroup string
		wantErrIs error
		// wantErrHas lists every substring the message must carry. A single expected substring
		// would not distinguish "named both groups" from "named the last one it looked at", and
		// an operator reading a half-message goes and grants RBAC on the wrong group.
		wantErrHas []string
	}{
		{
			name:      "both served and both authorized: storage-foundation wins",
			served:    bothGroupsServing(resource),
			allow:     map[string]bool{FoundationGroup: true, LegacyGroup: true},
			wantGroup: FoundationGroup,
		},
		{
			name:      "only storage-foundation served",
			served:    map[string][]string{FoundationGroupVersion.String(): {resource}},
			allow:     map[string]bool{FoundationGroup: true},
			wantGroup: FoundationGroup,
		},
		{
			name:      "only the older producer served",
			served:    map[string][]string{LegacyGroupVersion.String(): {resource}},
			allow:     map[string]bool{LegacyGroup: true},
			wantGroup: LegacyGroup,
		},
		{
			// The case this whole mechanism exists for: an edition that carries both CRDs but
			// grants the user rights on the older one only.
			name:      "both served, authorized on the older producer only",
			served:    bothGroupsServing(resource),
			allow:     map[string]bool{LegacyGroup: true},
			wantGroup: LegacyGroup,
		},
		{
			name:      "both served, authorized on neither",
			served:    bothGroupsServing(resource),
			allow:     map[string]bool{},
			wantErrIs: ErrForbidden,
			wantErrHas: []string{
				"auth can-i",
				FoundationGroupVersion.String(),
				LegacyGroupVersion.String(),
			},
		},
		{
			name:       "served by storage-foundation only, and forbidden",
			served:     map[string][]string{FoundationGroupVersion.String(): {resource}},
			allow:      map[string]bool{LegacyGroup: true},
			wantErrIs:  ErrForbidden,
			wantErrHas: []string{FoundationGroup},
		},
		{
			// Rights left behind by a previous edition must not be mistaken for an installed
			// module: nothing serves the resource, so no group can be addressed at all.
			name:       "authorized everywhere, served nowhere",
			served:     map[string][]string{},
			allow:      map[string]bool{FoundationGroup: true, LegacyGroup: true},
			wantErrIs:  ErrNoBackend,
			wantErrHas: []string{"neither"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, err := resolve(
				context.Background(),
				&fakeLister{served: tt.served},
				&fakeReviewer{allow: tt.allow},
				resource, "my-ns", nil,
			)

			if tt.wantErrIs != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				for _, want := range tt.wantErrHas {
					assert.Contains(t, err.Error(), want)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantGroup, backend.GroupVersion.Group)
			assert.Equal(t, Version, backend.GroupVersion.Version)
			assert.Equal(t, tt.wantGroup == LegacyGroup, backend.Legacy())
		})
	}
}

// TestResolve_NoBackendMessageNamesBothModules guards the operator-facing half of the ErrNoBackend
// message: the error has to say which modules would provide the resource, because "not served" on
// its own leaves the reader with nothing to enable.
func TestResolve_NoBackendMessageNamesBothModules(t *testing.T) {
	t.Parallel()

	_, err := resolve(
		context.Background(),
		&fakeLister{served: map[string][]string{}},
		&fakeReviewer{},
		ResourceDataImports, "my-ns", nil,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoBackend)
	assert.Contains(t, err.Error(), foundationModule)
	assert.Contains(t, err.Error(), legacyModule)
	assert.Contains(t, err.Error(), ResourceDataImports)
}

// TestResolve_AsksAboutTheCallersNamespace pins the question, not just the answer. A review run
// against the wrong namespace (or the empty one, which asks about cluster-wide permission) returns
// a confidently wrong verdict for a user who holds rights in exactly one namespace.
func TestResolve_AsksAboutTheCallersNamespace(t *testing.T) {
	t.Parallel()

	reviewer := &fakeReviewer{allow: map[string]bool{FoundationGroup: true}}

	_, err := resolve(
		context.Background(),
		&fakeLister{served: bothGroupsServing(ResourceDataExports)},
		reviewer,
		ResourceDataExports, "team-a", nil,
	)
	require.NoError(t, err)

	require.NotEmpty(t, reviewer.asked)
	first := reviewer.asked[0]
	assert.Equal(t, "team-a", first.Namespace)
	assert.Equal(t, "get", first.Verb)
	assert.Equal(t, ResourceDataExports, first.Resource)
	assert.Equal(t, FoundationGroup, first.Group)
	assert.Equal(t, Version, first.Version)
}

// TestResolve_SkipsReviewForUnservedGroups keeps resolution from spending a round trip asking about
// a group the server does not serve, whose answer could not change the outcome.
func TestResolve_SkipsReviewForUnservedGroups(t *testing.T) {
	t.Parallel()

	reviewer := &fakeReviewer{allow: map[string]bool{FoundationGroup: true, LegacyGroup: true}}

	_, err := resolve(
		context.Background(),
		&fakeLister{served: map[string][]string{FoundationGroupVersion.String(): {ResourceDataExports}}},
		reviewer,
		ResourceDataExports, "my-ns", nil,
	)
	require.NoError(t, err)

	assert.Equal(t, 1, reviewer.askCount, "only the served group is worth asking about")
}

// TestResolve_UnavailableReviewFallsBackToDiscoveryOrder covers a cluster whose role bindings deny
// SelfSubjectAccessReview itself. The question could not be asked, which is not an answer of "no":
// refusing to act would break a user whose only fault is a non-standard binding of
// system:basic-user, and a real denial still arrives as a 403 on the request that follows.
func TestResolve_UnavailableReviewFallsBackToDiscoveryOrder(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		reviewer *fakeReviewer
	}{
		{
			name:     "review request rejected",
			reviewer: &fakeReviewer{err: apierrors.NewForbidden(schema.GroupResource{}, "", errors.New("denied"))},
		},
		{
			name:     "authorizer could not evaluate",
			reviewer: &fakeReviewer{evalErr: map[string]string{FoundationGroup: "webhook timeout"}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, err := resolve(
				context.Background(),
				&fakeLister{served: bothGroupsServing(ResourceDataExports)},
				tt.reviewer,
				ResourceDataExports, "my-ns", nil,
			)

			require.NoError(t, err)
			assert.Equal(t, FoundationGroup, backend.GroupVersion.Group)
		})
	}
}

// TestResolve_BrokenDiscoveryIsAnError separates "the server says this group does not exist" from
// "the server could not be asked". Only the first is an answer; guessing past the second would
// silently address the wrong producer.
func TestResolve_BrokenDiscoveryIsAnError(t *testing.T) {
	t.Parallel()

	_, err := resolve(
		context.Background(),
		&fakeLister{err: errDiscoveryBroken},
		&fakeReviewer{allow: map[string]bool{FoundationGroup: true}},
		ResourceDataExports, "my-ns", nil,
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, errDiscoveryBroken)
	assert.NotErrorIs(t, err, ErrNoBackend, "an unreachable discovery endpoint is not proof of an absent module")
}

// TestServes_IgnoresSubresources pins the exact-match rule: a listing that carries
// "dataexports/status" but not "dataexports" describes a group that cannot serve the resource, and
// a prefix match would read it as one that can.
func TestServes_IgnoresSubresources(t *testing.T) {
	t.Parallel()

	lister := &fakeLister{served: map[string][]string{
		FoundationGroupVersion.String(): {"dataexports/status", "dataimports"},
	}}

	found, err := serves(lister, FoundationGroupVersion, ResourceDataExports)
	require.NoError(t, err)
	assert.False(t, found, "a subresource entry must not count as the resource itself")

	found, err = serves(lister, FoundationGroupVersion, ResourceDataImports)
	require.NoError(t, err)
	assert.True(t, found)
}

// TestResolve_ResolvesPerResource covers a cluster that serves one CRD of the pair from each
// producer. Resolving per group instead of per resource would pick one producer for both and then
// address a resource it does not serve.
func TestResolve_ResolvesPerResource(t *testing.T) {
	t.Parallel()

	lister := &fakeLister{served: map[string][]string{
		FoundationGroupVersion.String(): {ResourceDataExports},
		LegacyGroupVersion.String():     {ResourceDataImports},
	}}
	allow := map[string]bool{FoundationGroup: true, LegacyGroup: true}

	exports, err := resolve(context.Background(), lister, &fakeReviewer{allow: allow}, ResourceDataExports, "my-ns", nil)
	require.NoError(t, err)
	assert.Equal(t, FoundationGroup, exports.GroupVersion.Group)

	imports, err := resolve(context.Background(), lister, &fakeReviewer{allow: allow}, ResourceDataImports, "my-ns", nil)
	require.NoError(t, err)
	assert.Equal(t, LegacyGroup, imports.GroupVersion.Group)
}

// TestResolve_ExpiredContextNamesTheStep pins that the earliest exit still says which step ran out
// of time. Unwrapped, the caller chain hands cobra a bare "context canceled", and the user is told
// only that something timed out — not that it was group resolution, which is the one step they had
// no way to know their command performs.
func TestResolve_ExpiredContextNamesTheStep(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Resolve(ctx, &rest.Config{Host: "https://127.0.0.1:1"}, ResourceDataExports, "my-ns", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled, "the cause must stay recognisable through the wrapping")
	assert.Contains(t, err.Error(), ResourceDataExports)
	assert.Contains(t, err.Error(), "API group")
}

// TestResolve_NilConfig keeps the exported entry point from panicking on a client that never
// obtained a configuration.
func TestResolve_NilConfig(t *testing.T) {
	t.Parallel()

	_, err := Resolve(context.Background(), nil, ResourceDataExports, "my-ns", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no REST config")
}

// TestResolve_StopsAtTheFirstQualifyingCandidate pins that resolution consults candidates one at a
// time and stops. Probing both up front is not merely wasteful: it makes every `d8 data` run
// depend on the health of a group the run was never going to address — see the sibling test for
// the failure that causes.
func TestResolve_StopsAtTheFirstQualifyingCandidate(t *testing.T) {
	t.Parallel()

	lister := &fakeLister{served: bothGroupsServing(ResourceDataExports)}
	reviewer := &fakeReviewer{allow: map[string]bool{FoundationGroup: true, LegacyGroup: true}}

	backend, err := resolve(context.Background(), lister, reviewer, ResourceDataExports, "my-ns", nil)
	require.NoError(t, err)
	assert.Equal(t, FoundationGroup, backend.GroupVersion.Group)

	assert.Equal(t, []string{FoundationGroupVersion.String()}, lister.asked,
		"the second candidate must not be discovered once the first one qualifies")
	assert.Equal(t, 1, reviewer.askCount, "the second candidate must not be reviewed either")
}

// TestResolve_UnusedGroupHealthDoesNotMatter covers the cluster this whole ordering exists for: a
// normal storage-foundation installation where the OTHER group's discovery endpoint is broken.
//
// storage.deckhouse.io is not exclusive to storage-volume-data-manager — other Deckhouse modules
// serve resources under it — so its endpoint can be unhealthy on a cluster that has nothing to do
// with that module. Before resolution became lazy, that unhealthy endpoint failed `d8 data`
// outright, on a cluster where the command had every reason to work.
func TestResolve_UnusedGroupHealthDoesNotMatter(t *testing.T) {
	t.Parallel()

	lister := &fakeLister{
		served: bothGroupsServing(ResourceDataExports),
		errFor: map[string]error{LegacyGroupVersion.String(): errDiscoveryBroken},
	}

	backend, err := resolve(
		context.Background(), lister,
		&fakeReviewer{allow: map[string]bool{FoundationGroup: true}},
		ResourceDataExports, "my-ns", nil,
	)

	require.NoError(t, err)
	assert.Equal(t, FoundationGroup, backend.GroupVersion.Group)
}

// TestDiscoveryBudget pins the timeout put on the discovery configuration.
//
// The value that must never come out is zero, and not because zero would leave the request
// unbounded — client-go's setDiscoveryDefaults substitutes 32s for it. Because that 32s is a limit
// nobody in this call chain chose: it ignores the caller's remaining time, which is the only reason
// this budget exists. Zero is also exactly what a naive "just use the remaining time" would produce
// for an already-expired context.
func TestDiscoveryBudget(t *testing.T) {
	t.Parallel()

	t.Run("no deadline and no configured timeout: the default", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, discoveryTimeout, discoveryBudget(context.Background(), 0))
	})

	t.Run("a nearer deadline wins over the default", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		got := discoveryBudget(ctx, 0)
		assert.Positive(t, got)
		assert.Less(t, got, discoveryTimeout)
	})

	t.Run("a farther deadline does not lengthen the default", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()

		assert.Equal(t, discoveryTimeout, discoveryBudget(ctx, 0))
	})

	t.Run("a shorter configured timeout is kept", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, time.Second, discoveryBudget(context.Background(), time.Second))
	})

	t.Run("a longer configured timeout is not honoured", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, discoveryTimeout, discoveryBudget(context.Background(), time.Hour))
	})

	t.Run("an expired deadline still yields a non-zero timeout", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
		defer cancel()

		assert.Positive(t, discoveryBudget(ctx, 0),
			"zero hands the bound to client-go's 32s default instead of the caller's deadline")
	})
}
