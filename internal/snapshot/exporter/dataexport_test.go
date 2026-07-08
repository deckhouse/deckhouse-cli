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

package exporter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
)

// runOwnerAnnotationKey is the exact annotation key EnsureDataExport stamps on the
// DataExport CRs a download run creates. It is duplicated here as a literal (the
// production constant is unexported) so a rename of the on-wire key fails a test.
const runOwnerAnnotationKey = "snapshot.deckhouse.io/download-run-id"

// captureWarnLogger returns a logger that writes WARN+ records as text into buf so
// tests can assert the foreign-owner adoption/skip messages and their attributes.
func captureWarnLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// newDEScheme returns a scheme with the DataExport types registered.
func newDEScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()

	require.NoError(t, deapi.AddToScheme(scheme))

	return scheme
}

func TestDataExportName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		leafName string
		wantName string
	}{
		{leafName: "my-disk-snap", wantName: "de-my-disk-snap"},
		{leafName: "nss-vs-pvc", wantName: "de-nss-vs-pvc"},
		{leafName: "", wantName: "de-"},
	}

	for _, tc := range cases {
		got := exporter.DataExportName(tc.leafName)
		assert.Equal(t, tc.wantName, got, "leafName=%q", tc.leafName)
	}
}

func TestEnsureDataExport_Creates(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		group     = "snapshot.storage.k8s.io"
		resource  = "volumesnapshots"
		kind      = "VolumeSnapshot"
		leafName  = "my-vs-leaf"
		ttl       = "3h"
	)

	scheme := newDEScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	ctx := context.Background()

	de, err := exporter.EnsureDataExport(ctx, c, namespace, group, resource, kind, leafName, ttl)
	require.NoError(t, err)
	require.NotNil(t, de)

	assert.Equal(t, exporter.DataExportName(leafName), de.Name)
	assert.Equal(t, namespace, de.Namespace)
	assert.Equal(t, group, de.Spec.TargetRef.Group)
	assert.Equal(t, resource, de.Spec.TargetRef.Resource)
	assert.Equal(t, kind, de.Spec.TargetRef.Kind)
	assert.Equal(t, leafName, de.Spec.TargetRef.Name)
	assert.Equal(t, ttl, de.Spec.TTL)

	// Marshal round-trip: the JSON must carry a non-empty "kind" key — the field the
	// kind-based API server rejects when absent (server-side structural CRD validation) —
	// and a non-empty "resource" key, which the deployed SVDM (mr135) GVR-based CRD
	// requires. TEMP REVERTME: both are sent for cross-server compatibility.
	raw, marshalErr := json.Marshal(de.Spec.TargetRef)
	require.NoError(t, marshalErr)
	assert.Contains(t, string(raw), `"kind":"VolumeSnapshot"`, "targetRef JSON must contain populated kind key")
	assert.Contains(t, string(raw), `"resource":"volumesnapshots"`, "targetRef JSON must contain populated resource key")
}

func TestEnsureDataExport_DomainLeaf(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		group     = "demo.deckhouse.io"
		resource  = "virtualdisksnapshots"
		kind      = "VirtualDiskSnapshot"
		leafName  = "disk-snap-1"
		ttl       = "1h"
	)

	scheme := newDEScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	ctx := context.Background()

	de, err := exporter.EnsureDataExport(ctx, c, namespace, group, resource, kind, leafName, ttl)
	require.NoError(t, err)
	require.NotNil(t, de)

	assert.Equal(t, exporter.DataExportName(leafName), de.Name)
	assert.Equal(t, group, de.Spec.TargetRef.Group)
	assert.Equal(t, resource, de.Spec.TargetRef.Resource)
	assert.Equal(t, kind, de.Spec.TargetRef.Kind)
	assert.Equal(t, leafName, de.Spec.TargetRef.Name)
}

func TestEnsureDataExport_DefaultTTL(t *testing.T) {
	t.Parallel()

	scheme := newDEScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	de, err := exporter.EnsureDataExport(context.Background(), c, "ns",
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, "leaf-vs", "")
	require.NoError(t, err)

	// Empty TTL should be replaced by the built-in default (non-empty).
	assert.NotEmpty(t, de.Spec.TTL)
}

func TestEnsureDataExport_Idempotent(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "idempotent-vs"
	)

	scheme := newDEScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	ctx := context.Background()

	de1, err := exporter.EnsureDataExport(ctx, c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, "1h")
	require.NoError(t, err)

	de2, err := exporter.EnsureDataExport(ctx, c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, "1h")
	require.NoError(t, err)

	assert.Equal(t, de1.Name, de2.Name)
	assert.Equal(t, de1.ResourceVersion, de2.ResourceVersion)
}

// TestEnsureDataExport_RecreatesWhenExpired verifies that a stale Expired DataExport
// left behind by a previous session (the producer never deletes it on TTL expiry;
// see storage-volume-data-manager's DataexportReconciler Case 2) is deleted and
// replaced by a fresh object instead of being returned forever, which would
// permanently block resume with ErrExpired.
func TestEnsureDataExport_RecreatesWhenExpired(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		group     = "snapshot.storage.k8s.io"
		resource  = "volumesnapshots"
		kind      = "VolumeSnapshot"
		leafName  = "expired-reuse-vs"
		ttl       = "1h"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	stale := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
		Spec: deapi.DataexportSpec{
			TTL: ttl,
			TargetRef: deapi.TargetRefSpec{
				Group:    group,
				Resource: resource,
				Kind:     kind,
				Name:     leafName,
			},
		},
		Status: deapi.DataExportStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Expired",
					Status: metav1.ConditionTrue,
					Reason: "TTLExpired",
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stale).WithStatusSubresource(stale).Build()

	ctx := context.Background()

	fresh, err := exporter.EnsureDataExport(ctx, c, namespace, group, resource, kind, leafName, ttl)
	require.NoError(t, err)
	require.NotNil(t, fresh)

	assert.Equal(t, deName, fresh.Name)
	assert.NotEqual(t, stale.ResourceVersion, fresh.ResourceVersion,
		"a fresh Create must have happened rather than reusing the stale object")

	for _, cond := range fresh.Status.Conditions {
		assert.NotEqual(t, "Expired", cond.Type, "a brand-new object must carry no Expired condition")
	}

	// A second call against the now-fresh (non-Expired) object must be idempotent,
	// matching TestEnsureDataExport_Idempotent's happy-path contract.
	again, err := exporter.EnsureDataExport(ctx, c, namespace, group, resource, kind, leafName, ttl)
	require.NoError(t, err)
	assert.Equal(t, fresh.Name, again.Name)
	assert.Equal(t, fresh.ResourceVersion, again.ResourceVersion)
}

// makeReadyDE returns a DataExport pre-populated with the Ready condition and a URL
// so that WaitReady exits on its first iteration without sleeping.
func makeReadyDE(namespace, leafName, baseURL, volumeMode string) *deapi.DataExport {
	deName := exporter.DataExportName(leafName)

	return &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
		Spec: deapi.DataexportSpec{
			TTL: "2h",
			TargetRef: deapi.TargetRefSpec{
				Group: aggapi.VolumeSnapshotGroup,
				Kind:  aggapi.VolumeSnapshotKind,
				Name:  leafName,
			},
		},
		Status: deapi.DataExportStatus{
			URL:        baseURL,
			VolumeMode: volumeMode,
			Conditions: []metav1.Condition{
				{
					Type:   "Ready",
					Status: metav1.ConditionTrue,
					Reason: "PodReady",
				},
			},
		},
	}
}

func TestWaitReady_AlreadyReady(t *testing.T) {
	t.Parallel()

	const (
		namespace  = "test-ns"
		leafName   = "ready-vs"
		baseURL    = "https://exporter.example.com"
		volumeMode = "Block"
	)

	scheme := newDEScheme(t)
	de := makeReadyDE(namespace, leafName, baseURL, volumeMode)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).WithStatusSubresource(de).Build()

	ctx := context.Background()

	got, err := exporter.WaitReady(ctx, c, slog.Default(), namespace, de.Name)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, baseURL, got.Status.URL)
	assert.Equal(t, volumeMode, got.Status.VolumeMode)
}

func TestWaitReady_Expired(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "expired-vs"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	de := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
		Status: deapi.DataExportStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Expired",
					Status: metav1.ConditionTrue,
					Reason: "TTLExpired",
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).WithStatusSubresource(de).Build()

	ctx := context.Background()

	_, err := exporter.WaitReady(ctx, c, slog.Default(), namespace, deName)
	require.Error(t, err)
	assert.True(t, errors.Is(err, exporter.ErrExpired), "expected ErrExpired, got: %v", err)
}

func TestWaitReady_DeadlineExceeded(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "never-ready-vs"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	de := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).Build()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	_, err := exporter.WaitReady(ctx, c, slog.Default(), namespace, deName)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "expected context.DeadlineExceeded, got: %v", err)
}

// TestWaitReady_DeadlineError_ContainsHintAndStatus verifies that the error returned
// on context deadline contains the inspection hint and the last observed DataExport
// status, so the user can immediately query the object for more details.
func TestWaitReady_DeadlineError_ContainsHintAndStatus(t *testing.T) {
	t.Parallel()

	const (
		namespace   = "test-ns"
		leafName    = "hint-check-vs"
		lastReason  = "TargetNotReady"
		lastMessage = "volume not provisioned yet"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	// DataExport with a known Ready condition reason so we can assert it appears in the error.
	de := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
		Status: deapi.DataExportStatus{
			Conditions: []metav1.Condition{
				{
					Type:    "Ready",
					Status:  metav1.ConditionFalse,
					Reason:  lastReason,
					Message: lastMessage,
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).WithStatusSubresource(de).Build()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	_, err := exporter.WaitReady(ctx, c, slog.Default(), namespace, deName)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded),
		"error must wrap context.DeadlineExceeded; got: %v", err)
	assert.Contains(t, err.Error(), "d8 k -n "+namespace+" get dataexport "+deName,
		"error must contain the inspection hint")
	assert.Contains(t, err.Error(), lastReason,
		"error must contain the last observed Ready condition reason")
}

func TestWaitReady_ContextCancelled(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "pending-vs"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	// Pending DataExport: no Ready or Expired conditions.
	de := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).Build()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so WaitReady exits without sleeping

	_, err := exporter.WaitReady(ctx, c, slog.Default(), namespace, deName)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got: %v", err)
}

func TestReleaseDataExport_Deletes(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "releaseme-vs"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	de := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).Build()

	ctx := context.Background()

	err := exporter.ReleaseDataExport(ctx, c, slog.Default(), namespace, deName, "")
	require.NoError(t, err)

	// Verify the DataExport is gone.
	check := new(deapi.DataExport)
	getErr := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: deName}, check)
	require.Error(t, getErr, "DataExport should be deleted")
}

func TestReleaseDataExport_Idempotent(t *testing.T) {
	t.Parallel()

	scheme := newDEScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	err := exporter.ReleaseDataExport(context.Background(), c, slog.Default(), "ns", "nonexistent-de", "")
	assert.NoError(t, err)
}

// TestEnsureDataExport_StampsRunOwnerOnCreate verifies that a DataExport created
// under WithRunOwner carries the creating run's ID in the run-owner annotation,
// the marker later used to decide who may release it (inv #10b).
func TestEnsureDataExport_StampsRunOwnerOnCreate(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "owned-vs"
		runID     = "run-aaaa"
	)

	scheme := newDEScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	de, err := exporter.EnsureDataExport(context.Background(), c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, "1h",
		exporter.WithRunOwner(runID, slog.Default()))
	require.NoError(t, err)
	require.NotNil(t, de)

	assert.Equal(t, runID, de.Annotations[runOwnerAnnotationKey],
		"a created DataExport must carry the creating run's owner annotation")
}

// TestEnsureDataExport_AdoptsForeignLiveCRWithWarn verifies that a second run
// (B) reusing the deterministic de-<leaf> CR created by another live run (A)
// reuses the endpoint for transfer, logs an explicit WARN naming A, does NOT
// overwrite the owner annotation, and — crucially — never deletes A's CR on its
// own release path (inv #10b). A's live export therefore survives B's release.
func TestEnsureDataExport_AdoptsForeignLiveCRWithWarn(t *testing.T) {
	t.Parallel()

	const (
		namespace  = "test-ns"
		leafName   = "shared-vs"
		ownerRun   = "run-a-1111"
		adopterRun = "run-b-2222"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	existing := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deName,
			Namespace:   namespace,
			Annotations: map[string]string{runOwnerAnnotationKey: ownerRun},
		},
		Spec: deapi.DataexportSpec{
			TTL: "1h",
			TargetRef: deapi.TargetRefSpec{
				Group:    aggapi.VolumeSnapshotGroup,
				Resource: aggapi.VolumeSnapshotResource,
				Kind:     aggapi.VolumeSnapshotKind,
				Name:     leafName,
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

	ctx := context.Background()

	var buf bytes.Buffer

	got, err := exporter.EnsureDataExport(ctx, c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, "1h",
		exporter.WithRunOwner(adopterRun, captureWarnLogger(&buf)))
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, ownerRun, got.Annotations[runOwnerAnnotationKey],
		"adopting a foreign live CR must not overwrite its owner annotation")

	logged := buf.String()
	assert.Contains(t, logged, ownerRun, "the adoption WARN must name the foreign owner run")
	assert.Contains(t, logged, "run_id", "the adoption WARN must carry the snake_case run_id attr")

	// B releases: A's CR must survive because B does not own it.
	require.NoError(t, exporter.ReleaseDataExport(ctx, c, slog.Default(), namespace, deName, adopterRun))

	check := new(deapi.DataExport)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: deName}, check),
		"a foreign run's live DataExport must survive the adopting run's release")
	assert.Equal(t, ownerRun, check.Annotations[runOwnerAnnotationKey])
}

// TestReleaseDataExport_OwnerDeletesWithUIDPrecondition verifies that the owning
// run's release deletes its own CR and that the delete carries a UID precondition
// (the guard that closes the check-then-delete race on a real cluster).
func TestReleaseDataExport_OwnerDeletesWithUIDPrecondition(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "owner-del-vs"
		runID     = "run-owner-9999"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	de := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deName,
			Namespace:   namespace,
			UID:         types.UID("uid-123"),
			Annotations: map[string]string{runOwnerAnnotationKey: runID},
		},
	}

	var (
		sawPrecondition bool
		gotUID          types.UID
	)

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				do := client.DeleteOptions{}
				do.ApplyOptions(opts)

				if do.Preconditions != nil && do.Preconditions.UID != nil {
					sawPrecondition = true
					gotUID = *do.Preconditions.UID
				}

				return cl.Delete(ctx, obj, opts...)
			},
		}).Build()

	ctx := context.Background()

	require.NoError(t, exporter.ReleaseDataExport(ctx, c, slog.Default(), namespace, deName, runID))

	assert.True(t, sawPrecondition, "the owner's release must pass a UID deletion precondition")
	assert.Equal(t, types.UID("uid-123"), gotUID, "the precondition must carry the observed CR's UID")

	check := new(deapi.DataExport)
	getErr := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: deName}, check)
	require.Error(t, getErr, "the owning run's DataExport must be deleted on release")
}

// TestReleaseDataExport_UIDConflictTreatedAsSuccess verifies that when the CR was
// replaced between the Get and the Delete (the UID precondition fails with
// Conflict), the release is treated as a successful no-op rather than an error:
// the object we observed is already gone and is not ours to delete.
func TestReleaseDataExport_UIDConflictTreatedAsSuccess(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "conflict-vs"
		runID     = "run-owner-7777"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	de := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deName,
			Namespace:   namespace,
			UID:         types.UID("uid-old"),
			Annotations: map[string]string{runOwnerAnnotationKey: runID},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
				return apierrors.NewConflict(
					schema.GroupResource{Group: "storage.deckhouse.io", Resource: "dataexports"},
					deName, errors.New("uid in precondition does not match"))
			},
		}).Build()

	err := exporter.ReleaseDataExport(context.Background(), c, slog.Default(), namespace, deName, runID)
	assert.NoError(t, err, "a UID precondition Conflict must be treated as a successful release")
}

// TestEnsureDataExport_ExpiredForeignCRRecreatedWithOwnership verifies that the
// expired-reclaim path stays owner-agnostic: an Expired CR owned by ANOTHER run
// is still deleted and recreated (existing stale-Expired behavior), and the fresh
// CR is stamped with THIS run's ownership. The reclaim is not a foreign adoption,
// so no adoption WARN is emitted.
func TestEnsureDataExport_ExpiredForeignCRRecreatedWithOwnership(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "expired-foreign-vs"
		ownerRun  = "run-a-crashed"
		freshRun  = "run-b-fresh"
		ttl       = "1h"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	stale := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deName,
			Namespace:   namespace,
			Annotations: map[string]string{runOwnerAnnotationKey: ownerRun},
		},
		Spec: deapi.DataexportSpec{
			TTL: ttl,
			TargetRef: deapi.TargetRefSpec{
				Group:    aggapi.VolumeSnapshotGroup,
				Resource: aggapi.VolumeSnapshotResource,
				Kind:     aggapi.VolumeSnapshotKind,
				Name:     leafName,
			},
		},
		Status: deapi.DataExportStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Expired",
					Status: metav1.ConditionTrue,
					Reason: "TTLExpired",
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stale).WithStatusSubresource(stale).Build()

	var buf bytes.Buffer

	fresh, err := exporter.EnsureDataExport(context.Background(), c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, ttl,
		exporter.WithRunOwner(freshRun, captureWarnLogger(&buf)))
	require.NoError(t, err)
	require.NotNil(t, fresh)

	assert.NotEqual(t, stale.ResourceVersion, fresh.ResourceVersion,
		"an Expired CR must be recreated regardless of its owner")
	assert.Equal(t, freshRun, fresh.Annotations[runOwnerAnnotationKey],
		"the recreated CR must be stamped with the reclaiming run's ownership")

	for _, cond := range fresh.Status.Conditions {
		assert.NotEqual(t, "Expired", cond.Type, "a brand-new object must carry no Expired condition")
	}

	assert.NotContains(t, buf.String(), "adopting DataExport",
		"an expired reclaim is a recreate, not a foreign adoption")
}
