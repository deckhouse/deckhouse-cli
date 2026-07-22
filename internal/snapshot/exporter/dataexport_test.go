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

// captureInfoLogger returns a logger that writes INFO+ records as text into buf so
// tests can assert the periodic terminating-wait progress line and its attributes.
func captureInfoLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
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

// TestEnsureDataExport_RecreatesWhenExpired verifies that a stale expired DataExport
// (Ready=False/Expired) left behind by a previous session — the producer's GC only deletes it after its
// retention TTL, so within that window it lingers — is deleted and replaced by a fresh object instead of
// being returned forever, which would permanently block resume with ErrExpired.
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
					// Expired is now signalled as Ready=False with reason "Expired" (the standalone
					// "Expired" condition type was removed from the catalog).
					Type:   "Ready",
					Status: metav1.ConditionFalse,
					Reason: "Expired",
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
		if cond.Type == "Ready" {
			assert.False(t, cond.Status == metav1.ConditionFalse && cond.Reason == "Expired",
				"a freshly recreated object must not be in the Ready=False/Expired state")
		}
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
					// Expired is now signalled as Ready=False with reason "Expired" (the standalone
					// "Expired" condition type was removed from the catalog).
					Type:   "Ready",
					Status: metav1.ConditionFalse,
					Reason: "Expired",
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
					schema.GroupResource{Group: "storage-foundation.deckhouse.io", Resource: "dataexports"},
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
					// Expired is now signalled as Ready=False with reason "Expired" (the standalone
					// "Expired" condition type was removed from the catalog).
					Type:   "Ready",
					Status: metav1.ConditionFalse,
					Reason: "Expired",
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
		if cond.Type == "Ready" {
			assert.False(t, cond.Status == metav1.ConditionFalse && cond.Reason == "Expired",
				"a freshly recreated object must not be in the Ready=False/Expired state")
		}
	}

	assert.NotContains(t, buf.String(), "adopting DataExport",
		"an expired reclaim is a recreate, not a foreign adoption")
}

// TestEnsureDataExport_WaitsOutTerminatingCRThenCreatesStamped reproduces the
// interrupt→resume race: an interrupted run's release defer deleted the deterministic
// de-<leaf> CR, but the controller leaves it TERMINATING (DeletionTimestamp set,
// finalizers unwinding) for a moment. A new run that Gets it in that window must NOT
// adopt the doomed object — it must wait for it to vanish and create a fresh, this-run
// owned CR. Finalizer removal on the FIRST Get simulates the object disappearing
// "between polls" without goroutines.
func TestEnsureDataExport_WaitsOutTerminatingCRThenCreatesStamped(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "terminating-vs"
		oldOwner  = "run-interrupted-aaaa"
		newOwner  = "run-resume-bbbb"
		ttl       = "1h"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	now := metav1.NewTime(time.Now())

	terminating := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:              deName,
			Namespace:         namespace,
			DeletionTimestamp: &now,
			Finalizers:        []string{"dataexport.deckhouse.io/test-hold"},
			Annotations:       map[string]string{runOwnerAnnotationKey: oldOwner},
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
	}

	// The FIRST Get returns the terminating CR, then drops its finalizer so the fake
	// deletes it; the next poll Get therefore observes NotFound. The re-fetch after the
	// subsequent Create returns the fresh (non-terminating) object untouched.
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(terminating).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := cl.Get(ctx, key, obj, opts...); err != nil {
					return err
				}

				de, ok := obj.(*deapi.DataExport)
				if ok && key.Name == deName && de.DeletionTimestamp != nil && len(de.Finalizers) > 0 {
					drop := de.DeepCopy()
					drop.Finalizers = nil

					if err := cl.Update(ctx, drop); err != nil {
						return err
					}
				}

				return nil
			},
		}).Build()

	var buf bytes.Buffer

	got, err := exporter.EnsureDataExport(context.Background(), c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, ttl,
		exporter.WithRunOwner(newOwner, captureWarnLogger(&buf)))
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Nil(t, got.DeletionTimestamp,
		"a terminating CR must never be returned; a fresh, non-terminating one is created")
	assert.Equal(t, newOwner, got.Annotations[runOwnerAnnotationKey],
		"the recreated CR must be stamped with THIS run's ownership, not the interrupted run's")
	assert.NotContains(t, buf.String(), "adopting DataExport",
		"waiting out a terminating CR is a recreate, not a foreign adoption")
}

// TestEnsureDataExport_TerminatingCRNeverGoneReturnsCtxDeadline verifies that when a
// terminating CR never vanishes (finalizer stuck), EnsureDataExport does not adopt it
// or hang forever: it returns a wrapped ctx.Err() once the caller's deadline elapses,
// and returns no CR.
func TestEnsureDataExport_TerminatingCRNeverGoneReturnsCtxDeadline(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "stuck-terminating-vs"
		ttl       = "1h"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	now := metav1.NewTime(time.Now())

	stuck := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:              deName,
			Namespace:         namespace,
			DeletionTimestamp: &now,
			Finalizers:        []string{"dataexport.deckhouse.io/test-hold"},
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
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stuck).Build()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	got, err := exporter.EnsureDataExport(ctx, c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, ttl)
	require.Error(t, err)
	assert.Nil(t, got, "no CR may be returned while the terminating CR is still present")
	assert.True(t, errors.Is(err, context.DeadlineExceeded),
		"the wait must return a wrapped context.DeadlineExceeded; got: %v", err)
}

// makeStuckTerminatingDE returns a DataExport that is permanently terminating: it
// carries a DeletionTimestamp and a finalizer that nothing ever removes, so a fake
// client keeps returning it (never NotFound) — the wedged-finalizer / downed-controller
// case waitForDataExportGone must not hang on.
func makeStuckTerminatingDE(namespace, leafName string) *deapi.DataExport {
	deName := exporter.DataExportName(leafName)
	now := metav1.NewTime(time.Now())

	return &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:              deName,
			Namespace:         namespace,
			DeletionTimestamp: &now,
			Finalizers:        []string{"dataexport.deckhouse.io/test-hold"},
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
}

// TestEnsureDataExport_TerminatingWaitBoundedByOption verifies the fix for the
// indefinite-hang bug: WithTerminatingWaitTimeout caps the terminating wait ON TOP
// OF ctx, so a permanently-terminating CR fails the call within the cap even when
// the caller passes a deadline-less ctx (context.Background, as the pipeline's
// stamp-Ensure did). The error must wrap context.DeadlineExceeded, return no CR,
// and carry the kubectl inspection hint naming the CR.
func TestEnsureDataExport_TerminatingWaitBoundedByOption(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "opt-bounded-terminating-vs"
		ttl       = "1h"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)
	stuck := makeStuckTerminatingDE(namespace, leafName)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stuck).Build()

	start := time.Now()

	// A deadline-less parent ctx (the raw run ctx the pipeline stamp-Ensure used):
	// only WithTerminatingWaitTimeout bounds the wait here.
	got, err := exporter.EnsureDataExport(context.Background(), c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, ttl,
		exporter.WithTerminatingWaitTimeout(50*time.Millisecond))
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Nil(t, got, "no CR may be returned while the terminating CR is still present")
	assert.True(t, errors.Is(err, context.DeadlineExceeded),
		"the wait must return a wrapped context.DeadlineExceeded; got: %v", err)
	assert.Less(t, elapsed, 5*time.Second,
		"the wait must be bounded by the option, not hang on the wedged finalizer")
	assert.Contains(t, err.Error(), "d8 k -n "+namespace+" get dataexport "+deName,
		"the deadline error must carry the inspection hint naming the CR")
}

// TestEnsureDataExport_TerminatingWaitLogsPeriodically verifies the wait emits an
// Info progress line (with the CR name and the kubectl inspection hint) while it
// polls, so a slow finalizer unwind is observable instead of a silent spinner. The
// first poll always logs, so even a short-lived wait produces the line deterministically.
func TestEnsureDataExport_TerminatingWaitLogsPeriodically(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "logged-terminating-vs"
		runID     = "run-log-1234"
		ttl       = "1h"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)
	stuck := makeStuckTerminatingDE(namespace, leafName)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stuck).Build()

	var buf bytes.Buffer

	_, err := exporter.EnsureDataExport(context.Background(), c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, ttl,
		exporter.WithRunOwner(runID, captureInfoLogger(&buf)),
		exporter.WithTerminatingWaitTimeout(50*time.Millisecond))
	require.Error(t, err)

	logged := buf.String()
	assert.Contains(t, logged, "waiting for terminating DataExport to be deleted",
		"the wait must emit a periodic progress line")
	assert.Contains(t, logged, deName, "the progress line must name the terminating CR")
	assert.Contains(t, logged, "inspect_hint", "the progress line must carry the snake_case inspect_hint attr")
	assert.Contains(t, logged, "get dataexport "+deName, "the inspect_hint must name the CR")
}

// TestEnsureDataExport_RefusesTargetRefMismatch verifies that a live same-named
// de-<leaf> CR whose Spec.TargetRef names a DIFFERENT object is NOT adopted: the
// deterministic name encodes only the leaf, so a CSI VolumeSnapshot and a domain
// snapshot CR can share it, or a stale run can leave one pointing elsewhere.
// Reusing that endpoint would stream the wrong object's bytes into this node dir
// and checksum as complete forever. EnsureDataExport must instead return a
// descriptive ErrTargetRefMismatch naming BOTH targets, and must leave the
// colliding CR untouched (still present, same UID) — never silently reuse it,
// never silently delete another target's live export.
func TestEnsureDataExport_RefusesTargetRefMismatch(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "aliased-name"
		ttl       = "1h"
	)

	deName := exporter.DataExportName(leafName)

	// The request always asks for a CSI VolumeSnapshot; each case pre-creates a
	// live CR of the same NAME but a different target that must be refused.
	cases := []struct {
		name         string
		existingRef  deapi.TargetRefSpec
		wantInErrGrp string
	}{
		{
			name: "mismatched kind",
			existingRef: deapi.TargetRefSpec{
				Group:    aggapi.VolumeSnapshotGroup,
				Resource: aggapi.VolumeSnapshotResource,
				Kind:     "VirtualDiskSnapshot",
				Name:     leafName,
			},
			wantInErrGrp: aggapi.VolumeSnapshotGroup,
		},
		{
			name: "mismatched group and resource",
			existingRef: deapi.TargetRefSpec{
				Group:    "demo.deckhouse.io",
				Resource: "virtualdisksnapshots",
				Kind:     aggapi.VolumeSnapshotKind,
				Name:     leafName,
			},
			wantInErrGrp: "demo.deckhouse.io",
		},
		{
			name: "mismatched name",
			existingRef: deapi.TargetRefSpec{
				Group:    aggapi.VolumeSnapshotGroup,
				Resource: aggapi.VolumeSnapshotResource,
				Kind:     aggapi.VolumeSnapshotKind,
				Name:     "some-other-object",
			},
			wantInErrGrp: aggapi.VolumeSnapshotGroup,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := newDEScheme(t)

			existing := &deapi.DataExport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deName,
					Namespace: namespace,
					UID:       types.UID("uid-existing"),
				},
				Spec: deapi.DataexportSpec{TTL: ttl, TargetRef: tc.existingRef},
			}

			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

			ctx := context.Background()

			got, err := exporter.EnsureDataExport(ctx, c, namespace,
				aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, ttl)
			require.Error(t, err)
			assert.Nil(t, got, "no CR may be returned on a targetRef mismatch")
			assert.True(t, errors.Is(err, exporter.ErrTargetRefMismatch),
				"error must wrap ErrTargetRefMismatch; got: %v", err)

			// The message must name BOTH the existing target and the request so
			// the operator can resolve the collision.
			assert.Contains(t, err.Error(), tc.existingRef.Name, "error must name the existing target")
			assert.Contains(t, err.Error(), tc.wantInErrGrp, "error must name the existing target's group")
			assert.Contains(t, err.Error(), leafName, "error must name the requested target")
			assert.Contains(t, err.Error(), aggapi.VolumeSnapshotKind, "error must name the requested kind")

			// The colliding CR must be left completely untouched: still present,
			// same UID (never reused, never deleted).
			check := new(deapi.DataExport)
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: deName}, check),
				"a mismatched CR must never be deleted")
			assert.Equal(t, types.UID("uid-existing"), check.UID, "the mismatched CR must be untouched")
			assert.Equal(t, tc.existingRef, check.Spec.TargetRef, "the mismatched CR's targetRef must be untouched")
		})
	}
}

// TestEnsureDataExport_AdoptsMatchingTargetRefAcrossRuns verifies the happy path
// is byte-for-byte unchanged when the live CR's targetRef MATCHES the request: it
// is adopted (no Create, same UID) even with WithRunOwner from a DIFFERENT run,
// which produces only the foreign-adoption WARN — the targetRef guard adds no new
// behavior for a matching target.
func TestEnsureDataExport_AdoptsMatchingTargetRefAcrossRuns(t *testing.T) {
	t.Parallel()

	const (
		namespace  = "test-ns"
		leafName   = "matching-vs"
		ownerRun   = "run-a-owner"
		adopterRun = "run-b-adopter"
		ttl        = "1h"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	existing := &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:        deName,
			Namespace:   namespace,
			UID:         types.UID("uid-match"),
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
	}

	createCalled := false

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				createCalled = true

				return cl.Create(ctx, obj, opts...)
			},
		}).Build()

	ctx := context.Background()

	var buf bytes.Buffer

	got, err := exporter.EnsureDataExport(ctx, c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, ttl,
		exporter.WithRunOwner(adopterRun, captureWarnLogger(&buf)))
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.False(t, createCalled, "a matching live CR must be adopted, never recreated")
	assert.Equal(t, types.UID("uid-match"), got.UID, "adoption must return the existing object")
	assert.Equal(t, ownerRun, got.Annotations[runOwnerAnnotationKey],
		"adoption must not overwrite the existing owner annotation")
	assert.Contains(t, buf.String(), ownerRun, "the adoption WARN must still name the foreign owner run")
}

// TestEnsureDataExport_AdoptsWhenServerPrunedResourceOrKind verifies the guard
// tolerates the deployed SVDM CRD pruning whichever of resource/kind it does not
// understand (TargetRefSpec's TEMP REVERTME): a re-fetched matching CR that
// carries an empty resource OR an empty kind is still adopted, not rejected as a
// mismatch. A field is a mismatch only when populated on both sides and differing.
func TestEnsureDataExport_AdoptsWhenServerPrunedResourceOrKind(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		ttl       = "1h"
	)

	cases := []struct {
		name        string
		leafName    string
		existingRef deapi.TargetRefSpec
	}{
		{
			name:     "kind pruned by GVR-based server",
			leafName: "pruned-kind-vs",
			existingRef: deapi.TargetRefSpec{
				Group:    aggapi.VolumeSnapshotGroup,
				Resource: aggapi.VolumeSnapshotResource,
				Kind:     "",
				Name:     "pruned-kind-vs",
			},
		},
		{
			name:     "resource pruned by kind-based server",
			leafName: "pruned-resource-vs",
			existingRef: deapi.TargetRefSpec{
				Group:    aggapi.VolumeSnapshotGroup,
				Resource: "",
				Kind:     aggapi.VolumeSnapshotKind,
				Name:     "pruned-resource-vs",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			deName := exporter.DataExportName(tc.leafName)

			scheme := newDEScheme(t)

			existing := &deapi.DataExport{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deName,
					Namespace: namespace,
					UID:       types.UID("uid-pruned"),
				},
				Spec: deapi.DataexportSpec{TTL: ttl, TargetRef: tc.existingRef},
			}

			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

			got, err := exporter.EnsureDataExport(context.Background(), c, namespace,
				aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, tc.leafName, ttl)
			require.NoError(t, err, "a pruned-but-matching targetRef must still be adopted")
			require.NotNil(t, got)
			assert.Equal(t, types.UID("uid-pruned"), got.UID, "the matching CR must be adopted unchanged")
		})
	}
}

// TestEnsureDataExport_LiveCRAdoptedNotRecreated is the regression guard for the
// unchanged happy path: a live, non-terminating CR is adopted as-is (same object,
// no Create), exactly as before the terminating-wait was added.
func TestEnsureDataExport_LiveCRAdoptedNotRecreated(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "live-adopt-vs"
		ownerRun  = "run-owner-live"
		otherRun  = "run-other-live"
		ttl       = "1h"
	)

	deName := exporter.DataExportName(leafName)

	scheme := newDEScheme(t)

	live := &deapi.DataExport{
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
	}

	createCalled := false

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(live).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				createCalled = true

				return cl.Create(ctx, obj, opts...)
			},
		}).Build()

	got, err := exporter.EnsureDataExport(context.Background(), c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, ttl,
		exporter.WithRunOwner(otherRun, slog.Default()))
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.False(t, createCalled, "a live, non-terminating CR must be adopted, never recreated")
	assert.Equal(t, live.ResourceVersion, got.ResourceVersion,
		"adoption must return the existing object unchanged")
	assert.Equal(t, ownerRun, got.Annotations[runOwnerAnnotationKey],
		"adoption must not overwrite the existing owner annotation")
}

// TestEnsureDataExport_RefusesTargetRefMismatchAfterCreateRace covers the
// Get→Create race window: the first Get sees NotFound (nothing stored yet), so
// EnsureDataExport falls through to Create — but a concurrent actor already
// created de-<leaf> targeting a DIFFERENT object, so Create returns AlreadyExists
// (swallowed) and the post-Create re-fetch yields the FOREIGN CR. The first-Get
// guard never ran (there was nothing to observe), so this path must re-run the
// targetRef check and refuse: return the same wrapped ErrTargetRefMismatch naming
// both targets, and leave the foreign CR untouched (no delete).
func TestEnsureDataExport_RefusesTargetRefMismatchAfterCreateRace(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "race-aliased-name"
		ttl       = "1h"
	)

	deName := exporter.DataExportName(leafName)

	// The request asks for a CSI VolumeSnapshot; the concurrently-stored CR targets
	// a domain snapshot CR sharing the same de-<leaf> name (different group/kind).
	foreignRef := deapi.TargetRefSpec{
		Group:    "demo.deckhouse.io",
		Resource: "virtualdisksnapshots",
		Kind:     "VirtualDiskSnapshot",
		Name:     leafName,
	}

	scheme := newDEScheme(t)

	// The fake starts empty (first Get -> NotFound), so EnsureDataExport reaches the
	// Create step. The Create interceptor injects the foreign CR into the store and
	// returns AlreadyExists, exactly as a concurrent winner of the race would: the
	// swallowed AlreadyExists then leads to a re-fetch of the foreign object.
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
				foreign := &deapi.DataExport{
					ObjectMeta: metav1.ObjectMeta{
						Name:      deName,
						Namespace: namespace,
						UID:       types.UID("uid-foreign"),
					},
					Spec: deapi.DataexportSpec{TTL: ttl, TargetRef: foreignRef},
				}

				if err := cl.Create(ctx, foreign); err != nil {
					return err
				}

				return apierrors.NewAlreadyExists(
					schema.GroupResource{Group: "storage-foundation.deckhouse.io", Resource: "dataexports"}, deName)
			},
		}).Build()

	ctx := context.Background()

	got, err := exporter.EnsureDataExport(ctx, c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, ttl)
	require.Error(t, err)
	assert.Nil(t, got, "no CR may be returned when the re-fetched CR targets a different object")
	assert.True(t, errors.Is(err, exporter.ErrTargetRefMismatch),
		"error must wrap ErrTargetRefMismatch; got: %v", err)
	assert.Contains(t, err.Error(), foreignRef.Group, "error must name the foreign target's group")
	assert.Contains(t, err.Error(), aggapi.VolumeSnapshotKind, "error must name the requested kind")

	// The foreign CR must be left completely untouched (never deleted).
	check := new(deapi.DataExport)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: deName}, check),
		"a mismatched re-fetched CR must never be deleted")
	assert.Equal(t, types.UID("uid-foreign"), check.UID, "the foreign CR must be untouched")
	assert.Equal(t, foreignRef, check.Spec.TargetRef, "the foreign CR's targetRef must be untouched")
}

// TestEnsureDataExport_AdoptsMatchingTargetRefAfterCreateRace is the regression
// guard for the create-race happy path: when Create returns AlreadyExists but the
// stored CR MATCHES the request (e.g. two concurrent runs of the SAME download
// both creating the same correct de-<leaf>), the re-fetched CR is returned
// successfully — the post-Create guard adds no new behavior for a matching target.
func TestEnsureDataExport_AdoptsMatchingTargetRefAfterCreateRace(t *testing.T) {
	t.Parallel()

	const (
		namespace = "test-ns"
		leafName  = "race-matching-vs"
		ttl       = "1h"
	)

	deName := exporter.DataExportName(leafName)

	// Kind is deliberately empty to also exercise the pruned-CRD tolerance
	// (a GVR-based server drops the kind it does not understand) on this path.
	matchingRef := deapi.TargetRefSpec{
		Group:    aggapi.VolumeSnapshotGroup,
		Resource: aggapi.VolumeSnapshotResource,
		Kind:     "",
		Name:     leafName,
	}

	scheme := newDEScheme(t)

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
				winner := &deapi.DataExport{
					ObjectMeta: metav1.ObjectMeta{
						Name:      deName,
						Namespace: namespace,
						UID:       types.UID("uid-winner"),
					},
					Spec: deapi.DataexportSpec{TTL: ttl, TargetRef: matchingRef},
				}

				if err := cl.Create(ctx, winner); err != nil {
					return err
				}

				return apierrors.NewAlreadyExists(
					schema.GroupResource{Group: "storage-foundation.deckhouse.io", Resource: "dataexports"}, deName)
			},
		}).Build()

	got, err := exporter.EnsureDataExport(context.Background(), c, namespace,
		aggapi.VolumeSnapshotGroup, aggapi.VolumeSnapshotResource, aggapi.VolumeSnapshotKind, leafName, ttl)
	require.NoError(t, err, "a matching re-fetched CR must be returned after an AlreadyExists race")
	require.NotNil(t, got)
	assert.Equal(t, types.UID("uid-winner"), got.UID, "the matching re-fetched CR must be returned")
	assert.Equal(t, matchingRef, got.Spec.TargetRef)
}
