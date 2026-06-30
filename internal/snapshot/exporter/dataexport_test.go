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
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
)

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

	err := exporter.ReleaseDataExport(ctx, c, namespace, deName)
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

	err := exporter.ReleaseDataExport(context.Background(), c, "ns", "nonexistent-de")
	assert.NoError(t, err)
}
