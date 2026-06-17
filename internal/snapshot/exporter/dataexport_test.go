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
		shadowVSName string
		wantName     string
	}{
		{shadowVSName: "d8-ss-aabb1122ccdd3344", wantName: "de-d8-ss-aabb1122ccdd3344"},
		{shadowVSName: "my-shadow", wantName: "de-my-shadow"},
		{shadowVSName: "", wantName: "de-"},
	}

	for _, tc := range cases {
		got := exporter.DataExportName(tc.shadowVSName)
		assert.Equal(t, tc.wantName, got, "shadowVSName=%q", tc.shadowVSName)
	}
}

func TestEnsureDataExport_Creates(t *testing.T) {
	t.Parallel()

	const (
		namespace    = "test-ns"
		shadowVSName = "d8-ss-deadbeef12345678"
		ttl          = "3h"
	)

	scheme := newDEScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	ctx := context.Background()

	de, err := exporter.EnsureDataExport(ctx, c, namespace, shadowVSName, ttl)
	require.NoError(t, err)
	require.NotNil(t, de)

	assert.Equal(t, exporter.DataExportName(shadowVSName), de.Name)
	assert.Equal(t, namespace, de.Namespace)
	assert.Equal(t, "VolumeSnapshot", de.Spec.TargetRef.Kind)
	assert.Equal(t, shadowVSName, de.Spec.TargetRef.Name)
	assert.Equal(t, ttl, de.Spec.TTL)
}

func TestEnsureDataExport_DefaultTTL(t *testing.T) {
	t.Parallel()

	scheme := newDEScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	de, err := exporter.EnsureDataExport(context.Background(), c, "ns", "shadow-vs", "")
	require.NoError(t, err)

	// Empty TTL should be replaced by the built-in default (non-empty).
	assert.NotEmpty(t, de.Spec.TTL)
}

func TestEnsureDataExport_Idempotent(t *testing.T) {
	t.Parallel()

	const (
		namespace    = "test-ns"
		shadowVSName = "d8-ss-idempotent"
	)

	scheme := newDEScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	ctx := context.Background()

	de1, err := exporter.EnsureDataExport(ctx, c, namespace, shadowVSName, "1h")
	require.NoError(t, err)

	de2, err := exporter.EnsureDataExport(ctx, c, namespace, shadowVSName, "1h")
	require.NoError(t, err)

	assert.Equal(t, de1.Name, de2.Name)
	assert.Equal(t, de1.ResourceVersion, de2.ResourceVersion)
}

// makeReadyDE returns a DataExport pre-populated with the Ready condition and a URL
// so that WaitReady exits on its first iteration without sleeping.
func makeReadyDE(namespace, shadowVSName, baseURL, volumeMode string) *deapi.DataExport {
	deName := exporter.DataExportName(shadowVSName)

	return &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
		},
		Spec: deapi.DataexportSpec{
			TTL: "2h",
			TargetRef: deapi.TargetRefSpec{
				Kind: "VolumeSnapshot",
				Name: shadowVSName,
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
		namespace    = "test-ns"
		shadowVSName = "d8-ss-ready"
		baseURL      = "https://exporter.example.com"
		volumeMode   = "Block"
	)

	scheme := newDEScheme(t)
	de := makeReadyDE(namespace, shadowVSName, baseURL, volumeMode)
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
		namespace    = "test-ns"
		shadowVSName = "d8-ss-expired"
	)

	deName := exporter.DataExportName(shadowVSName)

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
		namespace    = "test-ns"
		shadowVSName = "d8-ss-never-ready"
	)

	deName := exporter.DataExportName(shadowVSName)

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

func TestWaitReady_ContextCancelled(t *testing.T) {
	t.Parallel()

	const (
		namespace    = "test-ns"
		shadowVSName = "d8-ss-pending"
	)

	deName := exporter.DataExportName(shadowVSName)

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
		namespace    = "test-ns"
		shadowVSName = "d8-ss-releaseme"
	)

	deName := exporter.DataExportName(shadowVSName)

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
