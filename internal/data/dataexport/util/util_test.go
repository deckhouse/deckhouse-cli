package util

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dataio "github.com/deckhouse/deckhouse-cli/internal/data"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
)

// readyCond builds a Ready condition with the given status/reason/message.
func readyCond(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:    "Ready",
		Status:  status,
		Reason:  reason,
		Message: message,
	}
}

func TestCreateDataExporterIfNeeded(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	ctx := context.Background()
	logger := slog.Default()

	tests := []struct {
		name          string
		input         string
		expectName    string
		expectKind    string
		expectCreated bool
	}{
		{
			name:          "PVC short alias",
			input:         "pvc/myvol",
			expectName:    "de-pvc-myvol",
			expectKind:    dataio.PersistentVolumeClaimKind,
			expectCreated: true,
		},
		{
			name:          "PVC long alias",
			input:         "persistentvolumeclaim/myvol",
			expectName:    "de-pvc-myvol",
			expectKind:    dataio.PersistentVolumeClaimKind,
			expectCreated: true,
		},
		{
			name:          "VolumeSnapshot short alias",
			input:         "vs/snap1",
			expectName:    "de-vs-snap1",
			expectKind:    dataio.VolumeSnapshotKind,
			expectCreated: true,
		},
		{
			name:          "VolumeSnapshot long alias",
			input:         "volumesnapshot/snap1",
			expectName:    "de-vs-snap1",
			expectKind:    dataio.VolumeSnapshotKind,
			expectCreated: true,
		},
		{
			name:          "Existing DataExport name",
			input:         "my-export",
			expectName:    "my-export",
			expectCreated: false,
		},
		{
			name:          "VirtualDisk short alias",
			input:         "vd/mydisk",
			expectName:    "de-vd-mydisk",
			expectKind:    dataio.VirtualDiskKind,
			expectCreated: true,
		},
		{
			name:          "VirtualDisk long alias",
			input:         "virtualdisk/mydisk",
			expectName:    "de-vd-mydisk",
			expectKind:    dataio.VirtualDiskKind,
			expectCreated: true,
		},
		{
			name:          "VirtualDiskSnapshot short alias",
			input:         "vds/snap2",
			expectName:    "de-vds-snap2",
			expectKind:    dataio.VirtualDiskSnapshotKind,
			expectCreated: true,
		},
		{
			name:          "VirtualDiskSnapshot long alias",
			input:         "virtualdisksnapshot/snap2",
			expectName:    "de-vds-snap2",
			expectKind:    dataio.VirtualDiskSnapshotKind,
			expectCreated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()

			returnedName, err := CreateDataExporterIfNeeded(ctx, logger, tt.input, "test-ns", false, "2m", c)
			require.NoError(t, err)
			require.Equal(t, tt.expectName, returnedName)

			var de v1alpha1.DataExport
			getErr := c.Get(ctx, ctrlclient.ObjectKey{Name: tt.expectName, Namespace: "test-ns"}, &de)
			if tt.expectCreated {
				require.NoError(t, getErr)
				require.Equal(t, tt.expectKind, de.Spec.TargetRef.Kind)
				require.Equal(t, "2m", de.Spec.TTL)
			} else {
				require.True(t, apierrors.IsNotFound(getErr))
			}
		})
	}
}

func TestGetDataExportWithRestart_ErrorMessages(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	ctx := context.Background()
	logger := slog.Default()

	tests := []struct {
		name               string
		de                 *v1alpha1.DataExport
		wantErr            bool
		wantErrContains    string
		wantErrNotContains string
	}{
		{
			// Regression: before the fix, the condition message was overwritten by "empty PublicURL"
			// because the else-if branch triggered whenever returnErr != nil.
			name: "PublishFailed condition: error shows condition message, not 'empty PublicURL'",
			de: &v1alpha1.DataExport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-de", Namespace: "test-ns"},
				Spec:       v1alpha1.DataexportSpec{Publish: true},
				Status: v1alpha1.DataExportStatus{
					URL: "https://10.0.0.1:8085/",
					Conditions: []metav1.Condition{
						readyCond(metav1.ConditionFalse, "PublishFailed",
							"failed to ensure public URL: ingressPublicDomain not defined"),
					},
				},
			},
			wantErr:            true,
			wantErrContains:    "ingressPublicDomain not defined",
			wantErrNotContains: "empty PublicURL",
		},
		{
			// Regression: before the fix, "Started (Pending)" was overwritten by "empty PublicURL"
			// because publish=true and PublicURL="" triggered the else-if even when Ready=False.
			name: "Pending condition with publish=true: error shows Pending reason, not 'empty PublicURL'",
			de: &v1alpha1.DataExport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-de", Namespace: "test-ns"},
				Spec:       v1alpha1.DataexportSpec{Publish: true},
				Status: v1alpha1.DataExportStatus{
					Conditions: []metav1.Condition{
						readyCond(metav1.ConditionFalse, "Pending", "Started"),
					},
				},
			},
			wantErr:            true,
			wantErrContains:    "Pending",
			wantErrNotContains: "empty PublicURL",
		},
		{
			// Ready=True but PublicURL not yet generated — race between controllers.
			name: "Ready=True, publish=true, PublicURL empty: error says waiting for public URL",
			de: &v1alpha1.DataExport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-de", Namespace: "test-ns"},
				Spec:       v1alpha1.DataexportSpec{Publish: true},
				Status: v1alpha1.DataExportStatus{
					URL: "https://10.0.0.1:8085/",
					Conditions: []metav1.Condition{
						readyCond(metav1.ConditionTrue, "PodReady", "Pod is ready and export started"),
					},
				},
			},
			wantErr:            true,
			wantErrContains:    "waiting for public URL",
			wantErrNotContains: "empty PublicURL",
		},
		{
			// URL empty and publish=true: "waiting for internal URL" takes priority over public URL check.
			name: "URL empty with publish=true: error is about waiting for internal URL, not PublicURL",
			de: &v1alpha1.DataExport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-de", Namespace: "test-ns"},
				Spec:       v1alpha1.DataexportSpec{Publish: true},
				Status: v1alpha1.DataExportStatus{
					Conditions: []metav1.Condition{
						readyCond(metav1.ConditionTrue, "PodReady", "Pod is ready and export started"),
					},
				},
			},
			wantErr:            true,
			wantErrContains:    "waiting for internal URL",
			wantErrNotContains: "empty PublicURL",
		},
		{
			// Timeout: after maxRetryAttempts=0 the error should include the d8 k recommendation.
			name: "timeout: error contains d8 k recommendation",
			de: &v1alpha1.DataExport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-de", Namespace: "test-ns"},
				Spec:       v1alpha1.DataexportSpec{Publish: false},
				Status: v1alpha1.DataExportStatus{
					Conditions: []metav1.Condition{
						readyCond(metav1.ConditionFalse, "Pending", "Started"),
					},
				},
			},
			wantErr:         true,
			wantErrContains: "d8 k -n",
		},
		{
			// Happy path: both internal URL and public URL are present.
			name: "Ready=True, publish=true, both URLs set: success",
			de: &v1alpha1.DataExport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-de", Namespace: "test-ns"},
				Spec:       v1alpha1.DataexportSpec{Publish: true},
				Status: v1alpha1.DataExportStatus{
					URL:       "https://10.0.0.1:8085/",
					PublicURL: "https://api.example.com/ns/pvc/myvol/",
					Conditions: []metav1.Condition{
						readyCond(metav1.ConditionTrue, "PodReady", "Pod is ready and export started"),
					},
				},
			},
			wantErr: false,
		},
		{
			// Happy path without publish: only internal URL is needed.
			name: "Ready=True, publish=false, URL set: success",
			de: &v1alpha1.DataExport{
				ObjectMeta: metav1.ObjectMeta{Name: "test-de", Namespace: "test-ns"},
				Spec:       v1alpha1.DataexportSpec{Publish: false},
				Status: v1alpha1.DataExportStatus{
					URL: "https://10.0.0.1:8085/",
					Conditions: []metav1.Condition{
						readyCond(metav1.ConditionTrue, "PodReady", "Pod is ready and export started"),
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Override maxRetryAttempts so the function returns after the very first
			// iteration instead of looping 60+ times.
			orig := maxRetryAttempts
			maxRetryAttempts = -1
			t.Cleanup(func() { maxRetryAttempts = orig })

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.de).
				Build()

			_, err := GetDataExportWithRestart(ctx, "test-de", "test-ns", c, logger)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				if tt.wantErrNotContains != "" {
					assert.NotContains(t, err.Error(), tt.wantErrNotContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEnsureDataExportPublish(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	ctx := context.Background()

	newDE := func(publish bool) *v1alpha1.DataExport {
		return &v1alpha1.DataExport{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-de",
				Namespace: "test-ns",
			},
			Spec: v1alpha1.DataexportSpec{
				Publish: publish,
			},
		}
	}

	tests := []struct {
		name          string
		deObj         *v1alpha1.DataExport
		publish       bool
		wantErr       bool
		wantPublishIn bool // expected Spec.Publish in store after call
	}{
		{
			name:          "publish=false, object has Publish=false: no patch",
			deObj:         newDE(false),
			publish:       false,
			wantPublishIn: false,
		},
		{
			name:          "publish=false, object has Publish=true: no downgrade",
			deObj:         newDE(true),
			publish:       false,
			wantPublishIn: true,
		},
		{
			name:          "publish=true, object already has Publish=true: no patch",
			deObj:         newDE(true),
			publish:       true,
			wantPublishIn: true,
		},
		{
			name:          "publish=true, object has Publish=false: patched to true",
			deObj:         newDE(false),
			publish:       true,
			wantPublishIn: true,
		},
		{
			name:    "nil deObj with publish=true: returns error",
			deObj:   nil,
			publish: true,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.deObj != nil {
				builder = builder.WithObjects(tt.deObj)
			}
			c := builder.Build()

			err := EnsureDataExportPublish(ctx, tt.deObj, tt.publish, c)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.deObj == nil {
				return
			}

			// Verify store state
			var stored v1alpha1.DataExport
			require.NoError(t, c.Get(ctx, ctrlclient.ObjectKey{Name: "test-de", Namespace: "test-ns"}, &stored))
			assert.Equal(t, tt.wantPublishIn, stored.Spec.Publish)
		})
	}
}
