package util

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/dataexport/api/v1alpha1"

	"log/slog"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

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
			expectKind:    PersistentVolumeClaimKind,
			expectCreated: true,
		},
		{
			name:          "PVC long alias",
			input:         "persistentvolumeclaim/myvol",
			expectName:    "de-pvc-myvol",
			expectKind:    PersistentVolumeClaimKind,
			expectCreated: true,
		},
		{
			name:          "VolumeSnapshot short alias",
			input:         "vs/snap1",
			expectName:    "de-vs-snap1",
			expectKind:    VolumeSnapshotKind,
			expectCreated: true,
		},
		{
			name:          "VolumeSnapshot long alias",
			input:         "volumesnapshot/snap1",
			expectName:    "de-vs-snap1",
			expectKind:    VolumeSnapshotKind,
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
			expectKind:    VirtualDiskKind,
			expectCreated: true,
		},
		{
			name:          "VirtualDisk long alias",
			input:         "virtualdisk/mydisk",
			expectName:    "de-vd-mydisk",
			expectKind:    VirtualDiskKind,
			expectCreated: true,
		},
		{
			name:          "VirtualDiskSnapshot short alias",
			input:         "vds/snap2",
			expectName:    "de-vds-snap2",
			expectKind:    VirtualDiskSnapshotKind,
			expectCreated: true,
		},
		{
			name:          "VirtualDiskSnapshot long alias",
			input:         "virtualdisksnapshot/snap2",
			expectName:    "de-vds-snap2",
			expectKind:    VirtualDiskSnapshotKind,
			expectCreated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).Build()

			returnedName, kind, err := CreateDataExporterIfNeeded(ctx, logger, tt.input, "test-ns", false, "2m", c)
			require.NoError(t, err)
			require.Equal(t, tt.expectName, returnedName)
			// kind may be empty when DataExport already exists
			if tt.expectKind != "" {
				require.Equal(t, tt.expectKind, kind)
			}

			var de v1alpha1.DataExport
			getErr := c.Get(ctx, ctrlclient.ObjectKey{Name: tt.expectName, Namespace: "test-ns"}, &de)
			if tt.expectCreated {
				require.NoError(t, getErr)
				require.Equal(t, tt.expectKind, de.Spec.TargetRef.Kind)
				require.Equal(t, "2m", de.Spec.Ttl)
			} else {
				require.True(t, apierrors.IsNotFound(getErr))
			}
		})
	}
}
