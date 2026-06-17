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
	"log/slog"
	"testing"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
)

func newSnapScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()

	err := snapv1.AddToScheme(scheme)
	require.NoError(t, err)

	err = corev1.AddToScheme(scheme)
	require.NoError(t, err)

	return scheme
}

// makeRealVSC builds a pre-provisioned VolumeSnapshotContent (spec.source.snapshotHandle set).
// This is the format used when a snapshot already exists in the storage backend.
func makeRealVSC(name, driver, snapshotHandle string) *snapv1.VolumeSnapshotContent {
	handle := snapshotHandle

	return &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         driver,
			Source: snapv1.VolumeSnapshotContentSource{
				SnapshotHandle: &handle,
			},
			VolumeSnapshotRef: corev1.ObjectReference{
				Name:      "original-vs",
				Namespace: "original-ns",
			},
		},
	}
}

// makeDynamicVSC builds a dynamically-provisioned VolumeSnapshotContent.
// spec.source.volumeHandle holds the source PVC volume id; the CSI snapshotter
// sidecar populates status.snapshotHandle once the snapshot is ready.
func makeDynamicVSC(name, driver, volumeHandle, statusSnapshotHandle string) *snapv1.VolumeSnapshotContent {
	vh := volumeHandle
	sh := statusSnapshotHandle

	return &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         driver,
			Source: snapv1.VolumeSnapshotContentSource{
				VolumeHandle: &vh,
			},
			VolumeSnapshotRef: corev1.ObjectReference{
				Name:      "original-vs",
				Namespace: "original-ns",
			},
		},
		Status: &snapv1.VolumeSnapshotContentStatus{
			SnapshotHandle: &sh,
		},
	}
}

func TestShadowName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		artifactName string
		wantPrefix   string
		wantLen      int
	}{
		{
			artifactName: "snapcontent-00000000-0000-0000-0000-000000000000",
			wantPrefix:   "d8-ss-",
			wantLen:      22,
		},
		{
			artifactName: "short",
			wantPrefix:   "d8-ss-",
			wantLen:      22,
		},
		{
			// Very long name — result must still be ≤ 63 chars.
			artifactName: "snapcontent-" + string(make([]byte, 200)),
			wantPrefix:   "d8-ss-",
			wantLen:      22,
		},
	}

	for _, tc := range cases {
		name := exporter.ShadowName(tc.artifactName)
		assert.Equal(t, tc.wantLen, len(name), "artifact=%q got %q", tc.artifactName, name)
		assert.True(t, len(name) <= 63, "must be ≤ 63 chars, got %d: %s", len(name), name)
	}

	// Determinism.
	a := exporter.ShadowName("foo")
	b := exporter.ShadowName("foo")
	assert.Equal(t, a, b)

	// Two different artifacts produce different shadow names.
	assert.NotEqual(t, exporter.ShadowName("foo"), exporter.ShadowName("bar"))
}

func TestEnsureShadowPair_CreatesObjects(t *testing.T) {
	t.Parallel()

	const (
		artifactName   = "snapcontent-aabbccdd"
		namespace      = "test-ns"
		driver         = "csi.test.driver"
		snapshotHandle = "snap-handle-123"
	)

	scheme := newSnapScheme(t)
	realVSC := makeRealVSC(artifactName, driver, snapshotHandle)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(realVSC).Build()

	ctx := context.Background()

	shadowVS, err := exporter.EnsureShadowPair(ctx, c, namespace, artifactName, exporter.ShadowMeta{})
	require.NoError(t, err)
	require.NotNil(t, shadowVS)

	pairName := exporter.ShadowName(artifactName)

	// Shadow VS should be in namespace with correct source.
	assert.Equal(t, pairName, shadowVS.Name)
	assert.Equal(t, namespace, shadowVS.Namespace)
	require.NotNil(t, shadowVS.Spec.Source.VolumeSnapshotContentName)
	assert.Equal(t, pairName, *shadowVS.Spec.Source.VolumeSnapshotContentName)

	// Shadow VSC must exist with correct fields.
	var shadowVSC snapv1.VolumeSnapshotContent

	err = c.Get(ctx, types.NamespacedName{Name: pairName}, &shadowVSC)
	require.NoError(t, err)

	assert.Equal(t, snapv1.VolumeSnapshotContentRetain, shadowVSC.Spec.DeletionPolicy)
	assert.Equal(t, driver, shadowVSC.Spec.Driver)
	require.NotNil(t, shadowVSC.Spec.Source.SnapshotHandle)
	assert.Equal(t, snapshotHandle, *shadowVSC.Spec.Source.SnapshotHandle)
	assert.Equal(t, pairName, shadowVSC.Spec.VolumeSnapshotRef.Name)
	assert.Equal(t, namespace, shadowVSC.Spec.VolumeSnapshotRef.Namespace)
	assert.Equal(t, "VolumeSnapshot", shadowVSC.Spec.VolumeSnapshotRef.Kind)
}

func TestEnsureShadowPair_Idempotent(t *testing.T) {
	t.Parallel()

	const (
		artifactName   = "snapcontent-idempotent"
		namespace      = "test-ns"
		driver         = "csi.idempotent"
		snapshotHandle = "snap-handle-idempotent"
	)

	scheme := newSnapScheme(t)
	realVSC := makeRealVSC(artifactName, driver, snapshotHandle)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(realVSC).Build()

	ctx := context.Background()

	vs1, err := exporter.EnsureShadowPair(ctx, c, namespace, artifactName, exporter.ShadowMeta{})
	require.NoError(t, err)

	vs2, err := exporter.EnsureShadowPair(ctx, c, namespace, artifactName, exporter.ShadowMeta{})
	require.NoError(t, err)

	// Both calls should return an object with the same name.
	assert.Equal(t, vs1.Name, vs2.Name)

	// Exactly one shadow VSC should exist.
	var vscList snapv1.VolumeSnapshotContentList

	err = c.List(ctx, &vscList)
	require.NoError(t, err)

	shadowCount := 0

	for _, v := range vscList.Items {
		if v.Name == exporter.ShadowName(artifactName) {
			shadowCount++
		}
	}

	assert.Equal(t, 1, shadowCount, "expected exactly one shadow VSC")
}

func TestEnsureShadowPair_MissingArtifact(t *testing.T) {
	t.Parallel()

	scheme := newSnapScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	ctx := context.Background()

	_, err := exporter.EnsureShadowPair(ctx, c, "ns", "does-not-exist", exporter.ShadowMeta{})
	require.Error(t, err)
}

// TestEnsureShadowPair_NoSnapshotHandle verifies that a VSC with neither
// status.snapshotHandle nor spec.source.snapshotHandle yields a clear error
// mentioning "snapshotHandle".
func TestEnsureShadowPair_NoSnapshotHandle(t *testing.T) {
	t.Parallel()

	scheme := newSnapScheme(t)

	noHandleVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: "no-handle-vsc"},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "csi.test",
			Source:         snapv1.VolumeSnapshotContentSource{
				// Neither VolumeHandle nor SnapshotHandle; no Status yet.
			},
			VolumeSnapshotRef: corev1.ObjectReference{Name: "vs", Namespace: "ns"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(noHandleVSC).Build()

	ctx := context.Background()

	_, err := exporter.EnsureShadowPair(ctx, c, "ns", "no-handle-vsc", exporter.ShadowMeta{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "snapshotHandle")
}

// TestEnsureShadowPair_DynamicVSCUsesStatus verifies that a dynamically-provisioned
// VSC (spec.source.volumeHandle + status.snapshotHandle) produces a shadow VSC
// whose snapshotHandle comes from status, not spec.
func TestEnsureShadowPair_DynamicVSCUsesStatus(t *testing.T) {
	t.Parallel()

	const (
		artifactName         = "snapcontent-dynamic"
		namespace            = "test-ns"
		driver               = "csi.dynamic.driver"
		volumeHandle         = "pvc-00000000-0000-0000-0000-000000000001"
		statusSnapshotHandle = "snap-49913f45-dynamic"
	)

	scheme := newSnapScheme(t)
	dynamicVSC := makeDynamicVSC(artifactName, driver, volumeHandle, statusSnapshotHandle)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dynamicVSC).Build()

	ctx := context.Background()

	shadowVS, err := exporter.EnsureShadowPair(ctx, c, namespace, artifactName, exporter.ShadowMeta{})
	require.NoError(t, err)
	require.NotNil(t, shadowVS)

	pairName := exporter.ShadowName(artifactName)

	// Shadow VSC must carry the status.snapshotHandle, not the volumeHandle.
	var shadowVSC snapv1.VolumeSnapshotContent

	err = c.Get(ctx, types.NamespacedName{Name: pairName}, &shadowVSC)
	require.NoError(t, err)

	require.NotNil(t, shadowVSC.Spec.Source.SnapshotHandle)
	assert.Equal(t, statusSnapshotHandle, *shadowVSC.Spec.Source.SnapshotHandle,
		"shadow VSC must use status.snapshotHandle, not the volumeHandle")
	assert.Equal(t, driver, shadowVSC.Spec.Driver)
}

// TestEnsureShadowPair_PreProvisionedFallback verifies that a pre-provisioned VSC
// (spec.source.snapshotHandle set, no status written yet) still produces a correct
// shadow pair via the spec fallback path.
func TestEnsureShadowPair_PreProvisionedFallback(t *testing.T) {
	t.Parallel()

	const (
		artifactName   = "snapcontent-preprov"
		namespace      = "test-ns"
		driver         = "csi.preprov.driver"
		snapshotHandle = "snap-preprov-handle"
	)

	scheme := newSnapScheme(t)
	// VSC with spec.source.snapshotHandle only, no status.
	preProvVSC := makeRealVSC(artifactName, driver, snapshotHandle)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(preProvVSC).Build()

	ctx := context.Background()

	shadowVS, err := exporter.EnsureShadowPair(ctx, c, namespace, artifactName, exporter.ShadowMeta{})
	require.NoError(t, err)
	require.NotNil(t, shadowVS)

	pairName := exporter.ShadowName(artifactName)

	var shadowVSC snapv1.VolumeSnapshotContent

	err = c.Get(ctx, types.NamespacedName{Name: pairName}, &shadowVSC)
	require.NoError(t, err)

	require.NotNil(t, shadowVSC.Spec.Source.SnapshotHandle)
	assert.Equal(t, snapshotHandle, *shadowVSC.Spec.Source.SnapshotHandle)
	assert.Equal(t, driver, shadowVSC.Spec.Driver)
}

// TestEnsureShadowPair_SetsRestoreSize verifies that when the real VSC has
// status.restoreSize the shadow VSC's status.restoreSize is set to match.
func TestEnsureShadowPair_SetsRestoreSize(t *testing.T) {
	t.Parallel()

	const (
		artifactName   = "snapcontent-restoresize"
		namespace      = "test-ns"
		driver         = "csi.test"
		snapshotHandle = "snap-handle-rs"
		restoreSize    = int64(1073741824) // 1 GiB
	)

	scheme := newSnapScheme(t)
	realVSC := makeRealVSC(artifactName, driver, snapshotHandle)
	realVSC.Status = &snapv1.VolumeSnapshotContentStatus{
		SnapshotHandle: &[]string{snapshotHandle}[0],
		RestoreSize:    &[]int64{restoreSize}[0],
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(realVSC).
		WithStatusSubresource(&snapv1.VolumeSnapshotContent{}).
		Build()

	ctx := context.Background()

	_, err := exporter.EnsureShadowPair(ctx, c, namespace, artifactName, exporter.ShadowMeta{})
	require.NoError(t, err)

	pairName := exporter.ShadowName(artifactName)

	var shadowVSC snapv1.VolumeSnapshotContent

	err = c.Get(ctx, types.NamespacedName{Name: pairName}, &shadowVSC)
	require.NoError(t, err)

	require.NotNil(t, shadowVSC.Status, "shadow VSC status must be set")
	require.NotNil(t, shadowVSC.Status.RestoreSize, "shadow VSC status.restoreSize must be set")
	assert.Equal(t, restoreSize, *shadowVSC.Status.RestoreSize)
}

// TestEnsureShadowPair_SetsAnnotations verifies that the shadow VolumeSnapshot
// receives the storage-class and volume-mode annotations from ShadowMeta.
func TestEnsureShadowPair_SetsAnnotations(t *testing.T) {
	t.Parallel()

	const (
		artifactName   = "snapcontent-annotations"
		namespace      = "test-ns"
		driver         = "csi.test"
		snapshotHandle = "snap-handle-ann"
		storageClass   = "csi-ceph-rbd"
		volumeMode     = "Block"
	)

	scheme := newSnapScheme(t)
	realVSC := makeRealVSC(artifactName, driver, snapshotHandle)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(realVSC).Build()

	ctx := context.Background()

	meta := exporter.ShadowMeta{StorageClass: storageClass, VolumeMode: volumeMode}

	shadowVS, err := exporter.EnsureShadowPair(ctx, c, namespace, artifactName, meta)
	require.NoError(t, err)
	require.NotNil(t, shadowVS)

	require.NotNil(t, shadowVS.Annotations)
	assert.Equal(t, storageClass, shadowVS.Annotations[exporter.AnnotationStorageClassName],
		"shadow VS must carry storage-class annotation")
	assert.Equal(t, volumeMode, shadowVS.Annotations[exporter.AnnotationVolumeMode],
		"shadow VS must carry volume-mode annotation")
}

func TestCleanupShadowPair_DeletesObjects(t *testing.T) {
	t.Parallel()

	const (
		artifactName   = "snapcontent-cleanup"
		namespace      = "test-ns"
		driver         = "csi.cleanup"
		snapshotHandle = "snap-handle-cleanup"
	)

	scheme := newSnapScheme(t)
	realVSC := makeRealVSC(artifactName, driver, snapshotHandle)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(realVSC).Build()

	ctx := context.Background()

	_, err := exporter.EnsureShadowPair(ctx, c, namespace, artifactName, exporter.ShadowMeta{})
	require.NoError(t, err)

	err = exporter.CleanupShadowPair(ctx, c, namespace, artifactName)
	require.NoError(t, err)

	pairName := exporter.ShadowName(artifactName)

	// Shadow VS must be gone.
	var shadowVS snapv1.VolumeSnapshot

	err = c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pairName}, &shadowVS)
	assert.True(t, kubeerrors.IsNotFound(err), "shadow VS should be deleted")

	// Shadow VSC must be gone.
	var shadowVSC snapv1.VolumeSnapshotContent

	err = c.Get(ctx, types.NamespacedName{Name: pairName}, &shadowVSC)
	assert.True(t, kubeerrors.IsNotFound(err), "shadow VSC should be deleted")

	// Real VSC must still exist.
	var realVSCAfter snapv1.VolumeSnapshotContent

	err = c.Get(ctx, types.NamespacedName{Name: artifactName}, &realVSCAfter)
	require.NoError(t, err, "real VSC must not be deleted")
}

func TestCleanupShadowPair_Idempotent(t *testing.T) {
	t.Parallel()

	scheme := newSnapScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	ctx := context.Background()

	// Cleanup with nothing pre-created — should not error.
	err := exporter.CleanupShadowPair(ctx, c, "ns", "nonexistent-artifact")
	assert.NoError(t, err)
}

// TestWaitShadowVSReady_Ready verifies that WaitShadowVSReady returns nil
// immediately when the shadow VS already has readyToUse=true and a non-nil
// restoreSize.
func TestWaitShadowVSReady_Ready(t *testing.T) {
	t.Parallel()

	const (
		shadowName  = "d8-ss-ready"
		namespace   = "ns"
		realVSCName = "vsc-real"
	)

	scheme := newSnapScheme(t)

	readyToUse := true
	restoreSize := resource.MustParse("1Gi")
	shadowVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: shadowName, Namespace: namespace},
		Status: &snapv1.VolumeSnapshotStatus{
			ReadyToUse:  &readyToUse,
			RestoreSize: &restoreSize,
		},
	}
	realVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: realVSCName},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "csi.test",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &[]string{"h"}[0]},
			VolumeSnapshotRef: corev1.ObjectReference{
				Name: shadowName, Namespace: namespace,
			},
		},
	}
	shadowVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: shadowName},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "csi.test",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &[]string{"h"}[0]},
			VolumeSnapshotRef: corev1.ObjectReference{
				Name: shadowName, Namespace: namespace,
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shadowVS, realVSC, shadowVSC).
		WithStatusSubresource(&snapv1.VolumeSnapshotContent{}).
		Build()

	ctx := context.Background()

	err := exporter.WaitShadowVSReady(ctx, c, slog.Default(), namespace, shadowName, realVSCName)
	require.NoError(t, err, "should return immediately when shadow VS is already ready")
}

// TestWaitShadowVSReady_Timeout verifies that WaitShadowVSReady returns an
// error containing the diagnostic message and ErrShadowNotReady when the
// context expires before the shadow VS becomes ready.
func TestWaitShadowVSReady_Timeout(t *testing.T) {
	t.Parallel()

	const (
		shadowName  = "d8-ss-timeout"
		namespace   = "ns"
		realVSCName = "vsc-real-timeout"
	)

	scheme := newSnapScheme(t)

	// Shadow VS with no status — never ready.
	shadowVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: shadowName, Namespace: namespace},
	}
	realVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: realVSCName},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "csi.test",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &[]string{"h"}[0]},
			VolumeSnapshotRef: corev1.ObjectReference{
				Name: shadowName, Namespace: namespace,
			},
		},
	}
	shadowVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: shadowName},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "csi.test",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &[]string{"h"}[0]},
			VolumeSnapshotRef: corev1.ObjectReference{
				Name: shadowName, Namespace: namespace,
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shadowVS, realVSC, shadowVSC).
		Build()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := exporter.WaitShadowVSReady(ctx, c, slog.Default(), namespace, shadowName, realVSCName)
	require.Error(t, err)
	assert.ErrorIs(t, err, exporter.ErrShadowNotReady,
		"error must wrap ErrShadowNotReady")
	assert.Contains(t, err.Error(), shadowName,
		"diagnostic must name the shadow VS")
	assert.Contains(t, err.Error(), realVSCName,
		"diagnostic must name the real VSC")
}

// TestWaitShadowVSReady_SetsVSCRestoreSize verifies that WaitShadowVSReady
// re-asserts the restoreSize from the real VSC onto the shadow VSC during
// the first poll before returning (when the shadow VS is already ready).
func TestWaitShadowVSReady_SetsVSCRestoreSize(t *testing.T) {
	t.Parallel()

	const (
		shadowName   = "d8-ss-setrs"
		namespace    = "ns"
		realVSCName  = "vsc-real-setrs"
		restoreBytes = int64(2147483648) // 2 GiB
	)

	scheme := newSnapScheme(t)

	readyToUse := true
	vsRestoreSize := resource.MustParse("2Gi")
	shadowVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: shadowName, Namespace: namespace},
		Status: &snapv1.VolumeSnapshotStatus{
			ReadyToUse:  &readyToUse,
			RestoreSize: &vsRestoreSize,
		},
	}

	// Real VSC has status.restoreSize so the loop will re-assert it on shadow VSC.
	realVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: realVSCName},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "csi.test",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &[]string{"h"}[0]},
			VolumeSnapshotRef: corev1.ObjectReference{
				Name: shadowName, Namespace: namespace,
			},
		},
		Status: &snapv1.VolumeSnapshotContentStatus{
			RestoreSize: &[]int64{restoreBytes}[0],
		},
	}

	// Shadow VSC starts without status.restoreSize.
	shadowVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{Name: shadowName},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
			Driver:         "csi.test",
			Source:         snapv1.VolumeSnapshotContentSource{SnapshotHandle: &[]string{"h"}[0]},
			VolumeSnapshotRef: corev1.ObjectReference{
				Name: shadowName, Namespace: namespace,
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(shadowVS, realVSC, shadowVSC).
		WithStatusSubresource(&snapv1.VolumeSnapshotContent{}).
		Build()

	ctx := context.Background()

	err := exporter.WaitShadowVSReady(ctx, c, slog.Default(), namespace, shadowName, realVSCName)
	require.NoError(t, err, "should return immediately when shadow VS is already ready")

	// Shadow VSC must now have restoreSize set from the real VSC.
	var updatedShadowVSC snapv1.VolumeSnapshotContent

	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: shadowName}, &updatedShadowVSC))
	require.NotNil(t, updatedShadowVSC.Status, "shadow VSC status must be set")
	require.NotNil(t, updatedShadowVSC.Status.RestoreSize, "shadow VSC status.restoreSize must be set")
	assert.Equal(t, restoreBytes, *updatedShadowVSC.Status.RestoreSize)
}
