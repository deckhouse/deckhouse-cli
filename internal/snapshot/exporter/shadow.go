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

package exporter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ArtifactKindVolumeSnapshotContent is the expected Artifact.Kind value in a
// SnapshotDataBinding when the data path used the CSI snapshot driver.
// Callers should verify this before calling EnsureShadowPair.
const ArtifactKindVolumeSnapshotContent = "VolumeSnapshotContent"

// Annotation keys placed on the shadow VolumeSnapshot so that the data-export
// controller can derive the storageClass and volumeMode for the export PVC.
// These must match the constants in storage-volume-data-manager
// (VirtualizationAnnotationStorageClassNameKey / VirtualizationAnnotationVolumeModeKey).
const (
	AnnotationStorageClassName = "virtualization.deckhouse.io/storage-class-name"
	AnnotationVolumeMode       = "virtualization.deckhouse.io/volume-mode"
)

// ShadowMeta carries the storage metadata sourced from the original PVC that
// backed the CSI snapshot. It is injected as annotations on the shadow
// VolumeSnapshot so that the data-export controller can provision the export PVC
// with the correct storageClass and volumeMode.
type ShadowMeta struct {
	StorageClass string
	VolumeMode   string
}

// ErrShadowNotReady is returned by WaitShadowVSReady when the context deadline
// expires before the shadow VolumeSnapshot reports readyToUse=true with a
// non-nil restoreSize.
var ErrShadowNotReady = errors.New("shadow VS did not become ready with a non-nil restoreSize")

// shadowVSPollInterval is how often WaitShadowVSReady re-checks the shadow VS.
const shadowVSPollInterval = 3 * time.Second

// ShadowName returns a deterministic Kubernetes-safe name for the shadow pair
// (both the cluster-scoped shadow VolumeSnapshotContent and the namespaced
// shadow VolumeSnapshot share this name). The name is derived from the
// artifact VolumeSnapshotContent name so resume is idempotent.
// Result is always ≤ 22 chars, well within the 63-char DNS label limit.
func ShadowName(artifactName string) string {
	h := sha256.Sum256([]byte(artifactName))

	return "d8-ss-" + hex.EncodeToString(h[:8])
}

// EnsureShadowPair idempotently creates a shadow VolumeSnapshotContent +
// shadow VolumeSnapshot pair so the real CSI snapshot (identified by its
// VolumeSnapshotContent named artifactName) is accessible as a namespaced
// VolumeSnapshot in namespace. The shadow VS can then be referenced in a
// DataExport.
//
// The real VolumeSnapshotContent is fetched to copy its driver, snapshotHandle,
// and restoreSize. The shadow VSC uses deletionPolicy=Retain so that cleanup
// removes only the shadow objects; the underlying storage snapshot is
// preserved.
//
// meta carries the storageClass and volumeMode of the original source PVC and
// is injected as annotations on the shadow VS so that the data-export controller
// can resolve the export PVC without needing the live source PVC.
//
// Returns the shadow VolumeSnapshot (newly created or pre-existing).
func EnsureShadowPair(
	ctx context.Context,
	c client.Client,
	namespace string,
	artifactName string,
	meta ShadowMeta,
) (*snapv1.VolumeSnapshot, error) {
	realVSC := new(snapv1.VolumeSnapshotContent)

	if err := c.Get(ctx, types.NamespacedName{Name: artifactName}, realVSC); err != nil {
		return nil, fmt.Errorf("get source VolumeSnapshotContent %q: %w", artifactName, err)
	}

	snapshotHandle, err := resolveSnapshotHandle(realVSC)
	if err != nil {
		return nil, fmt.Errorf("source VolumeSnapshotContent %q: %w", artifactName, err)
	}

	pairName := ShadowName(artifactName)

	if err := ensureShadowVSC(ctx, c, pairName, namespace, realVSC.Spec.Driver, snapshotHandle); err != nil {
		return nil, err
	}

	// Propagate restoreSize from the real VSC to the shadow VSC so the
	// snapshot-controller can sync it to the shadow VS status.restoreSize.
	// The data-export controller requires a non-nil VS.status.restoreSize.
	if realVSC.Status != nil && realVSC.Status.RestoreSize != nil {
		if err := setVSCRestoreSize(ctx, c, pairName, *realVSC.Status.RestoreSize); err != nil {
			return nil, fmt.Errorf("set restoreSize on shadow VSC %q: %w", pairName, err)
		}
	}

	return ensureShadowVS(ctx, c, namespace, pairName, meta)
}

// resolveSnapshotHandle returns the CSI snapshot identifier for the given
// VolumeSnapshotContent, trying the authoritative location first.
//
// Resolution order:
//  1. status.snapshotHandle — populated by the CSI snapshotter sidecar for
//     both dynamically-provisioned and bound pre-provisioned content. This is
//     the authoritative source once the snapshot is ready.
//  2. spec.source.snapshotHandle — set only for statically pre-provisioned
//     content (when the snapshot already exists in the storage backend). Used
//     as a fallback for VSCs whose status has not been written yet.
//
// If neither field is set the snapshot is not yet ready and an error is returned.
func resolveSnapshotHandle(vsc *snapv1.VolumeSnapshotContent) (string, error) {
	if vsc.Status != nil && vsc.Status.SnapshotHandle != nil && *vsc.Status.SnapshotHandle != "" {
		return *vsc.Status.SnapshotHandle, nil
	}

	if vsc.Spec.Source.SnapshotHandle != nil && *vsc.Spec.Source.SnapshotHandle != "" {
		return *vsc.Spec.Source.SnapshotHandle, nil
	}

	return "", fmt.Errorf("no snapshotHandle available (snapshot may not be ready yet)")
}

// setVSCRestoreSize updates the shadow VSC's status.restoreSize to restoreSize
// (bytes) when it is not yet set, wrapped in RetryOnConflict.
func setVSCRestoreSize(ctx context.Context, c client.Client, name string, restoreSize int64) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		shadowVSC := new(snapv1.VolumeSnapshotContent)

		if err := c.Get(ctx, types.NamespacedName{Name: name}, shadowVSC); err != nil {
			return err
		}

		if shadowVSC.Status != nil && shadowVSC.Status.RestoreSize != nil {
			return nil
		}

		if shadowVSC.Status == nil {
			shadowVSC.Status = &snapv1.VolumeSnapshotContentStatus{}
		}

		shadowVSC.Status.RestoreSize = &restoreSize

		return c.Status().Update(ctx, shadowVSC)
	})
}

// WaitShadowVSReady polls the shadow VolumeSnapshot (shadowName in namespace)
// until both status.readyToUse==true and status.restoreSize!=nil.
//
// Inside each poll iteration it re-reads the real VolumeSnapshotContent
// (realVSCName) and re-asserts its restoreSize onto the shadow VSC via
// setVSCRestoreSize. This counteracts the CSI snapshotter sidecar, which on
// pre-provisioned content reconciles the VSC status from the driver and may
// clear a value that was set manually.
//
// The data-export controller hard-requires a non-nil VS.status.restoreSize to
// provision the export PVC. Only after this function returns should a
// DataExport be created for the shadow VS.
//
// The caller must bound the wait via ctx. On expiry the function returns
// ErrShadowNotReady wrapped with a diagnostic that names the shadow VS/VSC and
// the real VSC restoreSize (if known).
func WaitShadowVSReady(
	ctx context.Context,
	c client.Client,
	log *slog.Logger,
	namespace,
	shadowName,
	realVSCName string,
) error {
	var lastRealRestoreSize *int64

	for {
		realVSC := new(snapv1.VolumeSnapshotContent)
		if getErr := c.Get(ctx, types.NamespacedName{Name: realVSCName}, realVSC); getErr == nil {
			if realVSC.Status != nil && realVSC.Status.RestoreSize != nil {
				lastRealRestoreSize = realVSC.Status.RestoreSize

				if assertErr := setVSCRestoreSize(ctx, c, shadowName, *realVSC.Status.RestoreSize); assertErr != nil {
					log.Warn("re-assert shadow VSC restoreSize failed (will retry)",
						slog.String("shadow_vsc", shadowName),
						slog.String("error", assertErr.Error()))
				}
			}
		}

		shadowVS := new(snapv1.VolumeSnapshot)
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: shadowName}, shadowVS); err != nil {
			return fmt.Errorf("get shadow VolumeSnapshot %s/%s: %w", namespace, shadowName, err)
		}

		if isVSReadyWithSize(shadowVS) {
			return nil
		}

		log.Info("waiting for shadow VS readyToUse and restoreSize",
			slog.String("namespace", namespace),
			slog.String("shadow", shadowName))

		select {
		case <-ctx.Done():
			var realRestoreStr string

			if lastRealRestoreSize != nil {
				realRestoreStr = fmt.Sprintf("%d bytes", *lastRealRestoreSize)
			} else {
				realRestoreStr = "unknown (real VSC has no status.restoreSize)"
			}

			return fmt.Errorf(
				"shadow VS %s/%s did not become ready within the deadline "+
					"(real VSC %s restoreSize=%s); "+
					"the data-export controller requires VS.status.restoreSize != nil to provision the export PVC; "+
					"verify that the CSI snapshotter sidecar has reconciled the snapshot status: %w",
				namespace, shadowName, realVSCName, realRestoreStr, ErrShadowNotReady)
		case <-time.After(shadowVSPollInterval):
		}
	}
}

// isVSReadyWithSize reports whether vs has readyToUse=true and a non-nil
// restoreSize, which are the preconditions the data-export controller requires
// before it will accept a VolumeSnapshot as an export source.
func isVSReadyWithSize(vs *snapv1.VolumeSnapshot) bool {
	return vs.Status != nil &&
		vs.Status.ReadyToUse != nil && *vs.Status.ReadyToUse &&
		vs.Status.RestoreSize != nil
}

func ensureShadowVSC(
	ctx context.Context,
	c client.Client,
	name string,
	namespace string,
	driver string,
	snapshotHandle string,
) error {
	existing := new(snapv1.VolumeSnapshotContent)

	err := c.Get(ctx, types.NamespacedName{Name: name}, existing)
	if err == nil {
		return nil
	}

	if !kubeerrors.IsNotFound(err) {
		return fmt.Errorf("get shadow VolumeSnapshotContent %q: %w", name, err)
	}

	vsc := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: snapv1.VolumeSnapshotContentSpec{
			DeletionPolicy: snapv1.VolumeSnapshotContentRetain,
			Driver:         driver,
			Source: snapv1.VolumeSnapshotContentSource{
				SnapshotHandle: &snapshotHandle,
			},
			VolumeSnapshotRef: corev1.ObjectReference{
				APIVersion: "snapshot.storage.k8s.io/v1",
				Kind:       "VolumeSnapshot",
				Name:       name,
				Namespace:  namespace,
			},
		},
	}

	if err := c.Create(ctx, vsc); err != nil {
		return fmt.Errorf("create shadow VolumeSnapshotContent %q: %w", name, err)
	}

	return nil
}

func ensureShadowVS(
	ctx context.Context,
	c client.Client,
	namespace string,
	name string,
	meta ShadowMeta,
) (*snapv1.VolumeSnapshot, error) {
	existing := new(snapv1.VolumeSnapshot)

	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, existing)
	if err == nil {
		return existing, nil
	}

	if !kubeerrors.IsNotFound(err) {
		return nil, fmt.Errorf("get shadow VolumeSnapshot %q: %w", name, err)
	}

	annotations := map[string]string{}
	if meta.StorageClass != "" {
		annotations[AnnotationStorageClassName] = meta.StorageClass
	}

	if meta.VolumeMode != "" {
		annotations[AnnotationVolumeMode] = meta.VolumeMode
	}

	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotations,
		},
		Spec: snapv1.VolumeSnapshotSpec{
			Source: snapv1.VolumeSnapshotSource{
				VolumeSnapshotContentName: &name,
			},
		},
	}

	if err := c.Create(ctx, vs); err != nil {
		return nil, fmt.Errorf("create shadow VolumeSnapshot %q: %w", name, err)
	}

	return vs, nil
}

// CleanupShadowPair deletes the shadow VolumeSnapshot and shadow
// VolumeSnapshotContent that were created for artifactName. The real
// VolumeSnapshotContent (and its underlying storage snapshot) is not touched:
// the shadow VSC's deletionPolicy=Retain ensures the CSI driver does not
// delete the real snapshot when the shadow VSC is removed.
// NotFound errors are silently ignored so cleanup is idempotent.
func CleanupShadowPair(
	ctx context.Context,
	c client.Client,
	namespace string,
	artifactName string,
) error {
	pairName := ShadowName(artifactName)

	shadowVS := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pairName,
			Namespace: namespace,
		},
	}

	if err := c.Delete(ctx, shadowVS); err != nil && !kubeerrors.IsNotFound(err) {
		return fmt.Errorf("delete shadow VolumeSnapshot %q: %w", pairName, err)
	}

	shadowVSC := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name: pairName,
		},
	}

	if err := c.Delete(ctx, shadowVSC); err != nil && !kubeerrors.IsNotFound(err) {
		return fmt.Errorf("delete shadow VolumeSnapshotContent %q: %w", pairName, err)
	}

	return nil
}
