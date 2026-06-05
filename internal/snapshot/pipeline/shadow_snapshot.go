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

package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/source"
)

const (
	snapshotStorageGroup   = "snapshot.storage.k8s.io"
	snapshotStorageVersion = "v1"

	annotationStorageClass = "virtualization.deckhouse.io/storage-class-name"
	annotationVolumeMode   = "virtualization.deckhouse.io/volume-mode"

	shadowVSCPrefix = "snap-shadow-vsc-"
	shadowVSPrefix  = "snap-shadow-vs-"

	shadowWaitTimeout  = 5 * time.Minute
	shadowWaitInterval = 5 * time.Second
)

var (
	shadowVSCGVK = schema.GroupVersionKind{Group: snapshotStorageGroup, Version: snapshotStorageVersion, Kind: "VolumeSnapshotContent"}
	shadowVSGVK  = schema.GroupVersionKind{Group: snapshotStorageGroup, Version: snapshotStorageVersion, Kind: "VolumeSnapshot"}
)

// shadowHash returns an 8-byte hex string uniquely identifying a shadow object
// created for the given node/vsc pair.
func shadowHash(nodeID, origVSCName string) string {
	sum := sha256.Sum256([]byte(nodeID + "/" + origVSCName))
	return hex.EncodeToString(sum[:8])
}

// shadowVSCName returns the deterministic name for the shadow VolumeSnapshotContent.
func shadowVSCName(nodeID, origVSCName string) string {
	return shadowVSCPrefix + shadowHash(nodeID, origVSCName)
}

// shadowVSName returns the deterministic name for the shadow VolumeSnapshot.
func shadowVSName(nodeID, origVSCName string) string {
	return shadowVSPrefix + shadowHash(nodeID, origVSCName)
}

// readOrigVSC fetches the original (state-snapshotter-owned) VolumeSnapshotContent
// and verifies it has a non-empty status.snapshotHandle.
func readOrigVSC(ctx context.Context, c ctrlrtclient.Client, vscName string) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(shadowVSCGVK)

	if err := c.Get(ctx, ctrlrtclient.ObjectKey{Name: vscName}, obj); err != nil {
		return nil, fmt.Errorf("get VolumeSnapshotContent %s: %w", vscName, err)
	}

	handle, _, _ := unstructured.NestedString(obj.Object, "status", "snapshotHandle")
	if handle == "" {
		return nil, fmt.Errorf("VolumeSnapshotContent %s has no snapshotHandle in status (not yet captured)", vscName)
	}

	return obj, nil
}

// buildShadowVSC constructs the cluster-scoped pre-provisioned VolumeSnapshotContent
// that points at the same CSI snapshot handle as the original VSC.
//
// DeletionPolicy is always Retain so that deleting the shadow never triggers a
// CSI DeleteSnapshot on the real snapshot owned by state-snapshotter.
// The volumeSnapshotRef is pre-populated to match the shadow VS so that
// external-snapshotter can immediately bind the pair.
func buildShadowVSC(vscName, vsName, snapshotHandle, driver, vscClassName, vsNamespace string) *unstructured.Unstructured {
	spec := map[string]any{
		"deletionPolicy": "Retain",
		"driver":         driver,
		"source": map[string]any{
			"snapshotHandle": snapshotHandle,
		},
		"volumeSnapshotRef": map[string]any{
			"apiVersion": snapshotStorageGroup + "/" + snapshotStorageVersion,
			"kind":       "VolumeSnapshot",
			"name":       vsName,
			"namespace":  vsNamespace,
		},
	}

	if vscClassName != "" {
		spec["volumeSnapshotClassName"] = vscClassName
	}

	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": snapshotStorageGroup + "/" + snapshotStorageVersion,
			"kind":       "VolumeSnapshotContent",
			"metadata": map[string]any{
				"name": vscName,
				"labels": map[string]any{
					"app.kubernetes.io/managed-by": "d8-snapshot-download",
				},
			},
			"spec": spec,
		},
	}
}

// buildShadowVS constructs the namespaced VolumeSnapshot that references the shadow VSC.
//
// Two annotations carry the resolved StorageClass name and volume mode so that the
// DataExport controller (storage-volume-data-manager) can build the export PVC
// without a live source PVC.
func buildShadowVS(vsName, vscName, namespace, vscClassName, storageClass, volumeMode string) *unstructured.Unstructured {
	spec := map[string]any{
		"source": map[string]any{
			"volumeSnapshotContentName": vscName,
		},
	}

	if vscClassName != "" {
		spec["volumeSnapshotClassName"] = vscClassName
	}

	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": snapshotStorageGroup + "/" + snapshotStorageVersion,
			"kind":       "VolumeSnapshot",
			"metadata": map[string]any{
				"name":      vsName,
				"namespace": namespace,
				"labels": map[string]any{
					"app.kubernetes.io/managed-by": "d8-snapshot-download",
				},
				"annotations": map[string]any{
					annotationStorageClass: storageClass,
					annotationVolumeMode:   volumeMode,
				},
			},
			"spec": spec,
		},
	}
}

// resolveStorageClassForDriver lists all StorageClasses and returns the name of
// the first one whose provisioner field matches the given CSI driver.
func resolveStorageClassForDriver(ctx context.Context, c ctrlrtclient.Client, driver string) (string, error) {
	scList := &unstructured.UnstructuredList{}
	scList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "storage.k8s.io",
		Version: "v1",
		Kind:    "StorageClassList",
	})

	if err := c.List(ctx, scList); err != nil {
		return "", fmt.Errorf("list StorageClasses: %w", err)
	}

	for i := range scList.Items {
		provisioner, _, _ := unstructured.NestedString(scList.Items[i].Object, "provisioner")
		if provisioner == driver {
			return scList.Items[i].GetName(), nil
		}
	}

	return "", fmt.Errorf("no StorageClass found for CSI driver %q", driver)
}

// detectVolumeMode determines the volume mode to request for the export PVC.
// Priority:
//  1. origVSC.spec.sourceVolumeMode (set by external-snapshotter from the source PVC)
//  2. The original PVC's spec.volumeMode (if dr carries PVC coordinates and PVC still exists)
//  3. "Filesystem" (safe default matching DataExport's DefaultPVCVolumeMode)
func detectVolumeMode(ctx context.Context, c ctrlrtclient.Client, origVSC *unstructured.Unstructured, dr source.DataRef) string {
	if vm, _, _ := unstructured.NestedString(origVSC.Object, "spec", "sourceVolumeMode"); vm != "" {
		return vm
	}

	if dr.PVCName != "" && dr.PVCNamespace != "" {
		pvc := &unstructured.Unstructured{}
		pvc.SetGroupVersionKind(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"})

		if err := c.Get(ctx, ctrlrtclient.ObjectKey{Namespace: dr.PVCNamespace, Name: dr.PVCName}, pvc); err == nil {
			if vm, _, _ := unstructured.NestedString(pvc.Object, "spec", "volumeMode"); vm != "" {
				return vm
			}
		}
	}

	return "Filesystem"
}

// waitShadowVSReady polls the shadow VolumeSnapshot until it is bound and
// readyToUse=true, or until the deadline or context is cancelled.
func waitShadowVSReady(ctx context.Context, c ctrlrtclient.Client, name, namespace string, log *slog.Logger) error {
	deadline := time.Now().Add(shadowWaitTimeout)

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for shadow VolumeSnapshot %s/%s to become ready", namespace, name)
		}

		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(shadowVSGVK)

		err := c.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: name}, obj)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("get shadow VolumeSnapshot %s/%s: %w", namespace, name, err)
			}
		} else {
			readyToUse, _, _ := unstructured.NestedBool(obj.Object, "status", "readyToUse")
			boundVSC, _, _ := unstructured.NestedString(obj.Object, "status", "boundVolumeSnapshotContentName")

			if readyToUse && boundVSC != "" {
				log.Debug("shadow VolumeSnapshot is ready", "name", name, "namespace", namespace)
				return nil
			}

			log.Debug("waiting for shadow VolumeSnapshot", "name", name, "readyToUse", readyToUse, "boundVSC", boundVSC)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(shadowWaitInterval):
		}
	}
}

// createAndWaitShadowPair orchestrates the full shadow-pair lifecycle:
//
//  1. Read the original VSC (state-snapshotter owned) to get its snapshotHandle.
//  2. Resolve the StorageClass (provisioner matching the VSC driver) and volume mode.
//  3. Create the shadow VSC and shadow VS (idempotent: ignores AlreadyExists).
//  4. Wait until the shadow VS is bound and readyToUse.
//
// It always returns a non-nil cleanup function. The caller MUST defer cleanup()
// regardless of the error return value, because partial creation is possible.
// Cleanup deletes the shadow VS and VSC (Retain policy means no CSI DeleteSnapshot).
func createAndWaitShadowPair(
	ctx context.Context,
	c ctrlrtclient.Client,
	nodeID, namespace string,
	dr source.DataRef,
	log *slog.Logger,
) (string, func(), error) {
	noop := func() {}

	origVSC, err := readOrigVSC(ctx, c, dr.VSCName)
	if err != nil {
		return "", noop, err
	}

	snapshotHandle, _, _ := unstructured.NestedString(origVSC.Object, "status", "snapshotHandle")
	driver, _, _ := unstructured.NestedString(origVSC.Object, "spec", "driver")
	vscClassName, _, _ := unstructured.NestedString(origVSC.Object, "spec", "volumeSnapshotClassName")

	storageClass, err := resolveStorageClassForDriver(ctx, c, driver)
	if err != nil {
		return "", noop, err
	}

	volumeMode := detectVolumeMode(ctx, c, origVSC, dr)

	sVSCName := shadowVSCName(nodeID, dr.VSCName)
	sVSName := shadowVSName(nodeID, dr.VSCName)

	log.Debug("creating shadow VolumeSnapshotContent", "name", sVSCName, "snapshotHandle", snapshotHandle, "driver", driver)

	vscObj := buildShadowVSC(sVSCName, sVSName, snapshotHandle, driver, vscClassName, namespace)
	if createErr := c.Create(ctx, vscObj); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
		return "", noop, fmt.Errorf("create shadow VolumeSnapshotContent %s: %w", sVSCName, createErr)
	}

	log.Debug("creating shadow VolumeSnapshot", "name", sVSName, "storageClass", storageClass, "volumeMode", volumeMode)

	vsObj := buildShadowVS(sVSName, sVSCName, namespace, vscClassName, storageClass, volumeMode)
	if createErr := c.Create(ctx, vsObj); createErr != nil && !apierrors.IsAlreadyExists(createErr) {
		// VS creation failed; VSC was created (or pre-existed). Clean up VSC.
		deleteShadowObjects(context.Background(), c, sVSCName, "", namespace, log)
		return "", noop, fmt.Errorf("create shadow VolumeSnapshot %s: %w", sVSName, createErr)
	}

	cleanupFn := func() {
		deleteShadowObjects(context.Background(), c, sVSCName, sVSName, namespace, log)
	}

	if waitErr := waitShadowVSReady(ctx, c, sVSName, namespace, log); waitErr != nil {
		return "", cleanupFn, waitErr
	}

	// Some CSI drivers (e.g. local.csi.storage.deckhouse.io) do not return restoreSize
	// from GetSnapshotStatus for pre-provisioned snapshots, so external-snapshotter
	// leaves VS.status.restoreSize empty even after readyToUse=true.
	// DataExport requires a non-nil restoreSize; copy it from the original VSC if missing.
	if err := ensureShadowVSRestoreSize(ctx, c, sVSName, namespace, origVSC, log); err != nil {
		log.Warn("failed to propagate restoreSize to shadow VS", "name", sVSName, "err", err)
		// Non-fatal: DataExport controller will keep retrying.
	}

	return sVSName, cleanupFn, nil
}

// ensureShadowVSRestoreSize patches the shadow VS status.restoreSize from the
// original VSC if the external-snapshotter did not populate it.
func ensureShadowVSRestoreSize(
	ctx context.Context,
	c ctrlrtclient.Client,
	vsName, namespace string,
	origVSC *unstructured.Unstructured,
	log *slog.Logger,
) error {
	vs := &unstructured.Unstructured{}
	vs.SetGroupVersionKind(shadowVSGVK)

	if err := c.Get(ctx, ctrlrtclient.ObjectKey{Namespace: namespace, Name: vsName}, vs); err != nil {
		return fmt.Errorf("get shadow VS: %w", err)
	}

	if existing, _, _ := unstructured.NestedFieldNoCopy(vs.Object, "status", "restoreSize"); existing != nil {
		return nil
	}

	// restoreSize is not set; copy from origVSC.status.restoreSize.
	restoreSize, found, _ := unstructured.NestedFieldNoCopy(origVSC.Object, "status", "restoreSize")
	if !found || restoreSize == nil {
		log.Debug("original VSC has no restoreSize to propagate", "orig_vsc", origVSC.GetName())
		return nil
	}

	log.Debug("propagating restoreSize from original VSC to shadow VS", "vs_name", vsName, "restore_size", restoreSize)

	// Use the fetched vs as both the base and the mutation target so that
	// its metadata.resourceVersion is preserved in the PATCH request.
	// A fresh Unstructured without resourceVersion causes a validation error.
	base := vs.DeepCopy()
	_ = unstructured.SetNestedField(vs.Object, restoreSize, "status", "restoreSize")

	return c.Status().Patch(ctx, vs, ctrlrtclient.MergeFrom(base))
}

// deleteShadowObjects removes the shadow VolumeSnapshot and VolumeSnapshotContent,
// logging but not propagating errors (best-effort cleanup in defer blocks).
// VS is deleted before VSC to allow the controller to observe the VS deletion.
func deleteShadowObjects(ctx context.Context, c ctrlrtclient.Client, vscName, vsName, namespace string, log *slog.Logger) {
	vsObj := &unstructured.Unstructured{}
	vsObj.SetGroupVersionKind(shadowVSGVK)
	vsObj.SetName(vsName)
	vsObj.SetNamespace(namespace)

	if err := c.Delete(ctx, vsObj); err != nil && !apierrors.IsNotFound(err) {
		log.Warn("failed to delete shadow VolumeSnapshot", "name", vsName, "namespace", namespace, "err", err)
	}

	vscObj := &unstructured.Unstructured{}
	vscObj.SetGroupVersionKind(shadowVSCGVK)
	vscObj.SetName(vscName)

	if err := c.Delete(ctx, vscObj); err != nil && !apierrors.IsNotFound(err) {
		log.Warn("failed to delete shadow VolumeSnapshotContent", "name", vscName, "err", err)
	}
}
