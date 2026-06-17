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
	"fmt"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ArtifactKindVolumeSnapshotContent is the expected Artifact.Kind value in a
// SnapshotDataBinding when the data path used the CSI snapshot driver.
// Callers should verify this before calling EnsureShadowPair.
const ArtifactKindVolumeSnapshotContent = "VolumeSnapshotContent"

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
// The real VolumeSnapshotContent is fetched to copy its driver and
// snapshotHandle. The shadow VSC uses deletionPolicy=Retain so that cleanup
// removes only the shadow objects; the underlying storage snapshot is
// preserved.
//
// Returns the shadow VolumeSnapshot (newly created or pre-existing).
func EnsureShadowPair(
	ctx context.Context,
	c client.Client,
	namespace string,
	artifactName string,
) (*snapv1.VolumeSnapshot, error) {
	realVSC := &snapv1.VolumeSnapshotContent{}

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

	return ensureShadowVS(ctx, c, namespace, pairName)
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

func ensureShadowVSC(
	ctx context.Context,
	c client.Client,
	name string,
	namespace string,
	driver string,
	snapshotHandle string,
) error {
	existing := &snapv1.VolumeSnapshotContent{}

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
) (*snapv1.VolumeSnapshot, error) {
	existing := &snapv1.VolumeSnapshot{}

	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, existing)
	if err == nil {
		return existing, nil
	}

	if !kubeerrors.IsNotFound(err) {
		return nil, fmt.Errorf("get shadow VolumeSnapshot %q: %w", name, err)
	}

	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
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
