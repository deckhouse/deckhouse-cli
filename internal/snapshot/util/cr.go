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

package util

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// DefaultNamespace is used when -n/--namespace is not provided.
const DefaultNamespace = "default"

// DefaultPollInterval / DefaultTimeout govern the wait loops.
const (
	DefaultPollInterval = 3 * time.Second
	DefaultTimeout      = 30 * time.Minute
)

// ResolveNamespace reads the -n/--namespace flag, defaulting to DefaultNamespace.
func ResolveNamespace(cmd *cobra.Command) string {
	ns, _ := cmd.Flags().GetString("namespace")
	if ns == "" {
		return DefaultNamespace
	}
	return ns
}

// NewClients builds the kube clients for a snapshot command: a SafeClient (data-pod HTTP + CA), a
// controller-runtime typed client (CR CRUD) and an APIClient (aggregated subresources).
func NewClients(cmd *cobra.Command) (*safeClient.SafeClient, ctrlclient.Client, *APIClient, error) {
	sc, err := safeClient.NewSafeClient(cmd.PersistentFlags())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build kube client: %w", err)
	}
	rt, err := sc.NewRTClient(v1alpha1.AddToScheme)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build typed client: %w", err)
	}
	api, err := NewAPIClient(sc)
	if err != nil {
		return nil, nil, nil, err
	}
	return sc, rt, api, nil
}

func condTrue(conditions []metav1.Condition, condType string) bool {
	return apimeta.IsStatusConditionTrue(conditions, condType)
}

// ---- SnapshotExport ----

// EnsureSnapshotExport creates the SnapshotExport if absent (AlreadyExists tolerated).
func EnsureSnapshotExport(ctx context.Context, rt ctrlclient.Client, ns, name, snapshotName string, publish bool) error {
	obj := &v1alpha1.SnapshotExport{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.SchemeGroupVersion.String(), Kind: "SnapshotExport"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.SnapshotExportSpec{
			SnapshotRef: v1alpha1.LocalSnapshotRef{Name: snapshotName},
			Publish:     publish,
		},
	}
	if err := rt.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create SnapshotExport %s/%s: %w", ns, name, err)
	}
	return nil
}

// GetSnapshotExport fetches a SnapshotExport.
func GetSnapshotExport(ctx context.Context, rt ctrlclient.Client, ns, name string) (*v1alpha1.SnapshotExport, error) {
	obj := &v1alpha1.SnapshotExport{}
	if err := rt.Get(ctx, ctrlclient.ObjectKey{Namespace: ns, Name: name}, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// DeleteSnapshotExport deletes a SnapshotExport. It returns existed=false (NotFound tolerated) so the
// caller can report accurately instead of claiming a delete that did nothing.
func DeleteSnapshotExport(ctx context.Context, rt ctrlclient.Client, ns, name string) (existed bool, err error) {
	obj := &v1alpha1.SnapshotExport{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	derr := rt.Delete(ctx, obj)
	if derr == nil {
		return true, nil
	}
	if apierrors.IsNotFound(derr) {
		return false, nil
	}
	return false, fmt.Errorf("delete SnapshotExport %s/%s: %w", ns, name, derr)
}

// WaitSnapshotExportReady polls until the SnapshotExport is Ready (index, manifests and all data
// endpoints published).
func WaitSnapshotExportReady(ctx context.Context, rt ctrlclient.Client, ns, name string, timeout time.Duration, progress func(string)) (*v1alpha1.SnapshotExport, error) {
	deadline := time.Now().Add(timeout)
	for {
		se, err := GetSnapshotExport(ctx, rt, ns, name)
		if err != nil {
			return nil, err
		}
		if condTrue(se.Status.Conditions, v1alpha1.SnapshotExportConditionReady) {
			return se, nil
		}
		if time.Now().After(deadline) {
			return se, fmt.Errorf("timed out waiting for SnapshotExport %s/%s to become Ready", ns, name)
		}
		if progress != nil {
			progress(fmt.Sprintf("waiting for SnapshotExport %s/%s to become Ready...", ns, name))
		}
		if err := sleep(ctx, DefaultPollInterval); err != nil {
			return nil, err
		}
	}
}

// ---- SnapshotImport ----

// EnsureSnapshotImport creates the SnapshotImport if absent. It returns created=true when a new object
// was created, and created=false when an object of the same name already existed (its spec is left
// untouched — a SnapshotImport spec is immutable once the controller starts driving it). Callers must
// report this accurately so the user is not misled into believing a new --snapshot/--storage-class-map
// took effect on a pre-existing import.
func EnsureSnapshotImport(ctx context.Context, rt ctrlclient.Client, ns, name, snapshotName string, scMapping map[string]string, publish bool) (created bool, err error) {
	obj := &v1alpha1.SnapshotImport{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.SchemeGroupVersion.String(), Kind: "SnapshotImport"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.SnapshotImportSpec{
			SnapshotName:        snapshotName,
			StorageClassMapping: scMapping,
			Publish:             publish,
		},
	}
	cerr := rt.Create(ctx, obj)
	if cerr == nil {
		return true, nil
	}
	if apierrors.IsAlreadyExists(cerr) {
		return false, nil
	}
	return false, fmt.Errorf("create SnapshotImport %s/%s: %w", ns, name, cerr)
}

// GetSnapshotImport fetches a SnapshotImport.
func GetSnapshotImport(ctx context.Context, rt ctrlclient.Client, ns, name string) (*v1alpha1.SnapshotImport, error) {
	obj := &v1alpha1.SnapshotImport{}
	if err := rt.Get(ctx, ctrlclient.ObjectKey{Namespace: ns, Name: name}, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// DeleteSnapshotImport deletes a SnapshotImport. It returns existed=false (NotFound tolerated) so the
// caller can report accurately instead of claiming a delete that did nothing.
func DeleteSnapshotImport(ctx context.Context, rt ctrlclient.Client, ns, name string) (existed bool, err error) {
	obj := &v1alpha1.SnapshotImport{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	derr := rt.Delete(ctx, obj)
	if derr == nil {
		return true, nil
	}
	if apierrors.IsNotFound(derr) {
		return false, nil
	}
	return false, fmt.Errorf("delete SnapshotImport %s/%s: %w", ns, name, derr)
}

// WaitImportUploadURLs polls until the index/manifests upload endpoints are published (Stage 0).
func WaitImportUploadURLs(ctx context.Context, rt ctrlclient.Client, ns, name string, timeout time.Duration, progress func(string)) (*v1alpha1.SnapshotImport, error) {
	deadline := time.Now().Add(timeout)
	for {
		imp, err := GetSnapshotImport(ctx, rt, ns, name)
		if err != nil {
			return nil, err
		}
		if imp.Status.IndexUploadURL != "" && imp.Status.ManifestsUploadURL != "" {
			return imp, nil
		}
		if time.Now().After(deadline) {
			return imp, fmt.Errorf("timed out waiting for SnapshotImport %s/%s upload endpoints", ns, name)
		}
		if progress != nil {
			progress(fmt.Sprintf("waiting for upload endpoints on %s/%s...", ns, name))
		}
		if err := sleep(ctx, DefaultPollInterval); err != nil {
			return nil, err
		}
	}
}

// WaitImportDataUploadsReady polls until every data node has an upload endpoint ready. It fails fast
// when the controller reports a StorageClass mapping is required (the user must re-create the import
// with spec.storageClassMapping).
func WaitImportDataUploadsReady(ctx context.Context, rt ctrlclient.Client, ns, name string, timeout time.Duration, progress func(string)) (*v1alpha1.SnapshotImport, error) {
	deadline := time.Now().Add(timeout)
	for {
		imp, err := GetSnapshotImport(ctx, rt, ns, name)
		if err != nil {
			return nil, err
		}
		if c := apimeta.FindStatusCondition(imp.Status.Conditions, v1alpha1.SnapshotImportConditionUploadsPrepared); c != nil && c.Status == metav1.ConditionFalse {
			switch c.Reason {
			case v1alpha1.SnapshotImportReasonStorageClassMappingRequired:
				return imp, fmt.Errorf("storage class mapping required: %s; delete and re-create the import with --storage-class-map src=dst ('d8 snapshot import delete %s')", c.Message, name)
			case v1alpha1.SnapshotImportReasonDataSizeUnknown:
				return imp, fmt.Errorf("a data node has unknown volume size: %s; the export bundle must be regenerated", c.Message)
			}
		}
		// Gate readiness on the authoritative UploadsPrepared=True condition (set atomically once every
		// endpoint is ready), not on the data-entry slice alone — the controller may publish entries
		// incrementally, so an early "all current entries ready" check can return a partial set.
		if condTrue(imp.Status.Conditions, v1alpha1.SnapshotImportConditionUploadsPrepared) &&
			(len(imp.Status.DataSnapshots) == 0 || allUploadReady(imp.Status.DataSnapshots)) {
			return imp, nil
		}
		if time.Now().After(deadline) {
			return imp, fmt.Errorf("timed out waiting for SnapshotImport %s/%s data upload endpoints", ns, name)
		}
		if progress != nil {
			progress(fmt.Sprintf("waiting for data upload endpoints on %s/%s...", ns, name))
		}
		if err := sleep(ctx, DefaultPollInterval); err != nil {
			return nil, err
		}
	}
}

// WaitImportReady polls until the SnapshotImport has pre-provisioned the whole tree (Ready).
func WaitImportReady(ctx context.Context, rt ctrlclient.Client, ns, name string, timeout time.Duration, progress func(string)) (*v1alpha1.SnapshotImport, error) {
	deadline := time.Now().Add(timeout)
	for {
		imp, err := GetSnapshotImport(ctx, rt, ns, name)
		if err != nil {
			return nil, err
		}
		if condTrue(imp.Status.Conditions, v1alpha1.SnapshotImportConditionReady) {
			return imp, nil
		}
		if time.Now().After(deadline) {
			return imp, fmt.Errorf("timed out waiting for SnapshotImport %s/%s to become Ready", ns, name)
		}
		if progress != nil {
			progress(fmt.Sprintf("waiting for SnapshotImport %s/%s to become Ready...", ns, name))
		}
		if err := sleep(ctx, DefaultPollInterval); err != nil {
			return nil, err
		}
	}
}

func allUploadReady(entries []v1alpha1.SnapshotImportDataEntry) bool {
	for i := range entries {
		if !entries[i].UploadReady {
			return false
		}
	}
	return true
}

// ExportDataEntryByID indexes export data entries by snapshot id.
func ExportDataEntryByID(entries []v1alpha1.SnapshotExportDataEntry) map[string]v1alpha1.SnapshotExportDataEntry {
	m := make(map[string]v1alpha1.SnapshotExportDataEntry, len(entries))
	for _, e := range entries {
		m[e.SnapshotID] = e
	}
	return m
}

// ImportDataEntryByID indexes import data entries by snapshot id.
func ImportDataEntryByID(entries []v1alpha1.SnapshotImportDataEntry) map[string]v1alpha1.SnapshotImportDataEntry {
	m := make(map[string]v1alpha1.SnapshotImportDataEntry, len(entries))
	for _, e := range entries {
		m[e.SnapshotID] = e
	}
	return m
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
