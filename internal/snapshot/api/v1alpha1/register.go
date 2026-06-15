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

// Package v1alpha1 contains the minimal locally-copied SnapshotExport / SnapshotImport
// API types the d8 snapshot CLI needs to create and watch those CRs. It mirrors the
// authoritative types in state-snapshotter (api/storage/v1alpha1); only the fields the
// thin client reads/writes are kept.
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// APIGroup is the storage group that hosts SnapshotExport / SnapshotImport.
	APIGroup = "storage.deckhouse.io"
	// APIVersion is the served version of the snapshot export/import CRs.
	APIVersion = "v1alpha1"
)

// SchemeGroupVersion is the group version used to register these objects.
var (
	SchemeGroupVersion = schema.GroupVersion{
		Group:   APIGroup,
		Version: APIVersion,
	}
	SchemeBuilder = runtime.NewSchemeBuilder(AddKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// AddKnownTypes registers the snapshot export/import types with the given scheme.
func AddKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&SnapshotExport{},
		&SnapshotExportList{},
		&SnapshotImport{},
		&SnapshotImportList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)

	return nil
}
