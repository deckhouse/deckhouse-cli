/*
Copyright 2023 Flant JSC

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataapi"
)

const (
	// APIGroup is the group storage-foundation serves DataExport under. It is the default this
	// package registers, not the only group these types are ever addressed through: a cluster
	// running storage-volume-data-manager instead serves the same kind under
	// dataapi.LegacyGroup, and callers reach it with AddToSchemeFor.
	APIGroup   = dataapi.FoundationGroup
	APIVersion = dataapi.Version
)

// SchemeGroupVersion is group version used to register these objects
var (
	SchemeGroupVersion = dataapi.FoundationGroupVersion
	SchemeBuilder      = runtime.NewSchemeBuilder(AddKnownTypes)
	AddToScheme        = SchemeBuilder.AddToScheme
)

// AddKnownTypes registers the DataExport types under SchemeGroupVersion (storage-foundation).
// Callers that resolved the served group at runtime use AddToSchemeFor instead.
func AddKnownTypes(scheme *runtime.Scheme) error {
	return addKnownTypesFor(SchemeGroupVersion, scheme)
}

// AddToSchemeFor returns a scheme builder that registers the DataExport types under gv.
//
// The Go types are shared by both producers because the wire shapes agree on everything the CLI
// sends and reads; only the group differs, and TargetRefSpec.Group — which the older CRD has no
// property for — is pruned by that CRD's structural schema rather than rejected. Registering one
// scheme per resolved group keeps exactly one GroupVersionKind mapped to each type, which is what
// the controller-runtime client requires to address the object at all.
func AddToSchemeFor(gv schema.GroupVersion) func(*runtime.Scheme) error {
	return func(scheme *runtime.Scheme) error {
		return addKnownTypesFor(gv, scheme)
	}
}

func addKnownTypesFor(gv schema.GroupVersion, scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(gv,
		&DataExport{},
		&DataExportList{},
	)
	metav1.AddToGroupVersion(scheme, gv)

	return nil
}
