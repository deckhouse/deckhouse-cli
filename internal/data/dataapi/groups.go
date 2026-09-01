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

// Package dataapi resolves which of the two API groups a cluster serves DataExport and
// DataImport under, and which of them the calling user is actually authorized to use.
//
// Two different modules produce the same pair of CRDs:
//
//   - storage-foundation serves them under FoundationGroup. It supersedes the older module
//     and is what a cluster with storage-foundation enabled exposes.
//   - storage-volume-data-manager serves them under LegacyGroup. Editions that ship that
//     module alone expose this group and nothing else.
//
// A single d8 binary has to work against both, so the group is a runtime decision rather than a
// compile-time constant. The decision cannot be made from the module list: `d8 data` is run by
// ordinary users, who are not authorized to read ModuleConfig, and whose RBAC may cover only one
// of the two groups even when the cluster serves both.
package dataapi

import "k8s.io/apimachinery/pkg/runtime/schema"

const (
	// FoundationGroup is the API group under which storage-foundation serves DataExport and
	// DataImport. Preferred whenever the cluster serves it and the user is authorized for it.
	FoundationGroup = "storage-foundation.deckhouse.io"

	// LegacyGroup is the API group under which storage-volume-data-manager serves DataExport
	// and DataImport. Used when the cluster does not serve FoundationGroup, or serves it but
	// denies the user access to it.
	LegacyGroup = "storage.deckhouse.io"

	// Version is the version both groups serve these CRDs under.
	Version = "v1alpha1"
)

// Resource plurals this package can resolve a group for. Resolution is per resource rather
// than per group because a cluster is free to serve one CRD of the pair and not the other.
const (
	ResourceDataExports = "dataexports"
	ResourceDataImports = "dataimports"
)

// Module names, used only in operator-facing messages that name what to enable.
const (
	foundationModule = "storage-foundation"
	legacyModule     = "storage-volume-data-manager"
)

var (
	// FoundationGroupVersion is the storage-foundation GroupVersion of DataExport/DataImport.
	FoundationGroupVersion = schema.GroupVersion{Group: FoundationGroup, Version: Version}

	// LegacyGroupVersion is the storage-volume-data-manager GroupVersion of the same pair.
	LegacyGroupVersion = schema.GroupVersion{Group: LegacyGroup, Version: Version}
)

// Backend is a resolved answer: the GroupVersion to address the CRD through, plus the module
// that serves it for messages.
type Backend struct {
	GroupVersion schema.GroupVersion
	Module       string
}

// Legacy reports whether the resolved backend is storage-volume-data-manager's group. Callers
// that build a request body differing between the two producers branch on this; callers that
// only address the object by GroupVersion do not need it.
func (b Backend) Legacy() bool {
	return b.GroupVersion.Group == LegacyGroup
}

// String renders the backend as its GroupVersion, e.g. "storage-foundation.deckhouse.io/v1alpha1".
func (b Backend) String() string {
	return b.GroupVersion.String()
}
