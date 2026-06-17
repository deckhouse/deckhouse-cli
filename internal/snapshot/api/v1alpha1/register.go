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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// API group and version constants for both snapshot CRD groups.
const (
	StorageGroup     = "storage.deckhouse.io"
	SnapshotterGroup = "state-snapshotter.deckhouse.io"
	Version          = "v1alpha1"
)

var (
	storageGV     = schema.GroupVersion{Group: StorageGroup, Version: Version}
	snapshotterGV = schema.GroupVersion{Group: SnapshotterGroup, Version: Version}
)

// AddToScheme registers all snapshot CRD types (both API groups) into the given scheme.
// Call once during controller-runtime client setup.
func AddToScheme(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(storageGV,
		&Snapshot{},
		&SnapshotList{},
		&SnapshotContent{},
		&SnapshotContentList{},
	)
	metav1.AddToGroupVersion(scheme, storageGV)

	scheme.AddKnownTypes(snapshotterGV,
		&ManifestCheckpoint{},
		&ManifestCheckpointList{},
		&ManifestCheckpointContentChunk{},
		&ManifestCheckpointContentChunkList{},
	)
	metav1.AddToGroupVersion(scheme, snapshotterGV)

	return nil
}
