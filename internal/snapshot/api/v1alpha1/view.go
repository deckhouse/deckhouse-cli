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

// SnapshotView mirrors the server's stable SnapshotView projection (state-snapshotter
// usecase/restore/view.go), served by the aggregated /view subresource and persisted into a bundle's
// view.json. Unlike the opaque index, the view IS meant to be parsed by clients: the CLI renders the
// `d8 snapshot list` tree from this stable shape and never parses index.json.
type SnapshotView struct {
	Version string           `json:"version"`
	Root    SnapshotViewNode `json:"root"`
}

// SnapshotViewNode is one node of the view tree.
type SnapshotViewNode struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	// HasData reports whether this node carries a restorable volume.
	HasData bool `json:"hasData"`
	// VolumeMode is the data volume mode (Block or Filesystem); empty for dataless nodes.
	VolumeMode string `json:"volumeMode,omitempty"`
	// SizeBytes is the volume size (from the source VolumeSnapshotContent restoreSize); 0 if unknown.
	SizeBytes int64              `json:"sizeBytes,omitempty"`
	Children  []SnapshotViewNode `json:"children,omitempty"`
}
