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

package aggapi

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// testMapper resolves the kinds used in the path-building tests to their plurals.
func testMapper() meta.RESTMapper {
	m := meta.NewDefaultRESTMapper(nil)
	m.Add(schema.GroupVersionKind{Group: StorageGroup, Version: "v1alpha1", Kind: "Snapshot"}, meta.RESTScopeNamespace)
	m.Add(schema.GroupVersionKind{Group: "demo.deckhouse.io", Version: "v1alpha1", Kind: "VirtualDiskSnapshot"}, meta.RESTScopeNamespace)

	return m
}

func TestIsVolumeSnapshotLeaf(t *testing.T) {
	cases := []struct {
		name string
		ref  NodeRef
		want bool
	}{
		{
			name: "csi volume snapshot leaf",
			ref:  NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot"},
			want: true,
		},
		{
			name: "core snapshot",
			ref:  NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot"},
			want: false,
		},
		{
			name: "domain snapshot",
			ref:  NodeRef{APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "VirtualDiskSnapshot"},
			want: false,
		},
		{
			name: "wrong kind in vs group",
			ref:  NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshotContent"},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.ref.IsVolumeSnapshotLeaf(); got != tc.want {
				t.Errorf("IsVolumeSnapshotLeaf() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestDownloadPath verifies that manifests-download is always served by the core
// subresources group for non-leaf nodes, and by the VS-connector group for CSI
// VolumeSnapshot leaves.
func TestDownloadPath(t *testing.T) {
	c := NewClient(nil, testMapper())

	cases := []struct {
		name string
		ref  NodeRef
		want string
	}{
		{
			name: "core snapshot",
			ref:  NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot", Name: "my-snap", Namespace: "ns"},
			want: "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/snapshots/my-snap/manifests-download",
		},
		{
			name: "domain snapshot still uses core group",
			ref:  NodeRef{APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "VirtualDiskSnapshot", Name: "vds-1", Namespace: "ns"},
			want: "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/virtualdisksnapshots/vds-1/manifests-download",
		},
		{
			name: "csi volume snapshot leaf uses vs-connector group",
			ref:  NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "vs-1", Namespace: "ns"},
			want: "/apis/subresources.snapshot.storage.k8s.io/v1/namespaces/ns/volumesnapshots/vs-1/manifests-download",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.downloadPath(tc.ref)
			if err != nil {
				t.Fatalf("downloadPath: %v", err)
			}

			if got != tc.want {
				t.Errorf("downloadPath:\n got  %q\n want %q", got, tc.want)
			}
		})
	}
}

// TestSubresourcePath verifies that restore/upload subresources are served by the
// node's OWN subresource group (core group for core Snapshot, domain-prefixed group
// for domain CRs, VS-connector group for CSI leaves).
func TestSubresourcePath(t *testing.T) {
	c := NewClient(nil, testMapper())

	cases := []struct {
		name string
		ref  NodeRef
		sub  string
		want string
	}{
		{
			name: "core snapshot restore",
			ref:  NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot", Name: "my-snap", Namespace: "ns"},
			sub:  SubManifestsRestore,
			want: "/apis/subresources.state-snapshotter.deckhouse.io/v1alpha1/namespaces/ns/snapshots/my-snap/manifests-with-data-restoration",
		},
		{
			name: "domain snapshot upload uses domain-prefixed group",
			ref:  NodeRef{APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "VirtualDiskSnapshot", Name: "vds-1", Namespace: "ns"},
			sub:  SubManifestsUpload,
			want: "/apis/subresources.demo.deckhouse.io/v1alpha1/namespaces/ns/virtualdisksnapshots/vds-1/manifests-and-children-refs-upload",
		},
		{
			name: "csi volume snapshot leaf restore uses vs-connector group",
			ref:  NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "vs-1", Namespace: "ns"},
			sub:  SubManifestsRestore,
			want: "/apis/subresources.snapshot.storage.k8s.io/v1/namespaces/ns/volumesnapshots/vs-1/manifests-with-data-restoration",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := c.subresourcePath(tc.ref, tc.sub)
			if err != nil {
				t.Fatalf("subresourcePath: %v", err)
			}

			if got != tc.want {
				t.Errorf("subresourcePath:\n got  %q\n want %q", got, tc.want)
			}
		})
	}
}

// TestSubresourceGroupVersion verifies the group/version selection for restore/upload.
func TestSubresourceGroupVersion(t *testing.T) {
	cases := []struct {
		name        string
		ref         NodeRef
		wantGroup   string
		wantVersion string
	}{
		{
			name:        "core snapshot",
			ref:         NodeRef{APIVersion: StorageGroup + "/v1alpha1", Kind: "Snapshot"},
			wantGroup:   CoreSubresourcesGroup,
			wantVersion: CoreSubresourcesVersion,
		},
		{
			name:        "domain snapshot",
			ref:         NodeRef{APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "VirtualDiskSnapshot"},
			wantGroup:   "subresources.demo.deckhouse.io",
			wantVersion: "v1alpha1",
		},
		{
			name:        "csi volume snapshot leaf",
			ref:         NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot"},
			wantGroup:   VSConnectorGroup,
			wantVersion: VSConnectorVersion,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			group, version, err := subresourceGroupVersion(tc.ref)
			if err != nil {
				t.Fatalf("subresourceGroupVersion: %v", err)
			}

			if group != tc.wantGroup {
				t.Errorf("group: got %q, want %q", group, tc.wantGroup)
			}

			if version != tc.wantVersion {
				t.Errorf("version: got %q, want %q", version, tc.wantVersion)
			}
		})
	}
}

// TestResourceFor_NoMapper verifies a clear error when a non-leaf ref must be resolved
// without a configured RESTMapper.
func TestResourceFor_NoMapper(t *testing.T) {
	c := NewClient(nil, nil)

	if _, err := c.resourceFor(NodeRef{APIVersion: "demo.deckhouse.io/v1alpha1", Kind: "VirtualDiskSnapshot"}); err == nil {
		t.Fatal("expected error when no RESTMapper is configured, got nil")
	}

	// CSI VolumeSnapshot leaves use a fixed plural and need no mapper.
	res, err := c.resourceFor(NodeRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot"})
	if err != nil {
		t.Fatalf("resourceFor(VolumeSnapshot leaf): %v", err)
	}

	if res != VolumeSnapshotResource {
		t.Errorf("resourceFor(VolumeSnapshot leaf): got %q, want %q", res, VolumeSnapshotResource)
	}
}
