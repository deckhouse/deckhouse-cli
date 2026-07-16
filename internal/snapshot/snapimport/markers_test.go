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

package snapimport

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// assertImportMarker verifies the unified spec.mode: Import marker is present.
func assertImportMarker(t *testing.T, obj *unstructured.Unstructured) {
	t.Helper()

	mode, found, err := unstructured.NestedString(obj.Object, "spec", "mode")
	if err != nil {
		t.Fatalf("read spec.mode: %v", err)
	}

	if !found {
		t.Fatalf("expected spec.mode marker to be set")
	}

	if mode != "Import" {
		t.Errorf("spec.mode = %q, want Import", mode)
	}
}

func TestImportMarkerCR_Snapshot(t *testing.T) {
	node := PlannedNode{APIVersion: "state-snapshotter.deckhouse.io/v1alpha1", Kind: "Snapshot", Name: "root"}

	obj, err := importMarkerCR(node, "ns")
	if err != nil {
		t.Fatalf("importMarkerCR: %v", err)
	}

	if obj.GetNamespace() != "ns" || obj.GetName() != "root" {
		t.Errorf("unexpected metadata: ns=%q name=%q", obj.GetNamespace(), obj.GetName())
	}

	assertImportMarker(t, obj)
}

func TestImportMarkerCR_VolumeSnapshot(t *testing.T) {
	node := PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "pvc-1"}

	obj, err := importMarkerCR(node, "ns")
	if err != nil {
		t.Fatalf("importMarkerCR: %v", err)
	}

	// CSI VolumeSnapshot leaves use the same unified marker as every other node; the leaf no
	// longer names its DataImport (matched server-side by targetRef reverse-lookup instead).
	assertImportMarker(t, obj)
}

func TestImportMarkerCR_DomainAggregator(t *testing.T) {
	// A DemoVirtualMachineSnapshot that references child snapshots but carries no own volume
	// data is a domain aggregator. It is reconstructed server-side as a NON-ROOT node, so it
	// gets the same unified spec.mode: Import marker as every other node (no error); the
	// genericbinder later aggregates its children's contents into the aggregator's content.
	node := PlannedNode{
		APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualMachineSnapshot",
		Name:       "vm-1",
		Children: []ChildRef{{
			APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDiskSnapshot",
			Name:       "dvd-1",
		}},
	}

	obj, err := importMarkerCR(node, "ns")
	if err != nil {
		t.Fatalf("importMarkerCR for domain aggregator: %v", err)
	}

	if obj.GetNamespace() != "ns" || obj.GetName() != "vm-1" {
		t.Errorf("unexpected metadata: ns=%q name=%q", obj.GetNamespace(), obj.GetName())
	}

	assertImportMarker(t, obj)
}

func TestImportMarkerCR_ManifestOnlyDomainNode(t *testing.T) {
	// A DemoVirtualMachineSnapshot with neither volume data nor child snapshots is a
	// manifest-only domain node: import-equivalent to a structural Snapshot, so it gets the
	// unified spec.mode: Import marker (no error).
	node := PlannedNode{APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1", Kind: "DemoVirtualMachineSnapshot", Name: "vm-1"}

	obj, err := importMarkerCR(node, "ns")
	if err != nil {
		t.Fatalf("importMarkerCR for manifest-only domain node: %v", err)
	}

	if obj.GetNamespace() != "ns" || obj.GetName() != "vm-1" {
		t.Errorf("unexpected metadata: ns=%q name=%q", obj.GetNamespace(), obj.GetName())
	}

	assertImportMarker(t, obj)
}

func TestImportMarkerCR_DomainDataLeaf(t *testing.T) {
	node := PlannedNode{
		APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1",
		Kind:       "DemoVirtualDiskSnapshot",
		Name:       "dvd-snap-1",
		DataFile:   "/archive/snapshots/demovirtualdisksnapshot_disk-a/data.bin",
	}

	obj, err := importMarkerCR(node, "ns")
	if err != nil {
		t.Fatalf("importMarkerCR: %v", err)
	}

	if obj.GetNamespace() != "ns" || obj.GetName() != "dvd-snap-1" {
		t.Errorf("unexpected metadata: ns=%q name=%q", obj.GetNamespace(), obj.GetName())
	}

	// Domain data leaves use the unified marker too; the captured-source identity is no longer
	// mirrored onto the marker (the DataImport carries it via targetRef).
	assertImportMarker(t, obj)
}

// TestPlannedNode_IsDomainAggregator verifies the classification that gates the standalone
// --node root restriction: only a domain node with no own volume data but WITH child refs is
// an aggregator. Aggregators are still importable as non-root nodes within a tree.
func TestPlannedNode_IsDomainAggregator(t *testing.T) {
	cases := []struct {
		name string
		node PlannedNode
		want bool
	}{
		{"core snapshot", PlannedNode{APIVersion: "state-snapshotter.deckhouse.io/v1alpha1", Kind: "Snapshot"}, false},
		{"csi volume snapshot", PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot"}, false},
		// A domain disk snapshot WITH volume data is a domain data leaf, not an aggregator.
		{"demo disk snapshot with block data", PlannedNode{
			APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDiskSnapshot",
			DataFile:   "/some/data.bin",
		}, false},
		// A domain snapshot with neither volume data nor children is manifest-only, not an aggregator.
		{"manifest-only demo vm snapshot", PlannedNode{APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1", Kind: "DemoVirtualMachineSnapshot"}, false},
		// A domain snapshot with no data but WITH children is a true aggregator.
		{"demo vm snapshot aggregator (has children)", PlannedNode{
			APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualMachineSnapshot",
			Children: []ChildRef{{
				APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1",
				Kind:       "DemoVirtualDiskSnapshot",
				Name:       "dvd-1",
			}},
		}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.node.isDomainAggregator(); got != tc.want {
				t.Errorf("isDomainAggregator() = %v, want %v", got, tc.want)
			}
		})
	}
}
