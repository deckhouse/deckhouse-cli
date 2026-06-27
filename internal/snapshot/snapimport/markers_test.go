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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// assertImportMarker verifies the unified spec.source.import: {} marker is present (and empty).
func assertImportMarker(t *testing.T, obj *unstructured.Unstructured) {
	t.Helper()

	m, found, err := unstructured.NestedMap(obj.Object, "spec", "source", "import")
	if err != nil {
		t.Fatalf("read spec.source.import: %v", err)
	}

	if !found {
		t.Fatalf("expected spec.source.import marker to be set")
	}

	if len(m) != 0 {
		t.Errorf("spec.source.import must be an empty map, got %v", m)
	}
}

func TestImportMarkerCR_Snapshot(t *testing.T) {
	node := PlannedNode{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "Snapshot", Name: "root"}

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

func TestImportMarkerCR_UnsupportedKind(t *testing.T) {
	// DemoVirtualMachineSnapshot with NO volume data is an unsupported intermediate aggregator.
	node := PlannedNode{APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1", Kind: "DemoVirtualMachineSnapshot", Name: "vm-1"}

	_, err := importMarkerCR(node, "ns")
	if err == nil {
		t.Fatal("expected unsupported-kind error, got nil")
	}

	if !strings.Contains(err.Error(), "not supported") {
		t.Errorf("error %q does not explain lack of support", err.Error())
	}
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

func TestPlannedNode_Supported(t *testing.T) {
	cases := []struct {
		name string
		node PlannedNode
		want bool
	}{
		{"core snapshot", PlannedNode{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "Snapshot"}, true},
		{"csi volume snapshot", PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot"}, true},
		// A domain disk snapshot WITH volume data is a domain data leaf → supported.
		{"demo disk snapshot with block data", PlannedNode{
			APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1",
			Kind:       "DemoVirtualDiskSnapshot",
			DataFile:   "/some/data.bin",
		}, true},
		// A domain disk snapshot WITHOUT volume data is NOT a data leaf → unsupported.
		{"demo disk snapshot no data", PlannedNode{APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1", Kind: "DemoVirtualDiskSnapshot"}, false},
		{"demo vm snapshot", PlannedNode{APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1", Kind: "DemoVirtualMachineSnapshot"}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.node.supported(); got != tc.want {
				t.Errorf("supported() = %v, want %v", got, tc.want)
			}
		})
	}
}
