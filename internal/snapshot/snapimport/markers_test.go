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

func TestImportMarkerCR_Snapshot(t *testing.T) {
	node := PlannedNode{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "Snapshot", Name: "root"}

	obj, err := importMarkerCR(node, "ns", "")
	if err != nil {
		t.Fatalf("importMarkerCR: %v", err)
	}

	if obj.GetNamespace() != "ns" || obj.GetName() != "root" {
		t.Errorf("unexpected metadata: ns=%q name=%q", obj.GetNamespace(), obj.GetName())
	}

	if _, found, _ := unstructured.NestedMap(obj.Object, "spec", "source", "import"); !found {
		t.Errorf("expected spec.source.import marker to be set")
	}
}

func TestImportMarkerCR_VolumeSnapshot(t *testing.T) {
	node := PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "pvc-1"}

	obj, err := importMarkerCR(node, "ns", "di-1")
	if err != nil {
		t.Fatalf("importMarkerCR: %v", err)
	}

	name, found, _ := unstructured.NestedString(obj.Object, "spec", "source", "dataImportName")
	if !found || name != "di-1" {
		t.Errorf("spec.source.dataImportName: found=%v value=%q, want %q", found, name, "di-1")
	}
}

func TestImportMarkerCR_VolumeSnapshot_RequiresDataImport(t *testing.T) {
	node := PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot", Name: "pvc-1"}

	if _, err := importMarkerCR(node, "ns", ""); err == nil {
		t.Fatal("expected error when DataImport name is empty, got nil")
	}
}

func TestImportMarkerCR_UnsupportedKind(t *testing.T) {
	node := PlannedNode{APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1", Kind: "DemoVirtualMachineSnapshot", Name: "vm-1"}

	_, err := importMarkerCR(node, "ns", "")
	if err == nil {
		t.Fatal("expected unsupported-kind error, got nil")
	}

	if !strings.Contains(err.Error(), "not supported") {
		t.Errorf("error %q does not explain lack of support", err.Error())
	}
}

func TestPlannedNode_Supported(t *testing.T) {
	cases := []struct {
		name string
		node PlannedNode
		want bool
	}{
		{"core snapshot", PlannedNode{APIVersion: "storage.deckhouse.io/v1alpha1", Kind: "Snapshot"}, true},
		{"csi volume snapshot", PlannedNode{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshot"}, true},
		{"demo disk snapshot", PlannedNode{APIVersion: "demo.state-snapshotter.deckhouse.io/v1alpha1", Kind: "DemoVirtualDiskSnapshot"}, false},
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
