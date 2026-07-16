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

package source

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func node(status map[string]interface{}) *unstructured.Unstructured {
	obj := map[string]interface{}{
		"apiVersion": "demo.state-snapshotter.deckhouse.io/v1alpha1",
		"kind":       "DemoVirtualDiskSnapshot",
		"metadata":   map[string]interface{}{"namespace": "ns", "name": "dvd-1", "uid": "snap-uid"},
	}
	if status != nil {
		obj["status"] = status
	}
	return &unstructured.Unstructured{Object: obj}
}

func validSourceRef() map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "demo.state-snapshotter.deckhouse.io/v1alpha1",
		"kind":       "DemoVirtualDisk",
		"namespace":  "ns",
		"name":       "disk-a",
		"uid":        "src-uid",
	}
}

func validData() map[string]interface{} {
	return map[string]interface{}{
		"source": map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "PersistentVolumeClaim",
			"namespace":  "ns",
			"name":       "disk-a",
			"uid":        "pvc-uid",
		},
		"artifact": map[string]interface{}{
			"apiVersion": "snapshot.storage.k8s.io/v1",
			"kind":       "VolumeSnapshotContent",
			"name":       "snapcontent-1",
			"uid":        "vsc-uid",
		},
		"size": "10Gi",
	}
}

func TestParseNodeStatus_AbsentFragmentsAreNil(t *testing.T) {
	ident, src, data, err := ParseNodeStatus(node(map[string]interface{}{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src != nil || data != nil {
		t.Errorf("absent fragments must be nil, got src=%v data=%v", src, data)
	}
	if ident.Name != "dvd-1" || ident.UID != "snap-uid" || ident.Kind != "DemoVirtualDiskSnapshot" {
		t.Errorf("identity not populated from object metadata: %+v", ident)
	}
}

func TestParseNodeStatus_Valid(t *testing.T) {
	_, src, data, err := ParseNodeStatus(node(map[string]interface{}{
		"sourceRef": validSourceRef(),
		"data":      validData(),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src == nil || src.Kind != "DemoVirtualDisk" || src.UID != "src-uid" {
		t.Errorf("sourceRef not decoded: %+v", src)
	}
	if data == nil {
		t.Fatal("data must be decoded")
	}
	if data.Source.Kind != "PersistentVolumeClaim" || data.Source.UID != "pvc-uid" {
		t.Errorf("data.source not decoded: %+v", data.Source)
	}
	if data.Artifact.Name != "snapcontent-1" || data.Size != "10Gi" {
		t.Errorf("data artifact/size not decoded: %+v", data)
	}
}

func TestParseNodeStatus_FailClosed(t *testing.T) {
	cases := []struct {
		name   string
		status map[string]interface{}
	}{
		{"sourceRef not an object", map[string]interface{}{"sourceRef": "oops"}},
		{"sourceRef missing kind", map[string]interface{}{"sourceRef": map[string]interface{}{
			"apiVersion": "v1", "name": "x", "namespace": "ns",
		}}},
		{"sourceRef missing namespace", map[string]interface{}{"sourceRef": map[string]interface{}{
			"apiVersion": "v1", "kind": "PersistentVolumeClaim", "name": "x", "uid": "u",
		}}},
		{"sourceRef missing uid", map[string]interface{}{"sourceRef": map[string]interface{}{
			"apiVersion": "v1", "kind": "PersistentVolumeClaim", "namespace": "ns", "name": "x",
		}}},
		{"data not an object", map[string]interface{}{"data": "oops"}},
		{"data missing source", map[string]interface{}{"data": map[string]interface{}{
			"artifact": map[string]interface{}{"apiVersion": "snapshot.storage.k8s.io/v1", "kind": "VolumeSnapshotContent", "name": "a"},
		}}},
		{"data missing artifact", map[string]interface{}{"data": map[string]interface{}{
			"source": map[string]interface{}{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "name": "x"},
		}}},
		{"data bad size", map[string]interface{}{"data": func() map[string]interface{} {
			d := validData()
			d["size"] = "not-a-quantity"
			return d
		}()}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := ParseNodeStatus(node(tc.status)); err == nil {
				t.Errorf("expected error for %q, got nil", tc.name)
			}
		})
	}
}

func TestRequireNodeData(t *testing.T) {
	complete := &NodeData{
		Source:   SourceRefIdentity{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "ns", Name: "disk-a", UID: "pvc-uid"},
		Artifact: ArtifactRef{APIVersion: "snapshot.storage.k8s.io/v1", Kind: "VolumeSnapshotContent", Name: "snapcontent-1"},
	}

	if _, err := RequireNodeData(nil); err == nil {
		t.Error("nil node must error")
	}
	if _, err := RequireNodeData(&Node{Kind: "X", Name: "y"}); err == nil {
		t.Error("node without data must error")
	}

	noUID := *complete
	noUID.Source.UID = ""
	if _, err := RequireNodeData(&Node{Data: &noUID}); err == nil {
		t.Error("data without source.uid must error")
	}

	noArtifact := *complete
	noArtifact.Artifact = ArtifactRef{}
	if _, err := RequireNodeData(&Node{Data: &noArtifact}); err == nil {
		t.Error("data without artifact identity must error")
	}

	got, err := RequireNodeData(&Node{Data: complete})
	if err != nil {
		t.Fatalf("complete data must not error: %v", err)
	}
	if got != complete {
		t.Error("RequireNodeData must return the node's data")
	}
}
