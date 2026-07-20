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

func TestParseNodeStatus_NamespaceSourceRefWithoutNamespaceOK(t *testing.T) {
	// The root capture-Snapshot's source is the cluster-scoped Namespace: sourceRef legitimately
	// carries no namespace (only apiVersion/kind/name/uid).
	_, src, _, err := ParseNodeStatus(node(map[string]interface{}{
		"sourceRef": map[string]interface{}{
			"apiVersion": "v1", "kind": "Namespace", "name": "my-app", "uid": "ns-uid",
		},
	}))
	if err != nil {
		t.Fatalf("v1/Namespace sourceRef without namespace must be accepted, got: %v", err)
	}
	if src == nil || src.Kind != "Namespace" || src.Namespace != "" || src.UID != "ns-uid" {
		t.Errorf("namespace sourceRef not decoded as expected: %+v", src)
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
		{"sourceRef PVC missing namespace", map[string]interface{}{"sourceRef": map[string]interface{}{
			"apiVersion": "v1", "kind": "PersistentVolumeClaim", "name": "x", "uid": "u",
		}}},
		{"sourceRef domain kind missing namespace", map[string]interface{}{"sourceRef": map[string]interface{}{
			"apiVersion": "demo.state-snapshotter.deckhouse.io/v1alpha1", "kind": "DemoVirtualDisk", "name": "x", "uid": "u",
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

func TestParseNodeStatus_IdentityFailClosed(t *testing.T) {
	// The node's own identity feeds resume/checksum/collision, so an incomplete one must fail
	// even when status.sourceRef/status.data are absent (fragments alone can't rescue it).
	full := map[string]interface{}{
		"apiVersion": "demo.state-snapshotter.deckhouse.io/v1alpha1",
		"kind":       "DemoVirtualDiskSnapshot",
		"namespace":  "ns",
		"name":       "dvd-1",
		"uid":        "snap-uid",
	}

	cases := []struct {
		name string
		drop string
	}{
		{"missing uid", "uid"},
		{"missing namespace", "namespace"},
		{"missing name", "name"},
		{"missing kind", "kind"},
		{"missing apiVersion", "apiVersion"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := map[string]interface{}{"namespace": full["namespace"], "name": full["name"], "uid": full["uid"]}
			delete(meta, tc.drop)
			obj := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": full["apiVersion"],
				"kind":       full["kind"],
				"metadata":   meta,
			}}
			if tc.drop == "apiVersion" || tc.drop == "kind" {
				delete(obj.Object, tc.drop)
			}
			if _, _, _, err := ParseNodeStatus(obj); err == nil {
				t.Errorf("expected error for incomplete identity (%s), got nil", tc.name)
			}
		})
	}
}
