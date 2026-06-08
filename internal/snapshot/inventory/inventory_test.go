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

package inventory_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/inventory"
)

var snapshotListGVK = schema.GroupVersionKind{
	Group:   "storage.deckhouse.io",
	Version: "v1alpha1",
	Kind:    "Snapshot",
}

// makeSnap builds an unstructured Snapshot object for use in tests.
func makeSnap(name, ns, content string, ready bool, numChildren int, created time.Time) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(snapshotListGVK)
	obj.SetName(name)
	obj.SetNamespace(ns)
	obj.SetCreationTimestamp(metav1.NewTime(created))

	readyStatus := "False"
	if ready {
		readyStatus = "True"
	}

	status := map[string]any{
		"boundSnapshotContentName": content,
		"conditions": []any{
			map[string]any{
				"type":   "Ready",
				"status": readyStatus,
			},
		},
	}

	if numChildren > 0 {
		children := make([]any, numChildren)
		for i := range children {
			children[i] = map[string]any{"name": "child"}
		}

		status["childrenSnapshotRefs"] = children
	}

	_ = unstructured.SetNestedField(obj.Object, status, "status")

	return obj
}

func fakeClient(objects ...runtime.Object) *fake.ClientBuilder {
	scheme := runtime.NewScheme()

	return fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...)
}

// TestList_AllNamespaces verifies that all snapshots are returned when no namespace filter is set.
func TestList_AllNamespaces(t *testing.T) {
	now := time.Now()
	snapA := makeSnap("snap-a", "ns1", "content-a", true, 0, now)
	snapB := makeSnap("snap-b", "ns2", "content-b", false, 2, now)

	client := fakeClient(snapA, snapB).Build()

	infos, err := inventory.List(context.Background(), client, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(infos) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(infos))
	}

	byName := make(map[string]inventory.SnapshotInfo, len(infos))
	for _, info := range infos {
		byName[info.Name] = info
	}

	a := byName["snap-a"]
	if a.Namespace != "ns1" {
		t.Errorf("snap-a namespace = %q, want ns1", a.Namespace)
	}

	if !a.Ready {
		t.Error("snap-a: expected Ready=true")
	}

	if a.Content != "content-a" {
		t.Errorf("snap-a content = %q, want content-a", a.Content)
	}

	if a.Children != 0 {
		t.Errorf("snap-a children = %d, want 0", a.Children)
	}

	b := byName["snap-b"]
	if b.Ready {
		t.Error("snap-b: expected Ready=false")
	}

	if b.Children != 2 {
		t.Errorf("snap-b children = %d, want 2", b.Children)
	}
}

// TestList_NamespaceFilter verifies that only snapshots in the given namespace are returned.
func TestList_NamespaceFilter(t *testing.T) {
	now := time.Now()
	snapA := makeSnap("snap-a", "ns1", "content-a", true, 0, now)
	snapB := makeSnap("snap-b", "ns2", "content-b", true, 0, now)

	client := fakeClient(snapA, snapB).Build()

	infos, err := inventory.List(context.Background(), client, "ns1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(infos) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(infos))
	}

	if infos[0].Name != "snap-a" {
		t.Errorf("expected snap-a, got %q", infos[0].Name)
	}

	if infos[0].Namespace != "ns1" {
		t.Errorf("namespace = %q, want ns1", infos[0].Namespace)
	}
}

// TestList_Empty verifies that an empty slice is returned when there are no snapshots.
func TestList_Empty(t *testing.T) {
	client := fakeClient().Build()

	infos, err := inventory.List(context.Background(), client, "ns1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(infos) != 0 {
		t.Fatalf("expected 0 snapshots, got %d", len(infos))
	}
}

// TestList_ReadyFalseWithReason verifies that the ReadyReason field is populated
// when Ready=False and a condition message is present.
func TestList_ReadyFalseWithReason(t *testing.T) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(snapshotListGVK)
	obj.SetName("failing-snap")
	obj.SetNamespace("ns1")

	_ = unstructured.SetNestedField(obj.Object, map[string]any{
		"conditions": []any{
			map[string]any{
				"type":    "Ready",
				"status":  "False",
				"message": "capture failed",
			},
		},
	}, "status")

	client := fakeClient(obj).Build()

	infos, err := inventory.List(context.Background(), client, "ns1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(infos) != 1 {
		t.Fatalf("expected 1, got %d", len(infos))
	}

	info := infos[0]
	if info.Ready {
		t.Error("expected Ready=false")
	}

	if info.ReadyReason != "capture failed" {
		t.Errorf("ReadyReason = %q, want \"capture failed\"", info.ReadyReason)
	}
}

// TestRender_HumanAllNamespaces verifies that the NAMESPACE column appears in human output
// when showNamespace=true.
func TestRender_HumanAllNamespaces(t *testing.T) {
	infos := []inventory.SnapshotInfo{
		{Namespace: "ns1", Name: "snap-a", Ready: true, Content: "sc-a", Children: 0, Created: time.Now().Add(-2 * time.Hour)},
		{Namespace: "ns2", Name: "snap-b", Ready: false, Children: 1, Created: time.Now().Add(-30 * time.Minute)},
	}

	var buf bytes.Buffer

	if err := inventory.Render(&buf, infos, inventory.FormatHuman, true); err != nil {
		t.Fatalf("Render: %v", err)
	}

	out := buf.String()

	if !strings.Contains(out, "NAMESPACE") {
		t.Error("expected NAMESPACE column in header")
	}

	if !strings.Contains(out, "ns1") {
		t.Error("expected ns1 in output")
	}

	if !strings.Contains(out, "True") {
		t.Error("expected True (Ready) in output")
	}
}

// TestRender_HumanSingleNamespace verifies that the NAMESPACE column is absent
// when showNamespace=false.
func TestRender_HumanSingleNamespace(t *testing.T) {
	infos := []inventory.SnapshotInfo{
		{Namespace: "ns1", Name: "snap-a", Ready: true, Content: "sc-a", Created: time.Now()},
	}

	var buf bytes.Buffer

	if err := inventory.Render(&buf, infos, inventory.FormatHuman, false); err != nil {
		t.Fatalf("Render: %v", err)
	}

	out := buf.String()

	if strings.Contains(out, "NAMESPACE") {
		t.Error("NAMESPACE column should not appear when showNamespace=false")
	}

	if !strings.Contains(out, "snap-a") {
		t.Error("expected snap-a in output")
	}
}

// TestRender_JSON verifies that JSON output is valid and contains expected fields.
func TestRender_JSON(t *testing.T) {
	infos := []inventory.SnapshotInfo{
		{Namespace: "ns1", Name: "snap-a", Ready: true, Content: "sc-a", Children: 3},
	}

	var buf bytes.Buffer

	if err := inventory.Render(&buf, infos, inventory.FormatJSON, true); err != nil {
		t.Fatalf("Render JSON: %v", err)
	}

	var parsed []map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("JSON parse: %v", err)
	}

	if len(parsed) != 1 {
		t.Fatalf("expected 1 item, got %d", len(parsed))
	}

	if parsed[0]["name"] != "snap-a" {
		t.Errorf("name = %v, want snap-a", parsed[0]["name"])
	}

	if parsed[0]["ready"] != true {
		t.Errorf("ready = %v, want true", parsed[0]["ready"])
	}
}

// TestRender_YAML verifies that YAML output is non-empty and contains expected fields.
func TestRender_YAML(t *testing.T) {
	infos := []inventory.SnapshotInfo{
		{Namespace: "ns1", Name: "snap-a", Ready: false, ReadyReason: "NotBound"},
	}

	var buf bytes.Buffer

	if err := inventory.Render(&buf, infos, inventory.FormatYAML, true); err != nil {
		t.Fatalf("Render YAML: %v", err)
	}

	out := buf.String()

	if !strings.Contains(out, "snap-a") {
		t.Error("expected snap-a in YAML output")
	}

	if !strings.Contains(out, "NotBound") {
		t.Error("expected NotBound reason in YAML output")
	}
}
