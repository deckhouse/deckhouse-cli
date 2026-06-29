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

package create

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newFakeDynamic(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	gvrToListKind := map[schema.GroupVersionResource]string{
		snapshotGVR: "SnapshotList",
	}

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
}

// readySnapshot builds a Snapshot already carrying Ready=True (used to drive --wait).
func readySnapshot(namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata":   map[string]interface{}{"namespace": namespace, "name": name},
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			},
		},
	}}
}

func TestBuildSnapshot_Empty(t *testing.T) {
	obj := buildSnapshot("ns", "snap", "", nil)

	if obj.GetKind() != "Snapshot" || obj.GetAPIVersion() != "storage.deckhouse.io/v1alpha1" {
		t.Fatalf("unexpected GVK: %s %s", obj.GetAPIVersion(), obj.GetKind())
	}

	if obj.GetNamespace() != "ns" || obj.GetName() != "snap" {
		t.Fatalf("unexpected metadata: ns=%q name=%q", obj.GetNamespace(), obj.GetName())
	}

	spec, found, _ := unstructured.NestedMap(obj.Object, "spec")
	if !found || len(spec) != 0 {
		t.Fatalf("empty create must produce an empty spec, got found=%v spec=%v", found, spec)
	}
}

func TestBuildSnapshot_WithClassAndSelector(t *testing.T) {
	obj := buildSnapshot("ns", "snap", "fast", map[string]interface{}{"app": "demo", "tier": "db"})

	sc, _, _ := unstructured.NestedString(obj.Object, "spec", "snapshotClassName")
	if sc != "fast" {
		t.Errorf("spec.snapshotClassName = %q, want fast", sc)
	}

	ml, found, _ := unstructured.NestedStringMap(obj.Object, "spec", "resourceSelector", "matchLabels")
	if !found {
		t.Fatalf("spec.resourceSelector.matchLabels not set")
	}

	if !reflect.DeepEqual(ml, map[string]string{"app": "demo", "tier": "db"}) {
		t.Errorf("matchLabels = %v, want {app:demo, tier:db}", ml)
	}
}

func TestParseMatchLabels(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    map[string]interface{}
		wantErr bool
	}{
		{"empty", "", nil, false},
		{"whitespace", "   ", nil, false},
		{"single", "app=demo", map[string]interface{}{"app": "demo"}, false},
		{"multi with spaces", " app=demo , tier=db ", map[string]interface{}{"app": "demo", "tier": "db"}, false},
		{"empty value", "app=", map[string]interface{}{"app": ""}, false},
		{"missing eq", "app", nil, true},
		{"empty key", "=demo", nil, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMatchLabels(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tc.in)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parseMatchLabels(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestRunCreate_CreatesSnapshot(t *testing.T) {
	dyn := newFakeDynamic()

	var buf bytes.Buffer
	opts := createOptions{namespace: "ns", name: "snap", outputFmt: "name", poll: time.Millisecond}

	if err := runCreate(context.Background(), dyn, &buf, opts, discardLogger()); err != nil {
		t.Fatalf("runCreate: %v", err)
	}

	// The Snapshot CR must exist in the target namespace.
	got, err := dyn.Resource(snapshotGVR).Namespace("ns").Get(context.Background(), "snap", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("created Snapshot not found: %v", err)
	}

	if got.GetName() != "snap" {
		t.Errorf("created name = %q, want snap", got.GetName())
	}

	if out := strings.TrimSpace(buf.String()); out != "snapshot.storage.deckhouse.io/snap created" {
		t.Errorf("confirmation = %q, want kubectl-style created line", out)
	}
}

func TestRunCreate_AlreadyExists(t *testing.T) {
	dyn := newFakeDynamic(readySnapshot("ns", "snap"))

	opts := createOptions{namespace: "ns", name: "snap", outputFmt: "name", poll: time.Millisecond}

	err := runCreate(context.Background(), dyn, io.Discard, opts, discardLogger())
	if err == nil {
		t.Fatal("expected already-exists error, got nil")
	}

	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error %q does not mention already exists", err.Error())
	}
}

func TestWaitReady_ReturnsWhenReady(t *testing.T) {
	dyn := newFakeDynamic(readySnapshot("ns", "snap"))

	obj, err := waitReady(context.Background(), dyn, "ns", "snap", time.Second, time.Millisecond, discardLogger())
	if err != nil {
		t.Fatalf("waitReady: %v", err)
	}

	status, _, _ := readyCondition(obj)
	if status != "True" {
		t.Errorf("waitReady returned status %q, want True", status)
	}
}

func TestWaitReady_TimesOut(t *testing.T) {
	// A Snapshot without a Ready=True condition never satisfies the wait.
	pending := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata":   map[string]interface{}{"namespace": "ns", "name": "snap"},
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "False", "reason": "Capturing", "message": "in progress"},
			},
		},
	}}

	dyn := newFakeDynamic(pending)

	_, err := waitReady(context.Background(), dyn, "ns", "snap", 30*time.Millisecond, time.Millisecond, discardLogger())
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !strings.Contains(err.Error(), "timeout") || !strings.Contains(err.Error(), "Capturing") {
		t.Errorf("timeout error should carry the last reason, got: %v", err)
	}
}

func TestReadyCondition(t *testing.T) {
	obj := readySnapshot("ns", "snap")

	status, _, _ := readyCondition(obj)
	if status != "True" {
		t.Errorf("status = %q, want True", status)
	}

	none := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{}}}
	if s, _, _ := readyCondition(none); s != "" {
		t.Errorf("no-condition status = %q, want empty", s)
	}
}

func TestRenderCreated(t *testing.T) {
	obj := readySnapshot("ns", "snap")

	var nameBuf bytes.Buffer
	if err := renderCreated(&nameBuf, obj, "name"); err != nil {
		t.Fatalf("renderCreated name: %v", err)
	}

	if !strings.Contains(nameBuf.String(), "snapshot.storage.deckhouse.io/snap created") {
		t.Errorf("name output = %q", nameBuf.String())
	}

	var jsonBuf bytes.Buffer
	if err := renderCreated(&jsonBuf, obj, "json"); err != nil {
		t.Fatalf("renderCreated json: %v", err)
	}

	if !strings.Contains(jsonBuf.String(), `"kind": "Snapshot"`) {
		t.Errorf("json output missing object:\n%s", jsonBuf.String())
	}

	if err := renderCreated(io.Discard, obj, "wide"); err == nil {
		t.Fatal("expected error for unsupported format")
	}
}

func TestNewCommand_ArgsValidation(t *testing.T) {
	cmd := NewCommand(slog.Default())
	cmd.SetArgs([]string{}) // missing <name>
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error when <name> argument is missing")
	}
}
