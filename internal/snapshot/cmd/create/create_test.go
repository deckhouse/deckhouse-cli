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

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
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
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata":   map[string]interface{}{"namespace": namespace, "name": name},
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			},
		},
	}}
}

func TestBuildSnapshot_Default(t *testing.T) {
	obj := buildSnapshot("ns", "snap", nil)

	if obj.GetKind() != "Snapshot" || obj.GetAPIVersion() != "state-snapshotter.deckhouse.io/v1alpha1" {
		t.Fatalf("unexpected GVK: %s %s", obj.GetAPIVersion(), obj.GetKind())
	}

	if obj.GetNamespace() != "ns" || obj.GetName() != "snap" {
		t.Fatalf("unexpected metadata: ns=%q name=%q", obj.GetNamespace(), obj.GetName())
	}

	// A default create sets spec.mode: Capture and nothing else.
	mode, _, _ := unstructured.NestedString(obj.Object, "spec", "mode")
	if mode != "Capture" {
		t.Errorf("spec.mode = %q, want Capture", mode)
	}

	spec, found, _ := unstructured.NestedMap(obj.Object, "spec")
	if !found || len(spec) != 1 {
		t.Fatalf("default create must produce spec={mode: Capture} only, got found=%v spec=%v", found, spec)
	}
}

func TestBuildSnapshot_WithSelector(t *testing.T) {
	obj := buildSnapshot("ns", "snap", map[string]interface{}{"app": "demo", "tier": "db"})

	mode, _, _ := unstructured.NestedString(obj.Object, "spec", "mode")
	if mode != "Capture" {
		t.Errorf("spec.mode = %q, want Capture", mode)
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
	t.Parallel()

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
		{"qualified key", "app.example.com/tier=backend-1", map[string]interface{}{"app.example.com/tier": "backend-1"}, false},
		{"empty value", "app=demo,tier=", map[string]interface{}{"app": "demo", "tier": ""}, false},
		{"empty components", ",", nil, true},
		{"whitespace components", " , ", nil, true},
		{"trailing component", "app=demo,", nil, true},
		{"double component", "app=demo,,tier=db", nil, true},
		{"duplicate key", "env=prod,env=staging", nil, true},
		{"extra equals", "a==b", nil, true},
		{"missing eq", "app", nil, true},
		{"empty key", "=demo", nil, true},
		{"invalid key", "bad/key/name=demo", nil, true},
		{"invalid value", "app=bad/value", nil, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseMatchLabels(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tc.in)
				}

				if !strings.Contains(err.Error(), "--selector") {
					t.Fatalf("error %q does not identify --selector", err)
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

func TestRun_InvalidSelectorDoesNotCreateSnapshot(t *testing.T) {
	t.Parallel()

	dyn := newFakeDynamic()
	cmd := NewCommand(discardLogger())

	if err := cmd.Flags().Set(flagNamespace, "ns"); err != nil {
		t.Fatalf("set --%s: %v", flagNamespace, err)
	}

	if err := cmd.Flags().Set(flagSelector, "app=demo,"); err != nil {
		t.Fatalf("set --%s: %v", flagSelector, err)
	}

	err := run(discardLogger(), cmd, []string{"snap"}, func(*cobra.Command) (dynamic.Interface, error) {
		return dyn, nil
	})
	if err == nil {
		t.Fatal("expected invalid selector error, got nil")
	}

	if !strings.Contains(err.Error(), "--selector") {
		t.Fatalf("error %q does not identify --selector", err)
	}

	for _, action := range dyn.Actions() {
		if action.GetVerb() == "create" {
			t.Fatalf("invalid selector performed Create action: %#v", action)
		}
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

	if out := strings.TrimSpace(buf.String()); out != "snapshot.state-snapshotter.deckhouse.io/snap created" {
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
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
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

	if !strings.Contains(nameBuf.String(), "snapshot.state-snapshotter.deckhouse.io/snap created") {
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
