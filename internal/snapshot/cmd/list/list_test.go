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

package list

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	sigsyaml "sigs.k8s.io/yaml"
)

// snapshotObj builds an unstructured Snapshot for tests. An empty ready/content
// is omitted; age==0 leaves creationTimestamp unset.
func snapshotObj(namespace, name, ready, content string, children int, age time.Duration) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata": map[string]interface{}{
			"namespace": namespace,
			"name":      name,
		},
	}}

	if age > 0 {
		obj.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-age)))
	}

	status := map[string]interface{}{}

	if ready != "" {
		status["conditions"] = []interface{}{
			map[string]interface{}{"type": "Ready", "status": ready},
		}
	}

	if content != "" {
		status["boundSnapshotContentName"] = content
	}

	if children > 0 {
		refs := make([]interface{}, 0, children)
		for i := 0; i < children; i++ {
			refs = append(refs, map[string]interface{}{
				"apiVersion": "storage.deckhouse.io/v1alpha1",
				"kind":       "Snapshot",
				"name":       fmt.Sprintf("child-%d", i),
			})
		}

		status["childrenSnapshotRefs"] = refs
	}

	if len(status) > 0 {
		obj.Object["status"] = status
	}

	return obj
}

func newFakeDynamic(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	gvrToListKind := map[schema.GroupVersionResource]string{
		snapshotGVR: "SnapshotList",
	}

	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), gvrToListKind, objs...)
}

func TestCommandUseAndAliases(t *testing.T) {
	cmd := NewCommand(slog.Default())

	if cmd.Use != "get" {
		t.Fatalf("expected Use==%q, got %q", "get", cmd.Use)
	}

	aliasSet := map[string]bool{}
	for _, a := range cmd.Aliases {
		aliasSet[a] = true
	}

	for _, want := range []string{"list", "ls"} {
		if !aliasSet[want] {
			t.Fatalf("expected alias %q in Aliases %v", want, cmd.Aliases)
		}
	}
}

func TestCommandRejectsPositionalArg(t *testing.T) {
	cmd := NewCommand(slog.Default())
	cmd.SetArgs([]string{"./some-dir"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when positional argument is provided")
	}
}

func TestReadyStatus(t *testing.T) {
	cases := []struct {
		name string
		obj  map[string]interface{}
		want string
	}{
		{
			name: "ready true",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			}}},
			want: "True",
		},
		{
			name: "ready false",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "False"},
			}}},
			want: "False",
		},
		{
			name: "ready unknown",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "Unknown"},
			}}},
			want: "Unknown",
		},
		{
			name: "other condition only",
			obj: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "VolumesReady", "status": "True"},
			}}},
			want: notAvailable,
		},
		{
			name: "no conditions",
			obj:  map[string]interface{}{"status": map[string]interface{}{}},
			want: notAvailable,
		},
		{
			name: "no status",
			obj:  map[string]interface{}{},
			want: notAvailable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := readyStatus(&unstructured.Unstructured{Object: tc.obj})
			if got != tc.want {
				t.Fatalf("readyStatus = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildSnapshotRows(t *testing.T) {
	items := []unstructured.Unstructured{
		*snapshotObj("ns1", "ready-snap", "True", "content-a", 2, 5*time.Minute),
		*snapshotObj("ns1", "bare-snap", "", "", 0, 0),
	}

	rows := buildSnapshotRows(items)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	if rows[0].Name != "ready-snap" || rows[0].Ready != "True" ||
		rows[0].Children != 2 {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}

	if rows[0].Age != "5m" {
		t.Fatalf("expected age 5m, got %q", rows[0].Age)
	}

	// Bare snapshot: no conditions fall back to "-".
	if rows[1].Ready != notAvailable || rows[1].Children != 0 || rows[1].Age != notAvailable {
		t.Fatalf("unexpected bare row: %+v", rows[1])
	}
}

func TestPrintSnapshotTableSingleNamespace(t *testing.T) {
	var buf bytes.Buffer

	rows := buildSnapshotRows([]unstructured.Unstructured{
		*snapshotObj("ns1", "snap-a", "True", "content-a", 1, time.Hour),
	})

	if err := printSnapshotTable(&buf, rows, false); err != nil {
		t.Fatalf("printSnapshotTable: %v", err)
	}

	out := buf.String()

	if !strings.Contains(out, "NAME") || !strings.Contains(out, "READY") ||
		!strings.Contains(out, "CHILDREN") || !strings.Contains(out, "AGE") {
		t.Fatalf("missing expected header columns:\n%s", out)
	}

	if strings.Contains(out, "SNAPSHOTCONTENT") {
		t.Fatalf("table must not include SNAPSHOTCONTENT column:\n%s", out)
	}

	if strings.Contains(out, "NAMESPACE") {
		t.Fatalf("single-namespace table must not include NAMESPACE column:\n%s", out)
	}

	if !strings.Contains(out, "snap-a") {
		t.Fatalf("missing row data:\n%s", out)
	}
}

func TestPrintSnapshotTableAllNamespaces(t *testing.T) {
	var buf bytes.Buffer

	rows := buildSnapshotRows([]unstructured.Unstructured{
		*snapshotObj("ns1", "snap-a", "True", "content-a", 0, time.Hour),
		*snapshotObj("ns2", "snap-b", "False", "", 0, time.Hour),
	})

	if err := printSnapshotTable(&buf, rows, true); err != nil {
		t.Fatalf("printSnapshotTable: %v", err)
	}

	out := buf.String()

	if !strings.HasPrefix(strings.TrimSpace(out), "NAMESPACE") {
		t.Fatalf("all-namespaces table must lead with NAMESPACE column:\n%s", out)
	}

	if !strings.Contains(out, "ns1") || !strings.Contains(out, "ns2") {
		t.Fatalf("missing namespace values:\n%s", out)
	}
}

func TestPrintSnapshotTableEmpty(t *testing.T) {
	var buf bytes.Buffer

	if err := printSnapshotTable(&buf, nil, false); err != nil {
		t.Fatalf("printSnapshotTable: %v", err)
	}

	if got := strings.TrimSpace(buf.String()); got != "No snapshots found." {
		t.Fatalf("empty list output = %q, want %q", got, "No snapshots found.")
	}
}

func TestRenderJSON(t *testing.T) {
	list := &unstructured.UnstructuredList{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "SnapshotList",
	}}
	list.Items = []unstructured.Unstructured{
		*snapshotObj("ns1", "snap-a", "True", "content-a", 0, time.Hour),
	}

	var buf bytes.Buffer
	if err := render(&buf, list, false, "json"); err != nil {
		t.Fatalf("render json: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, `"items"`) || !strings.Contains(out, "snap-a") ||
		!strings.Contains(out, "content-a") || !strings.Contains(out, "boundSnapshotContentName") {
		t.Fatalf("json output missing expected content:\n%s", out)
	}
}

func TestRenderYAML(t *testing.T) {
	list := &unstructured.UnstructuredList{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "SnapshotList",
	}}
	list.Items = []unstructured.Unstructured{
		*snapshotObj("ns1", "snap-a", "True", "content-a", 0, time.Hour),
	}

	var buf bytes.Buffer
	if err := render(&buf, list, false, "yaml"); err != nil {
		t.Fatalf("render yaml: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "items:") || !strings.Contains(out, "snap-a") {
		t.Fatalf("yaml output missing expected content:\n%s", out)
	}
}

func TestRenderUnsupportedFormat(t *testing.T) {
	list := &unstructured.UnstructuredList{}

	err := render(io.Discard, list, false, "wide")
	if !errors.Is(err, errUnsupportedFormat) {
		t.Fatalf("expected errUnsupportedFormat, got %v", err)
	}
}

func TestListSnapshots(t *testing.T) {
	dyn := newFakeDynamic(
		snapshotObj("ns1", "snap-a", "True", "content-a", 0, time.Hour),
		snapshotObj("ns1", "snap-b", "False", "", 0, time.Hour),
		snapshotObj("ns2", "snap-c", "True", "content-c", 0, time.Hour),
	)

	t.Run("single namespace", func(t *testing.T) {
		list, err := listSnapshots(context.Background(), dyn, "ns1", false)
		if err != nil {
			t.Fatalf("listSnapshots: %v", err)
		}

		if len(list.Items) != 2 {
			t.Fatalf("expected 2 snapshots in ns1, got %d", len(list.Items))
		}
	})

	t.Run("all namespaces", func(t *testing.T) {
		list, err := listSnapshots(context.Background(), dyn, "", true)
		if err != nil {
			t.Fatalf("listSnapshots: %v", err)
		}

		if len(list.Items) != 3 {
			t.Fatalf("expected 3 snapshots across all namespaces, got %d", len(list.Items))
		}
	})
}

// TestRender_OutputPassthrough verifies that -o json and -o yaml emit the raw
// Snapshot object(s) with full field fidelity — no stripping of apiVersion,
// kind, metadata, or status fields. The list is fetched via listSnapshots using
// a seeded fake dynamic client so the full pipeline (client → render → output)
// is exercised.
func TestRender_OutputPassthrough(t *testing.T) {
	obj := snapshotObj("ns1", "snap-pt", "True", "content-x", 2, time.Hour)
	dyn := newFakeDynamic(obj)

	list, err := listSnapshots(context.Background(), dyn, "ns1", false)
	if err != nil {
		t.Fatalf("listSnapshots: %v", err)
	}

	cases := []struct {
		name   string
		format string
	}{
		{name: "json passthrough", format: "json"},
		{name: "yaml passthrough", format: "yaml"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			if err := render(&buf, list, false, tc.format); err != nil {
				t.Fatalf("render(%s): %v", tc.format, err)
			}

			var got map[string]interface{}

			switch tc.format {
			case "json":
				if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
					t.Fatalf("json.Unmarshal: %v", err)
				}
			case "yaml":
				if err := sigsyaml.Unmarshal(buf.Bytes(), &got); err != nil {
					t.Fatalf("yaml.Unmarshal: %v", err)
				}
			}

			// items array must be present and non-empty.
			items, ok := got["items"].([]interface{})
			if !ok || len(items) == 0 {
				t.Fatalf("items missing or empty in %s output", tc.format)
			}

			item, ok := items[0].(map[string]interface{})
			if !ok {
				t.Fatalf("items[0] is not a map: %T", items[0])
			}

			// Raw apiVersion and kind must survive passthrough.
			if item["apiVersion"] != "storage.deckhouse.io/v1alpha1" {
				t.Fatalf("apiVersion not preserved: %v", item["apiVersion"])
			}

			if item["kind"] != "Snapshot" {
				t.Fatalf("kind not preserved: %v", item["kind"])
			}

			// metadata.name and metadata.namespace must survive.
			metadata, _ := item["metadata"].(map[string]interface{})

			if metadata["name"] != "snap-pt" {
				t.Fatalf("metadata.name not preserved: %v", metadata["name"])
			}

			if metadata["namespace"] != "ns1" {
				t.Fatalf("metadata.namespace not preserved: %v", metadata["namespace"])
			}

			// status.boundSnapshotContentName must survive — no field stripping.
			status, ok := item["status"].(map[string]interface{})
			if !ok {
				t.Fatalf("status missing or not a map: %v", item["status"])
			}

			if status["boundSnapshotContentName"] != "content-x" {
				t.Fatalf("boundSnapshotContentName not preserved: %v", status["boundSnapshotContentName"])
			}
		})
	}
}

func TestRenderUnsupportedFormat_ErrorIs(t *testing.T) {
	list := &unstructured.UnstructuredList{}

	cases := []struct {
		name   string
		format string
	}{
		{name: "wide", format: "wide"},
		{name: "custom", format: "custom"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := render(io.Discard, list, false, tc.format)
			if !errors.Is(err, errUnsupportedFormat) {
				t.Fatalf("expected errUnsupportedFormat for format %q, got %v", tc.format, err)
			}
		})
	}
}

func TestRunMutuallyExclusiveScope(t *testing.T) {
	cmd := NewCommand(slog.Default())
	cmd.SetArgs([]string{"--all-namespaces", "--namespace", "ns1"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when -A and -n are combined")
	}

	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected mutually-exclusive error, got %v", err)
	}
}

func TestHumanAge(t *testing.T) {
	if got := humanAge(time.Time{}); got != notAvailable {
		t.Fatalf("zero time age = %q, want %q", got, notAvailable)
	}

	if got := humanAge(time.Now().Add(-5 * time.Minute)); got != "5m" {
		t.Fatalf("age = %q, want %q", got, "5m")
	}
}
