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

package delete

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sort"
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

// snapshotObj builds an unstructured Snapshot with optional labels.
func snapshotObj(namespace, name string, labels map[string]interface{}) *unstructured.Unstructured {
	meta := map[string]interface{}{"namespace": namespace, "name": name}
	if len(labels) > 0 {
		meta["labels"] = labels
	}

	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "storage.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata":   meta,
	}}
}

func existingNames(t *testing.T, dyn *dynamicfake.FakeDynamicClient, namespace string) []string {
	t.Helper()

	list, err := dyn.Resource(snapshotGVR).Namespace(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].GetName())
	}

	sort.Strings(names)

	return names
}

func TestValidateScope(t *testing.T) {
	cases := []struct {
		name                         string
		haveNames, haveSelector, all bool
		wantErr                      bool
		errContains                  string
	}{
		{name: "names only", haveNames: true},
		{name: "selector only", haveSelector: true},
		{name: "all only", all: true},
		{name: "none", wantErr: true, errContains: "specify"},
		{name: "names+selector", haveNames: true, haveSelector: true, wantErr: true, errContains: "mutually exclusive"},
		{name: "selector+all", haveSelector: true, all: true, wantErr: true, errContains: "mutually exclusive"},
		{name: "all three", haveNames: true, haveSelector: true, all: true, wantErr: true, errContains: "mutually exclusive"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateScope(tc.haveNames, tc.haveSelector, tc.all)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}

				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errContains)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestRunDelete_ByName(t *testing.T) {
	dyn := newFakeDynamic(
		snapshotObj("ns", "snap-a", nil),
		snapshotObj("ns", "snap-b", nil),
		snapshotObj("ns", "snap-c", nil),
	)

	var buf bytes.Buffer
	opts := deleteOptions{namespace: "ns", names: []string{"snap-a", "snap-b"}, poll: time.Millisecond}

	if err := runDelete(context.Background(), dyn, &buf, opts, discardLogger()); err != nil {
		t.Fatalf("runDelete: %v", err)
	}

	if remaining := existingNames(t, dyn, "ns"); len(remaining) != 1 || remaining[0] != "snap-c" {
		t.Fatalf("remaining = %v, want [snap-c]", remaining)
	}

	out := buf.String()
	if !strings.Contains(out, "snapshot.storage.deckhouse.io/snap-a deleted") ||
		!strings.Contains(out, "snapshot.storage.deckhouse.io/snap-b deleted") {
		t.Fatalf("missing deleted confirmations:\n%s", out)
	}
}

func TestRunDelete_ByNameNotFound(t *testing.T) {
	dyn := newFakeDynamic(snapshotObj("ns", "snap-a", nil))

	opts := deleteOptions{namespace: "ns", names: []string{"ghost"}, poll: time.Millisecond}

	err := runDelete(context.Background(), dyn, io.Discard, opts, discardLogger())
	if err == nil {
		t.Fatal("expected not-found error, got nil")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error %q does not mention not found", err.Error())
	}
}

func TestRunDelete_ByNameIgnoreNotFound(t *testing.T) {
	dyn := newFakeDynamic(snapshotObj("ns", "snap-a", nil))

	opts := deleteOptions{namespace: "ns", names: []string{"ghost"}, ignoreNotFound: true, poll: time.Millisecond}

	if err := runDelete(context.Background(), dyn, io.Discard, opts, discardLogger()); err != nil {
		t.Fatalf("runDelete with ignore-not-found: %v", err)
	}
}

func TestRunDelete_BySelector(t *testing.T) {
	dyn := newFakeDynamic(
		snapshotObj("ns", "keep", map[string]interface{}{"app": "other"}),
		snapshotObj("ns", "drop-1", map[string]interface{}{"app": "demo"}),
		snapshotObj("ns", "drop-2", map[string]interface{}{"app": "demo"}),
	)

	var buf bytes.Buffer
	opts := deleteOptions{namespace: "ns", selector: "app=demo", poll: time.Millisecond}

	if err := runDelete(context.Background(), dyn, &buf, opts, discardLogger()); err != nil {
		t.Fatalf("runDelete: %v", err)
	}

	if remaining := existingNames(t, dyn, "ns"); len(remaining) != 1 || remaining[0] != "keep" {
		t.Fatalf("remaining = %v, want [keep]", remaining)
	}
}

func TestRunDelete_All(t *testing.T) {
	dyn := newFakeDynamic(
		snapshotObj("ns", "snap-a", nil),
		snapshotObj("ns", "snap-b", nil),
		snapshotObj("other", "snap-x", nil),
	)

	opts := deleteOptions{namespace: "ns", all: true, poll: time.Millisecond}

	if err := runDelete(context.Background(), dyn, io.Discard, opts, discardLogger()); err != nil {
		t.Fatalf("runDelete: %v", err)
	}

	if remaining := existingNames(t, dyn, "ns"); len(remaining) != 0 {
		t.Fatalf("namespace ns should be empty, got %v", remaining)
	}

	// Other namespaces are untouched.
	if remaining := existingNames(t, dyn, "other"); len(remaining) != 1 {
		t.Fatalf("namespace other should keep its snapshot, got %v", remaining)
	}
}

func TestRunDelete_SelectorNoMatches(t *testing.T) {
	dyn := newFakeDynamic(snapshotObj("ns", "snap-a", map[string]interface{}{"app": "other"}))

	var buf bytes.Buffer
	opts := deleteOptions{namespace: "ns", selector: "app=demo", poll: time.Millisecond}

	if err := runDelete(context.Background(), dyn, &buf, opts, discardLogger()); err != nil {
		t.Fatalf("runDelete: %v", err)
	}

	if got := strings.TrimSpace(buf.String()); got != "No snapshots found." {
		t.Fatalf("no-match output = %q, want %q", got, "No snapshots found.")
	}

	if remaining := existingNames(t, dyn, "ns"); len(remaining) != 1 {
		t.Fatalf("non-matching snapshot must survive, got %v", remaining)
	}
}

func TestRunDelete_Wait(t *testing.T) {
	dyn := newFakeDynamic(snapshotObj("ns", "snap-a", nil))

	opts := deleteOptions{namespace: "ns", names: []string{"snap-a"}, wait: true, timeout: time.Second, poll: time.Millisecond}

	// Fake client deletes synchronously, so waitGone observes NotFound at once.
	if err := runDelete(context.Background(), dyn, io.Discard, opts, discardLogger()); err != nil {
		t.Fatalf("runDelete with wait: %v", err)
	}
}

func TestWaitGone_TimesOut(t *testing.T) {
	dyn := newFakeDynamic(snapshotObj("ns", "snap-a", nil))

	// snap-a is never deleted here, so waitGone must time out.
	err := waitGone(context.Background(), dyn, "ns", "snap-a", 30*time.Millisecond, time.Millisecond, discardLogger())
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("error %q does not mention timeout", err.Error())
	}
}

func TestNewCommand_NoScopeFails(t *testing.T) {
	cmd := NewCommand(slog.Default())
	cmd.SetArgs([]string{}) // no names, no -l, no --all
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when no selection mode is given")
	}

	if !strings.Contains(err.Error(), "specify") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewCommand_MutuallyExclusiveScope(t *testing.T) {
	cmd := NewCommand(slog.Default())
	cmd.SetArgs([]string{"snap-a", "--all"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when names and --all are combined")
	}

	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("unexpected error: %v", err)
	}
}
