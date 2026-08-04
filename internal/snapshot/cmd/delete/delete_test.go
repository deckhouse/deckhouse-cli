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
	"errors"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
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

type getDynamic struct {
	dynamic.Interface
	get func(context.Context, string, metav1.GetOptions, ...string) (*unstructured.Unstructured, error)
}

func (d *getDynamic) Resource(schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return &getResource{get: d.get}
}

type getResource struct {
	dynamic.NamespaceableResourceInterface
	get func(context.Context, string, metav1.GetOptions, ...string) (*unstructured.Unstructured, error)
}

func (r *getResource) Namespace(string) dynamic.ResourceInterface {
	return r
}

func (r *getResource) Get(
	ctx context.Context,
	name string,
	opts metav1.GetOptions,
	subresources ...string,
) (*unstructured.Unstructured, error) {
	return r.get(ctx, name, opts, subresources...)
}

// snapshotObj builds an unstructured Snapshot with optional labels.
func snapshotObj(namespace, name string, labels map[string]interface{}) *unstructured.Unstructured {
	return snapshotObjWithUID(namespace, name, types.UID(name+"-uid"), labels)
}

func snapshotObjWithUID(namespace, name string, uid types.UID, labels map[string]interface{}) *unstructured.Unstructured {
	meta := map[string]interface{}{"namespace": namespace, "name": name, "uid": string(uid)}
	if len(labels) > 0 {
		meta["labels"] = labels
	}

	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
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
	dyn.PrependReactor("delete", "snapshots", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)
		if deleteAction.GetDeleteOptions().Preconditions != nil {
			t.Errorf("explicit-name delete %q has unexpected preconditions: %#v", deleteAction.GetName(), deleteAction.GetDeleteOptions().Preconditions)
		}

		return false, nil, nil
	})

	var buf bytes.Buffer
	opts := deleteOptions{namespace: "ns", names: []string{"snap-a", "snap-b"}, poll: time.Millisecond}

	if err := runDelete(context.Background(), dyn, &buf, opts, discardLogger()); err != nil {
		t.Fatalf("runDelete: %v", err)
	}

	if remaining := existingNames(t, dyn, "ns"); len(remaining) != 1 || remaining[0] != "snap-c" {
		t.Fatalf("remaining = %v, want [snap-c]", remaining)
	}

	out := buf.String()
	if !strings.Contains(out, "snapshot.state-snapshotter.deckhouse.io/snap-a deleted") ||
		!strings.Contains(out, "snapshot.state-snapshotter.deckhouse.io/snap-b deleted") {
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

func TestRunDelete_AggregatesErrorsAndContinues(t *testing.T) {
	t.Parallel()

	errFirst := errors.New("first delete failed")
	errSecond := errors.New("second delete failed")
	dyn := newFakeDynamic(
		snapshotObj("ns", "fail-first", nil),
		snapshotObj("ns", "delete-me", nil),
		snapshotObj("ns", "fail-second", nil),
	)
	dyn.PrependReactor("delete", "snapshots", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)

		switch deleteAction.GetName() {
		case "fail-first":
			return true, nil, errFirst
		case "fail-second":
			return true, nil, errSecond
		default:
			return false, nil, nil
		}
	})

	opts := deleteOptions{
		namespace: "ns",
		names:     []string{"fail-first", "delete-me", "fail-second"},
		poll:      time.Millisecond,
	}

	err := runDelete(context.Background(), dyn, io.Discard, opts, discardLogger())
	if !errors.Is(err, errFirst) || !errors.Is(err, errSecond) {
		t.Fatalf("joined error = %v, want both delete failures", err)
	}

	if remaining := existingNames(t, dyn, "ns"); len(remaining) != 2 ||
		remaining[0] != "fail-first" || remaining[1] != "fail-second" {
		t.Fatalf("remaining = %v, want [fail-first fail-second]", remaining)
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

func TestRunDelete_SelectedReplacementUsesUIDPrecondition(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		selector string
		all      bool
		labels   map[string]interface{}
	}{
		{
			name:     "selector: replacement survives and later target is deleted",
			selector: "app=demo",
			labels:   map[string]interface{}{"app": "demo"},
		},
		{
			name:   "all: replacement survives and later target is deleted",
			all:    true,
			labels: map[string]interface{}{"app": "demo"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const (
				listedUID      = types.UID("uid-a")
				replacementUID = types.UID("uid-b")
			)

			dyn := newFakeDynamic(
				snapshotObjWithUID("ns", "race", listedUID, tc.labels),
				snapshotObjWithUID("ns", "continue", types.UID("uid-continue"), tc.labels),
			)

			expectedUIDs := map[string]types.UID{
				"race":     listedUID,
				"continue": types.UID("uid-continue"),
			}
			deleteCalls := make(map[string]int)
			dyn.PrependReactor("delete", "snapshots", func(action clienttesting.Action) (bool, runtime.Object, error) {
				deleteAction := action.(clienttesting.DeleteAction)
				deleteCalls[deleteAction.GetName()]++

				preconditions := deleteAction.GetDeleteOptions().Preconditions
				if preconditions == nil || preconditions.UID == nil {
					t.Errorf("selected delete %q has no UID precondition", deleteAction.GetName())

					return true, nil, errors.New("missing UID precondition")
				}

				if got, want := *preconditions.UID, expectedUIDs[deleteAction.GetName()]; got != want {
					t.Errorf("delete %q UID precondition = %q, want %q", deleteAction.GetName(), got, want)
				}

				if deleteAction.GetName() != "race" {
					return false, nil, nil
				}

				if err := dyn.Tracker().Delete(snapshotGVR, "ns", "race", metav1.DeleteOptions{}); err != nil {
					t.Fatalf("delete listed Snapshot from tracker: %v", err)
				}

				replacement := snapshotObjWithUID("ns", "race", replacementUID, map[string]interface{}{"app": "other"})
				if err := dyn.Tracker().Create(snapshotGVR, replacement, "ns", metav1.CreateOptions{}); err != nil {
					t.Fatalf("create replacement Snapshot in tracker: %v", err)
				}

				return true, nil, kubeerrors.NewConflict(
					snapshotGVR.GroupResource(),
					"race",
					errors.New("UID precondition failed"),
				)
			})

			opts := deleteOptions{
				namespace: "ns",
				selector:  tc.selector,
				all:       tc.all,
				poll:      time.Millisecond,
			}

			err := runDelete(context.Background(), dyn, io.Discard, opts, discardLogger())
			if err == nil {
				t.Fatal("expected UID precondition error, got nil")
			}

			if !strings.Contains(err.Error(), "deleting Snapshot ns/race") ||
				!strings.Contains(err.Error(), "UID precondition failed") {
				t.Fatalf("error %q does not expose the precondition failure", err)
			}

			if got := deleteCalls["race"]; got != 1 {
				t.Fatalf("race delete calls = %d, want 1 (no name-only retry)", got)
			}

			if got := deleteCalls["continue"]; got != 1 {
				t.Fatalf("continue delete calls = %d, want 1", got)
			}

			replacement, getErr := dyn.Resource(snapshotGVR).Namespace("ns").Get(
				context.Background(),
				"race",
				metav1.GetOptions{},
			)
			if getErr != nil {
				t.Fatalf("get replacement Snapshot: %v", getErr)
			}

			if got := replacement.GetUID(); got != replacementUID {
				t.Fatalf("replacement UID = %q, want %q", got, replacementUID)
			}

			if remaining := existingNames(t, dyn, "ns"); len(remaining) != 1 || remaining[0] != "race" {
				t.Fatalf("remaining = %v, want [race]", remaining)
			}
		})
	}
}

func TestRunDelete_SelectedDisappearanceRemainsSilent(t *testing.T) {
	t.Parallel()

	dyn := newFakeDynamic(
		snapshotObjWithUID("ns", "gone", types.UID("uid-gone"), map[string]interface{}{"app": "demo"}),
		snapshotObjWithUID("ns", "continue", types.UID("uid-continue"), map[string]interface{}{"app": "demo"}),
	)

	dyn.PrependReactor("delete", "snapshots", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)
		if deleteAction.GetName() != "gone" {
			return false, nil, nil
		}

		preconditions := deleteAction.GetDeleteOptions().Preconditions
		if preconditions == nil || preconditions.UID == nil || *preconditions.UID != types.UID("uid-gone") {
			t.Errorf("gone delete UID precondition = %#v, want uid-gone", preconditions)
		}

		if err := dyn.Tracker().Delete(snapshotGVR, "ns", "gone", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete disappeared Snapshot from tracker: %v", err)
		}

		return true, nil, kubeerrors.NewNotFound(snapshotGVR.GroupResource(), "gone")
	})

	opts := deleteOptions{namespace: "ns", selector: "app=demo", poll: time.Millisecond}
	if err := runDelete(context.Background(), dyn, io.Discard, opts, discardLogger()); err != nil {
		t.Fatalf("runDelete: %v", err)
	}

	if remaining := existingNames(t, dyn, "ns"); len(remaining) != 0 {
		t.Fatalf("namespace ns should be empty, got %v", remaining)
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

func TestRunDelete_SelectedWaitStopsAtReplacement(t *testing.T) {
	t.Parallel()

	const (
		listedUID      = types.UID("uid-a")
		replacementUID = types.UID("uid-b")
	)

	dyn := newFakeDynamic(snapshotObjWithUID("ns", "snap-a", listedUID, map[string]interface{}{"app": "demo"}))
	dyn.PrependReactor("delete", "snapshots", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)
		preconditions := deleteAction.GetDeleteOptions().Preconditions
		if preconditions == nil || preconditions.UID == nil || *preconditions.UID != listedUID {
			t.Errorf("delete UID precondition = %#v, want %q", preconditions, listedUID)
		}

		if err := dyn.Tracker().Delete(snapshotGVR, "ns", "snap-a", metav1.DeleteOptions{}); err != nil {
			t.Fatalf("delete listed Snapshot from tracker: %v", err)
		}

		replacement := snapshotObjWithUID("ns", "snap-a", replacementUID, map[string]interface{}{"app": "other"})
		if err := dyn.Tracker().Create(snapshotGVR, replacement, "ns", metav1.CreateOptions{}); err != nil {
			t.Fatalf("create replacement Snapshot in tracker: %v", err)
		}

		return true, nil, nil
	})

	opts := deleteOptions{
		namespace: "ns",
		selector:  "app=demo",
		wait:      true,
		timeout:   30 * time.Millisecond,
		poll:      time.Millisecond,
	}
	if err := runDelete(context.Background(), dyn, io.Discard, opts, discardLogger()); err != nil {
		t.Fatalf("runDelete with replacement wait: %v", err)
	}

	replacement, err := dyn.Resource(snapshotGVR).Namespace("ns").Get(
		context.Background(),
		"snap-a",
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get replacement Snapshot: %v", err)
	}

	if got := replacement.GetUID(); got != replacementUID {
		t.Fatalf("replacement UID = %q, want %q", got, replacementUID)
	}
}

func TestWaitGone_TimesOut(t *testing.T) {
	dyn := newFakeDynamic(snapshotObj("ns", "snap-a", nil))

	// snap-a is never deleted here, so waitGone must time out.
	target := snapshotTarget{name: "snap-a"}
	err := waitGone(context.Background(), dyn, "ns", target, 30*time.Millisecond, time.Millisecond, discardLogger())
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("error %q does not mention timeout", err.Error())
	}
}

func TestWaitGone_ContextBoundsGet(t *testing.T) {
	t.Parallel()

	errAPI := errors.New("API unavailable")
	cases := []struct {
		name         string
		timeout      time.Duration
		cancelParent bool
		apiErr       error
		present      bool
		wantIs       error
		wantContains string
	}{
		{
			name:         "timeout: blocking GET is bounded",
			timeout:      20 * time.Millisecond,
			wantIs:       context.DeadlineExceeded,
			wantContains: "timeout waiting for Snapshot ns/snap-a",
		},
		{
			name:         "timeout: poll interval is bounded",
			timeout:      20 * time.Millisecond,
			present:      true,
			wantIs:       context.DeadlineExceeded,
			wantContains: "timeout waiting for Snapshot ns/snap-a",
		},
		{
			name:         "cancel: parent cancellation aborts blocking GET",
			timeout:      time.Second,
			cancelParent: true,
			wantIs:       context.Canceled,
		},
		{
			name:         "error: ordinary API error retains object identity",
			timeout:      time.Second,
			apiErr:       errAPI,
			wantIs:       errAPI,
			wantContains: "get Snapshot ns/snap-a",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			cancelParent := func() {}
			if tc.cancelParent {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancelParent = cancel
				t.Cleanup(cancel)
			}

			dyn := &getDynamic{
				get: func(getCtx context.Context, _ string, _ metav1.GetOptions, _ ...string) (*unstructured.Unstructured, error) {
					if tc.apiErr != nil {
						return nil, tc.apiErr
					}

					if tc.present {
						return snapshotObj("ns", "snap-a", nil), nil
					}

					if tc.cancelParent {
						cancelParent()
					}

					<-getCtx.Done()

					return nil, getCtx.Err()
				},
			}

			started := time.Now()
			target := snapshotTarget{name: "snap-a"}
			err := waitGone(ctx, dyn, "ns", target, tc.timeout, time.Hour, discardLogger())
			if !errors.Is(err, tc.wantIs) {
				t.Fatalf("waitGone error = %v, want errors.Is(_, %v)", err, tc.wantIs)
			}

			if tc.wantContains != "" && !strings.Contains(err.Error(), tc.wantContains) {
				t.Fatalf("waitGone error = %q, want substring %q", err, tc.wantContains)
			}

			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("waitGone returned after %s, want at most 1s", elapsed)
			}
		})
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
