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
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// snapWithReady builds a Snapshot with a single Ready condition (status/reason).
func snapWithReady(name, status, reason string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "state-snapshotter.deckhouse.io/v1alpha1",
		"kind":       "Snapshot",
		"metadata":   map[string]interface{}{"namespace": targetNS, "name": name},
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": status, "reason": reason, "message": "m"},
			},
		},
	}}
}

// snapWithPhase adds status.captureState.domainSpecificController.phase to a Ready snapshot.
func snapWithPhase(name, status, reason, phase string) *unstructured.Unstructured {
	obj := snapWithReady(name, status, reason)
	if err := unstructured.SetNestedField(obj.Object, phase,
		"status", "captureState", "domainSpecificController", "phase"); err != nil {
		panic(err)
	}

	return obj
}

// waitCfg builds a minimal Config sufficient for waitNamespacedReady.
func waitCfg(dyn *dynamicfake.FakeDynamicClient, timeout time.Duration) Config {
	return Config{
		Namespace:    targetNS,
		Dynamic:      dyn,
		Log:          discardLogger(),
		PollInterval: time.Millisecond,
		Timeout:      timeout,
	}
}

// TestWaitNamespacedReady_ReadyTrue verifies the success path: Ready=True returns nil.
func TestWaitNamespacedReady_ReadyTrue(t *testing.T) {
	dyn := newFakeDynamic(snapWithReady("n1", "True", ""))

	if err := waitNamespacedReady(context.Background(), waitCfg(dyn, 2*time.Second), snapshotGVR, "n1", snapshotKind); err != nil {
		t.Fatalf("waitNamespacedReady with Ready=True: %v", err)
	}
}

// TestWaitNamespacedReady_PhaseFailed verifies the monotonic terminal sink:
// captureState.domainSpecificController.phase=Failed is an immediate error even when the
// Ready reason itself is non-terminal, and it must not wait out the timeout.
func TestWaitNamespacedReady_PhaseFailed(t *testing.T) {
	dyn := newFakeDynamic(snapWithPhase("n1", "False", "DataCapturePending", capturePhaseFailed))

	start := time.Now()
	err := waitNamespacedReady(context.Background(), waitCfg(dyn, 10*time.Second), snapshotGVR, "n1", snapshotKind)

	if err == nil {
		t.Fatal("expected immediate error on phase=Failed, got nil")
	}

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("phase=Failed must fail fast, took %v (waited out the timeout)", elapsed)
	}

	if !strings.Contains(err.Error(), "phase=Failed") {
		t.Errorf("error should mention phase=Failed, got: %v", err)
	}
}

// TestWaitNamespacedReady_TerminalReason verifies that Ready=False with a terminal reason
// (both the normative enum and the import-leaf terminals outside it) is an immediate error.
func TestWaitNamespacedReady_TerminalReason(t *testing.T) {
	for _, reason := range []string{
		"ChildrenFailed",
		"VolumeCaptureFailed",
		"GraphPlanningFailed",
		"DataImportAmbiguous",
		"DataArtifactInvalid",
	} {
		t.Run(reason, func(t *testing.T) {
			dyn := newFakeDynamic(snapWithReady("n1", "False", reason))

			start := time.Now()
			err := waitNamespacedReady(context.Background(), waitCfg(dyn, 10*time.Second), snapshotGVR, "n1", snapshotKind)

			if err == nil {
				t.Fatalf("expected immediate error on terminal reason %q, got nil", reason)
			}

			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Errorf("terminal reason %q must fail fast, took %v", reason, elapsed)
			}

			if !strings.Contains(err.Error(), reason) {
				t.Errorf("error should mention reason %q, got: %v", reason, err)
			}
		})
	}
}

// TestWaitNamespacedReady_NonTerminalWaitsThenTimeout verifies that Ready=False with a
// non-terminal reason (e.g. ChildrenPending) is NOT treated as an error: the wait continues
// and eventually reports a timeout, surfacing the last Ready status/reason.
func TestWaitNamespacedReady_NonTerminalWaitsThenTimeout(t *testing.T) {
	dyn := newFakeDynamic(snapWithReady("n1", "False", "ChildrenPending"))

	timeout := 40 * time.Millisecond

	start := time.Now()
	err := waitNamespacedReady(context.Background(), waitCfg(dyn, timeout), snapshotGVR, "n1", snapshotKind)

	if err == nil {
		t.Fatal("expected timeout error for a non-terminal Ready=False, got nil")
	}

	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("non-terminal Ready=False must produce a timeout error, got: %v", err)
	}

	if !strings.Contains(err.Error(), "ChildrenPending") {
		t.Errorf("timeout error should surface the last Ready reason, got: %v", err)
	}

	if elapsed := time.Since(start); elapsed < timeout {
		t.Errorf("must wait at least the timeout (%v) before giving up, waited %v", timeout, elapsed)
	}
}

// TestIsTerminalReadyReason verifies terminal-reason classification, including that unknown
// and non-terminal reasons stay non-terminal (resolved by timeout, never a false error).
func TestIsTerminalReadyReason(t *testing.T) {
	cases := map[string]bool{
		"ChildrenFailed":      true,
		"VolumeCaptureFailed": true,
		"ChildSnapshotLost":   true,
		"DataImportAmbiguous": true,
		"DataArtifactInvalid": true,
		"ChildrenPending":     false,
		"DataCapturePending":  false,
		"SomeUnknownReason":   false,
		"":                    false,
	}

	for reason, want := range cases {
		if got := isTerminalReadyReason(reason); got != want {
			t.Errorf("isTerminalReadyReason(%q) = %v, want %v", reason, got, want)
		}
	}
}
