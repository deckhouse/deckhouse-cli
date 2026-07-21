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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// conditionTrue reports whether status.conditions[type==condType].status == "True".
func conditionTrue(obj *unstructured.Unstructured, condType string) bool {
	conds, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}

	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}

		t, _, _ := unstructured.NestedString(m, "type")
		if t != condType {
			continue
		}

		status, _, _ := unstructured.NestedString(m, "status")

		return status == string(metav1.ConditionTrue)
	}

	return false
}

// readyConditionState returns the status/reason/message of the Ready condition, or empty
// strings when the object carries no Ready condition yet. The reason drives the terminal-vs-
// pending decision in waitNamespacedReady.
func readyConditionState(obj *unstructured.Unstructured) (string, string, string) {
	conds, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return "", "", ""
	}

	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}

		t, _, _ := unstructured.NestedString(m, "type")
		if t != conditionReady {
			continue
		}

		status, _, _ := unstructured.NestedString(m, "status")
		reason, _, _ := unstructured.NestedString(m, "reason")
		message, _, _ := unstructured.NestedString(m, "message")

		return status, reason, message
	}

	return "", "", ""
}

// domainCapturePhase returns status.captureState.domainSpecificController.phase, or "" when
// absent (e.g. on import-mode objects, which carry no captureState). phase == "Failed" is a
// monotonic terminal sink independent of the (free-form) Ready reason.
func domainCapturePhase(obj *unstructured.Unstructured) string {
	phase, _, _ := unstructured.NestedString(obj.Object, "status", "captureState", "domainSpecificController", "phase")

	return phase
}

// capturePhaseFailed is the monotonic terminal capture phase in
// state-snapshotter status.captureState.domainSpecificController.phase.
const capturePhaseFailed = "Failed"

// terminalReadyReasons mirrors state-snapshotter api/storage/v1alpha1/conditions.go
// TerminalReadyReasons, plus the two import-leaf terminal reasons that live outside that
// enum (genericbinder/import.go). Keep synchronized with the controller; an unknown reason
// stays non-terminal and is resolved by timeout — safer than a false-terminal error.
var terminalReadyReasons = map[string]struct{}{
	// api/storage/v1alpha1/conditions.go TerminalReadyReasons.
	"ListFailed":               {},
	"ManifestCheckpointFailed": {},
	"NamespaceNotFound":        {},
	"VolumeCaptureFailed":      {},
	"DuplicateCoveredPVCUID":   {},
	"ChildrenFailed":           {},
	"GraphPlanningFailed":      {},
	"CreateChildFailed":        {},
	"ChildSnapshotLost":        {},
	// Import-leaf terminals outside the enum (genericbinder/import.go).
	"DataImportAmbiguous": {},
	"DataArtifactInvalid": {},
}

// isTerminalReadyReason reports whether a Ready=False reason is a known terminal signal.
func isTerminalReadyReason(reason string) bool {
	_, ok := terminalReadyReasons[reason]

	return ok
}

// sleepCtx sleeps for d or returns false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
