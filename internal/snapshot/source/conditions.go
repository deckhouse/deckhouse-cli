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

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

// readyConditionType is the status condition reporting overall snapshot readiness.
// Duplicated (not shared) from internal/snapshot/cmd/list.readyConditionType and
// internal/snapshot/restore.readyConditionType: both packages already keep their own
// unexported copy of this literal rather than a shared exported constant, so this package
// follows the same established convention instead of introducing a new cross-package export.
const readyConditionType = "Ready"

// DegradedReadyReasons is the canonical set of Ready=False reasons that represent a
// recoverable degradation: capture already completed, the captured data is intact in the
// content-layer recycle bin, and the snapshot degraded without failing outright.
//
// This MUST be kept byte-identical to the SSOT in state-snapshotter
// api/storage/v1alpha1/conditions.go's DegradedReadyReasons. That set is explicitly
// documented there as free to grow, so this slice is not assumed to stay single-member
// forever — re-verify against the sibling repo before relying on its exact contents.
var DegradedReadyReasons = []string{"ChildSnapshotDeleted"}

// IsDegradedReason reports whether a Ready=False reason is a recoverable degradation (capture
// done, data intact, recoverable by manual intervention). Mirrors state-snapshotter's
// api/storage/v1alpha1.IsReasonDegraded.
func IsDegradedReason(reason string) bool {
	for _, r := range DegradedReadyReasons {
		if r == reason {
			return true
		}
	}

	return false
}

// NodeReadyStatus is a node's own Ready condition, read verbatim from the snapshot CR's
// status.conditions entry whose type == "Ready". The zero value (all empty strings) means the
// node carries no Ready condition at all; that is not an error, just an unpopulated status.
type NodeReadyStatus struct {
	// Status is the condition's status field ("True"/"False"/"Unknown").
	Status string
	// Reason is the condition's reason field (CamelCase, e.g. "ChildSnapshotDeleted").
	Reason string
	// Message is the condition's human-readable message field.
	Message string
}

// parseReadyCondition reads the "Ready" entry out of an unstructured object's
// status.conditions slice. It returns the zero NodeReadyStatus when status.conditions is
// absent, malformed, or has no Ready-typed entry — populating Ready status is best-effort
// presentation data, never a reason to fail tree construction.
func parseReadyCondition(obj *unstructured.Unstructured) NodeReadyStatus {
	conds, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return NodeReadyStatus{}
	}

	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}

		condType, _, _ := unstructured.NestedString(m, "type")
		if condType != readyConditionType {
			continue
		}

		status, _, _ := unstructured.NestedString(m, "status")
		reason, _, _ := unstructured.NestedString(m, "reason")
		message, _, _ := unstructured.NestedString(m, "message")

		return NodeReadyStatus{Status: status, Reason: reason, Message: message}
	}

	return NodeReadyStatus{}
}
