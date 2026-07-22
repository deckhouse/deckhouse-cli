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

import "testing"

// TestIsDegradedReason verifies every DegradedReadyReasons catalog member classifies as
// degraded and an arbitrary other reason does not.
func TestIsDegradedReason(t *testing.T) {
	t.Parallel()

	for _, reason := range DegradedReadyReasons {
		reason := reason

		t.Run("degraded: "+reason, func(t *testing.T) {
			t.Parallel()

			if !IsDegradedReason(reason) {
				t.Errorf("IsDegradedReason(%q) = false, want true", reason)
			}
		})
	}

	tests := []struct {
		name   string
		reason string
		want   bool
	}{
		{name: "not degraded: terminal reason", reason: "ChildSnapshotLost", want: false},
		{name: "not degraded: empty reason", reason: "", want: false},
		{name: "not degraded: unrelated reason", reason: "ArtifactMissing", want: false},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := IsDegradedReason(tt.reason); got != tt.want {
				t.Errorf("IsDegradedReason(%q) = %v, want %v", tt.reason, got, tt.want)
			}
		})
	}
}
