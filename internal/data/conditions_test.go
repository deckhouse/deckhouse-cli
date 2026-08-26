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

package dataio

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func cond(condType string, status metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{
		Type:    condType,
		Status:  status,
		Reason:  reason,
		Message: reason,
	}
}

// TestIsExpired covers both producers' spellings of expiry plus the states that must not be read as
// expiry. Reading only one spelling is not a cosmetic bug: the caller keeps polling an object the
// producer will not revive, for as long as that producer's retention TTL lasts.
func TestIsExpired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		conditions []metav1.Condition
		want       bool
	}{
		{
			name:       "storage-foundation spelling: Ready=False with reason Expired",
			conditions: []metav1.Condition{cond(ConditionTypeReady, metav1.ConditionFalse, ReasonExpired)},
			want:       true,
		},
		{
			// The importer pod raises this before the controller mirrors it onto Ready, so a real
			// object passes through exactly this pairing.
			name: "older producer spelling: standalone Expired=True while Ready is still True",
			conditions: []metav1.Condition{
				cond(ConditionTypeExpired, metav1.ConditionTrue, ReasonExpired),
				cond(ConditionTypeReady, metav1.ConditionTrue, "PodReady"),
			},
			want: true,
		},
		{
			name: "older producer, after the controller mirrored it",
			conditions: []metav1.Condition{
				cond(ConditionTypeExpired, metav1.ConditionTrue, ReasonExpired),
				cond(ConditionTypeReady, metav1.ConditionFalse, ReasonExpired),
			},
			want: true,
		},
		{
			name:       "an Expired condition that is False says the object has not expired",
			conditions: []metav1.Condition{cond(ConditionTypeExpired, metav1.ConditionFalse, "Pending")},
			want:       false,
		},
		{
			// Guards against too broad a predicate: a not-Ready object is not an expired one, and
			// recreating it would destroy an import that was merely still working.
			name:       "Ready=False for another reason is not expiry",
			conditions: []metav1.Condition{cond(ConditionTypeReady, metav1.ConditionFalse, "Completed")},
			want:       false,
		},
		{
			name:       "healthy object",
			conditions: []metav1.Condition{cond(ConditionTypeReady, metav1.ConditionTrue, "PodReady")},
			want:       false,
		},
		{
			name:       "no conditions at all",
			conditions: nil,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsExpired(tt.conditions))
		})
	}
}

// TestNotReady pins the distinction between "reported as not ready" and "has not reported yet". An
// object carrying no Ready condition has not been reconciled, and treating that as a failure turns
// the first poll of a freshly created object into an error.
func TestNotReady(t *testing.T) {
	t.Parallel()

	t.Run("absent Ready condition is not a failure", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, NotReady(nil))
		assert.Nil(t, NotReady([]metav1.Condition{cond(ConditionTypeExpired, metav1.ConditionFalse, "Pending")}))
	})

	t.Run("Ready=True is not a failure", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, NotReady([]metav1.Condition{cond(ConditionTypeReady, metav1.ConditionTrue, "PodReady")}))
	})

	t.Run("Ready=False is returned with its reason", func(t *testing.T) {
		t.Parallel()

		got := NotReady([]metav1.Condition{cond(ConditionTypeReady, metav1.ConditionFalse, "TargetNotFound")})
		require.NotNil(t, got)
		assert.Equal(t, "TargetNotFound", got.Reason)
	})

	t.Run("Ready=Unknown is returned too", func(t *testing.T) {
		t.Parallel()

		got := NotReady([]metav1.Condition{cond(ConditionTypeReady, metav1.ConditionUnknown, "Pending")})
		require.NotNil(t, got)
		assert.Equal(t, "Pending", got.Reason)
	})
}
