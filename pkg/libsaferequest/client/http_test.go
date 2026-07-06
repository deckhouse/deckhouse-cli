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

package client

import (
	"testing"
)

// TestSafeClient_SetQPS asserts that SetQPS mutates the underlying rest.Config's
// QPS/Burst fields to exactly the values passed, and that RESTConfig() (the deep
// copy callers use to build their own clients, e.g. the aggregated-API client)
// reflects them.
func TestSafeClient_SetQPS(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetQPS(50, 100)

	got := sc.RESTConfig()
	if got.QPS != 50 {
		t.Errorf("RESTConfig().QPS = %v, want 50", got.QPS)
	}

	if got.Burst != 100 {
		t.Errorf("RESTConfig().Burst = %d, want 100", got.Burst)
	}
}

// TestSafeClient_SetQPS_DefaultUnchangedWithoutCall asserts that a SafeClient
// which never calls SetQPS leaves rest.Config's QPS/Burst at their unset zero
// value (client-go's own client constructors substitute rest.DefaultQPS/
// DefaultBurst for a zero value at request time) — SetQPS must be strictly
// opt-in per caller, not a change to NewSafeClient's own default, so commands
// that never call it are unaffected.
func TestSafeClient_SetQPS_DefaultUnchangedWithoutCall(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	got := sc.RESTConfig()
	if got.QPS != 0 {
		t.Errorf("RESTConfig().QPS = %v, want 0 (unset) when SetQPS was never called", got.QPS)
	}

	if got.Burst != 0 {
		t.Errorf("RESTConfig().Burst = %d, want 0 (unset) when SetQPS was never called", got.Burst)
	}
}
