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
	"time"

	"k8s.io/client-go/rest"
)

func TestNewSafeClientForConfig(t *testing.T) {
	t.Parallel()

	t.Run("success: mutating the derived client leaves the original config untouched", func(t *testing.T) {
		t.Parallel()

		original := &rest.Config{Host: "https://original.example:6443"}

		derived := NewSafeClientForConfig(original)
		derived.SetProbeEndpoint(5*time.Second, "https://probe.example:443", "probe.example")

		if original.Host != "https://original.example:6443" {
			t.Errorf("original.Host = %q, want unchanged", original.Host)
		}

		if original.TLSClientConfig.ServerName != "" {
			t.Errorf("original.TLSClientConfig.ServerName = %q, want unchanged (empty)", original.TLSClientConfig.ServerName)
		}

		if derived.restConfig.Host != "https://probe.example:443" {
			t.Errorf("derived.restConfig.Host = %q, want %q", derived.restConfig.Host, "https://probe.example:443")
		}
	})

	t.Run("error: nil config panics inside rest.CopyConfig", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if recover() == nil {
				t.Fatal("NewSafeClientForConfig(nil) did not panic; rest.CopyConfig no longer dereferences a nil config")
			}
		}()

		NewSafeClientForConfig(nil)
	})
}
