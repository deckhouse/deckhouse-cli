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
	"crypto/tls"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
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

// testCACertificatePEM spins up a throwaway TLS server and returns its leaf
// certificate PEM-encoded, giving tests a syntactically valid CA bundle
// without depending on a real one.
func testCACertificatePEM(t *testing.T) []byte {
	t.Helper()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	t.Cleanup(srv.Close)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
}

// stubRoundTripper is a non-*http.Transport RoundTripper used to exercise the
// pass-through branch of SetTLSCAData's WrapTransport wrapper.
type stubRoundTripper struct{}

// RoundTrip always fails; stubRoundTripper is never actually used to send a
// request in these tests.
func (stubRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	return nil, http.ErrNotSupported
}

func TestSafeClient_SetTLSCAData_ForcesVerification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		caData       []byte
		presetCAData []byte
	}{
		{name: "nil CA data", caData: nil},
		{name: "valid CA data", caData: testCACertificatePEM(t)},
		{name: "kubeconfig CA data", caData: nil, presetCAData: testCACertificatePEM(t)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := &SafeClient{restConfig: &rest.Config{
				TLSClientConfig: rest.TLSClientConfig{
					Insecure:   true,
					ServerName: "api.example",
					CAData:     tc.presetCAData,
				},
			}}

			sc.SetTLSCAData(tc.caData)

			if sc.restConfig.TLSClientConfig.Insecure {
				t.Error("TLSClientConfig.Insecure = true, want false")
			}

			if sc.restConfig.TLSClientConfig.ServerName != "" {
				t.Errorf("TLSClientConfig.ServerName = %q, want empty", sc.restConfig.TLSClientConfig.ServerName)
			}

			if sc.restConfig.TLSClientConfig.CAData != nil {
				t.Errorf("TLSClientConfig.CAData = %v, want nil", sc.restConfig.TLSClientConfig.CAData)
			}

			if sc.restConfig.TLSClientConfig.CAFile != "" {
				t.Errorf("TLSClientConfig.CAFile = %q, want empty", sc.restConfig.TLSClientConfig.CAFile)
			}

			// exercising the inherited-insecure bypass this test guards against
			orig := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true, ServerName: "api.example"}}

			wrapped := sc.restConfig.WrapTransport(orig)

			clonedTransport, ok := wrapped.(*http.Transport)
			if !ok {
				t.Fatalf("wrapped transport is %T, want *http.Transport", wrapped)
			}

			if clonedTransport.TLSClientConfig.InsecureSkipVerify {
				t.Error("cloned TLSClientConfig.InsecureSkipVerify = true, want false")
			}

			if clonedTransport.TLSClientConfig.ServerName != "" {
				t.Errorf("cloned TLSClientConfig.ServerName = %q, want empty", clonedTransport.TLSClientConfig.ServerName)
			}

			if clonedTransport.TLSClientConfig.RootCAs == nil {
				t.Error("cloned TLSClientConfig.RootCAs = nil, want non-nil")
			}

			if !orig.TLSClientConfig.InsecureSkipVerify {
				t.Error("orig.TLSClientConfig.InsecureSkipVerify was mutated, want unchanged (true)")
			}

			if orig.TLSClientConfig.ServerName != "api.example" {
				t.Errorf("orig.TLSClientConfig.ServerName = %q, want unchanged (%q)", orig.TLSClientConfig.ServerName, "api.example")
			}
		})
	}
}

func TestSafeClient_SetTLSCAData_PassThroughNonTransport(t *testing.T) {
	t.Parallel()

	sc := NewSafeClientForConfig(&rest.Config{})
	sc.SetTLSCAData(nil)

	got := sc.restConfig.WrapTransport(stubRoundTripper{})
	if got == nil {
		t.Fatal("wrapped RoundTripper = nil, want non-nil")
	}

	stub, ok := got.(stubRoundTripper)
	if !ok {
		t.Fatalf("wrapped RoundTripper is %T, want stubRoundTripper", got)
	}

	if stub != (stubRoundTripper{}) {
		t.Error("wrapped RoundTripper is not the same stubRoundTripper instance")
	}
}

func TestSafeClient_SetTLSCAData_ChainsExistingWrapTransport(t *testing.T) {
	t.Parallel()

	t.Run("success: prior wrapper and CA injection both survive", func(t *testing.T) {
		t.Parallel()

		called := false
		sc := NewSafeClientForConfig(&rest.Config{})
		sc.restConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
			called = true

			transport, ok := rt.(*http.Transport)
			if !ok {
				return rt
			}

			clonedTransport := transport.Clone()
			clonedTransport.ResponseHeaderTimeout = 42 * time.Millisecond

			return clonedTransport
		}

		sc.SetTLSCAData(nil)

		wrapped := sc.restConfig.WrapTransport(&http.Transport{})

		if !called {
			t.Error("prior WrapTransport was not called")
		}

		clonedTransport, ok := wrapped.(*http.Transport)
		if !ok {
			t.Fatalf("wrapped transport is %T, want *http.Transport", wrapped)
		}

		if clonedTransport.ResponseHeaderTimeout != 42*time.Millisecond {
			t.Errorf("ResponseHeaderTimeout = %v, want %v", clonedTransport.ResponseHeaderTimeout, 42*time.Millisecond)
		}

		if clonedTransport.TLSClientConfig == nil || clonedTransport.TLSClientConfig.RootCAs == nil {
			t.Error("TLSClientConfig.RootCAs = nil, want non-nil")
		}
	})

	t.Run("success: prior wrapper returning a non-transport degrades to pass-through", func(t *testing.T) {
		t.Parallel()

		sc := NewSafeClientForConfig(&rest.Config{})
		sc.restConfig.WrapTransport = func(_ http.RoundTripper) http.RoundTripper {
			return stubRoundTripper{}
		}

		sc.SetTLSCAData(nil)

		got := sc.restConfig.WrapTransport(&http.Transport{})
		if got == nil {
			t.Fatal("wrapped RoundTripper = nil, want non-nil")
		}

		if _, ok := got.(stubRoundTripper); !ok {
			t.Fatalf("wrapped RoundTripper is %T, want stubRoundTripper", got)
		}
	})
}

func TestSafeClient_SetTLSCAData_ClonesTransport(t *testing.T) {
	t.Parallel()

	sc := NewSafeClientForConfig(&rest.Config{})
	sc.SetTLSCAData(nil)

	orig := &http.Transport{}

	wrapped := sc.restConfig.WrapTransport(orig)

	clonedTransport, ok := wrapped.(*http.Transport)
	if !ok {
		t.Fatalf("wrapped transport is %T, want *http.Transport", wrapped)
	}

	if clonedTransport == orig {
		t.Error("wrapped transport is the same pointer as orig, want a clone")
	}

	if clonedTransport.TLSClientConfig == nil {
		t.Fatal("cloned TLSClientConfig = nil, want non-nil")
	}

	if clonedTransport.TLSClientConfig.RootCAs == nil {
		t.Error("cloned TLSClientConfig.RootCAs = nil, want non-nil")
	}

	// http.Transport.Clone() itself lazily initializes the receiver's
	// TLSClientConfig (nil -> non-nil) as an unrelated HTTP/2 auto-configuration
	// side effect (net/http's http2configureTransports), independent of our fix;
	// the invariant this test guards is that our own code never writes the
	// merged CA pool onto orig, only onto the returned clone.
	if clonedTransport.TLSClientConfig == orig.TLSClientConfig {
		t.Error("cloned TLSClientConfig is the same pointer as orig.TLSClientConfig, want a distinct clone")
	}

	if orig.TLSClientConfig != nil && orig.TLSClientConfig.RootCAs != nil {
		t.Error("orig.TLSClientConfig.RootCAs was mutated, want nil (RootCAs must only be set on the clone)")
	}
}
