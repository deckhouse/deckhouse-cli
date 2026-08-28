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

func TestSafeClient_SetTLSCAData_InvalidCAData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		caData []byte
	}{
		{name: "success: garbage bytes do not panic and verification stays forced on", caData: []byte("not a certificate")},
		{name: "success: empty non-nil slice does not panic and verification stays forced on", caData: []byte{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := &SafeClient{restConfig: &rest.Config{
				TLSClientConfig: rest.TLSClientConfig{
					Insecure:   true,
					ServerName: "api.example",
				},
			}}

			sc.SetTLSCAData(tc.caData)

			if sc.restConfig.TLSClientConfig.Insecure {
				t.Error("TLSClientConfig.Insecure = true, want false")
			}

			if sc.restConfig.TLSClientConfig.ServerName != "" {
				t.Errorf("TLSClientConfig.ServerName = %q, want empty", sc.restConfig.TLSClientConfig.ServerName)
			}

			wrapped := sc.restConfig.WrapTransport(&http.Transport{})

			clonedTransport, ok := wrapped.(*http.Transport)
			if !ok {
				t.Fatalf("wrapped transport is %T, want *http.Transport", wrapped)
			}

			if clonedTransport.TLSClientConfig.InsecureSkipVerify {
				t.Error("cloned TLSClientConfig.InsecureSkipVerify = true, want false")
			}

			if clonedTransport.TLSClientConfig.RootCAs == nil {
				t.Error("cloned TLSClientConfig.RootCAs = nil, want non-nil (system pool at minimum)")
			}
		})
	}
}

func TestSafeClient_SetTLSCAData_PreservesClientCertConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "success: CertData/KeyData/CertFile/KeyFile survive untouched"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := &SafeClient{restConfig: &rest.Config{
				TLSClientConfig: rest.TLSClientConfig{
					Insecure: true,
					CertData: []byte("client-cert"),
					KeyData:  []byte("client-key"),
					CertFile: "/etc/certs/client.crt",
					KeyFile:  "/etc/certs/client.key",
				},
			}}

			sc.SetTLSCAData(nil)

			if string(sc.restConfig.TLSClientConfig.CertData) != "client-cert" {
				t.Errorf("CertData = %q, want unchanged (%q)", sc.restConfig.TLSClientConfig.CertData, "client-cert")
			}

			if string(sc.restConfig.TLSClientConfig.KeyData) != "client-key" {
				t.Errorf("KeyData = %q, want unchanged (%q)", sc.restConfig.TLSClientConfig.KeyData, "client-key")
			}

			if sc.restConfig.TLSClientConfig.CertFile != "/etc/certs/client.crt" {
				t.Errorf("CertFile = %q, want unchanged (%q)", sc.restConfig.TLSClientConfig.CertFile, "/etc/certs/client.crt")
			}

			if sc.restConfig.TLSClientConfig.KeyFile != "/etc/certs/client.key" {
				t.Errorf("KeyFile = %q, want unchanged (%q)", sc.restConfig.TLSClientConfig.KeyFile, "/etc/certs/client.key")
			}
		})
	}
}

func TestSafeClient_SetTLSCAData_CalledTwiceChainsWithoutInfiniteRecursion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "success: second call wraps over the first and both apply their CA pool"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc := NewSafeClientForConfig(&rest.Config{})

			sc.SetTLSCAData(testCACertificatePEM(t))
			sc.SetTLSCAData(testCACertificatePEM(t))

			// Guards against prev-chaining turning self-referential: if the second
			// call's WrapTransport captured itself as prev instead of the first
			// call's closure, invoking it here would recurse until stack overflow.
			done := make(chan http.RoundTripper, 1)

			go func() {
				done <- sc.restConfig.WrapTransport(&http.Transport{})
			}()

			select {
			case wrapped := <-done:
				clonedTransport, ok := wrapped.(*http.Transport)
				if !ok {
					t.Fatalf("wrapped transport is %T, want *http.Transport", wrapped)
				}

				if clonedTransport.TLSClientConfig == nil || clonedTransport.TLSClientConfig.RootCAs == nil {
					t.Error("cloned TLSClientConfig.RootCAs = nil, want non-nil")
				}

				if clonedTransport.TLSClientConfig.InsecureSkipVerify {
					t.Error("cloned TLSClientConfig.InsecureSkipVerify = true, want false")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("WrapTransport did not return within timeout; suspected infinite recursion in chained wrappers")
			}
		})
	}
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
