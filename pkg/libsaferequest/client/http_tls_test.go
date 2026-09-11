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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/client-go/rest"
)

// testCACertificatePEM spins up a throwaway TLS server and returns its leaf
// certificate PEM-encoded, giving tests a syntactically valid CA bundle
// without depending on a real one.
func testCACertificatePEM(t *testing.T) []byte {
	t.Helper()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	t.Cleanup(srv.Close)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
}

// testSelfSignedCACertificatePEM returns a freshly generated, PEM-encoded
// self-signed certificate distinct from testCACertificatePEM's fixed
// httptest leaf, so a test can tell the two CA sources apart in an
// x509.CertPool instead of comparing byte-identical data.
func testSelfSignedCACertificatePEM(t *testing.T) []byte {
	t.Helper()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-kubeconfig-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, priv.Public(), priv)
	if err != nil {
		t.Fatalf("create self-signed CA certificate: %v", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
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

	if _, ok := got.(stubRoundTripper); !ok {
		t.Fatalf("wrapped RoundTripper is %T, want stubRoundTripper", got)
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
		{name: "second call does not recurse and still forces verification"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			explicitCAData := testCACertificatePEM(t)

			// Simulates a CA already present on the rest.Config (e.g. from
			// kubeconfig) before either SetTLSCAData call, distinct from
			// explicitCAData so the two are distinguishable in the resulting pool.
			kubeconfigCAData := testSelfSignedCACertificatePEM(t)

			sc := NewSafeClientForConfig(&rest.Config{
				TLSClientConfig: rest.TLSClientConfig{CAData: kubeconfigCAData},
			})

			sc.SetTLSCAData(explicitCAData)
			sc.SetTLSCAData(explicitCAData)

			// Guards against prev-chaining turning self-referential: if the second
			// call's WrapTransport captured itself as prev instead of the first
			// call's closure, invoking it here would recurse until stack overflow.
			done := make(chan http.RoundTripper, 1)

			go func() {
				done <- sc.restConfig.WrapTransport(&http.Transport{})
			}()

			var clonedTransport *http.Transport

			select {
			case wrapped := <-done:
				var ok bool

				clonedTransport, ok = wrapped.(*http.Transport)
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

			// The first call folds kubeconfigCAData into its pool and then clears
			// TLSClientConfig.CAData (see SetTLSCAData), so by the time the second
			// call builds its own sysPool, CAData is already nil and
			// kubeconfigCAData is not folded in again. The second call's
			// WrapTransport then clones over the first call's cloned transport and
			// overwrites RootCAs wholesale rather than merging it with the first
			// call's pool. The net effect: the final RootCAs traces only from the
			// second SetTLSCAData(explicitCAData) call — identical to what a single,
			// standalone call with the same explicitCAData would produce, and
			// without kubeconfigCAData ever having survived into it.
			soloSC := NewSafeClientForConfig(&rest.Config{})
			soloSC.SetTLSCAData(explicitCAData)

			soloWrapped := soloSC.restConfig.WrapTransport(&http.Transport{})

			soloTransport, ok := soloWrapped.(*http.Transport)
			if !ok {
				t.Fatalf("solo wrapped transport is %T, want *http.Transport", soloWrapped)
			}

			if !clonedTransport.TLSClientConfig.RootCAs.Equal(soloTransport.TLSClientConfig.RootCAs) {
				t.Error("chained second-call RootCAs != solo second-call RootCAs; kubeconfig CA leaked into the final pool")
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
