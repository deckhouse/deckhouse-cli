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
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
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

// TestSafeClient_SetResponseHeaderTimeout_AppliesToTransport asserts that
// SetResponseHeaderTimeout installs a WrapTransport that sets exactly the given
// ResponseHeaderTimeout on the transport rest.HTTPClientFor would build.
func TestSafeClient_SetResponseHeaderTimeout_AppliesToTransport(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetResponseHeaderTimeout(25 * time.Millisecond)

	wrap := sc.RESTConfig().WrapTransport
	if wrap == nil {
		t.Fatal("WrapTransport is nil after SetResponseHeaderTimeout")
	}

	wrapped, ok := wrap(&http.Transport{}).(*http.Transport)
	if !ok {
		t.Fatal("wrapped transport is not an *http.Transport")
	}

	if wrapped.ResponseHeaderTimeout != 25*time.Millisecond {
		t.Errorf("ResponseHeaderTimeout = %v, want 25ms", wrapped.ResponseHeaderTimeout)
	}
}

// TestSafeClient_SetResponseHeaderTimeout_ChainsExistingWrapTransport asserts
// that SetResponseHeaderTimeout composes with an already-installed WrapTransport
// (SetTLSCAData's CA injection) instead of clobbering it: both the RootCAs pool
// and the response-header timeout must survive.
func TestSafeClient_SetResponseHeaderTimeout_ChainsExistingWrapTransport(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetTLSCAData(nil)
	sc.SetResponseHeaderTimeout(15 * time.Millisecond)

	wrapped, ok := sc.RESTConfig().WrapTransport(&http.Transport{}).(*http.Transport)
	if !ok {
		t.Fatal("wrapped transport is not an *http.Transport")
	}

	if wrapped.ResponseHeaderTimeout != 15*time.Millisecond {
		t.Errorf("ResponseHeaderTimeout = %v, want 15ms", wrapped.ResponseHeaderTimeout)
	}

	if wrapped.TLSClientConfig == nil || wrapped.TLSClientConfig.RootCAs == nil {
		t.Error("chained CA WrapTransport lost: RootCAs is nil")
	}
}

// TestSafeClient_ResponseHeaderTimeout_DefaultUnchangedWithoutCall asserts that a
// SafeClient which never calls SetResponseHeaderTimeout leaves WrapTransport
// unset, so every other libsaferequest consumer is unaffected (opt-in only).
func TestSafeClient_ResponseHeaderTimeout_DefaultUnchangedWithoutCall(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	if sc.RESTConfig().WrapTransport != nil {
		t.Error("WrapTransport must be nil when SetResponseHeaderTimeout was never called")
	}
}

// stubRoundTripper is a non-*http.Transport http.RoundTripper used to exercise
// the WrapTransport pass-through branch.
type stubRoundTripper struct{}

func (stubRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, http.ErrNotSupported
}

// TestSafeClient_SetTLSCAData_ClonesTransport asserts that SetTLSCAData's
// WrapTransport clones a real *http.Transport and injects the CA pool as
// RootCAs, leaving the original transport untouched.
func TestSafeClient_SetTLSCAData_ClonesTransport(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetTLSCAData(nil)

	orig := &http.Transport{}

	got := sc.RESTConfig().WrapTransport(orig)

	wrapped, ok := got.(*http.Transport)
	if !ok {
		t.Fatalf("wrapped transport is not an *http.Transport: %T", got)
	}

	if wrapped == orig {
		t.Error("expected a distinct clone, got the original transport")
	}

	if wrapped.TLSClientConfig == nil || wrapped.TLSClientConfig.RootCAs == nil {
		t.Error("RootCAs not populated on cloned transport")
	}
}

// TestSafeClient_SetTLSCAData_PassThroughNonTransport asserts that when
// WrapTransport receives a RoundTripper that is not an *http.Transport it
// returns the SAME instance unchanged (non-nil), instead of a typed-nil
// *http.Transport that would nil-panic on RoundTrip.
func TestSafeClient_SetTLSCAData_PassThroughNonTransport(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetTLSCAData(nil)

	stub := stubRoundTripper{}

	got := sc.RESTConfig().WrapTransport(stub)
	if got == nil {
		t.Fatal("WrapTransport returned nil for a non-*http.Transport RoundTripper")
	}

	gotStub, ok := got.(stubRoundTripper)
	if !ok {
		t.Fatalf("expected the original stubRoundTripper back, got %T", got)
	}

	if gotStub != stub {
		t.Error("expected the same stub RoundTripper instance to be returned unchanged")
	}
}

// TestSafeClient_SetResponseHeaderTimeout_FailsFastOnHeaderStall asserts the
// configured transport aborts a request whose server accepts the connection but
// never sends response headers, within the response-header timeout.
func TestSafeClient_SetResponseHeaderTimeout_FailsFastOnHeaderStall(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetResponseHeaderTimeout(75 * time.Millisecond)

	rt, ok := sc.RESTConfig().WrapTransport(&http.Transport{}).(*http.Transport)
	if !ok {
		t.Fatal("wrapped transport is not an *http.Transport")
	}

	cl := &http.Client{Transport: rt}

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	start := time.Now()

	resp, err := cl.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected a response-header timeout error, got nil")
	}

	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("did not fail fast on header stall: %v", elapsed)
	}
}

func TestPersistentHTTPClient_ReusesAndClosesConnections(t *testing.T) {
	t.Parallel()

	var (
		newConnections    atomic.Int64
		closedConnections atomic.Int64
	)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			newConnections.Add(1)
		case http.StateClosed:
			closedConnections.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)

	sc := &SafeClient{restConfig: &rest.Config{}}

	httpClient, err := sc.NewPersistentHTTPClient()
	if err != nil {
		t.Fatalf("NewPersistentHTTPClient: %v", err)
	}

	const requestCount = 100

	for range requestCount {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		if reqErr != nil {
			t.Fatalf("NewRequestWithContext: %v", reqErr)
		}

		resp, doErr := httpClient.Do(req)
		if doErr != nil {
			t.Fatalf("Do: %v", doErr)
		}

		if _, copyErr := io.Copy(io.Discard, resp.Body); copyErr != nil {
			_ = resp.Body.Close()
			t.Fatalf("drain response body: %v", copyErr)
		}

		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Fatalf("close response body: %v", closeErr)
		}
	}

	if got := newConnections.Load(); got > 2 {
		t.Fatalf("new connections = %d for %d requests, want <= 2", got, requestCount)
	}

	httpClient.CloseIdleConnections()

	requireEventually(t, time.Second, func() bool {
		return closedConnections.Load() == newConnections.Load()
	})
}

func TestPersistentHTTPClient_PreservesAuthenticationWrappers(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(t *testing.T, config *rest.Config)
		wantHeader string
	}{
		{
			name: "bearer",
			configure: func(_ *testing.T, config *rest.Config) {
				config.BearerToken = "bearer-token"
			},
			wantHeader: "Bearer bearer-token",
		},
		{
			name: "basic",
			configure: func(_ *testing.T, config *rest.Config) {
				config.Username = "user"
				config.Password = "password"
			},
			wantHeader: "Basic dXNlcjpwYXNzd29yZA==",
		},
		{
			name: "exec",
			configure: func(_ *testing.T, config *rest.Config) {
				config.ExecProvider = &clientcmdapi.ExecConfig{
					APIVersion:      "client.authentication.k8s.io/v1beta1",
					Command:         "sh",
					Args:            []string{"-c", `printf '{"apiVersion":"client.authentication.k8s.io/v1beta1","kind":"ExecCredential","status":{"token":"exec-token"}}'`},
					InteractiveMode: clientcmdapi.NeverExecInteractiveMode,
				}
			},
			wantHeader: "Bearer exec-token",
		},
		{
			name: "auth provider",
			configure: func(t *testing.T, config *rest.Config) {
				t.Helper()

				const pluginName = "deckhouse-cli-persistent-http-test"

				err := rest.RegisterAuthProviderPlugin(pluginName, func(
					_ string,
					_ map[string]string,
					_ rest.AuthProviderConfigPersister,
				) (rest.AuthProvider, error) {
					return persistentTestAuthProvider{}, nil
				})
				if err != nil {
					t.Fatalf("RegisterAuthProviderPlugin: %v", err)
				}

				config.AuthProvider = &clientcmdapi.AuthProviderConfig{Name: pluginName}
			},
			wantHeader: "Bearer provider-token",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotHeader string

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotHeader = r.Header.Get("Authorization")
				w.WriteHeader(http.StatusNoContent)
			}))
			defer srv.Close()

			config := &rest.Config{Host: srv.URL}
			tc.configure(t, config)

			sc := &SafeClient{restConfig: config}

			httpClient, err := sc.NewPersistentHTTPClient()
			if err != nil {
				t.Fatalf("NewPersistentHTTPClient: %v", err)
			}
			defer httpClient.CloseIdleConnections()

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
			if err != nil {
				t.Fatalf("NewRequestWithContext: %v", err)
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}

			if closeErr := resp.Body.Close(); closeErr != nil {
				t.Fatalf("close response body: %v", closeErr)
			}

			if gotHeader != tc.wantHeader {
				t.Errorf("Authorization = %q, want %q", gotHeader, tc.wantHeader)
			}
		})
	}
}

func TestPersistentHTTPClient_PreservesCertificateAuth(t *testing.T) {
	var receivedClientCertificate atomic.Bool

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedClientCertificate.Store(len(r.TLS.PeerCertificates) > 0)
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.TLS = &tls.Config{
		ClientAuth: tls.RequireAnyClientCert,
		MinVersion: tls.VersionTLS12,
	}
	srv.StartTLS()
	defer srv.Close()

	serverCertificate := srv.TLS.Certificates[0]
	certData := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertificate.Certificate[0]})

	privateKey, err := x509.MarshalPKCS8PrivateKey(serverCertificate.PrivateKey)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}

	keyData := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKey})
	sc := &SafeClient{restConfig: &rest.Config{
		Host: srv.URL,
		TLSClientConfig: rest.TLSClientConfig{
			CertData: certData,
			KeyData:  keyData,
		},
	}}
	sc.SetTLSCAData(certData)

	httpClient, err := sc.NewPersistentHTTPClient()
	if err != nil {
		t.Fatalf("NewPersistentHTTPClient: %v", err)
	}
	defer httpClient.CloseIdleConnections()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	if closeErr := resp.Body.Close(); closeErr != nil {
		t.Fatalf("close response body: %v", closeErr)
	}

	if !receivedClientCertificate.Load() {
		t.Error("server did not receive the configured client certificate")
	}
}

func TestPersistentHTTPClient_PreservesProxyAndDial(t *testing.T) {
	t.Parallel()

	var (
		proxyCalls atomic.Int64
		dialCalls  atomic.Int64
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	dialer := &net.Dialer{}
	sc := &SafeClient{restConfig: &rest.Config{
		Host: srv.URL,
		Proxy: func(*http.Request) (*url.URL, error) {
			proxyCalls.Add(1)

			return nil, nil
		},
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			dialCalls.Add(1)

			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, fmt.Errorf("dial test server: %w", err)
			}

			return conn, nil
		},
	}}

	httpClient, err := sc.NewPersistentHTTPClient()
	if err != nil {
		t.Fatalf("NewPersistentHTTPClient: %v", err)
	}
	defer httpClient.CloseIdleConnections()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	if closeErr := resp.Body.Close(); closeErr != nil {
		t.Fatalf("close response body: %v", closeErr)
	}

	if proxyCalls.Load() == 0 {
		t.Error("configured proxy function was not called")
	}

	if dialCalls.Load() == 0 {
		t.Error("configured dial function was not called")
	}
}

type persistentTestAuthProvider struct{}

func (persistentTestAuthProvider) WrapTransport(rt http.RoundTripper) http.RoundTripper {
	return persistentTestAuthRoundTripper{rt: rt}
}

func (persistentTestAuthProvider) Login() error {
	return nil
}

type persistentTestAuthRoundTripper struct {
	rt http.RoundTripper
}

func (rt persistentTestAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.Header.Set("Authorization", "Bearer provider-token")

	return rt.rt.RoundTrip(cloned)
}

func requireEventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition was not satisfied before timeout")
}
