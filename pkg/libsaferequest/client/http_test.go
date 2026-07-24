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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	utilnet "k8s.io/apimachinery/pkg/util/net"
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

func TestPersistentHTTPClient_IsolatesHTTP2PoolsAndCleanup(t *testing.T) {
	t.Parallel()

	var (
		newConnections    atomic.Int64
		closedConnections atomic.Int64
	)

	activeStarted := make(chan struct{})
	releaseActive := make(chan struct{}, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/active" {
			close(activeStarted)
			<-releaseActive
		}

		_, _ = io.WriteString(w, "ok")
	}))
	srv.EnableHTTP2 = true
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			newConnections.Add(1)
		case http.StateClosed:
			closedConnections.Add(1)
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		select {
		case releaseActive <- struct{}{}:
		default:
		}
	})

	sc := newSharedHTTP2SafeClient(t, srv)
	results := make(chan persistentClientResult, 2)

	for range 2 {
		go func() {
			httpClient, err := newPersistentTestClient(sc, srv, time.Second)
			results <- persistentClientResult{client: httpClient, err: err}
		}()
	}

	clients := make([]*PersistentHTTPClient, 0, 2)
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("NewPersistentHTTPClient: %v", result.err)
		}

		clients = append(clients, result.client)
	}

	t.Cleanup(func() {
		for _, httpClient := range clients {
			httpClient.CloseIdleConnections()
		}
	})

	for _, httpClient := range clients {
		for range 10 {
			protoMajor, err := persistentRequest(context.Background(), httpClient, srv.URL)
			if err != nil {
				t.Fatalf("request: %v", err)
			}

			if protoMajor != 2 {
				t.Fatalf("response protocol major = %d, want HTTP/2", protoMajor)
			}
		}
	}

	if got := newConnections.Load(); got != 2 {
		t.Fatalf("new connections = %d, want one private HTTP/2 connection per client", got)
	}

	activeResult := make(chan error, 1)
	go func() {
		_, err := persistentRequest(context.Background(), clients[1], srv.URL+"/active")
		activeResult <- err
	}()

	<-activeStarted
	clients[0].CloseIdleConnections()

	requireEventually(t, time.Second, func() bool {
		return closedConnections.Load() == 1
	})

	select {
	case err := <-activeResult:
		t.Fatalf("closing the other client disrupted an active HTTP/2 stream: %v", err)
	default:
	}

	releaseActive <- struct{}{}

	if err := <-activeResult; err != nil {
		t.Fatalf("active request after other client cleanup: %v", err)
	}

	protoMajor, err := persistentRequest(context.Background(), clients[1], srv.URL)
	if err != nil {
		t.Fatalf("request through surviving client: %v", err)
	}

	if protoMajor != 2 {
		t.Fatalf("surviving response protocol major = %d, want HTTP/2", protoMajor)
	}

	if got := newConnections.Load(); got != 2 {
		t.Fatalf("surviving client opened a new connection after other client cleanup: %d", got)
	}

	clients[1].CloseIdleConnections()

	requireEventually(t, time.Second, func() bool {
		return closedConnections.Load() == newConnections.Load()
	})
}

func TestPersistentHTTPClient_IsolatesHTTP2ResponseHeaderTimeouts(t *testing.T) {
	t.Parallel()

	shortStarted := make(chan struct{})
	longStarted := make(chan struct{})
	releaseLong := make(chan struct{}, 1)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/short":
			close(shortStarted)
			<-r.Context().Done()
		case "/long":
			close(longStarted)
			<-releaseLong
			_, _ = io.WriteString(w, "ok")
		default:
			http.NotFound(w, r)
		}
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		select {
		case releaseLong <- struct{}{}:
		default:
		}
	})

	sc := newSharedHTTP2SafeClient(t, srv)
	shortClient, err := newPersistentTestClient(sc, srv, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("build short-timeout client: %v", err)
	}
	t.Cleanup(shortClient.CloseIdleConnections)

	longClient, err := newPersistentTestClient(sc, srv, time.Second)
	if err != nil {
		t.Fatalf("build long-timeout client: %v", err)
	}
	t.Cleanup(longClient.CloseIdleConnections)

	longResult := make(chan error, 1)
	go func() {
		protoMajor, requestErr := persistentRequest(context.Background(), longClient, srv.URL+"/long")
		if requestErr == nil && protoMajor != 2 {
			requestErr = fmt.Errorf("response protocol major = %d, want HTTP/2", protoMajor)
		}

		longResult <- requestErr
	}()

	<-longStarted
	start := time.Now()
	_, err = persistentRequest(context.Background(), shortClient, srv.URL+"/short")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("short-timeout HTTP/2 request unexpectedly succeeded")
	}

	var timeoutError net.Error
	if !errors.As(err, &timeoutError) || !timeoutError.Timeout() {
		t.Fatalf("short HTTP/2 request error = %v, want response-header timeout", err)
	}

	if elapsed < 25*time.Millisecond {
		t.Fatalf("short HTTP/2 response-header timeout took only %v", elapsed)
	}

	if elapsed > 500*time.Millisecond {
		t.Fatalf("short HTTP/2 response-header timeout took %v, want under 500ms", elapsed)
	}

	<-shortStarted

	select {
	case err := <-longResult:
		t.Fatalf("short client timeout disrupted long client: %v", err)
	default:
	}

	releaseLong <- struct{}{}

	if err := <-longResult; err != nil {
		t.Fatalf("long-timeout HTTP/2 request: %v", err)
	}
}

func TestTLSIdentityClient_PreservesAuthenticationWrappers(t *testing.T) {
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

			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotHeader = r.Header.Get("Authorization")
				w.WriteHeader(http.StatusNoContent)
			}))
			defer srv.Close()

			config := &rest.Config{Host: srv.URL}
			tc.configure(t, config)

			sc := &SafeClient{restConfig: config}

			if err := sc.SetTLSIdentityCAData(certificatePEM(t, srv)); err != nil {
				t.Fatalf("SetTLSIdentityCAData: %v", err)
			}

			httpClient, err := sc.NewPersistentHTTPSClientForOrigin(srv.URL)
			if err != nil {
				t.Fatalf("NewPersistentHTTPSClientForOrigin: %v", err)
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

func TestTLSIdentityClient_IsolatesConcurrentOriginsAuthenticationAndCARoots(t *testing.T) {
	t.Parallel()

	const requestCount = 50

	type serverState struct {
		token        string
		requests     atomic.Int64
		wrongHeaders atomic.Int64
	}

	newServer := func(state *serverState, serial int64) *httptest.Server {
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			state.requests.Add(1)

			if request.Header.Get("Authorization") != "Bearer "+state.token {
				state.wrongHeaders.Add(1)
				http.Error(writer, "wrong authorization", http.StatusUnauthorized)

				return
			}

			writer.WriteHeader(http.StatusNoContent)
		}))
		server.TLS = &tls.Config{
			Certificates: []tls.Certificate{newPersistentTestCertificate(t, serial)},
			MinVersion:   tls.VersionTLS12,
		}
		server.EnableHTTP2 = true
		server.StartTLS()

		return server
	}

	firstState := &serverState{token: "first-token"}
	secondState := &serverState{token: "second-token"}
	firstServer := newServer(firstState, 1)
	secondServer := newServer(secondState, 2)
	t.Cleanup(firstServer.Close)
	t.Cleanup(secondServer.Close)

	newClient := func(server *httptest.Server, token string) *PersistentHTTPClient {
		t.Helper()

		certificate := server.Certificate()
		if certificate == nil {
			t.Fatal("TLS server has no certificate")
		}

		caData := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
		safeClient := &SafeClient{restConfig: &rest.Config{BearerToken: token}}

		if err := safeClient.SetTLSIdentityCAData(caData); err != nil {
			t.Fatalf("SetTLSIdentityCAData: %v", err)
		}

		httpClient, err := safeClient.NewPersistentHTTPSClientForOrigin(server.URL)
		if err != nil {
			t.Fatalf("NewPersistentHTTPSClientForOrigin: %v", err)
		}

		return httpClient
	}

	firstClient := newClient(firstServer, firstState.token)
	secondClient := newClient(secondServer, secondState.token)
	t.Cleanup(firstClient.CloseIdleConnections)
	t.Cleanup(secondClient.CloseIdleConnections)

	var group sync.WaitGroup
	group.Add(2)

	errs := make(chan error, 2)

	go func() {
		defer group.Done()

		for range requestCount {
			if _, err := persistentRequest(context.Background(), firstClient, firstServer.URL); err != nil {
				errs <- fmt.Errorf("first client request: %w", err)

				return
			}
		}
	}()

	go func() {
		defer group.Done()

		for range requestCount {
			if _, err := persistentRequest(context.Background(), secondClient, secondServer.URL); err != nil {
				errs <- fmt.Errorf("second client request: %w", err)

				return
			}
		}
	}()

	group.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	if firstState.requests.Load() != requestCount || secondState.requests.Load() != requestCount {
		t.Fatalf(
			"request counts = (%d,%d), want (%d,%d)",
			firstState.requests.Load(),
			secondState.requests.Load(),
			requestCount,
			requestCount,
		)
	}
	if firstState.wrongHeaders.Load() != 0 || secondState.wrongHeaders.Load() != 0 {
		t.Fatalf(
			"wrong authorization headers = (%d,%d), want (0,0)",
			firstState.wrongHeaders.Load(),
			secondState.wrongHeaders.Load(),
		)
	}

	if _, err := persistentRequest(context.Background(), firstClient, secondServer.URL); err == nil {
		t.Fatal("first client unexpectedly trusted the second origin's independent CA")
	}
	if _, err := persistentRequest(context.Background(), secondClient, firstServer.URL); err == nil {
		t.Fatal("second client unexpectedly trusted the first origin's independent CA")
	}

	if firstState.requests.Load() != requestCount || secondState.requests.Load() != requestCount {
		t.Fatal("cross-origin TLS rejection reached a server or leaked credentials")
	}
}

func newPersistentTestCertificate(t *testing.T, serial int64) tls.Certificate {
	t.Helper()

	return newPersistentTestCertificateForNames(t, serial, []net.IP{net.ParseIP("127.0.0.1")}, nil)
}

func newPersistentTestCertificateForNames(
	t *testing.T,
	serial int64,
	ipAddresses []net.IP,
	dnsNames []string,
) tls.Certificate {
	t.Helper()

	seed := bytes.Repeat([]byte{byte(serial)}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: fmt.Sprintf("persistent-test-%d", serial)},
		NotBefore:    time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:     time.Date(2035, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  ipAddresses,
		DNSNames:     dnsNames,
	}

	certificateDER, err := x509.CreateCertificate(
		bytes.NewReader(bytes.Repeat([]byte{byte(serial + 16)}, 128)),
		template,
		template,
		privateKey.Public(),
		privateKey,
	)
	if err != nil {
		t.Fatalf("create test certificate: %v", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{certificateDER},
		PrivateKey:  privateKey,
	}
}

func TestPersistentHTTPClientForOrigin_RejectsCrossOriginBeforeAuth(t *testing.T) {
	t.Parallel()

	var targetRequests atomic.Int64

	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(target.Close)

	const token = "origin-bound-token"

	var wrongSourceAuth atomic.Int64

	source := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+token {
			wrongSourceAuth.Add(1)
			http.Error(writer, "wrong authorization", http.StatusUnauthorized)

			return
		}

		switch request.URL.Path {
		case "/ok":
			writer.WriteHeader(http.StatusNoContent)
		case "/same-origin":
			http.Redirect(writer, request, "/ok", http.StatusTemporaryRedirect)
		case "/cross-origin":
			http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(source.Close)

	safeClient := &SafeClient{restConfig: &rest.Config{BearerToken: token}}
	httpClient, err := safeClient.NewPersistentHTTPClientForOrigin(source.URL)
	if err != nil {
		t.Fatalf("NewPersistentHTTPClientForOrigin: %v", err)
	}
	t.Cleanup(httpClient.CloseIdleConnections)

	if _, err := persistentRequest(context.Background(), httpClient, source.URL+"/same-origin"); err != nil {
		t.Fatalf("same-origin redirect: %v", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, source.URL+"/cross-origin", nil)
	if err != nil {
		t.Fatalf("build cross-origin redirect request: %v", err)
	}

	resp, err := httpClient.Do(req)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatal("cross-origin redirect unexpectedly succeeded")
	}

	req, err = http.NewRequestWithContext(context.Background(), http.MethodGet, target.URL, nil)
	if err != nil {
		t.Fatalf("build direct cross-origin request: %v", err)
	}

	resp, err = httpClient.Do(req)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatal("direct cross-origin request unexpectedly succeeded")
	}

	if targetRequests.Load() != 0 {
		t.Fatalf("cross-origin target requests = %d, want 0", targetRequests.Load())
	}
	if wrongSourceAuth.Load() != 0 {
		t.Fatalf("wrong source authorization headers = %d, want 0", wrongSourceAuth.Load())
	}
}

func TestTLSIdentityClient_PinsProducerCertificateAndOrigin(t *testing.T) {
	t.Parallel()

	const token = "producer-token"

	var (
		sourceRequests atomic.Int64
		targetRequests atomic.Int64
	)

	target := newPersistentTLSServer(t, 42, nil, &targetRequests)
	source := newPersistentTLSServer(t, 41, func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+token {
			http.Error(writer, "wrong authorization", http.StatusUnauthorized)

			return
		}

		sourceRequests.Add(1)

		switch request.URL.Path {
		case "/ok":
			writer.WriteHeader(http.StatusNoContent)
		case "/same-origin":
			http.Redirect(writer, request, "/ok", http.StatusTemporaryRedirect)
		case "/cross-origin":
			http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
		default:
			http.NotFound(writer, request)
		}
	}, nil)

	sourceCA := certificatePEM(t, source)
	unrelatedCA := certificatePEM(t, target)
	safeClient := &SafeClient{restConfig: &rest.Config{
		BearerToken: token,
		TLSClientConfig: rest.TLSClientConfig{
			CAData:     unrelatedCA,
			Insecure:   true,
			ServerName: "unrelated.invalid",
		},
	}}

	if err := safeClient.SetTLSIdentityCAData(sourceCA); err != nil {
		t.Fatalf("SetTLSIdentityCAData: %v", err)
	}

	httpClient, err := safeClient.NewPersistentHTTPSClientForOrigin(source.URL)
	if err != nil {
		t.Fatalf("NewPersistentHTTPSClientForOrigin: %v", err)
	}
	t.Cleanup(httpClient.CloseIdleConnections)

	protoMajor, err := persistentRequest(context.Background(), httpClient, source.URL+"/same-origin")
	if err != nil {
		t.Fatalf("same-origin producer request: %v", err)
	}
	if protoMajor != 2 {
		t.Fatalf("producer response protocol major = %d, want HTTP/2", protoMajor)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, source.URL+"/cross-origin", nil)
	if err != nil {
		t.Fatalf("build redirect request: %v", err)
	}

	resp, err := httpClient.Do(req)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatal("cross-origin redirect unexpectedly succeeded")
	}

	req, err = http.NewRequestWithContext(context.Background(), http.MethodGet, target.URL, nil)
	if err != nil {
		t.Fatalf("build direct request: %v", err)
	}

	resp, err = httpClient.Do(req)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatal("cross-origin direct request unexpectedly succeeded")
	}

	if sourceRequests.Load() != 3 {
		t.Fatalf("source authenticated requests = %d, want 3", sourceRequests.Load())
	}
	if targetRequests.Load() != 0 {
		t.Fatalf("cross-origin target requests = %d, want 0", targetRequests.Load())
	}
}

func TestTLSIdentityClient_FailsClosed(t *testing.T) {
	t.Parallel()

	var requests atomic.Int64

	source := newPersistentTLSServer(t, 51, nil, &requests)
	unrelated := newPersistentTLSServer(t, 52, nil, nil)
	wrongSAN := newPersistentTLSServer(t, 53, nil, nil, "producer.invalid")

	tests := []struct {
		name    string
		rawURL  string
		caData  []byte
		request string
	}{
		{
			name:   "plaintext origin",
			rawURL: strings.Replace(source.URL, "https://", "http://", 1),
			caData: certificatePEM(t, source),
		},
		{
			name:   "missing CA",
			rawURL: source.URL,
		},
		{
			name:   "malformed CA",
			rawURL: source.URL,
			caData: []byte("not a certificate"),
		},
		{
			name:   "certificate-less PEM",
			rawURL: source.URL,
			caData: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("key")}),
		},
		{
			name:    "mismatched CA despite inherited insecure and roots",
			rawURL:  source.URL,
			caData:  certificatePEM(t, unrelated),
			request: source.URL,
		},
		{
			name:    "wrong host SAN",
			rawURL:  wrongSAN.URL,
			caData:  certificatePEM(t, wrongSAN),
			request: wrongSAN.URL,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			safeClient := &SafeClient{restConfig: &rest.Config{
				BearerToken: "must-not-leak",
				TLSClientConfig: rest.TLSClientConfig{
					CAData:   certificatePEM(t, source),
					Insecure: true,
				},
			}}

			if err := ValidateHTTPSIdentity(tc.rawURL, tc.caData); err != nil {
				if tc.request != "" {
					t.Fatalf("ValidateHTTPSIdentity unexpectedly rejected request-time case: %v", err)
				}

				return
			}
			if tc.request == "" {
				t.Fatal("ValidateHTTPSIdentity unexpectedly accepted invalid identity")
			}

			if err := safeClient.SetTLSIdentityCAData(tc.caData); err != nil {
				t.Fatalf("SetTLSIdentityCAData: %v", err)
			}

			httpClient, err := safeClient.NewPersistentHTTPSClientForOrigin(tc.rawURL)
			if err != nil {
				t.Fatalf("NewPersistentHTTPSClientForOrigin: %v", err)
			}
			defer httpClient.CloseIdleConnections()

			if _, err := persistentRequest(context.Background(), httpClient, tc.request); err == nil {
				t.Fatal("request unexpectedly succeeded with invalid producer identity")
			}
		})
	}

	if requests.Load() != 0 {
		t.Fatalf("failed identities reached authenticated source handler %d times", requests.Load())
	}
}

func newPersistentTLSServer(
	t *testing.T,
	serial int64,
	handler http.HandlerFunc,
	requests *atomic.Int64,
	dnsNames ...string,
) *httptest.Server {
	t.Helper()

	if handler == nil {
		handler = func(writer http.ResponseWriter, _ *http.Request) {
			if requests != nil {
				requests.Add(1)
			}

			writer.WriteHeader(http.StatusNoContent)
		}
	}

	server := httptest.NewUnstartedServer(handler)
	certificate := newPersistentTestCertificate(t, serial)
	if len(dnsNames) > 0 {
		certificate = newPersistentTestCertificateForNames(t, serial, nil, dnsNames)
	}

	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	return server
}

func certificatePEM(t *testing.T, server *httptest.Server) []byte {
	t.Helper()

	certificate := server.Certificate()
	if certificate == nil {
		t.Fatal("TLS test server has no certificate")
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
}

func TestTLSIdentityClient_PreservesCertificateAuth(t *testing.T) {
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
	if err := sc.SetTLSIdentityCAData(certData); err != nil {
		t.Fatalf("SetTLSIdentityCAData: %v", err)
	}

	httpClient, err := sc.NewPersistentHTTPSClientForOrigin(srv.URL)
	if err != nil {
		t.Fatalf("NewPersistentHTTPSClientForOrigin: %v", err)
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

func TestTLSIdentityClient_PreservesProxyAndDial(t *testing.T) {
	t.Parallel()

	var (
		proxyCalls atomic.Int64
		dialCalls  atomic.Int64
	)

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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

	if err := sc.SetTLSIdentityCAData(certificatePEM(t, srv)); err != nil {
		t.Fatalf("SetTLSIdentityCAData: %v", err)
	}

	httpClient, err := sc.NewPersistentHTTPSClientForOrigin(srv.URL)
	if err != nil {
		t.Fatalf("NewPersistentHTTPSClientForOrigin: %v", err)
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

func TestSafeClient_SetNetworkTimeouts_ConfiguresEveryTransportPhase(t *testing.T) {
	t.Parallel()

	sc := &SafeClient{restConfig: &rest.Config{}}
	timeouts := NetworkTimeouts{
		Connect:        11 * time.Second,
		TLSHandshake:   12 * time.Second,
		ResponseHeader: 13 * time.Second,
		WriteIdle:      14 * time.Second,
		ReadIdle:       15 * time.Second,
		ResponseTotal:  16 * time.Second,
		ResponseBytes:  17,
	}

	if err := sc.SetNetworkTimeouts(timeouts); err != nil {
		t.Fatalf("SetNetworkTimeouts: %v", err)
	}

	transport, ok := sc.RESTConfig().WrapTransport(&http.Transport{}).(*http.Transport)
	if !ok {
		t.Fatal("wrapped transport is not an *http.Transport")
	}

	if transport.DialContext == nil {
		t.Fatal("DialContext is nil")
	}

	if transport.TLSHandshakeTimeout != timeouts.TLSHandshake {
		t.Errorf("TLSHandshakeTimeout = %v, want %v", transport.TLSHandshakeTimeout, timeouts.TLSHandshake)
	}

	if transport.ResponseHeaderTimeout != timeouts.ResponseHeader {
		t.Errorf("ResponseHeaderTimeout = %v, want %v", transport.ResponseHeaderTimeout, timeouts.ResponseHeader)
	}

	persistent, err := sc.NewPersistentHTTPClient()
	if err != nil {
		t.Fatalf("NewPersistentHTTPClient: %v", err)
	}
	t.Cleanup(persistent.CloseIdleConnections)

	if persistent.networkTimeouts != timeouts {
		t.Errorf("persistent network timeouts = %+v, want %+v", persistent.networkTimeouts, timeouts)
	}
}

func TestPersistentHTTPClient_RequestBodyProgressResetsWriteIdleDeadline(t *testing.T) {
	t.Parallel()

	progress := make(chan struct{}, 4)
	continueRead := make(chan struct{}, 4)
	roundTrip := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		defer func() { _ = req.Body.Close() }()

		buf := make([]byte, 1)
		for {
			count, err := req.Body.Read(buf)
			if count > 0 {
				progress <- struct{}{}
			}

			if err != nil {
				return nil, err
			}

			select {
			case <-continueRead:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
	})

	client, timers := newManualTimeoutClient(t, roundTrip)
	body := io.NopCloser(bytes.NewReader([]byte("progress")))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, "https://upload.test", body)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	req.ContentLength = int64(len("progress"))

	result := make(chan error, 1)
	go func() {
		_, requestErr := client.Do(req)
		result <- requestErr
	}()

	<-progress
	timer := timers.require(t, 0)

	timers.advance(50 * time.Minute)
	continueRead <- struct{}{}
	<-progress

	timers.advance(10 * time.Minute)
	timer.fire()

	select {
	case err := <-result:
		t.Fatalf("active body progress did not reset the old idle deadline: %v", err)
	default:
	}

	for range 2 {
		continueRead <- struct{}{}
		<-progress
	}

	if resets := timer.resetCount(); resets < 4 {
		t.Fatalf("write-idle timer resets = %d, want at least 4 successful body reads", resets)
	}

	timers.advance(time.Hour)
	timer.fire()

	err = <-result
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("write stall error = %v, want context.DeadlineExceeded", err)
	}
}

func TestPersistentHTTPClient_EarlyResponseReportsLateBodyStall(t *testing.T) {
	t.Parallel()

	progress := make(chan struct{})
	bodyReaderDone := make(chan struct{})
	roundTrip := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		go func() {
			defer close(bodyReaderDone)

			buf := make([]byte, 1)
			_, _ = req.Body.Read(buf)
			close(progress)
			<-req.Context().Done()
			_ = req.Body.Close()
		}()

		return &http.Response{
			StatusCode: http.StatusCreated,
			Header:     make(http.Header),
			Body:       http.NoBody,
		}, nil
	})

	client, timers := newManualTimeoutClient(t, roundTrip)
	body := &notifyingRequestBody{
		Reader:  bytes.NewReader([]byte("partial body")),
		stalled: make(chan error, 1),
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, "https://upload.test", body)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	req.ContentLength = int64(len("partial body"))

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	<-progress
	timers.advance(time.Hour)
	timers.require(t, 0).fire()

	stallErr := <-body.stalled
	if !errors.Is(stallErr, context.DeadlineExceeded) {
		t.Fatalf("late request-body stall = %v, want context.DeadlineExceeded", stallErr)
	}

	<-bodyReaderDone
}

func TestPersistentHTTPClient_ResponseHeaderStallUsesDeadlineCause(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	roundTrip := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		<-req.Context().Done()

		return nil, req.Context().Err()
	})

	client, timers := newManualTimeoutClient(t, roundTrip)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodHead, "https://upload.test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, requestErr := client.Do(req)
		result <- requestErr
	}()

	<-started
	timers.advance(time.Hour)
	timers.require(t, 0).fire()

	err = <-result
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("header stall error = %v, want context.DeadlineExceeded", err)
	}
}

func TestPersistentHTTPClient_ResponseDrainProgressThenStall(t *testing.T) {
	t.Parallel()

	readBlocked := make(chan struct{})
	roundTrip := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: &partialStallBody{
				ctx:     req.Context(),
				blocked: readBlocked,
			},
			Header: make(http.Header),
		}, nil
	})

	client, timers := newManualTimeoutClient(t, roundTrip)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://upload.test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, drainErr := io.Copy(io.Discard, resp.Body)
		result <- errors.Join(drainErr, resp.Body.Close())
	}()

	<-readBlocked

	readTimer := timers.require(t, 1)
	if resets := readTimer.resetCount(); resets < 2 {
		t.Fatalf("read-idle timer resets = %d, want arm plus partial-body progress", resets)
	}

	timers.advance(time.Hour)
	readTimer.fire()

	err = <-result
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("response drain stall error = %v, want context.DeadlineExceeded", err)
	}
}

func TestPersistentHTTPClient_ResponseDrainContinuousTrickleHitsTotalBudget(t *testing.T) {
	t.Parallel()

	readStarted := make(chan struct{}, 1)
	releaseRead := make(chan struct{})
	roundTrip := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: &controlledTrickleBody{
				ctx:         req.Context(),
				readStarted: readStarted,
				releaseRead: releaseRead,
			},
			Header: make(http.Header),
		}, nil
	})

	client, timers := newManualTimeoutClient(t, roundTrip)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://upload.test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, drainErr := io.Copy(io.Discard, resp.Body)
		result <- errors.Join(drainErr, resp.Body.Close())
	}()

	<-readStarted
	for range 4 {
		releaseRead <- struct{}{}
		<-readStarted
	}

	if resets := timers.require(t, 1).resetCount(); resets < 5 {
		t.Fatalf("read-idle timer resets = %d, want arm plus four trickled bytes", resets)
	}

	timers.advance(time.Hour)
	timers.require(t, 2).fire()

	err = <-result
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("continuous trickle error = %v, want context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrResponseBodyLimitExceeded) {
		t.Fatalf("continuous trickle hit byte limit before total duration: %v", err)
	}
}

func TestPersistentHTTPClient_RealTransportTrickleHitsTotalBudget(t *testing.T) {
	tests := []struct {
		name      string
		enableTLS bool
		wantProto int
	}{
		{name: "HTTP/1.1", wantProto: 1},
		{name: "HTTP/2", enableTLS: true, wantProto: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			emit := make(chan struct{})
			sent := make(chan struct{})
			handlerStarted := make(chan struct{})
			handlerDone := make(chan struct{})

			server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				defer close(handlerDone)

				flusher, ok := writer.(http.Flusher)
				if !ok {
					t.Error("response writer does not support flushing")

					return
				}

				writer.WriteHeader(http.StatusOK)
				flusher.Flush()
				close(handlerStarted)

				for {
					select {
					case <-emit:
						if _, err := writer.Write([]byte("x")); err != nil {
							return
						}

						flusher.Flush()
						sent <- struct{}{}
					case <-request.Context().Done():
						return
					}
				}
			}))
			server.EnableHTTP2 = tc.enableTLS
			if tc.enableTLS {
				server.StartTLS()
			} else {
				server.Start()
			}
			t.Cleanup(server.Close)

			client, timers := newManualTimeoutClient(t, server.Client().Transport)
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatalf("NewRequestWithContext: %v", err)
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			if resp.ProtoMajor != tc.wantProto {
				t.Fatalf("response protocol major = %d, want %d", resp.ProtoMajor, tc.wantProto)
			}

			<-handlerStarted

			bodyRead := make(chan struct{}, 4)
			resp.Body = &signalingReadCloser{
				ReadCloser: resp.Body,
				read:       bodyRead,
			}

			result := make(chan error, 1)
			go func() {
				_, drainErr := io.Copy(io.Discard, resp.Body)
				result <- errors.Join(drainErr, resp.Body.Close())
			}()

			for range 4 {
				emit <- struct{}{}
				<-sent
				<-bodyRead
			}

			if resets := timers.require(t, 1).resetCount(); resets < 5 {
				t.Fatalf("read-idle timer resets = %d, want arm plus four real transport bytes", resets)
			}

			timers.advance(time.Hour)
			timers.require(t, 2).fire()

			if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("real transport trickle error = %v, want context.DeadlineExceeded", err)
			}

			<-handlerDone
		})
	}
}

func TestPersistentHTTPClient_ResponseDrainByteBudget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		size      int
		wantError bool
	}{
		{name: "just under limit", size: 16},
		{name: "over limit", size: 17, wantError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			roundTrip := roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(strings.Repeat("x", tc.size))),
					Header:     make(http.Header),
				}, nil
			})

			client, _ := newManualTimeoutClient(t, roundTrip)
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://upload.test", nil)
			if err != nil {
				t.Fatalf("NewRequestWithContext: %v", err)
			}

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}

			count, drainErr := io.Copy(io.Discard, resp.Body)
			err = errors.Join(drainErr, resp.Body.Close())

			if tc.wantError {
				if !errors.Is(err, ErrResponseBodyLimitExceeded) {
					t.Fatalf("drain error = %v, want ErrResponseBodyLimitExceeded", err)
				}

				return
			}

			if err != nil {
				t.Fatalf("drain response: %v", err)
			}
			if count != int64(tc.size) {
				t.Fatalf("drained bytes = %d, want %d", count, tc.size)
			}
		})
	}
}

func TestPersistentHTTPClient_ResponseDrainParentCancellation(t *testing.T) {
	t.Parallel()

	readStarted := make(chan struct{}, 1)
	roundTrip := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: &controlledTrickleBody{
				ctx:         req.Context(),
				readStarted: readStarted,
				releaseRead: make(chan struct{}),
			},
			Header: make(http.Header),
		}, nil
	})

	client, _ := newManualTimeoutClient(t, roundTrip)
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://upload.test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, drainErr := io.Copy(io.Discard, resp.Body)
		result <- errors.Join(drainErr, resp.Body.Close())
	}()

	<-readStarted
	cancel()

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("response drain error = %v, want context.Canceled", err)
	}
}

func TestPersistentHTTPClient_ParentCancellationPreservesCause(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	roundTrip := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		<-req.Context().Done()

		return nil, req.Context().Err()
	})

	client, _ := newManualTimeoutClient(t, roundTrip)
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://upload.test", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, requestErr := client.Do(req)
		result <- requestErr
	}()

	<-started
	cancel()

	err = <-result
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v, want context.Canceled", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation error = %v, unexpectedly reports a stall deadline", err)
	}
}

func TestPersistentHTTPClient_ConcurrentStreamTimeoutIsolation(t *testing.T) {
	t.Parallel()

	stalledStarted := make(chan struct{})
	roundTrip := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/stalled" {
			close(stalledStarted)
			<-req.Context().Done()

			return nil, req.Context().Err()
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("healthy")),
		}, nil
	})

	client, timers := newManualTimeoutClient(t, roundTrip)
	stalledResult := make(chan error, 1)
	go func() {
		_, err := persistentRequest(context.Background(), client, "https://upload.test/stalled")
		stalledResult <- err
	}()

	<-stalledStarted
	stalledTimer := timers.require(t, 0)

	if _, err := persistentRequest(context.Background(), client, "https://upload.test/healthy"); err != nil {
		t.Fatalf("healthy concurrent request: %v", err)
	}

	timers.advance(time.Hour)
	stalledTimer.fire()

	err := <-stalledResult
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled request error = %v, want context.DeadlineExceeded", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type manualIdleTimer struct {
	mu       sync.Mutex
	callback func()
	active   bool
	resets   int
}

func (t *manualIdleTimer) Reset(time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	wasActive := t.active
	t.active = true
	t.resets++

	return wasActive
}

func (t *manualIdleTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	wasActive := t.active
	t.active = false

	return wasActive
}

func (t *manualIdleTimer) fire() {
	t.mu.Lock()
	if !t.active {
		t.mu.Unlock()

		return
	}

	t.active = false
	callback := t.callback
	t.mu.Unlock()

	callback()
}

func (t *manualIdleTimer) resetCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.resets
}

func (t *manualIdleTimer) isActive() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.active
}

type manualTimerSet struct {
	mu     sync.Mutex
	timers []*manualIdleTimer
	now    time.Time
}

func (s *manualTimerSet) new(_ time.Duration, callback func()) idleTimer {
	timer := &manualIdleTimer{callback: callback, active: true, resets: 1}

	s.mu.Lock()
	s.timers = append(s.timers, timer)
	s.mu.Unlock()

	return timer
}

func (s *manualTimerSet) require(t *testing.T, index int) *manualIdleTimer {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if index >= len(s.timers) {
		t.Fatalf("timer index %d is absent; have %d timer(s)", index, len(s.timers))
	}

	return s.timers[index]
}

func (s *manualTimerSet) currentTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.now
}

func (s *manualTimerSet) advance(elapsed time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.now = s.now.Add(elapsed)
}

func (s *manualTimerSet) requireAllStopped(t *testing.T) {
	t.Helper()

	s.mu.Lock()
	timers := append([]*manualIdleTimer(nil), s.timers...)
	s.mu.Unlock()

	for index, timer := range timers {
		if timer.isActive() {
			t.Errorf("idle timer %d remained active after request completion", index)
		}
	}
}

func newManualTimeoutClient(t *testing.T, roundTrip http.RoundTripper) (*PersistentHTTPClient, *manualTimerSet) {
	t.Helper()

	timers := &manualTimerSet{}
	sc := &SafeClient{
		restConfig:       &rest.Config{Transport: roundTrip},
		idleTimerFactory: timers.new,
		idleNow:          timers.currentTime,
	}

	err := sc.SetNetworkTimeouts(NetworkTimeouts{
		Connect:        time.Hour,
		TLSHandshake:   time.Hour,
		ResponseHeader: time.Hour,
		WriteIdle:      time.Hour,
		ReadIdle:       time.Hour,
		ResponseTotal:  time.Hour,
		ResponseBytes:  16,
	})
	if err != nil {
		t.Fatalf("SetNetworkTimeouts: %v", err)
	}

	client, err := sc.NewPersistentHTTPClient()
	if err != nil {
		t.Fatalf("NewPersistentHTTPClient: %v", err)
	}
	t.Cleanup(client.CloseIdleConnections)
	t.Cleanup(func() {
		timers.requireAllStopped(t)
	})

	return client, timers
}

type partialStallBody struct {
	ctx      context.Context
	blocked  chan struct{}
	sentByte bool
	once     sync.Once
}

func (b *partialStallBody) Read(p []byte) (int, error) {
	if !b.sentByte {
		b.sentByte = true
		p[0] = 'x'

		return 1, nil
	}

	b.once.Do(func() { close(b.blocked) })
	<-b.ctx.Done()

	return 0, b.ctx.Err()
}

func (b *partialStallBody) Close() error {
	return nil
}

type controlledTrickleBody struct {
	ctx         context.Context
	readStarted chan<- struct{}
	releaseRead <-chan struct{}
}

func (b *controlledTrickleBody) Read(p []byte) (int, error) {
	select {
	case b.readStarted <- struct{}{}:
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	}

	select {
	case <-b.releaseRead:
		p[0] = 'x'

		return 1, nil
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	}
}

func (b *controlledTrickleBody) Close() error {
	return nil
}

type signalingReadCloser struct {
	io.ReadCloser
	read chan<- struct{}
}

func (b *signalingReadCloser) Read(p []byte) (int, error) {
	count, err := b.ReadCloser.Read(p)
	if count > 0 {
		b.read <- struct{}{}
	}

	return count, err
}

type notifyingRequestBody struct {
	*bytes.Reader
	stalled chan error
}

func (b *notifyingRequestBody) Close() error {
	return nil
}

func (b *notifyingRequestBody) NetworkStall(err error) {
	b.stalled <- err
}

type persistentClientResult struct {
	client *PersistentHTTPClient
	err    error
}

func newSharedHTTP2SafeClient(t *testing.T, srv *httptest.Server) *SafeClient {
	t.Helper()

	certificate := srv.Certificate()
	if certificate == nil {
		t.Fatal("TLS test server has no certificate")
	}

	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(certificate)

	sharedTransport := utilnet.SetTransportDefaults(&http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    rootCAs,
		},
	})
	t.Cleanup(sharedTransport.CloseIdleConnections)

	return &SafeClient{restConfig: &rest.Config{Transport: sharedTransport}}
}

func newPersistentTestClient(
	sc *SafeClient,
	srv *httptest.Server,
	responseHeaderTimeout time.Duration,
) (*PersistentHTTPClient, error) {
	certificate := srv.Certificate()
	if certificate == nil {
		return nil, errors.New("TLS test server has no certificate")
	}

	caData := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
	sub := sc.Copy()
	sub.SetTLSCAData(caData)
	sub.SetResponseHeaderTimeout(responseHeaderTimeout)

	httpClient, err := sub.NewPersistentHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("build persistent test client: %w", err)
	}

	return httpClient, nil
}

func persistentRequest(
	ctx context.Context,
	httpClient *PersistentHTTPClient,
	rawURL string,
) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("do request: %w", err)
	}

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		_ = resp.Body.Close()

		return 0, fmt.Errorf("drain response body: %w", err)
	}

	if err := resp.Body.Close(); err != nil {
		return 0, fmt.Errorf("close response body: %w", err)
	}

	return resp.ProtoMajor, nil
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
