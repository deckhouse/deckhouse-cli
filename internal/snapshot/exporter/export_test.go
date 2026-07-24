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

package exporter

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
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

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

func TestBuildSubClients_IsolatesConcurrentExportCAs(t *testing.T) {
	t.Parallel()

	serverA := newTLSServer(t)
	serverB := newTLSServerWithIdentity(
		t,
		nil,
		nil,
		[]string{"localhost"},
	)
	serverBURL := serverURLForHost(t, serverB, "localhost")

	sc, err := safeClient.NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	type clientPair struct {
		data       *safeClient.PersistentHTTPClient
		sourceHash *safeClient.PersistentHTTPClient
	}

	pairs := make([]clientPair, 2)
	exports := []*deapi.DataExport{
		{Status: deapi.DataExportStatus{URL: serverA.URL, CA: encodedServerCA(t, serverA)}},
		{Status: deapi.DataExportStatus{URL: serverBURL, CA: encodedServerCA(t, serverB)}},
	}

	var (
		wg   sync.WaitGroup
		errs = make(chan error, len(exports))
	)

	for i := range exports {
		index := i

		wg.Add(1)

		go func() {
			defer wg.Done()

			dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, exports[index])
			if err != nil {
				errs <- fmt.Errorf("build client pair %d: %w", index, err)

				return
			}

			pairs[index] = clientPair{data: dataHTTPClient, sourceHash: sourceHashHTTPClient}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}

	for _, pair := range pairs {
		defer pair.data.CloseIdleConnections()
		defer pair.sourceHash.CloseIdleConnections()
	}

	requestAndClose(t, pairs[0].data, http.MethodGet, serverA.URL)
	requestAndClose(t, pairs[0].sourceHash, http.MethodHead, serverA.URL)
	requestAndClose(t, pairs[1].data, http.MethodGet, serverBURL)
	requestAndClose(t, pairs[1].sourceHash, http.MethodHead, serverBURL)

	assertRequestFailure(t, pairs[0].data, http.MethodGet, serverBURL)
	assertRequestFailure(t, pairs[1].data, http.MethodGet, serverA.URL)
}

func TestBuildSubClients_RejectsInvalidPublishedIdentity(t *testing.T) {
	t.Parallel()

	server := newTLSServer(t)
	validCA := encodedServerCA(t, server)
	certificateLessPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("key")})

	tests := []struct {
		name   string
		rawURL string
		ca     string
	}{
		{
			name:   "plaintext URL",
			rawURL: strings.Replace(server.URL, "https://", "http://", 1),
			ca:     validCA,
		},
		{
			name:   "empty CA",
			rawURL: server.URL,
		},
		{
			name:   "malformed base64 CA",
			rawURL: server.URL,
			ca:     "%%%",
		},
		{
			name:   "malformed PEM CA",
			rawURL: server.URL,
			ca:     base64.StdEncoding.EncodeToString([]byte("not PEM")),
		},
		{
			name:   "certificate-less PEM CA",
			rawURL: server.URL,
			ca:     base64.StdEncoding.EncodeToString(certificateLessPEM),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sc, err := safeClient.NewSafeClient()
			if err != nil {
				t.Fatalf("NewSafeClient: %v", err)
			}

			dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
				Status: deapi.DataExportStatus{
					URL: tc.rawURL,
					CA:  tc.ca,
				},
			})
			if dataHTTPClient != nil {
				dataHTTPClient.CloseIdleConnections()
			}
			if sourceHashHTTPClient != nil {
				sourceHashHTTPClient.CloseIdleConnections()
			}
			if err == nil {
				t.Fatal("buildSubClients unexpectedly accepted an invalid published identity")
			}
		})
	}
}

func TestBuildSubClients_RejectsWrongCAAndSANBeforeHTTPAuth(t *testing.T) {
	t.Parallel()

	var (
		sourceRequests   atomic.Int64
		wrongSANRequests atomic.Int64
	)

	source := newTLSServerWithIdentity(
		t,
		func(w http.ResponseWriter, _ *http.Request) {
			sourceRequests.Add(1)
			w.WriteHeader(http.StatusNoContent)
		},
		[]net.IP{net.ParseIP("127.0.0.1")},
		nil,
	)
	unrelated := newTLSServer(t)
	wrongSAN := newTLSServerWithIdentity(
		t,
		func(w http.ResponseWriter, _ *http.Request) {
			wrongSANRequests.Add(1)
			w.WriteHeader(http.StatusNoContent)
		},
		nil,
		[]string{"producer.invalid"},
	)

	sc, err := safeClient.NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	inheritedCAs := append(serverCAPEM(t, source), serverCAPEM(t, wrongSAN)...)
	sc.SetTLSCAData(inheritedCAs)
	sc.SetProbeEndpoint(time.Minute, source.URL, "producer.invalid")

	tests := []struct {
		name          string
		rawURL        string
		ca            string
		wantTLSError  func() any
		requestsCount *atomic.Int64
	}{
		{
			name:          "wrong CA replaces inherited cluster trust",
			rawURL:        source.URL,
			ca:            encodedServerCA(t, unrelated),
			wantTLSError:  func() any { return &x509.UnknownAuthorityError{} },
			requestsCount: &sourceRequests,
		},
		{
			name:          "wrong SAN replaces inherited ServerName",
			rawURL:        wrongSAN.URL,
			ca:            encodedServerCA(t, wrongSAN),
			wantTLSError:  func() any { return &x509.HostnameError{} },
			requestsCount: &wrongSANRequests,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
				Status: deapi.DataExportStatus{
					URL: tc.rawURL,
					CA:  tc.ca,
				},
			})
			if err != nil {
				t.Fatalf("buildSubClients: %v", err)
			}
			defer dataHTTPClient.CloseIdleConnections()
			defer sourceHashHTTPClient.CloseIdleConnections()

			assertTLSFailure(t, dataHTTPClient, http.MethodGet, tc.rawURL, tc.wantTLSError())
			assertTLSFailure(t, sourceHashHTTPClient, http.MethodHead, tc.rawURL, tc.wantTLSError())

			if got := tc.requestsCount.Load(); got != 0 {
				t.Fatalf("failed TLS identity reached authenticated handler %d times, want 0", got)
			}
		})
	}
}

func TestBuildSubClients_BindsBothClientsToPublishedOrigin(t *testing.T) {
	t.Parallel()

	const authorization = "Bearer download-credential"

	var (
		authFailures   atomic.Int64
		targetRequests atomic.Int64
	)

	target := newTLSServerWithIdentity(
		t,
		func(w http.ResponseWriter, _ *http.Request) {
			targetRequests.Add(1)
			w.WriteHeader(http.StatusNoContent)
		},
		[]net.IP{net.ParseIP("127.0.0.1")},
		nil,
	)
	source := newTLSServerWithIdentity(
		t,
		func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != authorization {
				authFailures.Add(1)
			}

			switch r.URL.Path {
			case "/ok":
				w.WriteHeader(http.StatusNoContent)
			case "/same-origin":
				http.Redirect(w, r, "/ok", http.StatusTemporaryRedirect)
			case "/cross-origin":
				http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
			default:
				http.NotFound(w, r)
			}
		},
		[]net.IP{net.ParseIP("127.0.0.1")},
		nil,
	)

	sc, err := safeClient.NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
		Status: deapi.DataExportStatus{
			URL: source.URL,
			CA:  encodedServerCA(t, source),
		},
	})
	if err != nil {
		t.Fatalf("buildSubClients: %v", err)
	}
	defer dataHTTPClient.CloseIdleConnections()
	defer sourceHashHTTPClient.CloseIdleConnections()

	clients := []struct {
		name   string
		method string
		client *safeClient.PersistentHTTPClient
	}{
		{name: "ordinary data", method: http.MethodGet, client: dataHTTPClient},
		{name: "source hash", method: http.MethodHead, client: sourceHashHTTPClient},
	}

	for _, tc := range clients {
		t.Run(tc.name, func(t *testing.T) {
			requestWithAuthAndClose(t, tc.client, tc.method, source.URL+"/same-origin", authorization)
			assertAuthenticatedRequestFailure(
				t,
				tc.client,
				tc.method,
				source.URL+"/cross-origin",
				authorization,
			)
			assertAuthenticatedRequestFailure(t, tc.client, tc.method, target.URL, authorization)
		})
	}

	if got := authFailures.Load(); got != 0 {
		t.Fatalf("same-origin requests without authorization = %d, want 0", got)
	}
	if got := targetRequests.Load(); got != 0 {
		t.Fatalf("cross-origin target requests = %d, want 0", got)
	}
}

func TestExport_CloseIdleConnectionsClosesEveryOwnedClient(t *testing.T) {
	t.Parallel()

	first := &countingIdleCloser{}
	second := &countingIdleCloser{}
	exp := NewExport("ns", "de-name", "Filesystem", "https://exporter", nil, first, second)

	exp.CloseIdleConnections()

	if got := first.calls.Load(); got != 1 {
		t.Errorf("first close calls = %d, want 1", got)
	}

	if got := second.calls.Load(); got != 1 {
		t.Errorf("second close calls = %d, want 1", got)
	}
}

type countingIdleCloser struct {
	calls atomic.Int64
}

func (c *countingIdleCloser) CloseIdleConnections() {
	c.calls.Add(1)
}

func newTLSServer(t *testing.T) *httptest.Server {
	t.Helper()

	return newTLSServerWithIdentity(
		t,
		nil,
		[]net.IP{net.ParseIP("127.0.0.1")},
		nil,
	)
}

func newTLSServerWithIdentity(
	t *testing.T,
	handler http.HandlerFunc,
	ipAddresses []net.IP,
	dnsNames []string,
) *httptest.Server {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate certificate serial: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  ipAddresses,
		DNSNames:     dnsNames,
	}

	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}

	certificate := tls.Certificate{
		Certificate: [][]byte{certificateDER},
		PrivateKey:  privateKey,
	}

	if handler == nil {
		handler = func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}
	}

	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	return srv
}

func serverURLForHost(t *testing.T, srv *httptest.Server, host string) string {
	t.Helper()

	parsed, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}

	_, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split server host and port: %v", err)
	}

	parsed.Host = net.JoinHostPort(host, port)

	return parsed.String()
}

func encodedServerCA(t *testing.T, srv *httptest.Server) string {
	t.Helper()

	return base64.StdEncoding.EncodeToString(serverCAPEM(t, srv))
}

func serverCAPEM(t *testing.T, srv *httptest.Server) []byte {
	t.Helper()

	certificate := srv.Certificate()
	if certificate == nil {
		t.Fatal("TLS test server has no certificate")
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
}

func requestAndClose(
	t *testing.T,
	httpClient *safeClient.PersistentHTTPClient,
	method,
	rawURL string,
) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, rawURL, err)
	}

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		_ = resp.Body.Close()
		t.Fatalf("drain %s %s response: %v", method, rawURL, err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close %s %s response: %v", method, rawURL, err)
	}

	if resp.ProtoMajor != 2 {
		t.Fatalf("%s %s response protocol major = %d, want HTTP/2", method, rawURL, resp.ProtoMajor)
	}
}

func requestWithAuthAndClose(
	t *testing.T,
	httpClient *safeClient.PersistentHTTPClient,
	method,
	rawURL,
	authorization string,
) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	req.Header.Set("Authorization", authorization)

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, rawURL, err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close %s %s response: %v", method, rawURL, err)
	}
}

func assertTLSFailure(
	t *testing.T,
	httpClient *safeClient.PersistentHTTPClient,
	method,
	rawURL string,
	want any,
) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	req.Header.Set("Authorization", "Bearer must-not-leak")

	resp, err := httpClient.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}

	if err == nil || !errors.As(err, want) {
		t.Fatalf("%s %s error = %v, want %T", method, rawURL, err, want)
	}
}

func assertRequestFailure(
	t *testing.T,
	httpClient *safeClient.PersistentHTTPClient,
	method,
	rawURL string,
) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	resp, err := httpClient.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatalf("%s %s unexpectedly succeeded", method, rawURL)
	}
}

func assertAuthenticatedRequestFailure(
	t *testing.T,
	httpClient *safeClient.PersistentHTTPClient,
	method,
	rawURL,
	authorization string,
) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), method, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	req.Header.Set("Authorization", authorization)

	resp, err := httpClient.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Fatalf("%s %s unexpectedly succeeded", method, rawURL)
	}
}
