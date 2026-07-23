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
	"crypto/rand"
	"crypto/rsa"
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
	serverB := newTLSServer(t)

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
		{Status: deapi.DataExportStatus{CA: encodedServerCA(t, serverA)}},
		{Status: deapi.DataExportStatus{CA: encodedServerCA(t, serverB)}},
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
	requestAndClose(t, pairs[1].data, http.MethodGet, serverB.URL)
	requestAndClose(t, pairs[1].sourceHash, http.MethodHead, serverB.URL)

	assertTLSTrustFailure(t, pairs[0].data, serverB.URL)
	assertTLSTrustFailure(t, pairs[1].data, serverA.URL)
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

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
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
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}

	certificate := tls.Certificate{
		Certificate: [][]byte{certificateDER},
		PrivateKey:  privateKey,
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	}
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	return srv
}

func encodedServerCA(t *testing.T, srv *httptest.Server) string {
	t.Helper()

	certificate := srv.Certificate()
	if certificate == nil {
		t.Fatal("TLS test server has no certificate")
	}

	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})

	return base64.StdEncoding.EncodeToString(pemBytes)
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

func assertTLSTrustFailure(t *testing.T, httpClient *safeClient.PersistentHTTPClient, rawURL string) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}

	resp, err := httpClient.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}

	var unknownAuthorityError x509.UnknownAuthorityError
	if err == nil || !errors.As(err, &unknownAuthorityError) {
		t.Fatalf("cross-export request error = %v, want x509.UnknownAuthorityError", err)
	}
}
