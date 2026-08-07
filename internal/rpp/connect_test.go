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

package rpp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

// tlsEndpoint starts a TLS server that answers every request, and returns its
// host:port together with its certificate in PEM form.
//
// Every server gets a freshly generated certificate of its own, unlike
// httptest.NewTLSServer which reuses one built-in certificate for all servers.
// Without that, two test endpoints would be indistinguishable to a client
// verifying against a CA, and the case this file is about could not be written.
func tlsEndpoint(t *testing.T, tagsBody string) (string, []byte) {
	t.Helper()

	certPEM, keyPEM := selfSignedLocalhostCert(t)

	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, tagsBody)
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	parsed, err := url.Parse(srv.URL)
	require.NoError(t, err)

	return parsed.Host, certPEM
}

func selfSignedLocalhostCert(t *testing.T) ([]byte, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "registry-packages-proxy-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// TestNewClusterClientSkipsCandidateWithUnverifiableCertificate is the case that
// motivated the candidate list: one endpoint answers with a certificate the
// published CA does not cover, and the CLI moves on instead of failing.
func TestNewClusterClientSkipsCandidateWithUnverifiableCertificate(t *testing.T) {
	const tagsBody = `{"name":"deckhouse-cli","tags":["v0.13.1"]}`

	strangerHost, _ := tlsEndpoint(t, tagsBody)
	trustedHost, trustedCA := tlsEndpoint(t, tagsBody)

	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints": fmt.Sprintf(`[%q,%q]`, strangerHost, trustedHost),
		"ca.crt":    string(trustedCA),
	}))

	client, err := NewClusterClient(context.Background(), kube, &rest.Config{}, dkplog.NewNop(), "", "", false)
	require.NoError(t, err)
	assert.Equal(t, "https://"+trustedHost, client.baseURL)

	tags, err := client.ListTags(context.Background(), CLIImage())
	require.NoError(t, err)
	assert.Equal(t, []string{"v0.13.1"}, tags)
}

func TestNewClusterClientPrefersTheFirstAnsweringCandidate(t *testing.T) {
	host, ca := tlsEndpoint(t, `{"name":"deckhouse-cli","tags":[]}`)

	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints":      fmt.Sprintf(`[%q]`, host),
		"ca.crt":         string(ca),
		"publicEndpoint": "https://registry-packages-proxy.example.com",
	}))

	client, err := NewClusterClient(context.Background(), kube, &rest.Config{}, dkplog.NewNop(), "", "", false)
	require.NoError(t, err)
	assert.Equal(t, "https://"+host, client.baseURL, "a verifiable master endpoint wins over the public host")
}

func TestNewClusterClientReportsEveryRejectedCandidate(t *testing.T) {
	strangerHost, _ := tlsEndpoint(t, `{}`)

	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints": fmt.Sprintf(`[%q]`, strangerHost),
		"ca.crt":    "not a certificate",
	}))

	_, err := NewClusterClient(context.Background(), kube, &rest.Config{}, dkplog.NewNop(), "", "", false)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEndpointDiscovery)
	assert.Contains(t, err.Error(), strangerHost, "the message names what was tried")
	assert.Contains(t, err.Error(), sourceMaster)
}

func TestNewClusterClientInsecureAcceptsAnyCandidate(t *testing.T) {
	// --rpp-insecure-skip-tls-verify replaces the published CA, which is how the
	// pod fallback stays usable on a cluster that publishes no certificate.
	strangerHost, _ := tlsEndpoint(t, `{"name":"deckhouse-cli","tags":["v0.13.1"]}`)

	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints": fmt.Sprintf(`[%q]`, strangerHost),
	}))

	client, err := NewClusterClient(context.Background(), kube, &rest.Config{}, dkplog.NewNop(), "", "", true)
	require.NoError(t, err)

	tags, err := client.ListTags(context.Background(), CLIImage())
	require.NoError(t, err)
	assert.Equal(t, []string{"v0.13.1"}, tags)
}

func TestNewClusterClientRejectsContradictoryFlags(t *testing.T) {
	kube := fake.NewSimpleClientset(proxyConfigMap(map[string]string{
		"endpoints": `["10.0.0.1:4219","10.0.0.2:4219"]`,
		"ca.crt":    "CA-PEM",
	}))

	_, err := NewClusterClient(context.Background(), kube, &rest.Config{}, dkplog.NewNop(),
		"", writeTempCA(t, []byte("CA-PEM")), true)
	require.Error(t, err)

	assert.ErrorIs(t, err, ErrUnsupportedConfig, "the caller matches on this")
	assert.NotErrorIs(t, err, ErrEndpointDiscovery, "the endpoints are fine, the flags are not")
	assert.NotContains(t, err.Error(), "10.0.0.2", "the flags are judged once, not per candidate")
}

func TestNewClusterClientUsesExplicitEndpointWithoutDiscovery(t *testing.T) {
	host, ca := tlsEndpoint(t, `{"name":"deckhouse-cli","tags":[]}`)

	caFile := writeTempCA(t, ca)

	// An empty cluster offers no candidates, so reaching the endpoint proves
	// discovery was skipped entirely.
	client, err := NewClusterClient(context.Background(), fake.NewSimpleClientset(), &rest.Config{},
		dkplog.NewNop(), "https://"+host, caFile, false)
	require.NoError(t, err)
	assert.Equal(t, "https://"+host, client.baseURL)
}

func writeTempCA(t *testing.T, caPEM []byte) string {
	t.Helper()

	path := t.TempDir() + "/ca.pem"
	require.NoError(t, os.WriteFile(path, caPEM, 0o600))

	return path
}
