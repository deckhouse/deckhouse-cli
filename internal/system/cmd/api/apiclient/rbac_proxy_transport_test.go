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

package apiclient

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const rbacProxyTestToken = "dh-test-secret-0123456789abcdef"

type proxyTestCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

type proxyTestIdentity struct {
	certPEM []byte
	keyPEM  []byte
	tlsCert tls.Certificate
}

func newProxyTestCA(t *testing.T) proxyTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: t.Name()},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return proxyTestCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca proxyTestCA) issue(t *testing.T, server bool, change func(*x509.Certificate)) proxyTestIdentity {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "debug-user", Organization: []string{"debug-readers"}},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if server {
		template.DNSNames = []string{kubeRBACProxyServerName}
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	if change != nil {
		change(template)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	identity := proxyTestIdentity{
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}
	identity.tlsCert, err = tls.X509KeyPair(identity.certPEM, identity.keyPEM)
	require.NoError(t, err)
	return identity
}

func newProxyTestServer(t *testing.T, identity proxyTestIdentity, clientCA *proxyTestCA, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{identity.tlsCert}, MinVersion: tls.VersionTLS12}
	if clientCA != nil {
		pool := x509.NewCertPool()
		require.True(t, pool.AppendCertsFromPEM(clientCA.pem))
		server.TLS.ClientCAs = pool
		server.TLS.ClientAuth = tls.RequireAndVerifyClientCert
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func TestRBACProxyTransportCredentials(t *testing.T) {
	serverCA := newProxyTestCA(t)
	clientCA := newProxyTestCA(t)
	clientIdentity := clientCA.issue(t, false, nil)

	for _, mode := range []string{"bearer", "token file", "client certificate", "client certificate files"} {
		t.Run(mode, func(t *testing.T) {
			config := &rest.Config{Host: "https://kubernetes.example.test"}
			var clientTrust *proxyTestCA
			switch mode {
			case "bearer":
				config.BearerToken = rbacProxyTestToken
			case "token file":
				config.BearerTokenFile = filepath.Join(t.TempDir(), "token")
				require.NoError(t, os.WriteFile(config.BearerTokenFile, []byte(rbacProxyTestToken), 0o600))
			case "client certificate":
				config.CertData, config.KeyData = clientIdentity.certPEM, clientIdentity.keyPEM
				clientTrust = &clientCA
			case "client certificate files":
				dir := t.TempDir()
				config.CertFile, config.KeyFile = filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key")
				require.NoError(t, os.WriteFile(config.CertFile, clientIdentity.certPEM, 0o600))
				require.NoError(t, os.WriteFile(config.KeyFile, clientIdentity.keyPEM, 0o600))
				clientTrust = &clientCA
			}
			server := newProxyTestServer(t, serverCA.issue(t, true, nil), clientTrust, func(w http.ResponseWriter, r *http.Request) {
				if clientTrust != nil {
					if len(r.TLS.VerifiedChains) == 0 || r.TLS.PeerCertificates[0].Subject.CommonName != "debug-user" {
						http.Error(w, "missing client identity", http.StatusUnauthorized)
						return
					}
				} else if r.Header.Get("Authorization") != "Bearer "+rbacProxyTestToken {
					http.Error(w, "missing bearer", http.StatusUnauthorized)
					return
				}
				_, _ = w.Write([]byte("profile"))
			})

			transport, err := NewRBACProxyTransport(config, server.URL, serverCA.pem)
			require.NoError(t, err)
			response, err := transport.Get(t.Context(), "/debug/pprof/heap", nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, "profile", string(response.Body))
		})
	}
}

func TestRBACProxyTransportExecCredentialsPreserveClusterInfo(t *testing.T) {
	apiCA := newProxyTestCA(t)
	proxyCA := newProxyTestCA(t)
	clientCA := newProxyTestCA(t)
	clientIdentity := clientCA.issue(t, false, nil)
	executable, err := os.Executable()
	require.NoError(t, err)

	for _, mode := range []string{"token", "certificate"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			credentialPath := filepath.Join(dir, "credential.json")
			observedPath := filepath.Join(dir, "exec-info.json")
			status := &clientauthv1.ExecCredentialStatus{Token: rbacProxyTestToken}
			var clientTrust *proxyTestCA
			if mode == "certificate" {
				status = &clientauthv1.ExecCredentialStatus{
					ClientCertificateData: string(clientIdentity.certPEM),
					ClientKeyData:         string(clientIdentity.keyPEM),
				}
				clientTrust = &clientCA
			}
			credential := &clientauthv1.ExecCredential{Status: status}
			credential.APIVersion = "client.authentication.k8s.io/v1"
			credential.Kind = "ExecCredential"
			encoded, err := json.Marshal(credential)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(credentialPath, encoded, 0o600))

			config := &rest.Config{
				Host: "https://kubernetes.example.test:6443",
				TLSClientConfig: rest.TLSClientConfig{
					CAData: apiCA.pem, ServerName: "kubernetes.example.test", Insecure: true,
				},
				ExecProvider: &clientcmdapi.ExecConfig{
					Command:            executable,
					Args:               []string{"-test.run=^TestRBACProxyExecCredentialHelper$"},
					APIVersion:         "client.authentication.k8s.io/v1",
					InteractiveMode:    clientcmdapi.NeverExecInteractiveMode,
					ProvideClusterInfo: true,
					Config:             &runtime.Unknown{Raw: []byte(`{"audience":"kubernetes"}`)},
					Env: []clientcmdapi.ExecEnvVar{
						{Name: "D8_TEST_PROXY_EXEC_CREDENTIAL", Value: credentialPath},
						{Name: "D8_TEST_PROXY_EXEC_INFO", Value: observedPath},
					},
				},
			}
			if mode == "token" {
				config.CAFile = filepath.Join(dir, "api-ca.crt")
				config.CAData = nil
				require.NoError(t, os.WriteFile(config.CAFile, apiCA.pem, 0o600))
			}
			original := rest.CopyConfig(config)
			server := newProxyTestServer(t, proxyCA.issue(t, true, nil), clientTrust, func(w http.ResponseWriter, r *http.Request) {
				if mode == "token" && r.Header.Get("Authorization") != "Bearer "+rbacProxyTestToken {
					http.Error(w, "missing exec token", http.StatusUnauthorized)
					return
				}
				if mode == "certificate" && r.TLS.PeerCertificates[0].Subject.CommonName != "debug-user" {
					http.Error(w, "wrong exec certificate", http.StatusUnauthorized)
					return
				}
				w.WriteHeader(http.StatusOK)
			})

			transport, err := NewRBACProxyTransport(config, server.URL, proxyCA.pem)
			require.NoError(t, err)
			response, err := transport.Get(t.Context(), "/debug/pprof/heap", nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, original, config)

			observedBytes, err := os.ReadFile(observedPath)
			require.NoError(t, err)
			var observed clientauthv1.ExecCredential
			require.NoError(t, json.Unmarshal(observedBytes, &observed))
			require.NotNil(t, observed.Spec.Cluster)
			require.Equal(t, original.Host, observed.Spec.Cluster.Server)
			require.Equal(t, original.ServerName, observed.Spec.Cluster.TLSServerName)
			require.Equal(t, original.Insecure, observed.Spec.Cluster.InsecureSkipTLSVerify)
			require.Equal(t, apiCA.pem, observed.Spec.Cluster.CertificateAuthorityData)
			require.JSONEq(t, `{"audience":"kubernetes"}`, string(observed.Spec.Cluster.Config.Raw))
		})
	}
}

// The test binary acts as an exec credential plugin without depending on a shell.
func TestRBACProxyExecCredentialHelper(t *testing.T) {
	credentialPath := os.Getenv("D8_TEST_PROXY_EXEC_CREDENTIAL")
	if credentialPath == "" {
		return
	}
	if err := os.WriteFile(os.Getenv("D8_TEST_PROXY_EXEC_INFO"), []byte(os.Getenv("KUBERNETES_EXEC_INFO")), 0o600); err != nil {
		os.Exit(1)
	}
	credential, err := os.ReadFile(credentialPath)
	if err != nil {
		os.Exit(1)
	}
	if _, err := os.Stdout.Write(credential); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func TestRBACProxyTransportValidatesServerCertificate(t *testing.T) {
	ca := newProxyTestCA(t)
	otherCA := newProxyTestCA(t)
	for _, scenario := range []struct {
		name      string
		trustedCA []byte
		change    func(*x509.Certificate)
	}{
		{name: "unknown CA", trustedCA: otherCA.pem},
		{name: "wrong SAN", trustedCA: ca.pem, change: func(cert *x509.Certificate) { cert.DNSNames = []string{"other.example.test"} }},
		{name: "expired", trustedCA: ca.pem, change: func(cert *x509.Certificate) {
			cert.NotBefore = time.Now().Add(-2 * time.Hour)
			cert.NotAfter = time.Now().Add(-time.Hour)
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var requests atomic.Int32
			server := newProxyTestServer(t, ca.issue(t, true, scenario.change), nil, func(w http.ResponseWriter, _ *http.Request) { requests.Add(1) })
			transport, err := NewRBACProxyTransport(&rest.Config{TLSClientConfig: rest.TLSClientConfig{Insecure: true}}, server.URL, scenario.trustedCA)
			require.NoError(t, err)
			_, err = transport.Get(t.Context(), "/readyz", nil)
			require.Error(t, err)
			require.Zero(t, requests.Load())
		})
	}
}

func TestRBACProxyTransportDoesNotFollowRedirect(t *testing.T) {
	serverCA := newProxyTestCA(t)
	clientCA := newProxyTestCA(t)
	clientIdentity := clientCA.issue(t, false, nil)
	serverIdentity := serverCA.issue(t, true, nil)
	var destinationHandshakes, destinationRequests atomic.Int32
	destination := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { destinationRequests.Add(1) }))
	destination.Config.ErrorLog = log.New(io.Discard, "", 0)
	destination.TLS = &tls.Config{
		Certificates: []tls.Certificate{serverIdentity.tlsCert},
		ClientAuth:   tls.RequestClientCert,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			destinationHandshakes.Add(1)
			return nil, nil
		},
	}
	destination.StartTLS()
	t.Cleanup(destination.Close)
	origin := newProxyTestServer(t, serverIdentity, &clientCA, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL+"/stolen", http.StatusFound)
	})
	config := &rest.Config{
		BearerToken:     rbacProxyTestToken,
		TLSClientConfig: rest.TLSClientConfig{CertData: clientIdentity.certPEM, KeyData: clientIdentity.keyPEM},
	}
	transport, err := NewRBACProxyTransport(config, origin.URL, serverCA.pem)
	require.NoError(t, err)
	response, err := transport.Get(t.Context(), "/debug/pprof/heap", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusFound, response.StatusCode)
	require.Zero(t, destinationHandshakes.Load())
	require.Zero(t, destinationRequests.Load())
}

func TestRBACProxyTransportRequestAndResponse(t *testing.T) {
	ca := newProxyTestCA(t)
	body := []byte{0, 0xff, 0x1f, 0x8b, '\n'}
	for _, status := range []int{http.StatusOK, http.StatusForbidden, http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			seen := make(chan *http.Request, 1)
			server := newProxyTestServer(t, ca.issue(t, true, nil), nil, func(w http.ResponseWriter, r *http.Request) {
				seen <- r
				w.WriteHeader(status)
				_, _ = w.Write(body)
			})
			transport, err := NewRBACProxyTransport(&rest.Config{}, server.URL+"/", ca.pem)
			require.NoError(t, err)
			query := url.Values{"name": {"a b", "x+y"}, "seconds": {"45"}}
			response, err := transport.Get(t.Context(), "/api/v1/packages/render/a%2Fb", query)
			require.NoError(t, err)
			require.Equal(t, status, response.StatusCode)
			require.Equal(t, body, response.Body)
			request := <-seen
			require.Equal(t, http.MethodGet, request.Method)
			require.Equal(t, "/controller/api/v1/packages/render/a%2Fb", request.URL.EscapedPath())
			require.Equal(t, query, request.URL.Query())
		})
	}
}

func TestRBACProxyTransportRejectsUnsafeConfiguration(t *testing.T) {
	ca := newProxyTestCA(t)
	for _, scenario := range []struct {
		name   string
		config *rest.Config
	}{
		{name: "nil config"},
		{name: "impersonated user", config: &rest.Config{Impersonate: rest.ImpersonationConfig{UserName: "other"}}},
		{name: "impersonated UID", config: &rest.Config{Impersonate: rest.ImpersonationConfig{UID: "1000"}}},
		{name: "impersonated groups", config: &rest.Config{Impersonate: rest.ImpersonationConfig{Groups: []string{"other"}}}},
		{name: "impersonated extras", config: &rest.Config{Impersonate: rest.ImpersonationConfig{Extra: map[string][]string{"scope": {"other"}}}}},
		{name: "custom transport", config: &rest.Config{Transport: http.DefaultTransport}},
		{name: "custom wrapper", config: &rest.Config{WrapTransport: func(rt http.RoundTripper) http.RoundTripper { return rt }}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			_, err := NewRBACProxyTransport(scenario.config, "https://127.0.0.1:4204", ca.pem)
			require.Error(t, err)
		})
	}
	for _, endpoint := range []string{"", "http://127.0.0.1:4204", "//127.0.0.1:4204", "https://", "https://user@127.0.0.1:4204", "https://127.0.0.1:4204/controller", "https://127.0.0.1:4204?x=1", "https://127.0.0.1:4204#fragment"} {
		t.Run(endpoint, func(t *testing.T) {
			_, err := NewRBACProxyTransport(&rest.Config{}, endpoint, ca.pem)
			require.Error(t, err)
		})
	}
	for _, bundle := range [][]byte{nil, []byte("not a certificate")} {
		_, err := NewRBACProxyTransport(&rest.Config{}, "https://127.0.0.1:4204", bundle)
		require.Error(t, err)
	}
}

func TestRBACProxyTransportRejectsUnsafePath(t *testing.T) {
	ca := newProxyTestCA(t)
	var requests atomic.Int32
	server := newProxyTestServer(t, ca.issue(t, true, nil), nil, func(http.ResponseWriter, *http.Request) { requests.Add(1) })
	transport, err := NewRBACProxyTransport(&rest.Config{}, server.URL, ca.pem)
	require.NoError(t, err)
	for _, path := range []string{"", "readyz", "https://other.example.test/readyz", "//other.example.test/readyz", "/../debug", "/%2e%2e/debug", "/a/./readyz", "/a%2F..%2Freadyz", "/readyz?token=x", "/readyz#fragment", "/bad%zz"} {
		t.Run(path, func(t *testing.T) {
			_, err := transport.Get(t.Context(), path, nil)
			require.Error(t, err)
		})
	}
	require.Zero(t, requests.Load())
}

func TestRBACProxyTransportCancellationAndTimeout(t *testing.T) {
	ca := newProxyTestCA(t)
	t.Run("cancel pending profile", func(t *testing.T) {
		arrived, disconnected := make(chan struct{}), make(chan struct{})
		server := newProxyTestServer(t, ca.issue(t, true, nil), nil, func(_ http.ResponseWriter, r *http.Request) {
			close(arrived)
			<-r.Context().Done()
			close(disconnected)
		})
		transport, err := NewRBACProxyTransport(&rest.Config{}, server.URL, ca.pem)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := transport.Get(ctx, "/debug/pprof/profile", url.Values{"seconds": {"60"}})
			result <- err
		}()
		select {
		case <-arrived:
		case <-time.After(5 * time.Second):
			t.Fatal("profile request did not reach the server")
		}
		cancel()
		select {
		case err := <-result:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("profile request did not stop after cancellation")
		}
		select {
		case <-disconnected:
		case <-time.After(5 * time.Second):
			t.Fatal("server did not observe the cancelled request")
		}
	})
	t.Run("configured timeout", func(t *testing.T) {
		server := newProxyTestServer(t, ca.issue(t, true, nil), nil, func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		})
		transport, err := NewRBACProxyTransport(&rest.Config{Timeout: 200 * time.Millisecond}, server.URL, ca.pem)
		require.NoError(t, err)
		_, err = transport.Get(t.Context(), "/debug/pprof/profile", nil)
		require.Error(t, err)
		var timeout interface{ Timeout() bool }
		require.True(t, errors.As(err, &timeout))
		require.True(t, timeout.Timeout())
	})
}
