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

package api

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
)

const commandTestBearerToken = "dh-test-secret-0123456789abcdef"

func TestAPICommandUsesRBACProxy(t *testing.T) {
	for _, scenario := range []struct {
		name         string
		podName      string
		args         []string
		podRequest   string
		proxyRequest string
		denyCA       bool
	}{
		{
			name:         "leader metrics",
			podName:      "deckhouse-leader",
			args:         []string{"metrics"},
			podRequest:   "/api/v1/namespaces/d8-system/pods?labelSelector=app%3Ddeckhouse%2Cleader%3Dtrue",
			proxyRequest: "/controller/metrics",
		},
		{
			name:         "named replica and query",
			podName:      "deckhouse-standby",
			args:         []string{"--pod", "deckhouse-standby", "get", "/api/v1/queues/dump?name=global&output=yaml"},
			podRequest:   "/api/v1/namespaces/d8-system/pods/deckhouse-standby",
			proxyRequest: "/controller/api/v1/queues/dump?name=global&output=yaml",
		},
		{
			name:         "escaped path and query",
			podName:      "deckhouse-leader",
			args:         []string{"get", "/debug/a%2Fb%25c?x=a%2Bb"},
			podRequest:   "/api/v1/namespaces/d8-system/pods?labelSelector=app%3Ddeckhouse%2Cleader%3Dtrue",
			proxyRequest: "/controller/debug/a%2Fb%25c?x=a%2Bb",
		},
		{
			name:       "CA access denied",
			podName:    "deckhouse-leader",
			args:       []string{"metrics"},
			podRequest: "/api/v1/namespaces/d8-system/pods?labelSelector=app%3Ddeckhouse%2Cleader%3Dtrue",
			denyCA:     true,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var apiRequests, proxyRequests requestLog
			serverIdentity, proxyCA := newCommandProxyCertificate(t)
			proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				proxyRequests.add(r.URL.RequestURI())
				if r.Header.Get("Authorization") != "Bearer "+commandTestBearerToken {
					http.Error(w, "missing kubeconfig identity", http.StatusUnauthorized)
					return
				}
				_, _ = w.Write([]byte("controller response\n"))
			}))
			proxy.TLS = &tls.Config{Certificates: []tls.Certificate{serverIdentity}, MinVersion: tls.VersionTLS12}
			proxy.StartTLS()
			t.Cleanup(proxy.Close)

			_, portString, err := net.SplitHostPort(proxy.Listener.Addr().String())
			require.NoError(t, err)
			proxyPort, err := strconv.Atoi(portString)
			require.NoError(t, err)
			pod := &corev1.Pod{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
				ObjectMeta: metav1.ObjectMeta{Name: scenario.podName, Namespace: "d8-system"},
				Spec: corev1.PodSpec{
					HostNetwork: true,
					Containers: []corev1.Container{{
						Name:  "deckhouse",
						Env:   []corev1.EnvVar{{Name: "ADDON_OPERATOR_LISTEN_ADDRESS", Value: "127.0.0.1"}},
						Ports: []corev1.ContainerPort{{Name: "https", ContainerPort: int32(proxyPort), Protocol: corev1.ProtocolTCP}},
					}},
				},
				Status: corev1.PodStatus{PodIP: "127.0.0.1"},
			}
			const caRequest = "/api/v1/namespaces/d8-system/configmaps/kube-rbac-proxy-ca.crt"
			apiServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				apiRequests.add(r.URL.RequestURI())
				w.Header().Set("Content-Type", "application/json")
				if r.Header.Get("Authorization") != "Bearer "+commandTestBearerToken {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				switch r.URL.RequestURI() {
				case scenario.podRequest:
					if r.URL.Path == "/api/v1/namespaces/d8-system/pods" {
						_ = json.NewEncoder(w).Encode(&corev1.PodList{
							TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"},
							Items:    []corev1.Pod{*pod},
						})
					} else {
						_ = json.NewEncoder(w).Encode(pod)
					}
				case caRequest:
					if scenario.denyCA {
						w.WriteHeader(http.StatusForbidden)
						_ = json.NewEncoder(w).Encode(&metav1.Status{
							TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
							Status:   metav1.StatusFailure,
							Reason:   metav1.StatusReasonForbidden,
							Code:     http.StatusForbidden,
							Message:  "reading proxy CA is forbidden",
						})
						return
					}
					_ = json.NewEncoder(w).Encode(&corev1.ConfigMap{
						TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
						ObjectMeta: metav1.ObjectMeta{Name: "kube-rbac-proxy-ca.crt", Namespace: "d8-system"},
						Data:       map[string]string{"ca.crt": string(proxyCA)},
					})
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(apiServer.Close)

			out, err := runAuthenticatedAPICommand(t, apiServer, scenario.args...)
			if scenario.denyCA {
				require.ErrorContains(t, err, "reading proxy CA is forbidden")
				require.Empty(t, out)
				require.Empty(t, proxyRequests.take())
			} else {
				require.NoError(t, err)
				require.Equal(t, "controller response\n", out)
				require.Equal(t, []string{scenario.proxyRequest}, proxyRequests.take())
			}
			require.Equal(t, []string{scenario.podRequest, caRequest}, apiRequests.take(), "no pods/proxy request or extra leader lookup")
		})
	}
}

func runAuthenticatedAPICommand(t *testing.T, apiServer *httptest.Server, args ...string) (string, error) {
	t.Helper()
	kubeconfigPath := filepath.Join(t.TempDir(), "kubeconfig")
	config := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{"test": {
			Server:                   apiServer.URL,
			CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: apiServer.Certificate().Raw}),
		}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"test": {Token: commandTestBearerToken}},
		Contexts:       map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test"}},
		CurrentContext: "test",
	}
	require.NoError(t, clientcmd.WriteToFile(config, kubeconfigPath))

	system := &cobra.Command{Use: "system", SilenceUsage: true, SilenceErrors: true}
	flags.AddPersistentFlags(system)
	system.AddCommand(NewCommand())
	var out bytes.Buffer
	system.SetOut(&out)
	system.SetErr(&out)
	system.SetArgs(append([]string{"api", "--kubeconfig", kubeconfigPath}, args...))
	err := system.ExecuteContext(t.Context())
	return out.String(), err
}

func newCommandProxyCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "command test proxy CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	server := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		DNSNames:     []string{"deckhouse-kube-rbac-proxy.d8-system.svc"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, server, ca, &serverKey.PublicKey, caKey)
	require.NoError(t, err)
	serverKeyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	require.NoError(t, err)
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKeyDER}),
	)
	require.NoError(t, err)
	return cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}
