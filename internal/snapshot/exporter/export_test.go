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
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	deapi "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/api/v1alpha1"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/aggapi"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/transport"
)

// newTestKubeconfigFlags builds a --kubeconfig flag pointing at a throwaway
// kubeconfig fixture, so transport.NewClient doesn't fall back to
// $HOME/.kube/config — a file that doesn't exist on CI runners and shouldn't
// be relied on by tests.
func newTestKubeconfigFlags(t *testing.T) *pflag.FlagSet {
	t.Helper()

	kubeconfigPath := filepath.Join(t.TempDir(), "config")
	kubeconfig := []byte(`apiVersion: v1
kind: Config
clusters:
- name: default
  cluster:
    server: https://test.invalid
contexts:
- name: default
  context:
    cluster: default
current-context: default
`)
	if err := os.WriteFile(kubeconfigPath, kubeconfig, 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("kubeconfig", "", "")

	if err := flags.Set("kubeconfig", kubeconfigPath); err != nil {
		t.Fatalf("set kubeconfig flag: %v", err)
	}

	return flags
}

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

	sc, err := transport.NewClient(newTestKubeconfigFlags(t))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	type clientPair struct {
		data       *transport.PersistentHTTPClient
		sourceHash *transport.PersistentHTTPClient
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

			dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, exports[index], false)
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

			sc, err := transport.NewClient(newTestKubeconfigFlags(t))
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}

			dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
				Status: deapi.DataExportStatus{
					URL: tc.rawURL,
					CA:  tc.ca,
				},
			}, false)
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

	sc, err := transport.NewClient(newTestKubeconfigFlags(t))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
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
			}, false)
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

	sc, err := transport.NewClient(newTestKubeconfigFlags(t))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
		Status: deapi.DataExportStatus{
			URL: source.URL,
			CA:  encodedServerCA(t, source),
		},
	}, false)
	if err != nil {
		t.Fatalf("buildSubClients: %v", err)
	}
	defer dataHTTPClient.CloseIdleConnections()
	defer sourceHashHTTPClient.CloseIdleConnections()

	clients := []struct {
		name   string
		method string
		client *transport.PersistentHTTPClient
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

// ---------------------------------------------------------------------------
// buildSubClients / OpenExport publish-mode tests
// ---------------------------------------------------------------------------

func newExportTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, deapi.AddToScheme(scheme))

	return scheme
}

func TestBuildSubClients_PublishUsesPublicURLAndMergedTrust(t *testing.T) {
	t.Parallel()

	server := newTLSServer(t)

	sc, err := transport.NewClient(newTestKubeconfigFlags(t))
	require.NoError(t, err)

	dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
		Status: deapi.DataExportStatus{
			PublicURL: server.URL,
			CA:        encodedServerCA(t, server),
		},
	}, true)
	require.NoError(t, err)
	defer dataHTTPClient.CloseIdleConnections()
	defer sourceHashHTTPClient.CloseIdleConnections()

	requestAndClose(t, dataHTTPClient, http.MethodGet, server.URL)
	requestAndClose(t, sourceHashHTTPClient, http.MethodHead, server.URL)
}

func TestBuildSubClients_PublishStillVerifiesCertificate(t *testing.T) {
	t.Parallel()

	server := newTLSServer(t)
	unrelated := newTLSServer(t)

	sc, err := transport.NewClient(newTestKubeconfigFlags(t))
	require.NoError(t, err)

	dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
		Status: deapi.DataExportStatus{
			PublicURL: server.URL,
			// CA belongs to an unrelated server: the real server's certificate must
			// not verify against it.
			CA: encodedServerCA(t, unrelated),
		},
	}, true)
	require.NoError(t, err)
	defer dataHTTPClient.CloseIdleConnections()
	defer sourceHashHTTPClient.CloseIdleConnections()

	assertTLSFailure(t, dataHTTPClient, http.MethodGet, server.URL, &x509.UnknownAuthorityError{})
}

// TestBuildSubClients_PublishIgnoresInsecureSkipTLSVerifyFromKubeconfig is the
// regression guard for the P1 fix landed in cf7d998f7 (originally on the upload
// side): an insecure-skip-tls-verify: true kubeconfig must NOT downgrade
// certificate verification on the publish download path either. SetTLSCAData
// unconditionally clears Insecure/ServerName on both the rest.Config and the
// materialized transport, so a client built from such a kubeconfig must still
// reject an untrusted certificate.
func TestBuildSubClients_PublishIgnoresInsecureSkipTLSVerifyFromKubeconfig(t *testing.T) {
	t.Parallel()

	server := newTLSServer(t)
	unrelated := newTLSServer(t)

	flags := newInsecureSkipTLSVerifyKubeconfigFlags(t)

	sc, err := transport.NewClient(flags)
	require.NoError(t, err)

	dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
		Status: deapi.DataExportStatus{
			PublicURL: server.URL,
			CA:        encodedServerCA(t, unrelated),
		},
	}, true)
	require.NoError(t, err)
	defer dataHTTPClient.CloseIdleConnections()
	defer sourceHashHTTPClient.CloseIdleConnections()

	assertTLSFailure(t, dataHTTPClient, http.MethodGet, server.URL, &x509.UnknownAuthorityError{})
	assertTLSFailure(t, sourceHashHTTPClient, http.MethodHead, server.URL, &x509.UnknownAuthorityError{})
}

// newInsecureSkipTLSVerifyKubeconfigFlags builds a --kubeconfig flag pointing at
// a kubeconfig whose cluster entry sets insecure-skip-tls-verify: true, so tests
// can assert that flag never reaches the publish-path transport.
func newInsecureSkipTLSVerifyKubeconfigFlags(t *testing.T) *pflag.FlagSet {
	t.Helper()

	kubeconfigPath := filepath.Join(t.TempDir(), "config")
	kubeconfig := []byte(`apiVersion: v1
kind: Config
clusters:
- name: default
  cluster:
    server: https://test.invalid
    insecure-skip-tls-verify: true
contexts:
- name: default
  context:
    cluster: default
current-context: default
`)
	require.NoError(t, os.WriteFile(kubeconfigPath, kubeconfig, 0o600))

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("kubeconfig", "", "")
	require.NoError(t, flags.Set("kubeconfig", kubeconfigPath))

	return flags
}

func TestBuildSubClients_PublishRejectsNonHTTPSPublicURL(t *testing.T) {
	t.Parallel()

	server := newTLSServer(t)

	tests := []struct {
		name      string
		publicURL string
	}{
		{name: "error: plaintext scheme", publicURL: strings.Replace(server.URL, "https://", "http://", 1)},
		{name: "error: empty URL", publicURL: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc, err := transport.NewClient(newTestKubeconfigFlags(t))
			require.NoError(t, err)

			dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
				Status: deapi.DataExportStatus{
					PublicURL: tt.publicURL,
					CA:        encodedServerCA(t, server),
				},
			}, true)
			if dataHTTPClient != nil {
				dataHTTPClient.CloseIdleConnections()
			}
			if sourceHashHTTPClient != nil {
				sourceHashHTTPClient.CloseIdleConnections()
			}
			require.Error(t, err)
		})
	}
}

func TestBuildSubClients_PublishRejectsUnparseableCA(t *testing.T) {
	t.Parallel()

	server := newTLSServer(t)

	tests := []struct {
		name string
		ca   string
	}{
		{name: "error: malformed base64 CA", ca: "%%%"},
		{name: "error: malformed PEM CA", ca: base64.StdEncoding.EncodeToString([]byte("not PEM"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sc, err := transport.NewClient(newTestKubeconfigFlags(t))
			require.NoError(t, err)

			dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
				Status: deapi.DataExportStatus{
					PublicURL: server.URL,
					CA:        tt.ca,
				},
			}, true)
			if dataHTTPClient != nil {
				dataHTTPClient.CloseIdleConnections()
			}
			if sourceHashHTTPClient != nil {
				sourceHashHTTPClient.CloseIdleConnections()
			}
			require.Error(t, err)
		})
	}
}

func TestBuildSubClients_PublishBindsBothClientsToPublicOrigin(t *testing.T) {
	t.Parallel()

	const authorization = "Bearer publish-credential"

	var (
		authFailures   atomic.Int64
		targetRequests atomic.Int64
	)

	target := newTLSServerWithIdentity(t, func(w http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}, []net.IP{net.ParseIP("127.0.0.1")}, nil)

	source := newTLSServerWithIdentity(t, func(w http.ResponseWriter, r *http.Request) {
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
	}, []net.IP{net.ParseIP("127.0.0.1")}, nil)

	sc, err := transport.NewClient(newTestKubeconfigFlags(t))
	require.NoError(t, err)

	dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
		Status: deapi.DataExportStatus{
			PublicURL: source.URL,
			CA:        encodedServerCA(t, source),
		},
	}, true)
	require.NoError(t, err)
	defer dataHTTPClient.CloseIdleConnections()
	defer sourceHashHTTPClient.CloseIdleConnections()

	clients := []struct {
		name   string
		method string
		client *transport.PersistentHTTPClient
	}{
		{name: "ordinary data", method: http.MethodGet, client: dataHTTPClient},
		{name: "source hash", method: http.MethodHead, client: sourceHashHTTPClient},
	}

	for _, tc := range clients {
		requestWithAuthAndClose(t, tc.client, tc.method, source.URL+"/same-origin", authorization)
		assertAuthenticatedRequestFailure(t, tc.client, tc.method, source.URL+"/cross-origin", authorization)
		assertAuthenticatedRequestFailure(t, tc.client, tc.method, target.URL, authorization)
	}

	assert.Zero(t, authFailures.Load(), "same-origin requests without authorization must not occur")
	assert.Zero(t, targetRequests.Load(), "cross-origin target requests must never be sent")
}

// TestBuildSubClients_PublishPreservesResponseHeaderTimeouts asserts that the
// publish path still produces two DISTINCT, independently usable persistent HTTP
// clients (the ordinary data client and the source-hash client), each carrying
// its own response-header-timeout configuration exactly as the non-publish path
// does (dataPlaneResponseHeaderTimeout vs sourceHashTimeoutCeiling) — the two
// underlying rest.Config WrapTransport chains are not the same object, and both
// remain independently functional.
func TestBuildSubClients_PublishPreservesResponseHeaderTimeouts(t *testing.T) {
	t.Parallel()

	server := newTLSServer(t)

	sc, err := transport.NewClient(newTestKubeconfigFlags(t))
	require.NoError(t, err)

	dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
		Status: deapi.DataExportStatus{
			PublicURL: server.URL,
			CA:        encodedServerCA(t, server),
		},
	}, true)
	require.NoError(t, err)
	defer dataHTTPClient.CloseIdleConnections()
	defer sourceHashHTTPClient.CloseIdleConnections()

	assert.NotSame(t, dataHTTPClient, sourceHashHTTPClient,
		"the ordinary data and source-hash clients must be distinct instances so each keeps its own timeout")

	requestAndClose(t, dataHTTPClient, http.MethodGet, server.URL)
	requestAndClose(t, sourceHashHTTPClient, http.MethodHead, server.URL)
}

// TestBuildSubClients_NonPublishUnchanged is the regression guard proving the
// publicEndpoint parameter addition left the !publicEndpoint path byte-for-byte
// equivalent: identity is pinned to status.ca (SetTLSIdentityCAData) rather than
// merged trust, and status.URL (not status.publicURL) is used as the origin.
func TestBuildSubClients_NonPublishUnchanged(t *testing.T) {
	t.Parallel()

	server := newTLSServer(t)
	unrelated := newTLSServer(t)

	sc, err := transport.NewClient(newTestKubeconfigFlags(t))
	require.NoError(t, err)

	// PublicURL deliberately left empty and pointed nowhere reachable: the
	// non-publish path must never consult it.
	dataHTTPClient, sourceHashHTTPClient, err := buildSubClients(sc, &deapi.DataExport{
		Status: deapi.DataExportStatus{
			URL:       server.URL,
			PublicURL: "http://unreachable.invalid",
			CA:        encodedServerCA(t, server),
		},
	}, false)
	require.NoError(t, err)
	defer dataHTTPClient.CloseIdleConnections()
	defer sourceHashHTTPClient.CloseIdleConnections()

	requestAndClose(t, dataHTTPClient, http.MethodGet, server.URL)

	// Wrong CA under pinned identity must still fail closed, exactly as before.
	dataHTTPClient2, sourceHashHTTPClient2, err := buildSubClients(sc, &deapi.DataExport{
		Status: deapi.DataExportStatus{
			URL: server.URL,
			CA:  encodedServerCA(t, unrelated),
		},
	}, false)
	require.NoError(t, err)
	defer dataHTTPClient2.CloseIdleConnections()
	defer sourceHashHTTPClient2.CloseIdleConnections()

	assertTLSFailure(t, dataHTTPClient2, http.MethodGet, server.URL, &x509.UnknownAuthorityError{})
}

// publishOpenExportDE builds a Ready DataExport whose status carries both an
// (unreachable) internal URL and a path-prefixed public URL pointing at a real
// TLS test server, for OpenExport publish-mode tests.
func publishOpenExportDE(
	namespace, deName, publicURL, ca string,
	targetUID types.UID,
	group, resource, kind, leafName string,
) *deapi.DataExport {
	return &deapi.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deName,
			Namespace: namespace,
			Annotations: map[string]string{
				targetUIDAnnotation: string(targetUID),
			},
		},
		Spec: deapi.DataexportSpec{
			TTL:     "1h",
			Publish: true,
			TargetRef: deapi.TargetRefSpec{
				Group:    group,
				Resource: resource,
				Kind:     kind,
				Name:     leafName,
			},
		},
		Status: deapi.DataExportStatus{
			URL:        "https://internal.invalid",
			PublicURL:  publicURL,
			CA:         ca,
			VolumeMode: "Block",
			Conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "PodReady"},
			},
		},
	}
}

// TestOpenExport_PublishBaseURLKeepsIngressPathPrefix is the regression guard for
// the most likely silent bug in the publish path: OpenExport must build its
// Export (and therefore every block/file request URL derived from it) from
// exportBaseURL(ready, true) — the PATH-PREFIXED public URL — not the bare
// status.url. Losing that prefix would make every subsequent data request 404
// against the Ingress. This is proven on the wire: the test TLS server only
// answers 200 at the exact prefixed path and 404 everywhere else.
func TestOpenExport_PublishBaseURLKeepsIngressPathPrefix(t *testing.T) {
	t.Parallel()

	const (
		namespace = "ns1"
		leafName  = "leaf-a"
		group     = aggapi.VolumeSnapshotGroup
		resource  = aggapi.VolumeSnapshotResource
		kind      = aggapi.VolumeSnapshotKind
		blockSize = 42
	)

	targetUID := types.UID("uid-prefix-guard")

	const prefixPath = "/" + namespace + "/volumesnapshot/" + leafName + "/"

	server := newTLSServerWithIdentity(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != prefixPath+"api/v1/block" {
			http.NotFound(w, r)

			return
		}

		w.Header().Set("Content-Length", fmt.Sprint(blockSize))
		w.WriteHeader(http.StatusOK)
	}, []net.IP{net.ParseIP("127.0.0.1")}, nil)

	publicURL := server.URL + prefixPath

	scheme := newExportTestScheme(t)
	deName := DataExportName(namespace, group, resource, kind, leafName, targetUID)
	de := publishOpenExportDE(namespace, deName, publicURL, encodedServerCA(t, server), targetUID, group, resource, kind, leafName)

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).WithStatusSubresource(de).Build()

	sc, err := transport.NewClient(newTestKubeconfigFlags(t))
	require.NoError(t, err)

	export, err := OpenExport(
		context.Background(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		c,
		namespace,
		group,
		resource,
		kind,
		leafName,
		"1h",
		sc,
		WithTargetUID(targetUID),
		WithPublish(true),
	)
	require.NoError(t, err)
	require.NotNil(t, export)
	defer export.CloseIdleConnections()

	assert.Equal(t, publicURL, export.BaseURL(),
		"the Export base URL must be the path-prefixed public URL, not the bare origin")

	blockURL, err := BlockURL(export.BaseURL())
	require.NoError(t, err)
	assert.Equal(t, publicURL+"api/v1/block", blockURL)

	size, err := export.Fetcher().HeadVolume(context.Background(), blockURL)
	require.NoError(t, err, "the prefixed URL must actually reach the server on the wire")
	assert.Equal(t, int64(blockSize), size)
}

// TestOpenExport_PublishDerivedFromEnsureOptions verifies OpenExport derives its
// publish/publicEndpoint decision from WithPublish uniformly for BOTH WaitReady
// (readiness gate) and buildSubClients (transport/base-URL selection): there is
// no way for the two to desync. With WithPublish(true) and an empty
// status.publicURL, OpenExport must NOT succeed using status.url — it must time
// out waiting for publicURL. Without WithPublish, the very same DataExport
// (status.url populated, publicURL empty) succeeds immediately.
func TestOpenExport_PublishDerivedFromEnsureOptions(t *testing.T) {
	t.Parallel()

	const (
		namespace = "ns2"
		leafName  = "leaf-b"
		group     = aggapi.VolumeSnapshotGroup
		resource  = aggapi.VolumeSnapshotResource
		kind      = aggapi.VolumeSnapshotKind
	)

	targetUID := types.UID("uid-derive-guard")
	server := newTLSServer(t)

	scheme := newExportTestScheme(t)
	deName := DataExportName(namespace, group, resource, kind, leafName, targetUID)

	buildDE := func() *deapi.DataExport {
		return &deapi.DataExport{
			ObjectMeta: metav1.ObjectMeta{
				Name:      deName,
				Namespace: namespace,
				Annotations: map[string]string{
					targetUIDAnnotation: string(targetUID),
				},
			},
			Spec: deapi.DataexportSpec{
				TTL: "1h",
				TargetRef: deapi.TargetRefSpec{
					Group: group, Resource: resource, Kind: kind, Name: leafName,
				},
			},
			Status: deapi.DataExportStatus{
				URL:        server.URL,
				PublicURL:  "",
				CA:         encodedServerCA(t, server),
				VolumeMode: "Block",
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionTrue, Reason: "PodReady"},
				},
			},
		}
	}

	sc, err := transport.NewClient(newTestKubeconfigFlags(t))
	require.NoError(t, err)

	t.Run("error: publish requested but publicURL empty times out", func(t *testing.T) {
		t.Parallel()

		de := buildDE()
		de.Name = deName + "-publish"
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).WithStatusSubresource(de).Build()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := OpenExport(
			ctx,
			slog.New(slog.NewTextHandler(io.Discard, nil)),
			c,
			namespace,
			group,
			resource,
			kind,
			leafName,
			"1h",
			sc,
			WithTargetUID(targetUID),
			WithPublish(true),
		)
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "got: %v", err)
	})

	t.Run("success: publish not requested uses status.url immediately", func(t *testing.T) {
		t.Parallel()

		de := buildDE()
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(de).WithStatusSubresource(de).Build()

		export, err := OpenExport(
			context.Background(),
			slog.New(slog.NewTextHandler(io.Discard, nil)),
			c,
			namespace,
			group,
			resource,
			kind,
			leafName,
			"1h",
			sc,
			WithTargetUID(targetUID),
		)
		require.NoError(t, err)
		require.NotNil(t, export)
		defer export.CloseIdleConnections()

		assert.Equal(t, server.URL, export.BaseURL())
	})
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
	httpClient *transport.PersistentHTTPClient,
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
	httpClient *transport.PersistentHTTPClient,
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
	httpClient *transport.PersistentHTTPClient,
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
	httpClient *transport.PersistentHTTPClient,
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
	httpClient *transport.PersistentHTTPClient,
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
