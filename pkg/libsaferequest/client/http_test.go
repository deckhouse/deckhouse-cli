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
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestSafeClient_HTTPDo_NoAuthPolicyAndInsecureConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	kubeconfigPath := t.TempDir() + "/config"
	writeTestKubeconfig(t, kubeconfigPath, "current", map[string]string{
		"current": server.URL,
	})
	t.Setenv(clientcmd.RecommendedConfigPathEnvVar, kubeconfigPath)

	tests := []struct {
		name          string
		supportNoAuth bool
		insecure      bool
		wantErr       bool
	}{
		{
			name:          "no auth allowed with secure config",
			supportNoAuth: true,
			insecure:      false,
		},
		{
			name:          "no auth allowed with insecure config",
			supportNoAuth: true,
			insecure:      true,
		},
		{
			name:          "no auth rejected with secure config",
			supportNoAuth: false,
			insecure:      false,
			wantErr:       true,
		},
		{
			name:          "no auth rejected with insecure config",
			supportNoAuth: false,
			insecure:      true,
			wantErr:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setClientGlobals(t, test.supportNoAuth, test.insecure)

			safeClient, err := NewSafeClient()
			if err != nil {
				t.Fatalf("NewSafeClient: %v", err)
			}

			if safeClient.restConfig.TLSClientConfig.Insecure != test.insecure {
				t.Fatalf(
					"TLSClientConfig.Insecure = %t, want %t",
					safeClient.restConfig.TLSClientConfig.Insecure,
					test.insecure,
				)
			}

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatalf("NewRequestWithContext: %v", err)
			}

			resp, err := safeClient.HTTPDo(req)
			if test.wantErr {
				if resp != nil {
					closeResponse(t, resp)
				}

				if err == nil {
					t.Fatal("HTTPDo error = nil, want no-auth rejection")
				}

				if err.Error() != "No auth" {
					t.Fatalf("HTTPDo error = %q, want %q", err, "No auth")
				}

				return
			}

			if err != nil {
				t.Fatalf("HTTPDo: %v", err)
			}

			if resp.StatusCode != http.StatusNoContent {
				closeResponse(t, resp)
				t.Fatalf("HTTPDo status = %d, want %d", resp.StatusCode, http.StatusNoContent)
			}

			closeResponse(t, resp)
		})
	}
}

func TestSafeClient_HTTPDo_AuthenticatedModes(t *testing.T) {
	setClientGlobals(t, false, false)

	tests := []struct {
		name      string
		configure func(t *testing.T, config *rest.Config)
		wantAuth  string
	}{
		{
			name: "bearer token",
			configure: func(_ *testing.T, config *rest.Config) {
				config.BearerToken = "configured-token"
			},
			wantAuth: "Bearer configured-token",
		},
		{
			name: "auth provider",
			configure: func(t *testing.T, config *rest.Config) {
				t.Helper()

				pluginName, err := registerRetainedTestAuthProvider()
				if err != nil {
					t.Fatalf("RegisterAuthProviderPlugin: %v", err)
				}

				config.AuthProvider = &clientcmdapi.AuthProviderConfig{Name: pluginName}
			},
			wantAuth: "Bearer provider-token",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotAuth := make(chan string, 1)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				gotAuth <- req.Header.Get("Authorization")
				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(server.Close)

			config := &rest.Config{
				Host: server.URL,
				TLSClientConfig: rest.TLSClientConfig{
					CAData: testServerCAPEM(t, server),
				},
			}
			test.configure(t, config)

			safeClient := &SafeClient{restConfig: config}
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatalf("NewRequestWithContext: %v", err)
			}

			resp, err := safeClient.HTTPDo(req)
			if err != nil {
				t.Fatalf("HTTPDo: %v", err)
			}
			closeResponse(t, resp)

			if got := <-gotAuth; got != test.wantAuth {
				t.Fatalf("Authorization = %q, want %q", got, test.wantAuth)
			}
		})
	}
}

func TestSafeClient_SetTLSCAData_ChangesRequestTrust(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)

	safeClient := &SafeClient{restConfig: &rest.Config{}}
	untrustedReq := newAuthorizedRequest(t, server.URL)

	resp, err := safeClient.HTTPDo(untrustedReq)
	if resp != nil {
		closeResponse(t, resp)
	}

	if err == nil {
		t.Fatal("HTTPDo error = nil before SetTLSCAData, want TLS trust failure")
	}

	safeClient.SetTLSCAData(testServerCAPEM(t, server))

	trustedReq := newAuthorizedRequest(t, server.URL)
	resp, err = safeClient.HTTPDo(trustedReq)
	if err != nil {
		t.Fatalf("HTTPDo after SetTLSCAData: %v", err)
	}

	if resp.StatusCode != http.StatusNoContent {
		closeResponse(t, resp)
		t.Fatalf("HTTPDo status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}

	closeResponse(t, resp)
}

func TestSafeClient_CopyIsIndependent(t *testing.T) {
	originalCAData := []byte("original CA data")
	original := &SafeClient{restConfig: &rest.Config{
		Host:    "https://original.example",
		Timeout: time.Second,
		TLSClientConfig: rest.TLSClientConfig{
			CAData:     originalCAData,
			ServerName: "original.example",
		},
	}}

	copied := original.Copy()
	copied.SetProbeEndpoint(2*time.Second, "https://copy.example", "copy.example")
	copied.SetTLSCAData(nil)

	if copied == original || copied.restConfig == original.restConfig {
		t.Fatal("Copy returned shared SafeClient state")
	}

	if original.restConfig.Host != "https://original.example" {
		t.Errorf("original Host = %q, want unchanged", original.restConfig.Host)
	}

	if original.restConfig.Timeout != time.Second {
		t.Errorf("original Timeout = %v, want %v", original.restConfig.Timeout, time.Second)
	}

	if original.restConfig.TLSClientConfig.ServerName != "original.example" {
		t.Errorf(
			"original ServerName = %q, want %q",
			original.restConfig.TLSClientConfig.ServerName,
			"original.example",
		)
	}

	if string(original.restConfig.TLSClientConfig.CAData) != string(originalCAData) {
		t.Errorf("original CAData = %q, want unchanged", original.restConfig.TLSClientConfig.CAData)
	}

	if original.restConfig.WrapTransport != nil {
		t.Error("original WrapTransport changed after mutating copy")
	}
}

func TestSafeClient_ZeroValueConfigFlagsIgnoreParsedOverrides(t *testing.T) {
	setClientGlobals(t, true, false)

	configDir := t.TempDir()
	environmentConfigPath := configDir + "/environment"
	overrideConfigPath := configDir + "/override"
	writeTestKubeconfig(t, environmentConfigPath, "current", map[string]string{
		"current":   "https://environment-current.example",
		"alternate": "https://environment-alternate.example",
	})
	writeTestKubeconfig(t, overrideConfigPath, "current", map[string]string{
		"current":   "https://override-current.example",
		"alternate": "https://override-alternate.example",
	})
	t.Setenv(clientcmd.RecommendedConfigPathEnvVar, environmentConfigPath)

	tests := []struct {
		name  string
		build func(flags *pflag.FlagSet) (*rest.Config, error)
	}{
		{
			name: "newRestConfig",
			build: func(flags *pflag.FlagSet) (*rest.Config, error) {
				return newRestConfig(flags)
			},
		},
		{
			name: "NewSafeClient",
			build: func(flags *pflag.FlagSet) (*rest.Config, error) {
				safeClient, err := NewSafeClient(flags)
				if err != nil {
					return nil, err
				}

				return safeClient.restConfig, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flags := pflag.NewFlagSet(test.name, pflag.ContinueOnError)
			flags.String("kubeconfig", "", "test kubeconfig path")
			flags.String("context", "", "test kubeconfig context")

			err := flags.Parse([]string{
				"--kubeconfig", overrideConfigPath,
				"--context", "alternate",
			})
			if err != nil {
				t.Fatalf("parse flags: %v", err)
			}

			config, err := test.build(flags)
			if err != nil {
				t.Fatalf("%s: %v", test.name, err)
			}

			if config.Host != "https://environment-current.example" {
				t.Fatalf(
					"Host = %q, want environment current context because zero-value ConfigFlags ignore parsed overrides",
					config.Host,
				)
			}
		})
	}
}

func setClientGlobals(t *testing.T, supportNoAuth, insecure bool) {
	t.Helper()

	previousSupportNoAuth := SupportNoAuth
	previousInsecure := Insecure
	SupportNoAuth = supportNoAuth
	Insecure = insecure

	t.Cleanup(func() {
		SupportNoAuth = previousSupportNoAuth
		Insecure = previousInsecure
	})
}

func writeTestKubeconfig(
	t *testing.T,
	path string,
	currentContext string,
	contextServers map[string]string,
) {
	t.Helper()

	config := clientcmdapi.Config{
		Clusters:       make(map[string]*clientcmdapi.Cluster, len(contextServers)),
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"test-user": {}},
		Contexts:       make(map[string]*clientcmdapi.Context, len(contextServers)),
		CurrentContext: currentContext,
	}

	for name, server := range contextServers {
		config.Clusters[name] = &clientcmdapi.Cluster{Server: server}
		config.Contexts[name] = &clientcmdapi.Context{
			Cluster:  name,
			AuthInfo: "test-user",
		}
	}

	if err := clientcmd.WriteToFile(config, path); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
}

func newAuthorizedRequest(t *testing.T, rawURL string) *http.Request {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	req.Header.Set("Authorization", "Bearer caller-token")

	return req
}

func testServerCAPEM(t *testing.T, server *httptest.Server) []byte {
	t.Helper()

	certificate := server.Certificate()
	if certificate == nil {
		t.Fatal("test TLS server certificate is nil")
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
}

func closeResponse(t *testing.T, resp *http.Response) {
	t.Helper()

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		_ = resp.Body.Close()
		t.Fatalf("drain response body: %v", err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close response body: %v", err)
	}
}

const retainedTestAuthProviderPluginName = "deckhouse-cli-retained-safe-client-test"

var registerRetainedTestAuthProvider = sync.OnceValues(func() (string, error) {
	err := rest.RegisterAuthProviderPlugin(retainedTestAuthProviderPluginName, func(
		_ string,
		_ map[string]string,
		_ rest.AuthProviderConfigPersister,
	) (rest.AuthProvider, error) {
		return retainedTestAuthProvider{}, nil
	})

	return retainedTestAuthProviderPluginName, err
})

type retainedTestAuthProvider struct{}

func (retainedTestAuthProvider) WrapTransport(roundTripper http.RoundTripper) http.RoundTripper {
	return retainedTestAuthRoundTripper{roundTripper: roundTripper}
}

func (retainedTestAuthProvider) Login() error {
	return nil
}

type retainedTestAuthRoundTripper struct {
	roundTripper http.RoundTripper
}

func (roundTripper retainedTestAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	cloned.Header.Set("Authorization", "Bearer provider-token")

	return roundTripper.roundTripper.RoundTrip(cloned)
}
