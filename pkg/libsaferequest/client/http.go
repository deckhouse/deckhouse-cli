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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/pflag"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // load all auth plugins
	"k8s.io/client-go/rest"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	SupportNoAuth = true
	Insecure      = false
)

func newRestConfig(flags ...*pflag.FlagSet) (*rest.Config, error) {
	kubeConfigFlags := genericclioptions.ConfigFlags{}

	if len(flags) == 0 {
		flags = []*pflag.FlagSet{pflag.CommandLine}
	}

	for _, f := range flags {
		if flags != nil {
			kubeConfigFlags.AddFlags(f)
		}
	}

	restConfig, err := kubeConfigFlags.ToRESTConfig()
	if err != nil {
		return nil, err
	}

	if Insecure {
		restConfig.TLSClientConfig.CAData = []byte{}
		restConfig.TLSClientConfig.CAFile = ""
		restConfig.TLSClientConfig.Insecure = true
	}

	return restConfig, nil
}

type SafeClient struct {
	restConfig *rest.Config
}

// PersistentHTTPClient owns one materialized client-go HTTP transport stack.
// It is safe for concurrent use. Call CloseIdleConnections when the caller's
// lifecycle ends so this client's private connection pool is released.
type PersistentHTTPClient struct {
	client             *http.Client
	ownedTransport     http.RoundTripper
	ownedHTTPTransport *http.Transport
	hasConfiguredAuth  bool
}

func NewSafeClient(flags ...*pflag.FlagSet) (*SafeClient, error) {
	restConfig, err := newRestConfig(flags...)
	if err != nil {
		return nil, err
	}

	return &SafeClient{restConfig}, nil
}

// NewPersistentHTTPClient materializes the current rest.Config exactly once,
// retaining client-go's TLS, proxy, dial, certificate, exec, auth-provider,
// bearer, and basic-auth behavior for every request made through the result.
//
// The standard client-go base *http.Transport is cloned before caller-installed
// WrapTransport functions run. Its inherited HTTP/2 upgrade closures are
// suppressed while those wrappers clone and customize the transport, then
// HTTP/2 is configured anew on the final private transport. Cleanup retains
// both the pre-auth wrapper stack and the private base transport.
func (c *SafeClient) NewPersistentHTTPClient() (*PersistentHTTPClient, error) {
	if c == nil || c.restConfig == nil {
		return nil, errors.New("build persistent HTTP client: no rest config")
	}

	config := rest.CopyConfig(c.restConfig)
	prev := config.WrapTransport

	var (
		ownedTransport     http.RoundTripper
		ownedHTTPTransport *http.Transport
	)

	config.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		var (
			clonedHTTPTransport *http.Transport
			enableHTTP2         bool
		)

		if transport, ok := rt.(*http.Transport); ok {
			cloned := transport.Clone()
			enableHTTP2 = cloned.ForceAttemptHTTP2 || cloned.TLSNextProto["h2"] != nil
			// Clone copies client-go's x/net/http2 TLSNextProto closures. An
			// empty map prevents intermediate caller wrappers from enabling or
			// copying HTTP/2 while they clone this transport.
			cloned.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
			cloned.ForceAttemptHTTP2 = false
			clonedHTTPTransport = cloned
			rt = cloned
		}

		if prev != nil {
			rt = prev(rt)
		}

		ownedTransport = rt

		ownedHTTPTransport = findHTTPTransport(rt)
		if ownedHTTPTransport == nil {
			ownedHTTPTransport = clonedHTTPTransport
		}

		if ownedHTTPTransport != nil && enableHTTP2 {
			ownedHTTPTransport.TLSNextProto = nil
			ownedHTTPTransport.ForceAttemptHTTP2 = true
			utilnet.SetTransportDefaults(ownedHTTPTransport)
		}

		return rt
	}

	httpClient, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, fmt.Errorf("build persistent HTTP client: %w", err)
	}

	return &PersistentHTTPClient{
		client:             httpClient,
		ownedTransport:     ownedTransport,
		ownedHTTPTransport: ownedHTTPTransport,
		hasConfiguredAuth:  hasConfiguredAuth(config),
	}, nil
}

// Do sends req through the persistent authenticated transport.
func (c *PersistentHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return c.HTTPDo(req)
}

// HTTPDo sends req through the persistent authenticated transport.
func (c *PersistentHTTPClient) HTTPDo(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Authorization") == "" && !c.hasConfiguredAuth && !SupportNoAuth {
		return nil, errors.New("No auth")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("persistent HTTP client do request: %w", err)
	}

	return resp, nil
}

// CloseIdleConnections closes this client's privately owned idle pool.
func (c *PersistentHTTPClient) CloseIdleConnections() {
	if c == nil {
		return
	}

	utilnet.CloseIdleConnectionsFor(c.ownedTransport)

	if c.ownedHTTPTransport != nil {
		c.ownedHTTPTransport.CloseIdleConnections()
	}
}

func findHTTPTransport(rt http.RoundTripper) *http.Transport {
	for rt != nil {
		if transport, ok := rt.(*http.Transport); ok {
			return transport
		}

		wrapper, ok := rt.(utilnet.RoundTripperWrapper)
		if !ok {
			return nil
		}

		rt = wrapper.WrappedRoundTripper()
	}

	return nil
}

func hasConfiguredAuth(config *rest.Config) bool {
	hasBasicAuth := config.Username != "" || config.Password != ""
	hasCertificateAuth := (config.CertData != nil || config.CertFile != "") &&
		(config.KeyData != nil || config.KeyFile != "")

	return hasBasicAuth ||
		config.BearerToken != "" ||
		config.BearerTokenFile != "" ||
		hasCertificateAuth ||
		config.ExecProvider != nil ||
		config.AuthProvider != nil
}

// SetProbeEndpoint configures host, TLS ServerName and timeout for probe requests.
func (c *SafeClient) SetProbeEndpoint(timeout time.Duration, targetHost, kubeServiceServerName string) {
	c.restConfig.Host = targetHost
	c.restConfig.TLSClientConfig.ServerName = kubeServiceServerName
	c.restConfig.Timeout = timeout
}

// SetQPS raises the underlying rest.Config's client-side rate limiter above
// client-go's built-in defaults (QPS=5, Burst=10). Callers with many
// concurrent short-lived requests against the SAME client (e.g. several
// DataExport Get/Create/Delete lifecycles racing to completion) opt into this
// explicitly; SafeClient's own default is unchanged for every other caller of
// NewSafeClient that never calls it.
func (c *SafeClient) SetQPS(qps float32, burst int) {
	c.restConfig.QPS = qps
	c.restConfig.Burst = burst
}

func (c *SafeClient) HTTPDo(req *http.Request) (*http.Response, error) {
	if len(req.Header.Get("Authorization")) != 0 {
		httpClient, err := rest.HTTPClientFor(c.restConfig)
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request header auth do request: %w", err)
		}

		return resp, nil
	}

	// BasicAuth || TokenAuth
	if len(c.restConfig.Password) != 0 || len(c.restConfig.BearerToken) != 0 || len(c.restConfig.BearerTokenFile) != 0 {
		httpClient, err := rest.HTTPClientFor(c.restConfig)
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("basic/token auth do request: %w", err)
		}

		return resp, nil
	}

	// CertAuth
	if (len(c.restConfig.CertData) != 0 || len(c.restConfig.CertFile) != 0) &&
		(len(c.restConfig.KeyData) != 0 || len(c.restConfig.KeyFile) != 0) {
		httpClient, err := rest.HTTPClientFor(c.restConfig)
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("certificate auth do request: %w", err)
		}

		return resp, nil
	}

	// Ather AuthProvider
	if c.restConfig.AuthProvider != nil {
		httpClient, err := rest.HTTPClientFor(c.restConfig)
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("auth provider do request: %w", err)
		}

		return resp, nil
	}

	if SupportNoAuth {
		httpClient := &http.Client{}

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("no auth do request: %w", err)
		}

		return resp, nil
	}

	return nil, errors.New("No auth")
}

func (c *SafeClient) NewRTClient(schemeFuncs ...func(s *apiruntime.Scheme) error) (ctrlrtclient.Client, error) {
	if c.restConfig == nil {
		return nil, fmt.Errorf("No rest config")
	}

	schemeFuncs = append(schemeFuncs, kubescheme.AddToScheme)

	scheme := apiruntime.NewScheme()
	for _, f := range schemeFuncs {
		if err := f(scheme); err != nil {
			return nil, err
		}
	}

	clientOpts := ctrlrtclient.Options{
		Scheme: scheme,
	}

	kubeRtClient, err := ctrlrtclient.New(c.restConfig, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("kubernetes runtime client error: %s", err.Error())
	}

	return kubeRtClient, nil
}

func (c *SafeClient) SetTLSCAData(caData []byte) {
	sysPool, err := x509.SystemCertPool()
	if err != nil || sysPool == nil {
		sysPool = x509.NewCertPool()
	}

	if len(caData) > 0 {
		sysPool.AppendCertsFromPEM(caData)
	}

	if c.restConfig.TLSClientConfig.CAData != nil {
		sysPool.AppendCertsFromPEM(c.restConfig.TLSClientConfig.CAData)
	}

	c.restConfig.TLSClientConfig.CAData = nil
	c.restConfig.TLSClientConfig.CAFile = ""
	c.restConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		transport, ok := rt.(*http.Transport)
		if !ok {
			// CA-pool injection is a best-effort enhancement over *http.Transport;
			// for any other RoundTripper degrade to pass-through so we never hand
			// back a typed-nil transport that nil-panics on RoundTrip.
			return rt
		}

		clonedTransport := transport.Clone()
		if clonedTransport.TLSClientConfig == nil {
			clonedTransport.TLSClientConfig = &tls.Config{}
		}

		clonedTransport.TLSClientConfig.RootCAs = sysPool

		return clonedTransport
	}
}

// SetResponseHeaderTimeout makes this client abort a request whose server
// accepts the TCP connection but does not send response headers within timeout,
// so a wedged endpoint that never answers fails fast instead of blocking a
// caller indefinitely (rest.HTTPClientFor builds its transport with
// restConfig.Timeout = 0, i.e. no response-header timeout by default).
//
// It is strictly opt-in and mutates only THIS client's rest.Config: it chains
// onto any existing WrapTransport (e.g. the one SetTLSCAData installs) rather
// than replacing it, and a SafeClient that never calls it keeps its previous
// behavior (WrapTransport unchanged). The timeout is applied to the transport
// that rest.HTTPClientFor builds for the credential-bearing branches of HTTPDo.
func (c *SafeClient) SetResponseHeaderTimeout(timeout time.Duration) {
	prev := c.restConfig.WrapTransport

	c.restConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		if prev != nil {
			rt = prev(rt)
		}

		transport, ok := rt.(*http.Transport)
		if !ok {
			return rt
		}

		cloned := transport.Clone()
		cloned.ResponseHeaderTimeout = timeout

		return cloned
	}
}

func (c *SafeClient) Copy() *SafeClient {
	return &SafeClient{rest.CopyConfig(c.restConfig)}
}

// RESTConfig returns a deep copy of the underlying *rest.Config so callers (e.g. the
// aggregated-API client) can build their own discovery REST client without mutating
// or depending on the SafeClient's auth handling.
func (c *SafeClient) RESTConfig() *rest.Config {
	if c.restConfig == nil {
		return nil
	}

	return rest.CopyConfig(c.restConfig)
}
