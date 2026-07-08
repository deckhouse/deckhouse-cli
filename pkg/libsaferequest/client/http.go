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

func NewSafeClient(flags ...*pflag.FlagSet) (*SafeClient, error) {
	restConfig, err := newRestConfig(flags...)
	if err != nil {
		return nil, err
	}

	return &SafeClient{restConfig}, nil
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
			return transport
		}

		clonedTrasport := transport.Clone()
		if clonedTrasport.TLSClientConfig == nil {
			clonedTrasport.TLSClientConfig = &tls.Config{}
		}

		clonedTrasport.TLSClientConfig.RootCAs = sysPool

		return clonedTrasport
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
