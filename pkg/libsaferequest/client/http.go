package client

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/spf13/pflag"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // load all auth plugins
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlrtclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	SupportNoAuth = true
	Insecure      = false
)

func newClientConfig(flags ...*pflag.FlagSet) clientcmd.ClientConfig {
	kubeConfigFlags := genericclioptions.ConfigFlags{}

	if len(flags) == 0 {
		flags = []*pflag.FlagSet{pflag.CommandLine}
	}

	for _, f := range flags {
		if flags != nil {
			kubeConfigFlags.AddFlags(f)
		}
	}

	return kubeConfigFlags.ToRawKubeConfigLoader()
}

func newRestConfig(clientConfig clientcmd.ClientConfig) (*rest.Config, error) {
	restConfig, err := clientConfig.ClientConfig()
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
	restConfig        *rest.Config
	clientConfig      clientcmd.ClientConfig
	skipDataTLSVerify bool
}

var warnSkipDataTLSVerifyOnce sync.Once

// SetSkipDataTLSVerify makes SetTLSCAData build a transport that does not verify
// the server certificate of the data endpoint. The setting is kept by Copy.
func (c *SafeClient) SetSkipDataTLSVerify(skip bool) {
	c.skipDataTLSVerify = skip
}

func NewSafeClient(flags ...*pflag.FlagSet) (*SafeClient, error) {
	clientConfig := newClientConfig(flags...)

	restConfig, err := newRestConfig(clientConfig)
	if err != nil {
		return nil, err
	}

	return &SafeClient{restConfig: restConfig, clientConfig: clientConfig}, nil
}

// Namespace returns the namespace of the kubeconfig context the client connects through, as kubectl
// does. When the context sets none, inside a pod it is POD_NAMESPACE, then the namespace of the
// pod's service account; otherwise it is "default".
func (c *SafeClient) Namespace() (string, error) {
	if c == nil || c.clientConfig == nil {
		return "", errors.New("the client was not built from a kubeconfig")
	}

	namespace, _, err := c.clientConfig.Namespace()
	if err != nil {
		return "", fmt.Errorf("resolve the namespace of the kubeconfig context: %w", err)
	}

	return namespace, nil
}

// NewSafeClientForConfig derives a SafeClient from an already-resolved REST
// configuration instead of re-parsing --kubeconfig/--context flags, so a probe
// built from it targets the same cluster the caller already resolved.
func NewSafeClientForConfig(config *rest.Config) *SafeClient {
	return &SafeClient{restConfig: rest.CopyConfig(config)}
}

// SetProbeEndpoint configures host, TLS ServerName and timeout for probe requests.
func (c *SafeClient) SetProbeEndpoint(timeout time.Duration, targetHost, kubeServiceServerName string) {
	c.restConfig.Host = targetHost
	c.restConfig.TLSClientConfig.ServerName = kubeServiceServerName
	c.restConfig.Timeout = timeout
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
	c.restConfig.TLSClientConfig.Insecure = false
	c.restConfig.TLSClientConfig.ServerName = ""
	prev := c.restConfig.WrapTransport
	skipVerify := c.skipDataTLSVerify

	if skipVerify {
		warnSkipDataTLSVerifyOnce.Do(func() {
			fmt.Fprintln(os.Stderr, "WARNING: TLS certificate verification of the data endpoint is disabled; "+
				"the identity of the server is not verified and the credentials may be exposed")
		})
	}

	c.restConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		if prev != nil {
			rt = prev(rt)
		}

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
		clonedTransport.TLSClientConfig.InsecureSkipVerify = skipVerify
		clonedTransport.TLSClientConfig.ServerName = ""

		return clonedTransport
	}
}

func (c *SafeClient) Copy() *SafeClient {
	return &SafeClient{
		restConfig:        rest.CopyConfig(c.restConfig),
		clientConfig:      c.clientConfig,
		skipDataTLSVerify: c.skipDataTLSVerify,
	}
}

// RESTConfig returns a copy of the client's Kubernetes configuration, for callers that need a
// client-go client this type does not build itself (discovery, SelfSubjectAccessReview). A copy
// rather than the original: the caller's client must not be able to retarget or re-authenticate
// every other client derived from this one, the way SetProbeEndpoint deliberately does.
// Returns nil when the client holds no configuration.
func (c *SafeClient) RESTConfig() *rest.Config {
	if c == nil || c.restConfig == nil {
		return nil
	}

	return rest.CopyConfig(c.restConfig)
}
