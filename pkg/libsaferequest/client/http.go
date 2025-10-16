package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
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

func (c *SafeClient) HTTPDo(req *http.Request) (*http.Response, error) {
	if len(req.Header.Get("Authorization")) != 0 {
		httpClient, err := rest.HTTPClientFor(c.restConfig)
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		return resp, err
	}

	// BasicAuth || TokenAuth
	if len(c.restConfig.Password) != 0 || len(c.restConfig.BearerToken) != 0 || len(c.restConfig.BearerTokenFile) != 0 {
		httpClient, err := rest.HTTPClientFor(c.restConfig)
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		return resp, err
	}

	// CertAuth
	if (len(c.restConfig.CertData) != 0 || len(c.restConfig.CertFile) != 0) &&
		(len(c.restConfig.KeyData) != 0 || len(c.restConfig.KeyFile) != 0) {
		httpClient, err := rest.HTTPClientFor(c.restConfig)
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		return resp, err
	}

	// Ather AuthProvider
	if c.restConfig.AuthProvider != nil {
		httpClient, err := rest.HTTPClientFor(c.restConfig)
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		return resp, err
	}

	if SupportNoAuth {
		httpClient := &http.Client{}

		resp, err := httpClient.Do(req)
		return resp, err
	}

	return nil, fmt.Errorf("No auth")
}

func (c *SafeClient) NewRTClient(schemeFuncs ...(func(s *runtime.Scheme) error)) (ctrlrtclient.Client, error) {
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

func (c *SafeClient) Copy() *SafeClient {
	return &SafeClient{rest.CopyConfig(c.restConfig)}
}
