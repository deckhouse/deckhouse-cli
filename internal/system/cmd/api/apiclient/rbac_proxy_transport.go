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
	"bytes"
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
)

const kubeRBACProxyServerName = "deckhouse-kube-rbac-proxy.d8-system.svc"

// RBACProxyTransport reaches the controller API through the Pod's HTTPS port,
// where each request is checked against Kubernetes RBAC.
// It presents kubeconfig credentials and verifies the serving certificate of that port.
type RBACProxyTransport struct {
	httpClient *http.Client
	endpoint   *url.URL
}

// NewRBACProxyTransport targets an HTTPS origin, such as https://<pod-ip>:4204.
// caPEM contains the CA of that origin, which can differ from the API server CA.
// The request context and config.Timeout bound requests, including long profiles.
func NewRBACProxyTransport(config *rest.Config, endpoint string, caPEM []byte) (*RBACProxyTransport, error) {
	if config == nil {
		return nil, fmt.Errorf("kubeconfig REST configuration is required")
	}

	proxyURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse protected controller API endpoint: %w", err)
	}

	if proxyURL.Scheme != "https" || proxyURL.Hostname() == "" || proxyURL.User != nil ||
		(proxyURL.Path != "" && proxyURL.Path != "/") || proxyURL.RawQuery != "" ||
		proxyURL.ForceQuery || proxyURL.Fragment != "" || proxyURL.Opaque != "" {
		return nil, fmt.Errorf("protected controller API endpoint must be an HTTPS origin without credentials, path, query or fragment")
	}

	if !x509.NewCertPool().AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("protected controller API CA must contain a PEM certificate")
	}

	// The endpoint authorizes the authenticated identity, not Impersonate-* headers.
	impersonation := config.Impersonate
	if impersonation.UserName != "" || impersonation.UID != "" || len(impersonation.Groups) != 0 || len(impersonation.Extra) != 0 {
		return nil, fmt.Errorf("protected controller API access does not support kubeconfig impersonation")
	}

	if config.Transport != nil || config.WrapTransport != nil {
		return nil, fmt.Errorf("protected controller API access does not support custom kubeconfig transports")
	}

	// Credential plugins need the original Kubernetes cluster information.
	kubeConfig := rest.CopyConfig(config)

	transportConfig, err := kubeConfig.TransportConfig()
	if err != nil {
		return nil, fmt.Errorf("configure kubeconfig authentication: %w", err)
	}

	// Keep client credentials, but verify the endpoint against its own CA and name.
	transportConfig.TLS.CAFile = ""
	transportConfig.TLS.CAData = bytes.Clone(caPEM)
	transportConfig.TLS.Insecure = false
	transportConfig.TLS.ServerName = kubeRBACProxyServerName

	roundTripper, err := transport.New(transportConfig)
	if err != nil {
		return nil, fmt.Errorf("create protected controller API transport: %w", err)
	}

	return &RBACProxyTransport{
		httpClient: &http.Client{
			Transport: roundTripper,
			Timeout:   config.Timeout,
			// Redirects must not send a bearer token or client certificate elsewhere.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		endpoint: proxyURL,
	}, nil
}

// Get prefixes an escaped controller path with /controller and preserves the response.
// Authentication failures and redirects are returned without a fallback transport.
func (t *RBACProxyTransport) Get(ctx context.Context, path string, query url.Values) (*Response, error) {
	route, err := url.ParseRequestURI(path)
	if err != nil || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") {
		return nil, fmt.Errorf("controller path must be an absolute URL path")
	}

	if route.RawQuery != "" || route.ForceQuery || strings.Contains(path, "#") {
		return nil, fmt.Errorf("controller path must not contain a query or fragment")
	}

	for segment := range strings.SplitSeq(route.Path, "/") {
		if segment == "." || segment == ".." {
			return nil, fmt.Errorf("controller path must not contain dot segments")
		}
	}

	requestURL := *t.endpoint
	requestURL.Path = "/controller" + route.Path

	requestURL.RawPath = ""
	if route.RawPath != "" {
		requestURL.RawPath = "/controller" + route.RawPath
	}

	requestURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build protected controller API request: %w", err)
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s through the protected controller API: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("GET %s through the protected controller API: read body: %w", path, err)
	}

	return &Response{StatusCode: resp.StatusCode, Body: body}, nil
}
