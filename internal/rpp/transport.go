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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"time"

	"k8s.io/client-go/rest"
)

const (
	// tlsHandshakeTimeout / responseHeaderTimeout bound connection setup and the
	// wait for response headers, so a wedged proxy cannot hang a command forever.
	// Body streaming is intentionally unbounded - image downloads may legitimately
	// take minutes.
	tlsHandshakeTimeout   = 10 * time.Second
	responseHeaderTimeout = 30 * time.Second
)

// buildHTTPClient builds an HTTP client that carries the kubeconfig identity
// (bearer token, client certificate or exec credentials) from restConfig, but
// verifies TLS against the proxy endpoint instead of the Kubernetes API server.
//
// The kubeconfig's API-server CA is dropped on purpose: the proxy endpoint
// (master hostPort or public Ingress) is served by a different certificate.
// Server trust is established from opts - a custom CA bundle, or the system
// roots by default, or skipped entirely when insecure.
//
// Notes:
//   - restConfig is copied; the caller's config is never mutated.
//   - The kubeconfig Proxy setting is kept, so RPP traffic uses the same HTTP
//     proxy (if any) as API-server traffic.
//   - The identity is attached to every request, so use the client only against
//     the proxy endpoint.
func buildHTTPClient(restConfig *rest.Config, opts options) (*http.Client, error) {
	cfg := rest.CopyConfig(restConfig)

	// CA injection (below) overrides the root CAs of the *http.Transport that
	// client-go builds from cfg.TLS. A caller-supplied base transport would defeat
	// that and silently skip verification, so reject it rather than fail open.
	if cfg.Transport != nil {
		return nil, fmt.Errorf("%w: rest.Config carries a custom Transport", ErrUnsupportedConfig)
	}

	// Reset server verification to a known baseline (system roots, verified), then
	// apply the requested mode. Identity fields (token/cert/exec) are left intact.
	cfg.TLSClientConfig.CAData = nil
	cfg.TLSClientConfig.CAFile = ""
	cfg.TLSClientConfig.Insecure = false

	var pool *x509.CertPool

	switch {
	case opts.insecure:
		cfg.TLSClientConfig.Insecure = true
	case len(opts.caData) > 0:
		var err error

		pool, err = certPoolWith(opts.caData)
		if err != nil {
			return nil, err
		}
	}

	cfg.WrapTransport = withTunedTransport(cfg.WrapTransport, pool)

	return rest.HTTPClientFor(cfg)
}

// certPoolWith returns the system certificate pool extended with the given PEM
// CA. A failure to load the system pool is fatal rather than silently narrowing
// trust to the supplied CA alone (which would contradict the documented "in
// addition to the system roots" behavior).
func certPoolWith(caPEM []byte) (*x509.CertPool, error) {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("load system certificate pool: %w", err)
	}

	if pool == nil {
		pool = x509.NewCertPool()
	}

	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("%w: no certificates parsed from CA data", ErrInvalidCA)
	}

	return pool, nil
}

// withTunedTransport returns a transport wrapper that, innermost to outermost:
//   - clones the base *http.Transport
//   - bounds connection setup (TLS handshake, response-header wait)
//   - overrides the root CAs when pool is non-nil
//   - applies any pre-existing wrapper (e.g. an OIDC auth-provider) on top
//
// Tuning must hit the raw base transport, so it runs innermost. If the base is
// not an *http.Transport the request fails closed, rather than proceeding without
// the intended trust and bounds.
func withTunedTransport(prev func(http.RoundTripper) http.RoundTripper, pool *x509.CertPool) func(http.RoundTripper) http.RoundTripper {
	return func(rt http.RoundTripper) http.RoundTripper {
		base, ok := rt.(*http.Transport)
		if !ok {
			return errorRoundTripper{err: fmt.Errorf("%w: base transport is %T, want *http.Transport", ErrUnsupportedConfig, rt)}
		}

		cloned := base.Clone()
		cloned.TLSHandshakeTimeout = tlsHandshakeTimeout
		cloned.ResponseHeaderTimeout = responseHeaderTimeout

		if pool != nil {
			if cloned.TLSClientConfig == nil {
				cloned.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
			}

			cloned.TLSClientConfig.RootCAs = pool
		}

		var wrapped http.RoundTripper = cloned
		if prev != nil {
			wrapped = prev(wrapped)
		}

		return wrapped
	}
}

// errorRoundTripper fails every request with a fixed error. It lets the CA
// wrapper fail closed, so a trust override never degrades into an unverified
// connection.
type errorRoundTripper struct {
	err error
}

func (e errorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, e.err
}
