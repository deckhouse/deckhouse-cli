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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"unicode"

	"k8s.io/client-go/rest"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

const (
	headerAccept  = "Accept"
	mediaTypeJSON = "application/json"

	loggerName = "rpp"

	// maxTagsResponseBytes caps the tags JSON read so a misbehaving endpoint cannot
	// make the client buffer an unbounded response; real tag lists are a few KiB.
	maxTagsResponseBytes int64 = 4 << 20

	// maxBodySnippetBytes bounds how much of an error response body is echoed
	// into the error message.
	maxBodySnippetBytes = 256
)

// options holds the TLS settings used to build the proxy HTTP transport.
type options struct {
	caFile   string
	caData   []byte
	insecure bool
}

// Option configures the proxy client transport.
type Option func(*options)

// WithCAFile verifies the proxy TLS certificate against the CA bundle in the
// given PEM file, in addition to the system roots. Mutually exclusive with
// WithCAData and WithInsecureSkipTLSVerify.
func WithCAFile(path string) Option {
	return func(o *options) {
		o.caFile = path
	}
}

// WithCAData is WithCAFile with the PEM bytes supplied directly. Mutually
// exclusive with WithCAFile and WithInsecureSkipTLSVerify.
func WithCAData(pem []byte) Option {
	return func(o *options) {
		o.caData = pem
	}
}

// WithInsecureSkipTLSVerify disables proxy TLS verification. Intended for
// debugging only. Mutually exclusive with WithCAFile and WithCAData.
func WithInsecureSkipTLSVerify() Option {
	return func(o *options) {
		o.insecure = true
	}
}

// validate rejects contradictory TLS options instead of silently resolving them.
func (o options) validate() error {
	if o.insecure && (o.caFile != "" || len(o.caData) > 0) {
		return fmt.Errorf("%w: insecure TLS verification and a CA bundle are mutually exclusive", ErrUnsupportedConfig)
	}

	if o.caFile != "" && len(o.caData) > 0 {
		return fmt.Errorf("%w: WithCAFile and WithCAData are mutually exclusive", ErrUnsupportedConfig)
	}

	return nil
}

// Client talks to the registry-packages-proxy CLI routes, authenticating with
// the caller's kubeconfig identity.
type Client struct {
	baseURL string
	http    *http.Client
	logger  *dkplog.Logger
}

// New builds a Client whose requests carry the kubeconfig identity from
// restConfig. baseURL is the proxy endpoint root, for example
// "https://10.0.0.1:4219".
func New(baseURL string, restConfig *rest.Config, logger *dkplog.Logger, opts ...Option) (*Client, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	if err := o.validate(); err != nil {
		return nil, err
	}

	if err := validateBaseURL(baseURL); err != nil {
		return nil, err
	}

	if o.caFile != "" {
		pem, err := os.ReadFile(o.caFile)
		if err != nil {
			return nil, fmt.Errorf("read RPP CA file %q: %w", o.caFile, err)
		}

		o.caData = pem
	}

	httpClient, err := buildHTTPClient(restConfig, o)
	if err != nil {
		return nil, fmt.Errorf("build RPP HTTP client: %w", err)
	}

	return newClient(baseURL, httpClient, logger), nil
}

// NewWithHTTPClient builds a Client around a pre-built HTTP client. It is used in
// tests and by callers that construct the transport themselves.
func NewWithHTTPClient(baseURL string, httpClient *http.Client, logger *dkplog.Logger) *Client {
	return newClient(baseURL, httpClient, logger)
}

func newClient(baseURL string, httpClient *http.Client, logger *dkplog.Logger) *Client {
	// The transport stamps the kubeconfig credential on EVERY hop, so following a
	// redirect would replay it to whatever host the response names. The proxy
	// serves /v1/images directly (no 3xx on the happy path; verified live), so
	// refuse redirects and let the 3xx surface as an unexpected-status error.
	// The caller's client is copied, not mutated.
	guarded := *httpClient
	guarded.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &guarded,
		logger:  logger.Named(loggerName),
	}
}

// validateBaseURL ensures the explicit endpoint is a usable https URL, so a
// misconfigured --rpp-endpoint fails with a clear message instead of an opaque
// transport error on the first request.
func validateBaseURL(raw string) error {
	if raw == "" {
		return fmt.Errorf("%w: endpoint is empty", ErrInvalidEndpoint)
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidEndpoint, err)
	}

	if parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("%w: %q must be an https URL with a host", ErrInvalidEndpoint, raw)
	}

	return nil
}

// ListTags returns the available tags (versions) of the image.
func (c *Client) ListTags(ctx context.Context, ref ImageRef) ([]string, error) {
	c.logger.Debug("listing tags", slog.String("image", ref.String()))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+ref.tagsPath(), nil)
	if err != nil {
		return nil, fmt.Errorf("build list-tags request: %w", err)
	}

	req.Header.Set(headerAccept, mediaTypeJSON)

	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	var body tagListResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxTagsResponseBytes)).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode tags response for %q: %w", ref.String(), err)
	}

	return body.Tags, nil
}

// PullImage downloads the image tag as a gzipped tar stream (the binary, and the
// contract when present, are files inside it). The caller owns the returned
// reader and must close it.
//
// The stream is returned as-is: this method performs NO integrity check (the
// proxy exposes only a manifest digest, not a hash of the gzip-tar body), so
// trust rests on the TLS-authenticated proxy channel; the caller may want to cap
// the read with an io.LimitReader.
func (c *Client) PullImage(ctx context.Context, ref ImageRef, tag string) (io.ReadCloser, error) {
	if err := validateTag(tag); err != nil {
		return nil, err
	}

	c.logger.Debug("pulling image", slog.String("image", ref.String()), slog.String("tag", tag))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+ref.tagPath(tag), nil)
	if err != nil {
		return nil, fmt.Errorf("build pull request: %w", err)
	}

	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// do executes the request and, on a non-2xx status, closes the body and maps the
// status to a sentinel error. On success the response is returned with its body
// still open for the caller to consume.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, err)
	}

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return resp, nil
	}

	defer func() { _ = resp.Body.Close() }()

	return nil, statusError(req, resp)
}

func statusError(req *http.Request, resp *http.Response) error {
	switch resp.StatusCode {
	case http.StatusNotFound:
		return fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, ErrNotFound)
	case http.StatusUnauthorized:
		return fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, ErrUnauthorized)
	case http.StatusForbidden:
		return fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, ErrForbidden)
	}

	if resp.StatusCode >= http.StatusInternalServerError {
		return fmt.Errorf("%s %s: %w (status %d)%s", req.Method, req.URL.Path, ErrUpstream, resp.StatusCode, bodySnippet(resp.Body))
	}

	return fmt.Errorf("%s %s: unexpected status %d%s", req.Method, req.URL.Path, resp.StatusCode, bodySnippet(resp.Body))
}

// bodySnippet returns a short printable fragment of an error response body - the
// proxy and its intermediaries put the actual reason there (e.g. kube-rbac-proxy
// authorization denials), which would otherwise be discarded.
func bodySnippet(r io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(r, maxBodySnippetBytes))

	msg := strings.TrimSpace(string(raw))
	if msg == "" {
		return ""
	}

	// Flatten control characters (newlines of an HTML error page, ANSI noise) so
	// the snippet stays a single readable error-message line.
	msg = strings.Map(func(c rune) rune {
		if unicode.IsControl(c) {
			return ' '
		}

		return c
	}, msg)

	return ": " + msg
}
