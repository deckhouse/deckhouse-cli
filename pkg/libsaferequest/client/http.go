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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
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
	restConfig       *rest.Config
	networkTimeouts  NetworkTimeouts
	idleTimerFactory idleTimerFactory
	idleNow          func() time.Time
}

// NetworkTimeouts bounds silent stalls in each HTTP transport phase without
// imposing one total duration on a request whose body keeps making progress.
type NetworkTimeouts struct {
	Connect        time.Duration
	TLSHandshake   time.Duration
	ResponseHeader time.Duration
	WriteIdle      time.Duration
	ReadIdle       time.Duration
	ResponseTotal  time.Duration
	ResponseBytes  int64
}

type idleTimer interface {
	Reset(time.Duration) bool
	Stop() bool
}

type idleTimerFactory func(time.Duration, func()) idleTimer

// PersistentHTTPClient owns one materialized client-go HTTP transport stack.
// It is safe for concurrent use. Call CloseIdleConnections when the caller's
// lifecycle ends so this client's private connection pool is released.
type PersistentHTTPClient struct {
	client             *http.Client
	ownedTransport     http.RoundTripper
	ownedHTTPTransport *http.Transport
	lifecycle          *ownedTransportLifecycle
	hasConfiguredAuth  bool
	origin             *httpOrigin
	networkTimeouts    NetworkTimeouts
	idleTimerFactory   idleTimerFactory
	idleNow            func() time.Time
}

type ownedTransportLifecycle struct {
	mu             sync.Mutex
	cond           *sync.Cond
	connections    map[*ownedConnection]struct{}
	closingStarted chan struct{}
	inFlight       int
	closing        bool
}

func newOwnedTransportLifecycle() *ownedTransportLifecycle {
	lifecycle := &ownedTransportLifecycle{
		connections:    make(map[*ownedConnection]struct{}),
		closingStarted: make(chan struct{}),
	}
	lifecycle.cond = sync.NewCond(&lifecycle.mu)

	return lifecycle
}

func (l *ownedTransportLifecycle) trackDials(transport *http.Transport) {
	if transport == nil || transport.DialContext == nil {
		return
	}

	dialContext := transport.DialContext
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}

		return l.trackConnection(conn), nil
	}
}

func (l *ownedTransportLifecycle) trackConnection(conn net.Conn) net.Conn {
	tracked := &ownedConnection{
		Conn:      conn,
		lifecycle: l,
	}

	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()

		_ = tracked.Close()

		return tracked
	}

	l.connections[tracked] = struct{}{}
	l.mu.Unlock()

	return tracked
}

func (l *ownedTransportLifecycle) beginRequest() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closing {
		return errors.New("persistent HTTP client is closed")
	}

	l.inFlight++

	return nil
}

func (l *ownedTransportLifecycle) endRequest() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.inFlight--
	if l.inFlight == 0 {
		l.cond.Broadcast()
	}
}

func (l *ownedTransportLifecycle) waitForRequests() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.closing {
		l.closing = true
		close(l.closingStarted)
	}

	for l.inFlight > 0 {
		l.cond.Wait()
	}
}

func (l *ownedTransportLifecycle) closeConnections() {
	l.mu.Lock()

	connections := make([]*ownedConnection, 0, len(l.connections))
	for conn := range l.connections {
		connections = append(connections, conn)
	}
	l.mu.Unlock()

	for _, conn := range connections {
		_ = conn.Close()
	}
}

func (l *ownedTransportLifecycle) removeConnection(conn *ownedConnection) {
	l.mu.Lock()
	delete(l.connections, conn)
	l.mu.Unlock()
}

type ownedConnection struct {
	net.Conn
	lifecycle *ownedTransportLifecycle
	closeOnce sync.Once
	closeErr  error
}

func (c *ownedConnection) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Conn.Close()
		c.lifecycle.removeConnection(c)
	})

	return c.closeErr
}

func NewSafeClient(flags ...*pflag.FlagSet) (*SafeClient, error) {
	restConfig, err := newRestConfig(flags...)
	if err != nil {
		return nil, err
	}

	return &SafeClient{restConfig: restConfig}, nil
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
	lifecycle := newOwnedTransportLifecycle()

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

		lifecycle.trackDials(ownedHTTPTransport)

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
		lifecycle:          lifecycle,
		hasConfiguredAuth:  hasConfiguredAuth(config),
		networkTimeouts:    c.networkTimeouts,
		idleTimerFactory:   c.idleTimerFactory,
		idleNow:            c.idleNow,
	}, nil
}

// NewPersistentHTTPClientForOrigin materializes a persistent client whose
// credential-bearing transport can only send to rawURL's HTTP origin. Direct
// cross-origin requests and cross-origin redirects are rejected before the auth
// wrappers see them; same-origin redirects retain the standard ten-hop limit.
func (c *SafeClient) NewPersistentHTTPClientForOrigin(rawURL string) (*PersistentHTTPClient, error) {
	origin, err := parseHTTPOrigin(rawURL)
	if err != nil {
		return nil, fmt.Errorf("build origin-bound persistent HTTP client: %w", err)
	}

	return c.newPersistentHTTPClientForOrigin(origin)
}

// NewPersistentHTTPSClientForOrigin materializes an origin-bound client for a
// credential-bearing HTTPS endpoint. Plaintext origins are rejected before the
// authenticated transport stack is built.
func (c *SafeClient) NewPersistentHTTPSClientForOrigin(rawURL string) (*PersistentHTTPClient, error) {
	origin, err := parseHTTPSOrigin(rawURL)
	if err != nil {
		return nil, fmt.Errorf("build HTTPS origin-bound persistent HTTP client: %w", err)
	}

	return c.newPersistentHTTPClientForOrigin(origin)
}

func (c *SafeClient) newPersistentHTTPClientForOrigin(origin httpOrigin) (*PersistentHTTPClient, error) {
	httpClient, err := c.NewPersistentHTTPClient()
	if err != nil {
		return nil, err
	}

	httpClient.origin = &origin
	httpClient.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}

		if !origin.matches(req.URL) {
			return fmt.Errorf("refuse redirect from upload origin %s://%s to %s", origin.scheme, origin.authority(), req.URL)
		}

		return nil
	}

	return httpClient, nil
}

// Do sends req through the persistent authenticated transport.
func (c *PersistentHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return c.HTTPDo(req)
}

// HTTPDo sends req through the persistent authenticated transport.
func (c *PersistentHTTPClient) HTTPDo(req *http.Request) (*http.Response, error) {
	if c.origin != nil && !c.origin.matches(req.URL) {
		return nil, fmt.Errorf(
			"persistent HTTP client origin is %s://%s, refuse request to %s",
			c.origin.scheme,
			c.origin.authority(),
			req.URL,
		)
	}

	if req.Header.Get("Authorization") == "" && !c.hasConfiguredAuth && !SupportNoAuth {
		return nil, errors.New("No auth")
	}

	if err := c.lifecycle.beginRequest(); err != nil {
		return nil, err
	}

	var lifecycleParts atomic.Int32
	lifecycleParts.Store(1)

	completeLifecyclePart := func() {
		if lifecycleParts.Add(-1) == 0 {
			c.lifecycle.endRequest()
		}
	}

	parentCtx := req.Context()
	requestCtx, cancelRequest := context.WithCancelCause(parentCtx)
	watchdog := newIdleWatchdog(cancelRequest, c.idleTimerFactory, c.idleNow)
	request := req.Clone(requestCtx)

	var (
		bodyTracker     *progressRequestBody
		headersReceived atomic.Bool
		responseHasBody atomic.Bool
	)

	if request.Body != nil && request.Body != http.NoBody && request.ContentLength != 0 {
		lifecycleParts.Add(1)

		if notifier, ok := request.Body.(networkStallNotifier); ok {
			watchdog.setNotifier(notifier.NetworkStall)
		}

		watchdog.arm("request body write", c.networkTimeouts.WriteIdle)
		bodyTracker = &progressRequestBody{
			ReadCloser: request.Body,
			onProgress: watchdog.progress,
			onClose: func() {
				defer completeLifecyclePart()

				if headersReceived.Load() {
					watchdog.stop()

					if !responseHasBody.Load() {
						cancelRequest(nil)
					}

					return
				}

				watchdog.arm("response headers", c.networkTimeouts.ResponseHeader)
			},
		}
		request.Body = bodyTracker
	} else {
		watchdog.arm("response headers", c.networkTimeouts.ResponseHeader)
	}

	resp, err := c.client.Do(request)
	if err != nil {
		watchdog.stop()

		cause := watchdog.cause()
		if cause == nil && parentCtx.Err() != nil {
			cause = context.Cause(parentCtx)
		}

		if cause == nil {
			var timeoutErr net.Error
			if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
				cause = context.DeadlineExceeded
			}
		}

		cancelRequest(nil)
		completeLifecyclePart()

		return nil, fmt.Errorf("persistent HTTP client do request: %w", errors.Join(err, cause))
	}

	if resp == nil {
		watchdog.stop()
		cancelRequest(nil)
		completeLifecyclePart()

		return nil, errors.New("persistent HTTP client returned a nil response without an error")
	}

	hasResponseBody := resp.Body != nil && resp.Body != http.NoBody
	responseHasBody.Store(hasResponseBody)
	headersReceived.Store(true)

	if bodyTracker == nil || bodyTracker.isClosed() {
		watchdog.stop()
	}

	if !hasResponseBody {
		if bodyTracker == nil || bodyTracker.isClosed() {
			cancelRequest(nil)
		}

		completeLifecyclePart()

		return resp, nil
	}

	resp.Body = newProgressResponseBody(
		resp.Body,
		parentCtx,
		cancelRequest,
		c.networkTimeouts.ReadIdle,
		c.networkTimeouts.ResponseTotal,
		c.networkTimeouts.ResponseBytes,
		c.idleTimerFactory,
		c.idleNow,
		completeLifecyclePart,
	)

	return resp, nil
}

type networkStallError struct {
	phase   string
	timeout time.Duration
}

func (e *networkStallError) Error() string {
	return fmt.Sprintf("%s made no progress for %s", e.phase, e.timeout)
}

func (e *networkStallError) Unwrap() error {
	return context.DeadlineExceeded
}

type idleWatchdog struct {
	mu           sync.Mutex
	cancel       context.CancelCauseFunc
	timerFactory idleTimerFactory
	now          func() time.Time
	timer        idleTimer
	timeout      time.Duration
	deadline     time.Time
	phase        string
	stallErr     error
	notify       func(error)
	stopped      bool
}

func newIdleWatchdog(
	cancel context.CancelCauseFunc,
	factory idleTimerFactory,
	now func() time.Time,
) *idleWatchdog {
	if factory == nil {
		factory = func(timeout time.Duration, fn func()) idleTimer {
			return time.AfterFunc(timeout, fn)
		}
	}

	if now == nil {
		now = time.Now
	}

	return &idleWatchdog{cancel: cancel, timerFactory: factory, now: now}
}

func (w *idleWatchdog) arm(phase string, timeout time.Duration) {
	if timeout <= 0 {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stopped || w.stallErr != nil {
		return
	}

	w.phase = phase
	w.timeout = timeout
	w.deadline = w.now().Add(timeout)

	if w.timer == nil {
		w.timer = w.timerFactory(timeout, w.expire)

		return
	}

	w.timer.Reset(timeout)
}

func (w *idleWatchdog) progress(count int) {
	if count <= 0 {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stopped || w.stallErr != nil || w.timer == nil {
		return
	}

	w.deadline = w.now().Add(w.timeout)
	w.timer.Reset(w.timeout)
}

func (w *idleWatchdog) setNotifier(notify func(error)) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.notify = notify
}

func (w *idleWatchdog) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.stopped = true

	if w.timer != nil {
		w.timer.Stop()
	}
}

func (w *idleWatchdog) expire() {
	w.mu.Lock()

	if w.stopped || w.stallErr != nil {
		w.mu.Unlock()

		return
	}

	if remaining := w.deadline.Sub(w.now()); remaining > 0 {
		w.timer.Reset(remaining)
		w.mu.Unlock()

		return
	}

	stallErr := &networkStallError{phase: w.phase, timeout: w.timeout}
	w.stallErr = stallErr
	notify := w.notify
	w.mu.Unlock()

	w.cancel(stallErr)

	if notify != nil {
		notify(stallErr)
	}
}

func (w *idleWatchdog) cause() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.stallErr
}

type progressRequestBody struct {
	io.ReadCloser
	onProgress func(int)
	onClose    func()
	closeOnce  sync.Once
	closed     atomic.Bool
}

type networkStallNotifier interface {
	NetworkStall(error)
}

func (b *progressRequestBody) Read(p []byte) (int, error) {
	count, err := b.ReadCloser.Read(p)
	b.onProgress(count)

	return count, err
}

func (b *progressRequestBody) Close() error {
	closeErr := b.ReadCloser.Close()
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		b.onClose()
	})

	return closeErr
}

func (b *progressRequestBody) isClosed() bool {
	return b.closed.Load()
}

type progressResponseBody struct {
	body          io.ReadCloser
	parentCtx     context.Context
	cancel        context.CancelCauseFunc
	idleWatchdog  *idleWatchdog
	totalWatchdog *idleWatchdog
	maxBytes      int64
	bytesRead     int64
	limitErr      error
	onDone        func()
	closeOnce     sync.Once
}

func newProgressResponseBody(
	body io.ReadCloser,
	parentCtx context.Context,
	cancel context.CancelCauseFunc,
	idleTimeout time.Duration,
	totalTimeout time.Duration,
	maxBytes int64,
	factory idleTimerFactory,
	now func() time.Time,
	onDone func(),
) *progressResponseBody {
	idleWatchdog := newIdleWatchdog(cancel, factory, now)
	idleWatchdog.arm("response body read", idleTimeout)

	totalWatchdog := newIdleWatchdog(cancel, factory, now)
	totalWatchdog.arm("response body total", totalTimeout)

	return &progressResponseBody{
		body:          body,
		parentCtx:     parentCtx,
		cancel:        cancel,
		idleWatchdog:  idleWatchdog,
		totalWatchdog: totalWatchdog,
		maxBytes:      maxBytes,
		onDone:        onDone,
	}
}

func (b *progressResponseBody) Read(p []byte) (int, error) {
	count, err := b.body.Read(p)
	b.idleWatchdog.progress(count)

	b.bytesRead += int64(count)
	if b.maxBytes > 0 && b.limitErr == nil && b.bytesRead > b.maxBytes {
		b.limitErr = &responseBodyLimitError{
			limit: b.maxBytes,
			read:  b.bytesRead,
		}
		b.cancel(b.limitErr)
	}

	if errors.Is(err, io.EOF) {
		b.stop()
	}

	cause := b.limitErr
	if cause == nil {
		cause = b.totalWatchdog.cause()
	}

	if cause == nil {
		cause = b.idleWatchdog.cause()
	}

	if cause == nil && b.parentCtx.Err() != nil {
		cause = context.Cause(b.parentCtx)
	}

	if cause == nil {
		var timeoutErr net.Error
		if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
			cause = context.DeadlineExceeded
		}
	}

	if cause == nil {
		return count, err
	}

	return count, errors.Join(err, cause)
}

func (b *progressResponseBody) Close() error {
	closeErr := b.body.Close()
	b.stop()

	return closeErr
}

func (b *progressResponseBody) stop() {
	b.closeOnce.Do(func() {
		b.idleWatchdog.stop()
		b.totalWatchdog.stop()
		b.cancel(nil)
		b.onDone()
	})
}

// ErrResponseBodyLimitExceeded identifies a response body that exceeded its
// configured finite control-response byte budget.
var ErrResponseBodyLimitExceeded = errors.New("response body limit exceeded")

type responseBodyLimitError struct {
	limit int64
	read  int64
}

func (e *responseBodyLimitError) Error() string {
	return fmt.Sprintf("response body read %d bytes, limit is %d: %v", e.read, e.limit, ErrResponseBodyLimitExceeded)
}

func (e *responseBodyLimitError) Unwrap() error {
	return ErrResponseBodyLimitExceeded
}

type httpOrigin struct {
	scheme string
	host   string
	port   string
}

func parseHTTPOrigin(rawURL string) (httpOrigin, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return httpOrigin{}, fmt.Errorf("parse origin URL: %w", err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return httpOrigin{}, fmt.Errorf("origin URL scheme %q is not HTTP or HTTPS", parsed.Scheme)
	}

	if parsed.Host == "" || parsed.Hostname() == "" {
		return httpOrigin{}, errors.New("origin URL has no host")
	}

	if parsed.User != nil {
		return httpOrigin{}, errors.New("origin URL must not contain user info")
	}

	port := parsed.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}

	return httpOrigin{
		scheme: scheme,
		host:   strings.ToLower(parsed.Hostname()),
		port:   port,
	}, nil
}

func parseHTTPSOrigin(rawURL string) (httpOrigin, error) {
	origin, err := parseHTTPOrigin(rawURL)
	if err != nil {
		return httpOrigin{}, err
	}

	if origin.scheme != "https" {
		return httpOrigin{}, fmt.Errorf("origin URL scheme %q is not HTTPS", origin.scheme)
	}

	return origin, nil
}

func (o httpOrigin) matches(candidate *url.URL) bool {
	if candidate == nil {
		return false
	}

	other, err := parseHTTPOrigin(candidate.String())
	if err != nil {
		return false
	}

	return o == other
}

func (o httpOrigin) authority() string {
	defaultPort := o.scheme == "http" && o.port == "80" || o.scheme == "https" && o.port == "443"
	if defaultPort {
		return o.host
	}

	return o.host + ":" + o.port
}

// CloseIdleConnections stops new requests, waits for owned request bodies to
// quiesce, and closes this client's privately owned connection pool.
func (c *PersistentHTTPClient) CloseIdleConnections() {
	if c == nil {
		return
	}

	c.lifecycle.waitForRequests()
	utilnet.CloseIdleConnectionsFor(c.ownedTransport)

	if c.ownedHTTPTransport != nil {
		c.ownedHTTPTransport.CloseIdleConnections()
	}

	c.lifecycle.closeConnections()
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

// SetRequestTimeout sets the rest.Config total timeout used by short control-plane
// requests. Streaming callers can clear it on a copied SafeClient before building
// a progress-aware persistent client.
func (c *SafeClient) SetRequestTimeout(timeout time.Duration) {
	c.restConfig.Timeout = timeout
}

// SetNetworkTimeouts installs finite connection, TLS-handshake, response-header,
// request-write-idle, response-read-idle, and finite total response bounds on
// persistent HTTP clients. WriteIdle and ReadIdle reset after every successful
// body read, while ResponseTotal and ResponseBytes independently bound control
// responses even when a peer continuously trickles bytes.
func (c *SafeClient) SetNetworkTimeouts(timeouts NetworkTimeouts) error {
	switch {
	case timeouts.Connect <= 0:
		return errors.New("network connect timeout must be positive")
	case timeouts.TLSHandshake <= 0:
		return errors.New("network TLS handshake timeout must be positive")
	case timeouts.ResponseHeader <= 0:
		return errors.New("network response header timeout must be positive")
	case timeouts.WriteIdle <= 0:
		return errors.New("network write idle timeout must be positive")
	case timeouts.ReadIdle <= 0:
		return errors.New("network read idle timeout must be positive")
	case timeouts.ResponseTotal <= 0:
		return errors.New("network response total timeout must be positive")
	case timeouts.ResponseBytes <= 0:
		return errors.New("network response byte limit must be positive")
	}

	c.networkTimeouts = timeouts
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
		cloned.ResponseHeaderTimeout = timeouts.ResponseHeader
		cloned.TLSHandshakeTimeout = timeouts.TLSHandshake

		baseDialContext := cloned.DialContext
		if baseDialContext == nil {
			dialer := &net.Dialer{}
			baseDialContext = dialer.DialContext
		}

		cloned.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			dialCtx, cancel := context.WithTimeout(ctx, timeouts.Connect)
			defer cancel()

			return baseDialContext(dialCtx, network, address)
		}

		if cloned.DialTLSContext != nil {
			baseDialTLSContext := cloned.DialTLSContext
			cloned.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				dialCtx, cancel := context.WithTimeout(ctx, timeouts.TLSHandshake)
				defer cancel()

				return baseDialTLSContext(dialCtx, network, address)
			}
		}

		return cloned
	}

	return nil
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
	prev := c.restConfig.WrapTransport

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

		return clonedTransport
	}
}

// ValidateHTTPSIdentity requires an HTTPS origin and a strictly parseable,
// non-empty PEM certificate bundle suitable for endpoint-specific trust.
func ValidateHTTPSIdentity(rawURL string, caData []byte) error {
	if _, err := parseHTTPSOrigin(rawURL); err != nil {
		return fmt.Errorf("validate HTTPS origin: %w", err)
	}

	if _, err := parseTLSIdentityCertPool(caData); err != nil {
		return fmt.Errorf("validate TLS identity CA: %w", err)
	}

	return nil
}

// SetTLSIdentityCAData replaces inherited server trust with the supplied
// endpoint-specific certificate bundle. Client credentials, proxy, dial, and
// transport wrappers remain intact, but inherited insecure verification,
// ServerName overrides, system roots, and cluster roots cannot bypass this CA.
func (c *SafeClient) SetTLSIdentityCAData(caData []byte) error {
	if c == nil || c.restConfig == nil {
		return errors.New("set TLS identity CA: no rest config")
	}

	rootCAs, err := parseTLSIdentityCertPool(caData)
	if err != nil {
		return fmt.Errorf("set TLS identity CA: %w", err)
	}

	c.restConfig.TLSClientConfig.CAData = nil
	c.restConfig.TLSClientConfig.CAFile = ""
	c.restConfig.TLSClientConfig.Insecure = false
	c.restConfig.TLSClientConfig.ServerName = ""
	prev := c.restConfig.WrapTransport

	c.restConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		if prev != nil {
			rt = prev(rt)
		}

		transport := findHTTPTransport(rt)
		if transport == nil {
			return tlsIdentityErrorRoundTripper{
				err: errors.New("endpoint TLS identity requires an HTTP transport"),
			}
		}

		tlsConfig := transport.TLSClientConfig
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		} else {
			tlsConfig = tlsConfig.Clone()
		}

		tlsConfig.RootCAs = rootCAs
		tlsConfig.InsecureSkipVerify = false
		tlsConfig.ServerName = ""
		transport.TLSClientConfig = tlsConfig

		return rt
	}

	return nil
}

func parseTLSIdentityCertPool(caData []byte) (*x509.CertPool, error) {
	remaining := bytes.TrimSpace(caData)
	if len(remaining) == 0 {
		return nil, errors.New("certificate bundle is empty")
	}

	pool := x509.NewCertPool()
	certificates := 0

	for len(remaining) > 0 {
		block, rest := pem.Decode(remaining)
		if block == nil {
			return nil, errors.New("certificate bundle contains malformed PEM data")
		}

		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("PEM block type %q is not CERTIFICATE", block.Type)
		}

		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse certificate: %w", err)
		}

		pool.AddCert(certificate)

		certificates++
		remaining = bytes.TrimSpace(rest)
	}

	if certificates == 0 {
		return nil, errors.New("certificate bundle has no certificates")
	}

	return pool, nil
}

type tlsIdentityErrorRoundTripper struct {
	err error
}

func (rt tlsIdentityErrorRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, rt.err
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
	return &SafeClient{
		restConfig:       rest.CopyConfig(c.restConfig),
		networkTimeouts:  c.networkTimeouts,
		idleTimerFactory: c.idleTimerFactory,
		idleNow:          c.idleNow,
	}
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
