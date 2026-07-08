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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestSafeClient_SetQPS asserts that SetQPS mutates the underlying rest.Config's
// QPS/Burst fields to exactly the values passed, and that RESTConfig() (the deep
// copy callers use to build their own clients, e.g. the aggregated-API client)
// reflects them.
func TestSafeClient_SetQPS(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetQPS(50, 100)

	got := sc.RESTConfig()
	if got.QPS != 50 {
		t.Errorf("RESTConfig().QPS = %v, want 50", got.QPS)
	}

	if got.Burst != 100 {
		t.Errorf("RESTConfig().Burst = %d, want 100", got.Burst)
	}
}

// TestSafeClient_SetQPS_DefaultUnchangedWithoutCall asserts that a SafeClient
// which never calls SetQPS leaves rest.Config's QPS/Burst at their unset zero
// value (client-go's own client constructors substitute rest.DefaultQPS/
// DefaultBurst for a zero value at request time) — SetQPS must be strictly
// opt-in per caller, not a change to NewSafeClient's own default, so commands
// that never call it are unaffected.
func TestSafeClient_SetQPS_DefaultUnchangedWithoutCall(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	got := sc.RESTConfig()
	if got.QPS != 0 {
		t.Errorf("RESTConfig().QPS = %v, want 0 (unset) when SetQPS was never called", got.QPS)
	}

	if got.Burst != 0 {
		t.Errorf("RESTConfig().Burst = %d, want 0 (unset) when SetQPS was never called", got.Burst)
	}
}

// TestSafeClient_SetResponseHeaderTimeout_AppliesToTransport asserts that
// SetResponseHeaderTimeout installs a WrapTransport that sets exactly the given
// ResponseHeaderTimeout on the transport rest.HTTPClientFor would build.
func TestSafeClient_SetResponseHeaderTimeout_AppliesToTransport(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetResponseHeaderTimeout(25 * time.Millisecond)

	wrap := sc.RESTConfig().WrapTransport
	if wrap == nil {
		t.Fatal("WrapTransport is nil after SetResponseHeaderTimeout")
	}

	wrapped, ok := wrap(&http.Transport{}).(*http.Transport)
	if !ok {
		t.Fatal("wrapped transport is not an *http.Transport")
	}

	if wrapped.ResponseHeaderTimeout != 25*time.Millisecond {
		t.Errorf("ResponseHeaderTimeout = %v, want 25ms", wrapped.ResponseHeaderTimeout)
	}
}

// TestSafeClient_SetResponseHeaderTimeout_ChainsExistingWrapTransport asserts
// that SetResponseHeaderTimeout composes with an already-installed WrapTransport
// (SetTLSCAData's CA injection) instead of clobbering it: both the RootCAs pool
// and the response-header timeout must survive.
func TestSafeClient_SetResponseHeaderTimeout_ChainsExistingWrapTransport(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetTLSCAData(nil)
	sc.SetResponseHeaderTimeout(15 * time.Millisecond)

	wrapped, ok := sc.RESTConfig().WrapTransport(&http.Transport{}).(*http.Transport)
	if !ok {
		t.Fatal("wrapped transport is not an *http.Transport")
	}

	if wrapped.ResponseHeaderTimeout != 15*time.Millisecond {
		t.Errorf("ResponseHeaderTimeout = %v, want 15ms", wrapped.ResponseHeaderTimeout)
	}

	if wrapped.TLSClientConfig == nil || wrapped.TLSClientConfig.RootCAs == nil {
		t.Error("chained CA WrapTransport lost: RootCAs is nil")
	}
}

// TestSafeClient_ResponseHeaderTimeout_DefaultUnchangedWithoutCall asserts that a
// SafeClient which never calls SetResponseHeaderTimeout leaves WrapTransport
// unset, so every other libsaferequest consumer is unaffected (opt-in only).
func TestSafeClient_ResponseHeaderTimeout_DefaultUnchangedWithoutCall(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	if sc.RESTConfig().WrapTransport != nil {
		t.Error("WrapTransport must be nil when SetResponseHeaderTimeout was never called")
	}
}

// stubRoundTripper is a non-*http.Transport http.RoundTripper used to exercise
// the WrapTransport pass-through branch.
type stubRoundTripper struct{}

func (stubRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, http.ErrNotSupported
}

// TestSafeClient_SetTLSCAData_ClonesTransport asserts that SetTLSCAData's
// WrapTransport clones a real *http.Transport and injects the CA pool as
// RootCAs, leaving the original transport untouched.
func TestSafeClient_SetTLSCAData_ClonesTransport(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetTLSCAData(nil)

	orig := &http.Transport{}

	got := sc.RESTConfig().WrapTransport(orig)

	wrapped, ok := got.(*http.Transport)
	if !ok {
		t.Fatalf("wrapped transport is not an *http.Transport: %T", got)
	}

	if wrapped == orig {
		t.Error("expected a distinct clone, got the original transport")
	}

	if wrapped.TLSClientConfig == nil || wrapped.TLSClientConfig.RootCAs == nil {
		t.Error("RootCAs not populated on cloned transport")
	}
}

// TestSafeClient_SetTLSCAData_PassThroughNonTransport asserts that when
// WrapTransport receives a RoundTripper that is not an *http.Transport it
// returns the SAME instance unchanged (non-nil), instead of a typed-nil
// *http.Transport that would nil-panic on RoundTrip.
func TestSafeClient_SetTLSCAData_PassThroughNonTransport(t *testing.T) {
	t.Parallel()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetTLSCAData(nil)

	stub := stubRoundTripper{}

	got := sc.RESTConfig().WrapTransport(stub)
	if got == nil {
		t.Fatal("WrapTransport returned nil for a non-*http.Transport RoundTripper")
	}

	gotStub, ok := got.(stubRoundTripper)
	if !ok {
		t.Fatalf("expected the original stubRoundTripper back, got %T", got)
	}

	if gotStub != stub {
		t.Error("expected the same stub RoundTripper instance to be returned unchanged")
	}
}

// TestSafeClient_SetResponseHeaderTimeout_FailsFastOnHeaderStall asserts the
// configured transport aborts a request whose server accepts the connection but
// never sends response headers, within the response-header timeout.
func TestSafeClient_SetResponseHeaderTimeout_FailsFastOnHeaderStall(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	sc, err := NewSafeClient()
	if err != nil {
		t.Fatalf("NewSafeClient: %v", err)
	}

	sc.SetResponseHeaderTimeout(75 * time.Millisecond)

	rt, ok := sc.RESTConfig().WrapTransport(&http.Transport{}).(*http.Transport)
	if !ok {
		t.Fatal("wrapped transport is not an *http.Transport")
	}

	cl := &http.Client{Transport: rt}

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	start := time.Now()

	resp, err := cl.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected a response-header timeout error, got nil")
	}

	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("did not fail fast on header stall: %v", elapsed)
	}
}
