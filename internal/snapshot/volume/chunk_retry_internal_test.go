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

package volume

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/deckhouse/deckhouse-cli/internal/dataplane"
)

// fastChunkRetryPolicy is a test-only policy with the same shape as
// dataplane.DefaultRetryPolicy but with a millisecond-scale backoff, so retry
// tests don't pay the production policy's multi-second budget. It is
// deliberately unexported and local to this file: no test in this package
// gets to change the production default via a shared knob.
//
// Cap is set well above the growth this Steps/Duration/Factor combination
// ever reaches (1ms -> 2ms -> 4ms -> 8ms for Steps=4): wait.Backoff.Step
// forces its internal step counter to 0 — ending the retry loop one
// invocation EARLIER than Steps would otherwise suggest — the moment a
// projected next duration exceeds Cap, so a tight Cap here would silently
// undercount the very attempts these tests assert on.
func fastChunkRetryPolicy() dataplane.RetryPolicy {
	return dataplane.RetryPolicy{
		Backoff: wait.Backoff{
			Steps:    4,
			Duration: time.Millisecond,
			Factor:   2,
			Cap:      50 * time.Millisecond,
		},
		MaxNoProgress: 3,
	}
}

// newRangeServer serves data at "/block" with Range-GET support
// (http.ServeContent), mirroring the data-exporter's block endpoint contract.
func newRangeServer(t *testing.T, data []byte) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/block", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, "data.img", time.Time{}, strings.NewReader(string(data)))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv
}

// cutBody wraps a response body and, after delivering exactly budget bytes,
// reports err instead of continuing to read from the underlying body.
type cutBody struct {
	r      io.ReadCloser
	budget int64
	err    error
}

func (b *cutBody) Read(p []byte) (int, error) {
	if b.budget <= 0 {
		return 0, b.err
	}

	if int64(len(p)) > b.budget {
		p = p[:b.budget]
	}

	n, err := b.r.Read(p)
	b.budget -= int64(n)

	if err == nil && b.budget <= 0 {
		err = b.err
	}

	return n, err
}

func (b *cutBody) Close() error {
	return b.r.Close()
}

// scriptedRangeDoer wraps a real dataplane.Doer and records every request's
// Range header in call order. cut, when non-nil, truncates every response
// body (every call, not just one) after cutBytes bytes with cutErr — standing
// in for a link that breaks on every attempt.
type scriptedRangeDoer struct {
	inner     dataplane.Doer
	cutBytes  int64
	cutErr    error           // nil disables truncation
	firstSeen chan<- struct{} // optional: signaled once, on the very first Do() call

	mu     sync.Mutex
	ranges []string
}

func (d *scriptedRangeDoer) Do(req *http.Request) (*http.Response, error) {
	d.mu.Lock()
	d.ranges = append(d.ranges, req.Header.Get("Range"))
	firstCall := len(d.ranges) == 1
	d.mu.Unlock()

	if firstCall && d.firstSeen != nil {
		d.firstSeen <- struct{}{}
	}

	resp, err := d.inner.Do(req)
	if err != nil {
		return resp, err
	}

	if d.cutErr != nil {
		resp.Body = &cutBody{r: resp.Body, budget: d.cutBytes, err: d.cutErr}
	}

	return resp, nil
}

func (d *scriptedRangeDoer) recordedRanges() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make([]string, len(d.ranges))
	copy(out, d.ranges)

	return out
}

func (d *scriptedRangeDoer) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return len(d.ranges)
}

// onceCutDoer wraps a real dataplane.Doer and truncates only the FIRST
// response body (with cutErr, after cutBytes bytes); every subsequent call —
// in particular the resumed retry — passes through untouched.
type onceCutDoer struct {
	inner    dataplane.Doer
	cutBytes int64
	cutErr   error

	mu     sync.Mutex
	calls  int
	ranges []string
}

func (d *onceCutDoer) Do(req *http.Request) (*http.Response, error) {
	d.mu.Lock()
	d.calls++
	callIdx := d.calls
	d.ranges = append(d.ranges, req.Header.Get("Range"))
	d.mu.Unlock()

	resp, err := d.inner.Do(req)
	if err != nil {
		return resp, err
	}

	if callIdx == 1 {
		resp.Body = &cutBody{r: resp.Body, budget: d.cutBytes, err: d.cutErr}
	}

	return resp, nil
}

func (d *onceCutDoer) recordedRanges() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make([]string, len(d.ranges))
	copy(out, d.ranges)

	return out
}

// TestChunkRetrier_ResumesFromDurableOffset proves the retry loop resumes
// each attempt from the exact durable offset the previous, interrupted
// attempt persisted — never from byte zero — and that onProgress credits
// across attempts sum to exactly rawLen with no double counting.
func TestChunkRetrier_ResumesFromDurableOffset(t *testing.T) {
	t.Parallel()

	payload := []byte("0123456789ABCDEFGHIJ") // 20 bytes
	const cutBytes = 12                       // attempt 1 delivers 12 bytes, then breaks

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	doer := &onceCutDoer{cutBytes: cutBytes, cutErr: io.ErrUnexpectedEOF}
	doer.inner = srv.Client()
	fetcher := dataplane.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	retrier := newChunkRetrier(fastChunkRetryPolicy())

	var (
		mu      sync.Mutex
		credits []int
	)

	onProgress := func(n int) {
		mu.Lock()
		credits = append(credits, n)
		mu.Unlock()
	}

	rawLen := int64(len(payload))

	err := retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, onProgress)
	if err != nil {
		t.Fatalf("fetchChunk: %v", err)
	}

	got, err := os.ReadFile(partPath)
	if err != nil {
		t.Fatalf("read partPath: %v", err)
	}

	if string(got) != string(payload) {
		t.Errorf("partPath content = %q, want %q", got, payload)
	}

	ranges := doer.recordedRanges()
	if len(ranges) != 2 {
		t.Fatalf("expected exactly 2 requests, got %d: %v", len(ranges), ranges)
	}

	if want := "bytes=0-19"; ranges[0] != want {
		t.Errorf("attempt 1 range = %q, want %q", ranges[0], want)
	}

	if want := "bytes=12-19"; ranges[1] != want {
		t.Errorf("attempt 2 range = %q, want %q (must resume from the durable offset, not byte 0)", ranges[1], want)
	}

	var sum int

	for _, c := range credits {
		sum += c
	}

	if int64(sum) != rawLen {
		t.Errorf("onProgress credits summed to %d, want %d (rawLen): %v", sum, rawLen, credits)
	}
}

// TestChunkRetrier_ResumesAfterUnrecognizedTransportBreak proves the chunk
// download survives the break that actually happens in the field: a proxy
// reload tears the HTTP/2 session down mid-body, and net/http reports it from
// a type it keeps in its own unexported copy of the http2 package — so no
// errors.As or errors.Is reaches it, and the fail-closed classifier calls it
// fatal. What makes it retryable is not its identity but the bytes it
// delivered first.
//
// This is the same rule TestRetrier_RetriesUnrecognizedErrorAfterDeliveredBytes
// pins on the policy itself; here it is checked through the real Range GET and
// the real ".part" file, because a rule expressed in the shared layer can
// still fail to reach its consumer — the attempt has to report its delivery
// honestly for the policy to act on it. The resumed Range header below is what
// shows it did.
func TestChunkRetrier_ResumesAfterUnrecognizedTransportBreak(t *testing.T) {
	t.Parallel()

	payload := []byte("0123456789ABCDEFGHIJ") // 20 bytes
	const cutBytes = 12                       // attempt 1 delivers 12 bytes, then breaks

	broken := errors.New("http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=NO_ERROR")

	// The premise: this error really is unrecognized. Should a later change
	// teach the classifier this shape, the test would keep passing while no
	// longer testing anything about progress — so it says so instead.
	if dataplane.IsTransientDataPlaneError(broken) {
		t.Fatalf("premise broken: %v is now a recognized transient error", broken)
	}

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	doer := &onceCutDoer{cutBytes: cutBytes, cutErr: broken}
	doer.inner = srv.Client()
	fetcher := dataplane.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	retrier := newChunkRetrier(fastChunkRetryPolicy())

	rawLen := int64(len(payload))

	err := retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, nil)
	if err != nil {
		t.Fatalf("fetchChunk: %v (a break after delivered bytes must resume, whatever its type)", err)
	}

	got, err := os.ReadFile(partPath)
	if err != nil {
		t.Fatalf("read partPath: %v", err)
	}

	if string(got) != string(payload) {
		t.Errorf("partPath content = %q, want %q", got, payload)
	}

	ranges := doer.recordedRanges()
	if len(ranges) != 2 {
		t.Fatalf("expected exactly 2 requests, got %d: %v", len(ranges), ranges)
	}

	if want := "bytes=12-19"; ranges[1] != want {
		t.Errorf("attempt 2 range = %q, want %q (resume from the durable offset, not byte 0)", ranges[1], want)
	}
}

// TestChunkRetrier_InheritedDurablePrefixIsNotCountedAsDelivery pins what an
// attempt reports when it fails BEFORE the network gives it anything: the
// durable prefix it inherited from an earlier run belongs to that earlier
// run, not to this attempt.
//
// Get that wrong and the damage is not in this chunk but in the retry
// ceiling: an endpoint that refuses every connection would look like it
// delivered the whole inherited prefix on every attempt, resetting the
// no-progress counter each time, and a dead address would cost the full retry
// budget instead of failing on the first attempt. The chunk still ends up
// correct either way, which is why the property needs its own guard: when it
// was broken deliberately (reporting Start as 0 instead of the inherited
// prefix), this was the only test in this package or in internal/dataplane
// that went red.
func TestChunkRetrier_InheritedDurablePrefixIsNotCountedAsDelivery(t *testing.T) {
	t.Parallel()

	payload := []byte("0123456789ABCDEFGHIJ") // 20 bytes
	const have = 12                           // durable prefix left by an earlier run

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	doer := &scriptedRangeDoer{inner: srv.Client()}
	fetcher := dataplane.NewFetcher(doer)

	// Take the listener away: every connection is now refused before a body
	// can exist, so this attempt delivers nothing by construction.
	// ECONNREFUSED is not in the transient list (the export never accepted the
	// connection at all), so the ONLY thing that could buy a second attempt
	// here is a miscounted delivery.
	srv.Close()

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	if err := os.WriteFile(partPath, payload[:have], 0o600); err != nil {
		t.Fatalf("seed durable partial: %v", err)
	}

	if err := os.WriteFile(partPath+partOffsetSuffix,
		[]byte(strconv.FormatInt(have, 10)), 0o600); err != nil {
		t.Fatalf("seed durable offset sidecar: %v", err)
	}

	retrier := newChunkRetrier(fastChunkRetryPolicy())

	rawLen := int64(len(payload))

	err := retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, nil)
	if err == nil {
		t.Fatal("expected a refused endpoint to fail the chunk, got nil")
	}

	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Errorf("expected errors.Is(err, syscall.ECONNREFUSED), got: %v", err)
	}

	if got := doer.callCount(); got != 1 {
		t.Errorf("expected exactly 1 request, got %d: the inherited %d-byte prefix is being counted as bytes "+
			"THIS attempt delivered, so a dead endpoint buys retries it has not earned", got, have)
	}
}

// TestChunkRetrier_KeepsTheCallerFatalPredicate proves newChunkRetrier EXTENDS
// the policy's fatal set rather than replacing it. Replacing it would demote
// whatever the caller had already named back to retryable, silently, and the
// only visible symptom would be a failure that takes a whole budget to arrive.
//
// The stub delivers bytes before failing, so the progress rule would retry
// this error: only the predicate can be what stops the loop on attempt one.
func TestChunkRetrier_KeepsTheCallerFatalPredicate(t *testing.T) {
	t.Parallel()

	payload := []byte("0123456789ABCDEFGHIJ")

	callerFatal := errors.New("a failure the caller recognises as final")

	policy := fastChunkRetryPolicy()
	policy.Fatal = func(err error) bool { return errors.Is(err, callerFatal) }

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	doer := &scriptedRangeDoer{inner: srv.Client(), cutBytes: 8, cutErr: callerFatal}
	fetcher := dataplane.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	retrier := newChunkRetrier(policy)

	rawLen := int64(len(payload))

	err := retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, nil)
	if err == nil {
		t.Fatal("expected the caller-named failure to be fatal, got nil")
	}

	if !errors.Is(err, callerFatal) {
		t.Errorf("expected errors.Is(err, callerFatal), got: %v", err)
	}

	if got := doer.callCount(); got != 1 {
		t.Errorf("expected exactly 1 request, got %d: the caller's predicate was dropped", got)
	}
}

// TestChunkRetrier_ExhaustsBudget proves that a link broken on every attempt
// exhausts exactly the policy's Steps budget and returns an error that still
// satisfies errors.Is against the underlying transient sentinel.
func TestChunkRetrier_ExhaustsBudget(t *testing.T) {
	t.Parallel()

	payload := make([]byte, 100)

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	doer := &scriptedRangeDoer{inner: srv.Client(), cutBytes: 5, cutErr: io.ErrUnexpectedEOF}
	fetcher := dataplane.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	policy := fastChunkRetryPolicy()
	retrier := newChunkRetrier(policy)

	rawLen := int64(len(payload))

	err := retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, nil)
	if err == nil {
		t.Fatal("expected an error once the retry budget is exhausted, got nil")
	}

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("expected errors.Is(err, io.ErrUnexpectedEOF), got: %v", err)
	}

	if got := doer.callCount(); got != policy.Backoff.Steps {
		t.Errorf("expected exactly %d requests (the full Steps budget), got %d", policy.Backoff.Steps, got)
	}

	// The durable partial and its offset sidecar must survive: a future
	// process run still has something to resume from.
	if _, statErr := os.Stat(partPath); statErr != nil {
		t.Errorf("expected partPath to survive exhaustion, stat failed: %v", statErr)
	}

	if _, statErr := os.Stat(partPath + partOffsetSuffix); statErr != nil {
		t.Errorf("expected the durable offset sidecar to survive exhaustion, stat failed: %v", statErr)
	}
}

// chunkWarnCapture is a slog.Handler that collects Warn-or-above log messages
// for assertions, mirroring volume_test's warnCapture (unavailable here: this
// file is the internal test package and cannot import volume_test).
type chunkWarnCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (h *chunkWarnCapture) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *chunkWarnCapture) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		h.mu.Lock()
		h.msgs = append(h.msgs, r.Message)
		h.mu.Unlock()
	}

	return nil
}

func (h *chunkWarnCapture) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *chunkWarnCapture) WithGroup(_ string) slog.Handler { return h }

func (h *chunkWarnCapture) warnMessages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]string, len(h.msgs))
	copy(out, h.msgs)

	return out
}

// TestChunkRetrier_ExhaustsBudget_CapCutsAttemptsShortOfSteps proves that when
// wait.Backoff.Cap forces the retry loop to stop before its declared Steps
// budget is reached (see the backoff doc comment behind
// dataplane.DefaultRetryPolicy), fetchChunk reports the ACTUAL number of
// attempts made — never the declared Steps — in its
// returned error, and logs each transient failure exactly once: one WARN per
// attempt, and the exhausted error is never separately logged anywhere in
// this call chain once it becomes final.
func TestChunkRetrier_ExhaustsBudget_CapCutsAttemptsShortOfSteps(t *testing.T) {
	t.Parallel()

	payload := make([]byte, 100)

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	doer := &scriptedRangeDoer{inner: srv.Client(), cutBytes: 5, cutErr: io.ErrUnexpectedEOF}
	fetcher := dataplane.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	// Steps=6 alone would suggest 6 attempts, but Cap=4ms forces the internal
	// step budget to 0 early: the projected delay grows 1ms -> 2ms -> 4ms,
	// and the 3rd projected delay (8ms) exceeds Cap, ending the loop after
	// exactly 3 real attempts — the same arithmetic the production backoff's
	// doc comment works out for its own parameters (5 of 6 there).
	policy := dataplane.RetryPolicy{
		Backoff: wait.Backoff{
			Steps:    6,
			Duration: time.Millisecond,
			Factor:   2,
			Cap:      4 * time.Millisecond,
		},
		MaxNoProgress: 3,
	}

	warns := &chunkWarnCapture{}
	log := slog.New(warns)

	retrier := newChunkRetrier(policy)

	rawLen := int64(len(payload))

	err := retrier.fetchChunk(context.Background(), nil, log, fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, nil)
	if err == nil {
		t.Fatal("expected an error once the retry budget is exhausted, got nil")
	}

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("expected errors.Is(err, io.ErrUnexpectedEOF), got: %v", err)
	}

	gotCalls := doer.callCount()
	if gotCalls != 3 {
		t.Fatalf("expected exactly 3 requests (Cap cuts Steps=6 short), got %d", gotCalls)
	}

	wantMsg := fmt.Sprintf("exhausted %d attempts", gotCalls)
	if !strings.Contains(err.Error(), wantMsg) {
		t.Errorf("error = %q, want it to name the actual attempt count (%q), not the declared Steps=%d",
			err.Error(), wantMsg, policy.Backoff.Steps)
	}

	if staleMsg := fmt.Sprintf("exhausted %d attempts", policy.Backoff.Steps); strings.Contains(err.Error(), staleMsg) {
		t.Errorf("error = %q, must not report the declared Steps budget (%d) as the attempt count",
			err.Error(), policy.Backoff.Steps)
	}

	// Every attempt here is a plain transient failure (never a no-progress or
	// fatal one), so each is warned about exactly once: the WARN count must
	// equal the number of attempts actually made — not more (no attempt
	// double-logged) and not fewer (a transient attempt silently dropped).
	if got := len(warns.warnMessages()); got != gotCalls {
		t.Errorf("warn log count = %d, want %d (one per attempt, no double-logging)", got, gotCalls)
	}
}

// TestChunkRetrier_DoesNotRetryFatal proves that a fatal error stops the
// retry loop on the very first attempt, and that errors.Is against the
// original sentinel still holds through fetchChunk's returned error.
//
// The first three cases fail before the exporter hands back a body, so the
// attempt delivers nothing and the progress rule never comes into it; the
// companion guard that those stay fatal even AFTER bytes have been delivered
// lives with the policy itself
// (TestRetrier_FatalErrorAfterDeliveredBytesIsNotRetried), where the two
// checks whose order it pins are written. The fourth case is the one that
// does deliver bytes first.
func TestChunkRetrier_DoesNotRetryFatal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		buildFetch func(t *testing.T) (fetcher *dataplane.Fetcher, blockURL string, callCount func() int)
		wantErr    error
	}{
		{
			name: "401 unauthorized",
			buildFetch: func(t *testing.T) (*dataplane.Fetcher, string, func() int) {
				t.Helper()
				return statusDoerFetcher(t, http.StatusUnauthorized)
			},
			wantErr: dataplane.ErrExportUnauthorized,
		},
		{
			name: "403 forbidden",
			buildFetch: func(t *testing.T) (*dataplane.Fetcher, string, func() int) {
				t.Helper()
				return statusDoerFetcher(t, http.StatusForbidden)
			},
			wantErr: dataplane.ErrExportUnauthorized,
		},
		{
			name: "content-range mismatch",
			buildFetch: func(t *testing.T) (*dataplane.Fetcher, string, func() int) {
				t.Helper()
				return mismatchedRangeFetcher(t)
			},
			wantErr: dataplane.ErrContentRangeMismatch,
		},
		{
			// Unlike the three above, this one delivers bytes BEFORE it
			// fails, so the progress rule in the shared policy would retry
			// it. It stays fatal only because newChunkRetrier names it in
			// the policy's Fatal predicate — which is what this case pins.
			name: "clean short read",
			buildFetch: func(t *testing.T) (*dataplane.Fetcher, string, func() int) {
				t.Helper()
				return shortReadFetcher(t)
			},
			wantErr: ErrShortChunkRead,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fetcher, blockURL, callCount := tc.buildFetch(t)

			dir := t.TempDir()
			partPath := filepath.Join(dir, "chunk_00000.part")

			retrier := newChunkRetrier(fastChunkRetryPolicy())

			err := retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher, blockURL,
				partPath, 0, 0, 19, 20, nil)
			if err == nil {
				t.Fatal("expected a fatal error, got nil")
			}

			if !errors.Is(err, tc.wantErr) {
				t.Errorf("expected errors.Is(err, %v), got: %v", tc.wantErr, err)
			}

			if got := callCount(); got != 1 {
				t.Errorf("expected exactly 1 request for a fatal error, got %d", got)
			}
		})
	}
}

// TestChunkRetrier_DoesNotRetryLocalWriteError proves a local filesystem
// failure (unreachable from dataplane.IsTransientDataPlaneError's allow-list)
// is fatal on the first attempt, exactly like a rejected/mismatched response.
func TestChunkRetrier_DoesNotRetryLocalWriteError(t *testing.T) {
	t.Parallel()

	payload := []byte("0123456789ABCDEFGHIJ")

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	doer := &scriptedRangeDoer{inner: srv.Client()}
	fetcher := dataplane.NewFetcher(doer)

	// partPath's parent directory does not exist, so opening it for append
	// fails with a local *os.PathError — not in the transient allow-list.
	partPath := filepath.Join(t.TempDir(), "missing-parent", "chunk_00000.part")

	retrier := newChunkRetrier(fastChunkRetryPolicy())

	rawLen := int64(len(payload))

	err := retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, nil)
	if err == nil {
		t.Fatal("expected a fatal error for a local write failure, got nil")
	}

	var pathErr *os.PathError
	if !errors.As(err, &pathErr) {
		t.Errorf("expected the error to unwrap to *os.PathError, got: %v", err)
	}

	if got := doer.callCount(); got != 1 {
		t.Errorf("expected exactly 1 request before the local write failure, got %d", got)
	}
}

// TestChunkRetrier_ContextCancelStopsRetryImmediately proves that cancelling
// ctx mid-backoff aborts the retry loop promptly instead of waiting out the
// full backoff budget.
func TestChunkRetrier_ContextCancelStopsRetryImmediately(t *testing.T) {
	t.Parallel()

	payload := []byte("0123456789ABCDEFGHIJ")

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	firstSeen := make(chan struct{}, 1)
	doer := &scriptedRangeDoer{inner: srv.Client(), cutBytes: 2, cutErr: io.ErrUnexpectedEOF, firstSeen: firstSeen}
	fetcher := dataplane.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	// A long backoff so cancellation, not a natural step timeout, is what
	// ends the loop.
	longBackoffPolicy := dataplane.RetryPolicy{
		Backoff: wait.Backoff{
			Steps:    6,
			Duration: 10 * time.Second,
			Factor:   2,
			Cap:      time.Minute,
		},
		MaxNoProgress: 3,
	}
	retrier := newChunkRetrier(longBackoffPolicy)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rawLen := int64(len(payload))

	var err error

	done := make(chan struct{})

	go func() {
		defer close(done)

		err = retrier.fetchChunk(ctx, nil, slog.Default(), fetcher, blockURL,
			partPath, 0, 0, rawLen-1, rawLen, nil)
	}()

	// Wait for the first request to actually be in flight before cancelling:
	// only then is "cancel stops an in-flight backoff" the guaranteed
	// condition under test, instead of a race against goroutine scheduling.
	<-firstSeen

	start := time.Now()

	cancel()

	<-done

	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error after context cancellation, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled), got: %v", err)
	}

	if elapsed > 2*time.Second {
		t.Errorf("cancellation took %s, expected it to interrupt the 10s backoff sleep promptly", elapsed)
	}

	if got := doer.callCount(); got != 1 {
		t.Errorf("expected exactly 1 request before cancellation stopped the retry, got %d", got)
	}
}

// TestChunkRetrier_BoundsNoProgressAttempts proves a server that accepts the
// range but delivers zero bytes on every attempt is stopped after
// maxNoProgress attempts, not the (larger) Steps budget.
func TestChunkRetrier_BoundsNoProgressAttempts(t *testing.T) {
	t.Parallel()

	payload := []byte("0123456789ABCDEFGHIJ")

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	// Every attempt is cut after 0 bytes: the durable offset never advances.
	doer := &scriptedRangeDoer{inner: srv.Client(), cutBytes: 0, cutErr: io.ErrUnexpectedEOF}
	fetcher := dataplane.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	policy := dataplane.RetryPolicy{
		Backoff: wait.Backoff{
			Steps:    6, // larger than maxNoProgress: no-progress must stop it first
			Duration: time.Millisecond,
			Factor:   2,
			Cap:      50 * time.Millisecond, // see fastChunkRetryPolicy: keep well above the growth curve
		},
		MaxNoProgress: 3,
	}
	retrier := newChunkRetrier(policy)

	rawLen := int64(len(payload))

	err := retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, nil)
	if err == nil {
		t.Fatal("expected an error once no-progress attempts are exhausted, got nil")
	}

	if !strings.Contains(err.Error(), "no progress") {
		t.Errorf("expected the error to name the no-progress condition, got: %v", err)
	}

	// The ceiling counts attempts that DELIVERED NOTHING, and every attempt
	// here delivers nothing — including the first, which has no earlier
	// attempt to be compared against. So the count is reached on attempt
	// MaxNoProgress exactly, with no extra baseline attempt in front of it.
	wantCalls := policy.MaxNoProgress
	if got := doer.callCount(); got != wantCalls {
		t.Errorf("expected exactly %d requests (one per zero-delivery attempt), got %d", wantCalls, got)
	}
}

// TestChunkProgressLedger_MonotonicAcrossAttempts is a pure unit test of
// chunkProgressLedger: it feeds three overlapping attempt credit sequences
// (each attempt re-crediting the prefix a previous, interrupted attempt
// already reported) and checks the ledger forwards only the strictly new
// suffix each time, summing to exactly rawLen with no double counting.
func TestChunkProgressLedger_MonotonicAcrossAttempts(t *testing.T) {
	t.Parallel()

	const rawLen = 20

	var forwarded []int

	ledger := &chunkProgressLedger{
		onProgress: func(n int) { forwarded = append(forwarded, n) },
	}

	// Attempt 1: resumes from 0, streams 5 then 3 bytes before breaking at 8.
	ledger.beginAttempt()
	ledger.credit(5)
	ledger.credit(3)

	// Attempt 2: resumes from the now-durable 8, re-credits that prefix, then
	// streams 4 more bytes before breaking at 12.
	ledger.beginAttempt()
	ledger.credit(8)
	ledger.credit(4)

	// Attempt 3: resumes from 12, re-credits that prefix, then streams the
	// remaining 8 bytes to complete the chunk.
	ledger.beginAttempt()
	ledger.credit(12)
	ledger.credit(8)

	var sum int

	for _, n := range forwarded {
		if n <= 0 {
			t.Errorf("forwarded a non-positive credit: %d (all: %v)", n, forwarded)
		}

		sum += n
	}

	if sum != rawLen {
		t.Errorf("forwarded credits summed to %d, want %d (rawLen): %v", sum, rawLen, forwarded)
	}

	// The re-credited prefixes (8 and 12) must never have been forwarded at
	// all: only the genuinely new suffix each attempt contributes is.
	want := []int{5, 3, 4, 8}

	if len(forwarded) != len(want) {
		t.Fatalf("forwarded = %v, want %v", forwarded, want)
	}

	for i, n := range forwarded {
		if n != want[i] {
			t.Errorf("forwarded[%d] = %d, want %d (all: %v)", i, n, want[i], forwarded)
		}
	}
}

// pathOnceFlakyDoer wraps a real dataplane.Doer and truncates (with cutErr,
// after cutBytes bytes) only the FIRST request whose URL path it sees —
// tracked per distinct path — so several concurrently-downloading chunks
// backed by DIFFERENT paths on the same doer each flake exactly once,
// independently, regardless of the order or overlap in which their requests
// actually arrive. attempts records, per path, how many requests that path
// has received (also useful for asserting "exactly one retry per chunk"
// under real concurrency, not just sequential simulation).
type pathOnceFlakyDoer struct {
	inner     dataplane.Doer
	cutBytes  int64
	cutErr    error
	firstSeen chan<- string // optional: signaled with path on that path's first Do() call

	mu        sync.Mutex
	triggered map[string]bool
	attempts  map[string]int
}

func (d *pathOnceFlakyDoer) Do(req *http.Request) (*http.Response, error) {
	path := req.URL.Path

	d.mu.Lock()

	if d.triggered == nil {
		d.triggered = make(map[string]bool)
		d.attempts = make(map[string]int)
	}

	d.attempts[path]++

	fireNow := !d.triggered[path]
	if fireNow {
		d.triggered[path] = true
	}

	d.mu.Unlock()

	if fireNow && d.firstSeen != nil {
		d.firstSeen <- path
	}

	resp, err := d.inner.Do(req)
	if err != nil {
		return resp, err
	}

	if fireNow {
		resp.Body = &cutBody{r: resp.Body, budget: d.cutBytes, err: d.cutErr}
	}

	return resp, nil
}

// attemptsFor returns how many requests path received, for post-hoc
// assertions once all concurrent goroutines have finished.
func (d *pathOnceFlakyDoer) attemptsFor(path string) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.attempts[path]
}

// TestChunkRetrier_ConcurrentChunksIndependentRecoveredCount proves that
// chunkRetrier.recovered — the single atomic counter shared by every
// concurrently-downloading chunk in a volume — accumulates correctly when
// MULTIPLE chunks are actually retrying AT THE SAME TIME (not one flaky
// chunk against a background of clean ones), and that the shared onProgress
// sink these concurrent goroutines all feed sums to exactly the total raw
// bytes with no lost or double-counted credits. Run with -race: the only
// thing keeping this safe is recovered's atomic.Int64 and onProgress's own
// internal synchronization, both of which this test exercises under genuine
// goroutine-level concurrency (an errgroup, not a sequential loop).
func TestChunkRetrier_ConcurrentChunksIndependentRecoveredCount(t *testing.T) {
	t.Parallel()

	const numChunks = 8

	payloads := make([][]byte, numChunks)
	mux := http.NewServeMux()

	for i := range numChunks {
		// Distinct sizes so a chunk's content can't accidentally match another
		// chunk's if the retry logic ever mixed up which durable file belongs
		// to which goroutine.
		payloads[i] = []byte(strings.Repeat(fmt.Sprintf("%d", i), 10+i))

		path := fmt.Sprintf("/chunk/%d", i)
		data := payloads[i]

		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			http.ServeContent(w, r, "data.img", time.Time{}, strings.NewReader(string(data)))
		})
	}

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	doer := &pathOnceFlakyDoer{cutBytes: 3, cutErr: io.ErrUnexpectedEOF}
	doer.inner = srv.Client()
	fetcher := dataplane.NewFetcher(doer)

	retrier := newChunkRetrier(fastChunkRetryPolicy())

	var (
		progressMu sync.Mutex
		total      int
	)

	onProgress := func(n int) {
		progressMu.Lock()
		total += n
		progressMu.Unlock()
	}

	dir := t.TempDir()

	var wg sync.WaitGroup

	errs := make([]error, numChunks)

	for i := range numChunks {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			rawLen := int64(len(payloads[idx]))
			partPath := filepath.Join(dir, fmt.Sprintf("chunk_%05d.part", idx))
			blockURL := srv.URL + fmt.Sprintf("/chunk/%d", idx)

			errs[idx] = retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher,
				blockURL, partPath, idx, 0, rawLen-1, rawLen, onProgress)
		}(i)
	}

	wg.Wait()

	var wantTotal int

	for i := range numChunks {
		if errs[i] != nil {
			t.Errorf("chunk %d: fetchChunk failed: %v", i, errs[i])
		}

		partPath := filepath.Join(dir, fmt.Sprintf("chunk_%05d.part", i))

		got, readErr := os.ReadFile(partPath)
		if readErr != nil {
			t.Errorf("chunk %d: read part file: %v", i, readErr)
			continue
		}

		if string(got) != string(payloads[i]) {
			t.Errorf("chunk %d: part file content = %q, want %q (cross-chunk corruption?)", i, got, payloads[i])
		}

		wantTotal += len(payloads[i])

		if got := doer.attemptsFor(fmt.Sprintf("/chunk/%d", i)); got != 2 {
			t.Errorf("chunk %d: expected exactly 2 attempts (flaky + resumed retry), got %d", i, got)
		}
	}

	if recovered := retrier.Recovered(); recovered != numChunks {
		t.Errorf("recovered = %d, want %d (one retry credited per concurrently-flaking chunk)", recovered, numChunks)
	}

	progressMu.Lock()
	defer progressMu.Unlock()

	if total != wantTotal {
		t.Errorf("shared onProgress sink summed to %d, want %d (sum of all chunks' raw lengths, no loss or double-count under concurrency)", total, wantTotal)
	}
}

// TestChunkRetrier_ConcurrentContextCancelStopsAllRetries proves that
// cancelling a context SHARED by several chunks that are all mid-backoff at
// the same time stops every one of them promptly — not just the single
// goroutine that happens to observe the cancellation first, and not after
// each independently exhausts its own sleep. This generalizes
// TestChunkRetrier_ContextCancelStopsRetryImmediately (one chunk, one
// goroutine) to genuine concurrent retry.
func TestChunkRetrier_ConcurrentContextCancelStopsAllRetries(t *testing.T) {
	t.Parallel()

	const numChunks = 6

	payloads := make([][]byte, numChunks)
	mux := http.NewServeMux()

	for i := range numChunks {
		payloads[i] = []byte(strings.Repeat("x", 20))

		path := fmt.Sprintf("/chunk/%d", i)
		data := payloads[i]

		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			http.ServeContent(w, r, "data.img", time.Time{}, strings.NewReader(string(data)))
		})
	}

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// Every chunk's first attempt is truncated, driving it into backoff. A
	// long backoff (same shape as the single-chunk cancellation test) means a
	// natural step timeout can never be what ends the loop — only the shared
	// ctx cancellation below can.
	firstSeen := make(chan string, numChunks)
	doer := &pathOnceFlakyDoer{cutBytes: 2, cutErr: io.ErrUnexpectedEOF, firstSeen: firstSeen}
	doer.inner = srv.Client()
	fetcher := dataplane.NewFetcher(doer)

	longBackoffPolicy := dataplane.RetryPolicy{
		Backoff: wait.Backoff{
			Steps:    6,
			Duration: 10 * time.Second,
			Factor:   2,
			Cap:      time.Minute,
		},
		MaxNoProgress: 3,
	}
	retrier := newChunkRetrier(longBackoffPolicy)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	dir := t.TempDir()

	var wg sync.WaitGroup

	errs := make([]error, numChunks)

	for i := range numChunks {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			rawLen := int64(len(payloads[idx]))
			partPath := filepath.Join(dir, fmt.Sprintf("chunk_%05d.part", idx))
			blockURL := srv.URL + fmt.Sprintf("/chunk/%d", idx)

			errs[idx] = retrier.fetchChunk(ctx, nil, slog.Default(), fetcher,
				blockURL, partPath, idx, 0, rawLen-1, rawLen, nil)
		}(i)
	}

	// Wait until every one of the numChunks goroutines has actually issued its
	// first request before cancelling: only then is "cancel stops an in-flight
	// backoff" the guaranteed condition under test, instead of a race against
	// goroutine scheduling.
	for range numChunks {
		<-firstSeen
	}

	start := time.Now()

	cancel()

	wg.Wait()

	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Errorf("all %d concurrent retries took %s to stop after cancellation, expected them to interrupt their 10s backoff sleeps promptly", numChunks, elapsed)
	}

	for i := range numChunks {
		if errs[i] == nil {
			t.Errorf("chunk %d: expected an error after context cancellation, got nil", i)
			continue
		}

		if !errors.Is(errs[i], context.Canceled) {
			t.Errorf("chunk %d: expected errors.Is(err, context.Canceled), got: %v", i, errs[i])
		}

		if got := doer.attemptsFor(fmt.Sprintf("/chunk/%d", i)); got != 1 {
			t.Errorf("chunk %d: expected exactly 1 request before cancellation stopped the retry, got %d", i, got)
		}
	}
}

// statusDoerFetcher builds a Fetcher whose RangeGet always fails with the
// given HTTP status.
func statusDoerFetcher(t *testing.T, status int) (*dataplane.Fetcher, string, func() int) {
	t.Helper()

	var calls int

	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()

		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)

	fetcher := dataplane.NewFetcher(srv.Client())

	return fetcher, srv.URL, func() int {
		mu.Lock()
		defer mu.Unlock()

		return calls
	}
}

// mismatchedRangeFetcher builds a Fetcher whose RangeGet always returns 206
// with a Content-Range header that does not match the requested range.
func mismatchedRangeFetcher(t *testing.T) (*dataplane.Fetcher, string, func() int) {
	t.Helper()

	var calls int

	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()

		w.Header().Set("Content-Range", "bytes 100-119/200")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(make([]byte, 20))
	}))
	t.Cleanup(srv.Close)

	fetcher := dataplane.NewFetcher(srv.Client())

	return fetcher, srv.URL, func() int {
		mu.Lock()
		defer mu.Unlock()

		return calls
	}
}

// shortReadFetcher builds a Fetcher whose RangeGet always returns a
// correctly-ranged 206 that ends in a clean EOF short of the promised range,
// standing in for a server that lied about how much data it would send.
func shortReadFetcher(t *testing.T) (*dataplane.Fetcher, string, func() int) {
	t.Helper()

	payload := []byte("0123456789ABCDEFGHIJ") // 20 bytes

	srv := newRangeServer(t, payload)

	doer := &scriptedRangeDoer{inner: srv.Client(), cutBytes: 10, cutErr: io.EOF}
	fetcher := dataplane.NewFetcher(doer)

	return fetcher, srv.URL + "/block", doer.callCount
}
