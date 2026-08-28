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
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
)

// fastChunkRetryPolicy is a test-only policy with the same shape as
// defaultChunkRetryPolicy but with a millisecond-scale backoff, so retry
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
func fastChunkRetryPolicy() chunkRetryPolicy {
	return chunkRetryPolicy{
		backoff: wait.Backoff{
			Steps:    4,
			Duration: time.Millisecond,
			Factor:   2,
			Cap:      50 * time.Millisecond,
		},
		maxNoProgress: 3,
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

// scriptedRangeDoer wraps a real exporter.Doer and records every request's
// Range header in call order. cut, when non-nil, truncates every response
// body (every call, not just one) after cutBytes bytes with cutErr — standing
// in for a link that breaks on every attempt.
type scriptedRangeDoer struct {
	inner    exporter.Doer
	cutBytes int64
	cutErr   error // nil disables truncation

	mu     sync.Mutex
	ranges []string
}

func (d *scriptedRangeDoer) Do(req *http.Request) (*http.Response, error) {
	d.mu.Lock()
	d.ranges = append(d.ranges, req.Header.Get("Range"))
	d.mu.Unlock()

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

// onceCutDoer wraps a real exporter.Doer and truncates only the FIRST
// response body (with cutErr, after cutBytes bytes); every subsequent call —
// in particular the resumed retry — passes through untouched.
type onceCutDoer struct {
	inner    exporter.Doer
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
	fetcher := exporter.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	retrier := &chunkRetrier{policy: fastChunkRetryPolicy()}

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

// TestChunkRetrier_ExhaustsBudget proves that a link broken on every attempt
// exhausts exactly the policy's Steps budget and returns an error that still
// satisfies errors.Is against the underlying transient sentinel.
func TestChunkRetrier_ExhaustsBudget(t *testing.T) {
	t.Parallel()

	payload := make([]byte, 100)

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	doer := &scriptedRangeDoer{inner: srv.Client(), cutBytes: 5, cutErr: io.ErrUnexpectedEOF}
	fetcher := exporter.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	policy := fastChunkRetryPolicy()
	retrier := &chunkRetrier{policy: policy}

	rawLen := int64(len(payload))

	err := retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, nil)
	if err == nil {
		t.Fatal("expected an error once the retry budget is exhausted, got nil")
	}

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("expected errors.Is(err, io.ErrUnexpectedEOF), got: %v", err)
	}

	if got := doer.callCount(); got != policy.backoff.Steps {
		t.Errorf("expected exactly %d requests (the full Steps budget), got %d", policy.backoff.Steps, got)
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

// TestChunkRetrier_DoesNotRetryFatal proves that every non-transient error
// stops the retry loop on the very first attempt, and that errors.Is against
// the original sentinel still holds through fetchChunk's returned error.
func TestChunkRetrier_DoesNotRetryFatal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		buildFetch func(t *testing.T) (fetcher *exporter.Fetcher, blockURL string, callCount func() int)
		wantErr    error
	}{
		{
			name: "401 unauthorized",
			buildFetch: func(t *testing.T) (*exporter.Fetcher, string, func() int) {
				t.Helper()
				return statusDoerFetcher(t, http.StatusUnauthorized)
			},
			wantErr: exporter.ErrExportUnauthorized,
		},
		{
			name: "403 forbidden",
			buildFetch: func(t *testing.T) (*exporter.Fetcher, string, func() int) {
				t.Helper()
				return statusDoerFetcher(t, http.StatusForbidden)
			},
			wantErr: exporter.ErrExportUnauthorized,
		},
		{
			name: "content-range mismatch",
			buildFetch: func(t *testing.T) (*exporter.Fetcher, string, func() int) {
				t.Helper()
				return mismatchedRangeFetcher(t)
			},
			wantErr: exporter.ErrContentRangeMismatch,
		},
		{
			name: "clean short read",
			buildFetch: func(t *testing.T) (*exporter.Fetcher, string, func() int) {
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

			retrier := &chunkRetrier{policy: fastChunkRetryPolicy()}

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
// failure (unreachable from exporter.IsTransientDataPlaneError's allow-list)
// is fatal on the first attempt, exactly like a rejected/mismatched response.
func TestChunkRetrier_DoesNotRetryLocalWriteError(t *testing.T) {
	t.Parallel()

	payload := []byte("0123456789ABCDEFGHIJ")

	srv := newRangeServer(t, payload)
	blockURL := srv.URL + "/block"

	doer := &scriptedRangeDoer{inner: srv.Client()}
	fetcher := exporter.NewFetcher(doer)

	// partPath's parent directory does not exist, so opening it for append
	// fails with a local *os.PathError — not in the transient allow-list.
	partPath := filepath.Join(t.TempDir(), "missing-parent", "chunk_00000.part")

	retrier := &chunkRetrier{policy: fastChunkRetryPolicy()}

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

	doer := &scriptedRangeDoer{inner: srv.Client(), cutBytes: 2, cutErr: io.ErrUnexpectedEOF}
	fetcher := exporter.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	// A long backoff so cancellation, not a natural step timeout, is what
	// ends the loop.
	longBackoffPolicy := chunkRetryPolicy{
		backoff: wait.Backoff{
			Steps:    6,
			Duration: 10 * time.Second,
			Factor:   2,
			Cap:      time.Minute,
		},
		maxNoProgress: 3,
	}
	retrier := &chunkRetrier{policy: longBackoffPolicy}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	rawLen := int64(len(payload))

	start := time.Now()

	err := retrier.fetchChunk(ctx, nil, slog.Default(), fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, nil)

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
	fetcher := exporter.NewFetcher(doer)

	dir := t.TempDir()
	partPath := filepath.Join(dir, "chunk_00000.part")

	policy := chunkRetryPolicy{
		backoff: wait.Backoff{
			Steps:    6, // larger than maxNoProgress: no-progress must stop it first
			Duration: time.Millisecond,
			Factor:   2,
			Cap:      50 * time.Millisecond, // see fastChunkRetryPolicy: keep well above the growth curve
		},
		maxNoProgress: 3,
	}
	retrier := &chunkRetrier{policy: policy}

	rawLen := int64(len(payload))

	err := retrier.fetchChunk(context.Background(), nil, slog.Default(), fetcher, blockURL,
		partPath, 0, 0, rawLen-1, rawLen, nil)
	if err == nil {
		t.Fatal("expected an error once no-progress attempts are exhausted, got nil")
	}

	if !strings.Contains(err.Error(), "no progress") {
		t.Errorf("expected the error to name the no-progress condition, got: %v", err)
	}

	// The very first attempt always establishes a baseline durable offset (0
	// bytes is still "progress" relative to no prior attempt at all — see
	// chunkRetrier.fetchChunk's lastDurable := -1 sentinel), so it takes
	// maxNoProgress+1 total attempts before maxNoProgress CONSECUTIVE
	// zero-advancement attempts have actually occurred.
	wantCalls := policy.maxNoProgress + 1
	if got := doer.callCount(); got != wantCalls {
		t.Errorf("expected exactly %d requests (maxNoProgress+1 for the baseline attempt), got %d", wantCalls, got)
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

// pathOnceFlakyDoer wraps a real exporter.Doer and truncates (with cutErr,
// after cutBytes bytes) only the FIRST request whose URL path it sees —
// tracked per distinct path — so several concurrently-downloading chunks
// backed by DIFFERENT paths on the same doer each flake exactly once,
// independently, regardless of the order or overlap in which their requests
// actually arrive. attempts records, per path, how many requests that path
// has received (also useful for asserting "exactly one retry per chunk"
// under real concurrency, not just sequential simulation).
type pathOnceFlakyDoer struct {
	inner    exporter.Doer
	cutBytes int64
	cutErr   error

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
	fetcher := exporter.NewFetcher(doer)

	retrier := &chunkRetrier{policy: fastChunkRetryPolicy()}

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

	if recovered := retrier.recovered.Load(); recovered != numChunks {
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
	doer := &pathOnceFlakyDoer{cutBytes: 2, cutErr: io.ErrUnexpectedEOF}
	doer.inner = srv.Client()
	fetcher := exporter.NewFetcher(doer)

	longBackoffPolicy := chunkRetryPolicy{
		backoff: wait.Backoff{
			Steps:    6,
			Duration: 10 * time.Second,
			Factor:   2,
			Cap:      time.Minute,
		},
		maxNoProgress: 3,
	}
	retrier := &chunkRetrier{policy: longBackoffPolicy}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)

	dir := t.TempDir()

	var wg sync.WaitGroup

	errs := make([]error, numChunks)

	start := time.Now()

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
func statusDoerFetcher(t *testing.T, status int) (*exporter.Fetcher, string, func() int) {
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

	fetcher := exporter.NewFetcher(srv.Client())

	return fetcher, srv.URL, func() int {
		mu.Lock()
		defer mu.Unlock()

		return calls
	}
}

// mismatchedRangeFetcher builds a Fetcher whose RangeGet always returns 206
// with a Content-Range header that does not match the requested range.
func mismatchedRangeFetcher(t *testing.T) (*exporter.Fetcher, string, func() int) {
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

	fetcher := exporter.NewFetcher(srv.Client())

	return fetcher, srv.URL, func() int {
		mu.Lock()
		defer mu.Unlock()

		return calls
	}
}

// shortReadFetcher builds a Fetcher whose RangeGet always returns a
// correctly-ranged 206 that ends in a clean EOF short of the promised range,
// standing in for a server that lied about how much data it would send.
func shortReadFetcher(t *testing.T) (*exporter.Fetcher, string, func() int) {
	t.Helper()

	payload := []byte("0123456789ABCDEFGHIJ") // 20 bytes

	srv := newRangeServer(t, payload)

	doer := &scriptedRangeDoer{inner: srv.Client(), cutBytes: 10, cutErr: io.EOF}
	fetcher := exporter.NewFetcher(doer)

	return fetcher, srv.URL + "/block", doer.callCount
}
