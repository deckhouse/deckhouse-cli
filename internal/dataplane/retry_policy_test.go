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

package dataplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// fastRetryPolicy is a test-only policy with the same shape as
// DefaultRetryPolicy but with a millisecond-scale backoff, so retry tests
// don't pay the production policy's multi-second budget.
//
// Cap is set well above the growth this Steps/Duration/Factor combination
// ever reaches (1ms -> 2ms -> 4ms -> 8ms for Steps=4): wait.Backoff.Step
// forces its internal step counter to 0 — ending the retry loop one
// invocation EARLIER than Steps would otherwise suggest — the moment a
// projected next duration exceeds Cap, so a tight Cap here would silently
// undercount the very attempts these tests assert on.
func fastRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Backoff: wait.Backoff{
			Steps:    4,
			Duration: time.Millisecond,
			Factor:   2,
			Cap:      50 * time.Millisecond,
		},
		MaxNoProgress: 3,
	}
}

// quietLogger returns a logger that discards everything: these tests assert
// on control flow, not on log output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// unrecognizedBreak is shaped like the failure the progress rule was written
// for: an HTTP/2 session torn down mid-body, which net/http reports from a
// type it keeps in its own unexported copy of the http2 package. Nothing
// outside that package can reach the type with errors.As or errors.Is, so a
// plain error carrying the same message is a faithful stand-in — both are
// equally opaque to every list this package keeps.
func unrecognizedBreak() error {
	return errors.New("http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=NO_ERROR")
}

// TestRetrier_RetriesUnrecognizedErrorAfterDeliveredBytes proves the rule the
// classifier alone cannot express: a transport failure of a kind neither
// IsTransientDataPlaneError nor IsFatalDataPlaneError recognizes still buys
// another attempt, PROVIDED the attempt delivered bytes before it broke.
//
// Without the rule the classifier's fail-closed default decides, and the
// transfer ends on the first attempt — which is exactly how a download that
// had already moved gigabytes could be lost to one reloaded proxy.
func TestRetrier_RetriesUnrecognizedErrorAfterDeliveredBytes(t *testing.T) {
	t.Parallel()

	broken := unrecognizedBreak()

	// The premise of this guard: the error really is unrecognized. If a later
	// change teaches the classifier this shape, the guard stops testing the
	// progress rule and starts testing the classifier — silently — so it says
	// so out loud instead.
	if IsTransientDataPlaneError(broken) {
		t.Fatalf("premise broken: %v is now a recognized transient error, so this test no longer exercises the progress rule", broken)
	}

	if IsFatalDataPlaneError(broken) {
		t.Fatalf("premise broken: %v is now a recognized fatal error", broken)
	}

	var attempts int

	retrier := NewRetrier(fastRetryPolicy())

	err := retrier.Resume(context.Background(), quietLogger(), "transfer",
		func(_ context.Context) (Progress, error) {
			attempts++

			if attempts == 1 {
				// Bytes landed durably, and only then did the link break.
				return Progress{Start: 0, Durable: 4096}, broken
			}

			return Progress{Start: 4096, Durable: 8192}, nil
		})
	if err != nil {
		t.Fatalf("Resume: %v (an unrecognized break AFTER delivered bytes must be retried, not fatal)", err)
	}

	if attempts != 2 {
		t.Errorf("attempts = %d, want 2 (one broken, one resumed)", attempts)
	}

	if got := retrier.Recovered(); got != 1 {
		t.Errorf("Recovered() = %d, want 1 (the absorbed retry must be counted)", got)
	}
}

// TestRetrier_UnrecognizedErrorWithoutDeliveryIsFatal is the other half of the
// rule above, and the reason the rule is safe: when nothing was delivered,
// an unrecognized error still ends the transfer on the first attempt. A setup
// that is simply broken — a destination that cannot be opened, a port nobody
// listens on — must not cost a full retry budget before it says so.
func TestRetrier_UnrecognizedErrorWithoutDeliveryIsFatal(t *testing.T) {
	t.Parallel()

	broken := unrecognizedBreak()

	var attempts int

	retrier := NewRetrier(fastRetryPolicy())

	err := retrier.Resume(context.Background(), quietLogger(), "transfer",
		func(_ context.Context) (Progress, error) {
			attempts++

			// Resumed from a durable prefix an EARLIER run left behind, and
			// added nothing to it: Start and Durable are equal and non-zero,
			// which is the case a rule keyed on "is the durable offset above
			// zero" would get wrong.
			return Progress{Start: 4096, Durable: 4096}, broken
		})
	if err == nil {
		t.Fatal("expected an unrecognized error with no delivery to be fatal, got nil")
	}

	if !errors.Is(err, broken) {
		t.Errorf("err = %v, want it to wrap the original failure", err)
	}

	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (nothing was delivered, so nothing justifies another attempt)", attempts)
	}
}

// TestRetrier_RetriesNotAcceptedWithoutAnyDelivery proves the one thing
// ErrDataPlaneNotAccepted exists to buy, and it is the exact opposite of the
// test above: an attempt that delivered NOTHING still gets another one.
//
// Zero delivery is the whole point, and a guard written on a delivering attempt
// would prove nothing — the progress rule already retries those, so such a guard
// would be green with the sentinel removed from the transient set entirely. It
// is the writing transfers that need this: an upload cannot measure how much of
// a dead request landed, so "the far end still answers" is the only evidence it
// has that the failure was the transport, and that evidence has to survive an
// attempt whose durable offset never moved.
//
// Start and Durable are equal and NON-ZERO for the same reason as in
// TestRetrier_UnrecognizedErrorWithoutDeliveryIsFatal: a rule keyed on "is the
// durable offset above zero" rather than on delivery would pass a zero-based
// version of this without ever consulting the transient set.
func TestRetrier_RetriesNotAcceptedWithoutAnyDelivery(t *testing.T) {
	t.Parallel()

	notAccepted := fmt.Errorf("upload chunk at offset 4096: %w", ErrDataPlaneNotAccepted)

	var attempts int

	retrier := NewRetrier(fastRetryPolicy())

	err := retrier.Resume(context.Background(), quietLogger(), "transfer",
		func(_ context.Context) (Progress, error) {
			attempts++

			if attempts < 3 {
				return Progress{Start: 4096, Durable: 4096}, notAccepted
			}

			return Progress{Start: 4096, Durable: 8192}, nil
		})
	if err != nil {
		t.Fatalf("Resume() = %v, want nil: a far end that answers must buy another attempt", err)
	}

	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

// TestRetrier_NotAcceptedIsStillBoundedWithoutDelivery holds the other side of
// the same sentinel: buying attempts without delivering is exactly what
// MaxNoProgress is for, so an endpoint that keeps answering and keeps refusing
// must still run out. Without this, the case above is satisfied by a sentinel
// that simply never stops.
func TestRetrier_NotAcceptedIsStillBoundedWithoutDelivery(t *testing.T) {
	t.Parallel()

	notAccepted := fmt.Errorf("refused at offset 4096: %w", ErrDataPlaneNotAccepted)

	policy := fastRetryPolicy()

	var attempts int

	retrier := NewRetrier(policy)

	err := retrier.Resume(context.Background(), quietLogger(), "transfer",
		func(_ context.Context) (Progress, error) {
			attempts++

			if attempts > policy.Backoff.Steps+policy.MaxNoProgress {
				t.Errorf("attempt %d: the loop is not bounded", attempts)

				return Progress{}, nil
			}

			return Progress{Start: 4096, Durable: 4096}, notAccepted
		})
	if err == nil {
		t.Fatal("Resume() = nil, want an error once the no-progress ceiling is reached")
	}

	if !errors.Is(err, ErrDataPlaneNotAccepted) {
		t.Errorf("err = %v, want it to carry the original refusal", err)
	}

	if attempts != policy.MaxNoProgress {
		t.Errorf("attempts = %d, want %d (the consecutive no-progress ceiling)", attempts, policy.MaxNoProgress)
	}
}

// TestRetrier_FatalErrorAfterDeliveredBytesIsNotRetried proves the fatal set
// outranks the progress rule.
//
// The delivered bytes are the whole point of this test, and testing these
// errors on a zero-delivery attempt would prove nothing: the most natural way
// to implement the progress rule is to check progress FIRST and hand anything
// that made progress straight to a retry, and on a zero-delivery attempt both
// orders behave identically. Only after bytes have been delivered do the two
// orders diverge — and each divergence is a real defect: a rejected
// credential would cost the whole retry budget instead of one attempt, and a
// body whose Content-Range does not cover the requested range would be read
// again from an offset the server never agreed to, which is how a destination
// gets silently filled with the wrong bytes.
//
// Context cancellation is deliberately NOT in this table. The backoff between
// attempts checks the context itself, so a cancelled transfer never reaches a
// second attempt under EITHER order of the two checks — a guard on it would
// be green whichever way the branches were written.
func TestRetrier_FatalErrorAfterDeliveredBytesIsNotRetried(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sentinel error
	}{
		{name: "authorization refused", sentinel: ErrExportUnauthorized},
		{name: "content-range mismatch", sentinel: ErrContentRangeMismatch},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var attempts int

			// A caller predicate that refuses everything, to pin the one
			// direction RetryPolicy.Fatal is allowed to work in: it may ADD
			// to the built-in fatal set and must not be able to subtract
			// from it.
			policy := fastRetryPolicy()
			policy.Fatal = func(error) bool { return false }

			retrier := NewRetrier(policy)

			err := retrier.Resume(context.Background(), quietLogger(), "transfer",
				func(_ context.Context) (Progress, error) {
					attempts++

					// Bytes first, refusal second: the order that separates a
					// correct implementation from a plausible wrong one.
					return Progress{Start: 0, Durable: 4096}, fmt.Errorf("range get: %w", tc.sentinel)
				})
			if err == nil {
				t.Fatal("expected a fatal error, got nil")
			}

			if !errors.Is(err, tc.sentinel) {
				t.Errorf("err = %v, want errors.Is(err, %v)", err, tc.sentinel)
			}

			if attempts != 1 {
				t.Errorf("attempts = %d, want 1 (a fatal error must not be retried, however many bytes preceded it)", attempts)
			}

			if got := retrier.Recovered(); got != 0 {
				t.Errorf("Recovered() = %d, want 0 (no retry was absorbed)", got)
			}
		})
	}
}

// TestRetrier_BudgetIsFiniteWhenEveryAttemptDelivers pins the bound on the
// one stream shape that can outlive a careless budget: every attempt delivers
// real bytes and then breaks, forever. The tempting reading of "progress
// deserves another try" is that progress REFRESHES the budget, and on this
// stream that reading never terminates.
//
// The MaxNoProgress ceiling does not cover this case at all — it counts
// attempts that delivered nothing, and here every attempt delivers. Only the
// backoff's step budget, which every attempt spends whether it delivered or
// not, ends this loop.
//
// The stub bounds ITSELF rather than trusting the loop to stop, because a
// budget-refreshing implementation would not fail this test — it would spin
// until the whole package's test binary timed out, taking every other test
// with it and proving nothing. Past its own ceiling the stub returns a fatal
// error, so the loop stops and the assertion below can report what happened.
func TestRetrier_BudgetIsFiniteWhenEveryAttemptDelivers(t *testing.T) {
	t.Parallel()

	policy := fastRetryPolicy()

	// Far above any budget this policy could legitimately spend, so tripping
	// it means the budget is not being spent at all.
	const selfLimit = 25

	var (
		attempts         int
		trippedSelfLimit bool
	)

	retrier := NewRetrier(policy)

	err := retrier.Resume(context.Background(), quietLogger(), "transfer",
		func(_ context.Context) (Progress, error) {
			attempts++

			if attempts > selfLimit {
				trippedSelfLimit = true

				return Progress{}, fmt.Errorf("self-limit reached: %w", ErrExportUnauthorized)
			}

			start := int64(attempts-1) * 4096

			return Progress{Start: start, Durable: start + 4096}, unrecognizedBreak()
		})

	if trippedSelfLimit {
		t.Fatalf("the retry loop made more than %d attempts on a stream that delivered bytes on every one: "+
			"delivered bytes are refreshing the budget instead of merely justifying one more attempt", selfLimit)
	}

	if err == nil {
		t.Fatal("expected an error once the retry budget is exhausted, got nil")
	}

	if attempts < 2 {
		t.Fatalf("attempts = %d: the stream must actually have been retried for this bound to mean anything", attempts)
	}

	if attempts > policy.Backoff.Steps {
		t.Errorf("attempts = %d, want at most the backoff step budget (%d)", attempts, policy.Backoff.Steps)
	}

	if !strings.Contains(err.Error(), "exhausted") {
		t.Errorf("err = %q, want it to say the budget was exhausted", err)
	}
}

// recordCapture collects every slog record a test emits, with its level and
// attributes, so a guard can assert on what an ordinary run would actually
// show rather than on a formatted string.
type recordCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordCapture) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *recordCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, r.Clone())

	return nil
}

func (h *recordCapture) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *recordCapture) WithGroup(_ string) slog.Handler { return h }

func (h *recordCapture) all() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]slog.Record, len(h.records))
	copy(out, h.records)

	return out
}

// attrsOf flattens one record's attributes into a map for assertions.
func attrsOf(r slog.Record) map[string]slog.Value {
	out := make(map[string]slog.Value, r.NumAttrs())

	r.Attrs(func(a slog.Attr) bool {
		out[a.Key] = a.Value

		return true
	})

	return out
}

// TestRetrier_LogsRecoveredBreakWhereBarsAreLive pins the only evidence a
// recovered transfer leaves behind. Once a retry succeeds, the transfer looks
// exactly like one that never broke — same bytes, same exit code — so without
// this line no one reading a run's output can tell whether a break happened
// and was survived, or never happened at all. On a live run that difference
// decides whether a resume path was exercised or merely not needed.
//
// The threshold below is WARN, not INFO, and that is the point: the commands
// that draw live progress bars replace their run logger with one admitting
// WARN and above, so a line at INFO would be dropped from precisely the runs
// this evidence is for. Checking INFO here would leave demoting the line to
// INFO a green change.
//
// THE FIXTURE IS SHAPED TO MAKE THE OTHER TWO FIELDS FALSIFIABLE, and both
// shapes are deliberate:
//
//   - the stream breaks TWICE, so the expected attempt numbers are 1 and 2.
//     Break it once and every attempt number in sight is 1, which a field
//     hard-wired to the constant 1 satisfies just as well — the assertion
//     would hold while the line lost the ability to tell one recovered break
//     from fifteen.
//
//   - every attempt resumes from a NON-ZERO offset, so resume_offset differs
//     from the bytes that attempt delivered. Resume from zero and the two are
//     equal, and a line reporting delivered bytes under the name
//     resume_offset reads as correct — on a chunk resumed from a million it
//     would then claim the retry restarted near the beginning.
func TestRetrier_LogsRecoveredBreakWhereBarsAreLive(t *testing.T) {
	t.Parallel()

	// One attempt per row, in order: where it resumed from, where it got to,
	// and whether it broke. The offsets are deliberately unequal to each
	// other and to every delivery, so no two fields can be swapped unnoticed.
	attemptRuns := []Progress{
		{Start: 1_000_000, Durable: 1_004_096}, // delivered 4096, breaks
		{Start: 1_004_096, Durable: 1_006_144}, // delivered 2048, breaks
		{Start: 1_006_144, Durable: 1_007_000}, // succeeds
	}

	// What the two recovered breaks must be reported as.
	wantRecords := []struct {
		attempt      int64
		resumeOffset int64
		delivered    int64
	}{
		{attempt: 1, resumeOffset: 1_004_096, delivered: 4096},
		{attempt: 2, resumeOffset: 1_006_144, delivered: 2048},
	}

	capture := &recordCapture{}

	var attempts int

	retrier := NewRetrier(fastRetryPolicy())

	err := retrier.Resume(context.Background(), slog.New(capture), "chunk 7",
		func(_ context.Context) (Progress, error) {
			if attempts >= len(attemptRuns) {
				// A regression that kept retrying past the successful attempt
				// would otherwise index out of range and panic; fail the loop
				// cleanly instead, so the attempt count below reports it.
				attempts++

				return Progress{}, fmt.Errorf("attempt %d was not scripted: %w", attempts, ErrExportUnauthorized)
			}

			progress := attemptRuns[attempts]
			attempts++

			if attempts < len(attemptRuns) {
				return progress, unrecognizedBreak()
			}

			return progress, nil
		})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if attempts != len(attemptRuns) {
		t.Fatalf("attempts = %d, want %d", attempts, len(attemptRuns))
	}

	var visible []slog.Record

	for _, r := range capture.all() {
		if r.Level >= slog.LevelWarn {
			visible = append(visible, r)
		}
	}

	if len(visible) != len(wantRecords) {
		t.Fatalf("got %d records at WARN or above, want %d (one per recovered break); "+
			"a run with live progress bars shows nothing below WARN. All records: %v",
			len(visible), len(wantRecords), capture.all())
	}

	for i, want := range wantRecords {
		got := visible[i]
		attrs := attrsOf(got)

		if attempt, ok := attrs["attempt"]; !ok || attempt.Int64() != want.attempt {
			t.Errorf("record %d %q has attempt=%v, want %d; without a real count this line cannot "+
				"tell one recovered break from fifteen", i, got.Message, attrs["attempt"], want.attempt)
		}

		if offset, ok := attrs["resume_offset"]; !ok || offset.Int64() != want.resumeOffset {
			t.Errorf("record %d %q has resume_offset=%v, want %d (the durable offset the retry continues "+
				"from, NOT the %d bytes this attempt delivered)", i, got.Message,
				attrs["resume_offset"], want.resumeOffset, want.delivered)
		}

		if delivered, ok := attrs["delivered_bytes"]; !ok || delivered.Int64() != want.delivered {
			t.Errorf("record %d %q has delivered_bytes=%v, want %d", i, got.Message,
				attrs["delivered_bytes"], want.delivered)
		}

		if transfer, ok := attrs["transfer"]; !ok || transfer.String() != "chunk 7" {
			t.Errorf("record %d %q has transfer=%v, want the subject; resume_offset is in the subject's "+
				"own coordinates and means little without it", i, got.Message, attrs["transfer"])
		}
	}
}

// TestNewRetrier_MissingNoProgressBoundFallsBackToTheDefault pins what a
// policy built without MaxNoProgress does. Left at zero it is not "no
// ceiling" but a ceiling of zero, which fires on the first attempt — the one
// that just delivered bytes — and explains itself as "no progress in 0
// consecutive attempts", contradicting what the caller can see happening.
// A forgotten struct field is a plausible mistake, and this is about the most
// misleading symptom it could produce.
func TestNewRetrier_MissingNoProgressBoundFallsBackToTheDefault(t *testing.T) {
	t.Parallel()

	policy := fastRetryPolicy()
	policy.MaxNoProgress = 0

	var attempts int

	retrier := NewRetrier(policy)

	err := retrier.Resume(context.Background(), quietLogger(), "transfer",
		func(_ context.Context) (Progress, error) {
			attempts++

			if attempts == 1 {
				return Progress{Start: 0, Durable: 4096}, fmt.Errorf("read body: %w", io.ErrUnexpectedEOF)
			}

			return Progress{Start: 4096, Durable: 8192}, nil
		})
	if err != nil {
		t.Fatalf("Resume: %v (an attempt that delivered bytes must not be stopped by an unset ceiling)", err)
	}

	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
}

// TestNewRetrier_MissingBackoffFallsBackToTheDefault pins the other half:
// with Backoff.Steps left at zero the backoff helper returns without running
// the attempt even once, so nothing is transferred at all and the failure
// reads as a bare wait timeout that names neither the transfer nor a cause.
func TestNewRetrier_MissingBackoffFallsBackToTheDefault(t *testing.T) {
	t.Parallel()

	var attempts int

	// Only MaxNoProgress set; Backoff is the zero value. The attempt succeeds
	// immediately, so the substituted production backoff costs no sleep.
	retrier := NewRetrier(RetryPolicy{MaxNoProgress: 3})

	err := retrier.Resume(context.Background(), quietLogger(), "transfer",
		func(_ context.Context) (Progress, error) {
			attempts++

			return Progress{Start: 0, Durable: 4096}, nil
		})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (an unset backoff must not mean zero attempts)", attempts)
	}
}

// TestNewRetrier_MissingBackoffIsReplacedWholesale pins the shape of the
// substitution, not just its presence. Filling in Steps alone would satisfy
// "an unset backoff still makes attempts" (the test above) while leaving
// Duration at zero — six attempts fired back to back at a server that has
// just dropped the connection, with no pause at all. That is a plausible
// simplification of withDefaults and it must not be a green one.
//
// The struct comparison covers every field at once, including the ones no
// timing assertion could distinguish (Factor, Jitter, Cap). The elapsed-time
// assertion below is what stops that comparison from being a statement about
// a stored value only: it shows the substituted Duration actually reaches the
// loop that sleeps on it.
func TestNewRetrier_MissingBackoffIsReplacedWholesale(t *testing.T) {
	t.Parallel()

	if got := NewRetrier(RetryPolicy{}).policy.Backoff; got != defaultFetchBackoff {
		t.Errorf("Backoff = %+v, want the whole default %+v (filling in Steps alone leaves Duration at zero)",
			got, defaultFetchBackoff)
	}

	var attempts int

	retrier := NewRetrier(RetryPolicy{MaxNoProgress: 3})

	start := time.Now()

	err := retrier.Resume(context.Background(), quietLogger(), "transfer",
		func(_ context.Context) (Progress, error) {
			attempts++

			if attempts == 1 {
				return Progress{Start: 0, Durable: 4096}, fmt.Errorf("read body: %w", io.ErrUnexpectedEOF)
			}

			return Progress{Start: 4096, Durable: 8192}, nil
		})

	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}

	// A literal floor, deliberately NOT derived from defaultFetchBackoff.
	// A threshold computed from the constant it is measuring shrinks along
	// with it, so dropping the default pause to zero would keep this
	// assertion green while the loop re-hit a far end that had just dropped
	// the connection, six times, with no pause at all. Nothing else in the
	// tree pins that constant.
	const pauseFloor = 500 * time.Millisecond

	// The floor separates "paused" from "did not pause" only while the
	// production pause stays comfortably above it. Should the default ever be
	// lowered that far, this test stops discriminating, and it says so rather
	// than failing with a bare timing number.
	if defaultFetchBackoff.Duration < 2*pauseFloor {
		t.Fatalf("premise broken: the default backoff pause is %s, no longer comfortably above this test's %s floor",
			defaultFetchBackoff.Duration, pauseFloor)
	}

	if elapsed < pauseFloor {
		t.Errorf("one retry took %s, want at least %s: the backoff substituted for an unset one is not "+
			"reaching the loop that sleeps on it", elapsed, pauseFloor)
	}
}

// TestRootCause proves rootCause unwraps a chain of %w-wrapped errors down to
// the deepest cause, and passes both nil and an already-unwrapped error
// through unchanged.
func TestRootCause(t *testing.T) {
	t.Parallel()

	base := errors.New("base failure")

	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "nil error returns nil", err: nil, want: nil},
		{name: "unwrapped error returns itself", err: base, want: base},
		{name: "single wrap returns the base", err: fmt.Errorf("attempt 1: %w", base), want: base},
		{
			name: "double wrap returns the deepest base",
			err:  fmt.Errorf("attempt 2: %w", fmt.Errorf("attempt 1: %w", base)),
			want: base,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := rootCause(tc.err); got != tc.want {
				t.Errorf("rootCause(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
