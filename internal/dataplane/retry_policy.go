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
	"log/slog"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// defaultFetchBackoff bounds one transfer's in-run retries: starting at 1s and
// doubling, capped at 30s. wait.Backoff's Cap does double duty as both a
// ceiling on any single sleep AND an early-termination trigger — once a
// projected next delay would exceed Cap, the step budget is forced to 0
// immediately, ending the loop one attempt sooner than Steps alone would
// suggest. With these parameters that yields 5 attempts in the worst case
// (not 6), for a total backoff of ~30-34s added to a transfer that eventually
// succeeds — negligible against the 650-790s a single 256 MiB range stream
// already takes on a WAN link — while a link that keeps breaking still fails
// loudly instead of grinding for hours.
var defaultFetchBackoff = wait.Backoff{
	Steps:    6,
	Duration: 1 * time.Second,
	Factor:   2.0,
	Jitter:   0.2,
	Cap:      30 * time.Second,
}

// defaultMaxNoProgressAttempts bounds attempts that re-issue the request and
// deliver zero bytes. A server that accepts the range and immediately closes
// would otherwise burn the whole backoff budget in a hot loop; three such
// attempts is proof the far end is not going to deliver.
const defaultMaxNoProgressAttempts = 3

// RetryPolicy bounds one transfer's retry loop: Retrier.Resume stops
// re-issuing the request once either Backoff's step budget or MaxNoProgress
// consecutive zero-delivery attempts is exhausted.
//
// The two bounds are not interchangeable, and only one of them survives a
// link that keeps delivering:
//
//   - MaxNoProgress counts CONSECUTIVE zero-delivery attempts and a
//     delivering attempt resets it to zero. It catches a far end that accepts
//     the request and sends nothing, and it catches nothing else.
//
//   - Backoff's step budget is spent by every attempt the loop makes,
//     delivering or not, and nothing refreshes it. It is therefore the only
//     thing standing between the loop and the nastiest shape of broken link:
//     one that delivers some bytes and then breaks, over and over, so that
//     MaxNoProgress is reset each time round.
type RetryPolicy struct {
	Backoff       wait.Backoff
	MaxNoProgress int

	// Fatal, when non-nil, names failures THIS caller knows are not a broken
	// transport, and so are not worth another attempt however many bytes
	// preceded them. It can only ADD to IsFatalDataPlaneError: Resume checks
	// the built-in set first, so a permissive predicate cannot talk a refused
	// credential or an untrustworthy Content-Range into being retried.
	//
	// It exists because the progress rule in Resume is deliberately blind to
	// error identity, and a caller that CAN identify a failure — a producer
	// that ended a response short of the range it promised, say — should not
	// have to spend a whole budget rediscovering that it will keep doing so.
	Fatal func(error) bool
}

// DefaultRetryPolicy returns the production retry policy applied to every
// resumable data-plane transfer.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Backoff:       defaultFetchBackoff,
		MaxNoProgress: defaultMaxNoProgressAttempts,
	}
}

// Progress is what one Attempt reports about how far the transfer got.
// Start is the durable offset the attempt resumed from; Durable is the offset
// durably persisted when the attempt ended. Both are in the caller's own
// coordinates (an absolute file offset, an offset within one chunk, ...) —
// Resume only ever subtracts one from the other, never interprets them.
//
// An attempt that fails before it can even establish where it stands should
// report the zero value, which reads as a zero-delivery attempt like any
// other.
type Progress struct {
	Start   int64
	Durable int64
}

// Delivered reports how many bytes this attempt added to the durable prefix.
// It is the sole progress signal Resume acts on. A report whose Durable ran
// backwards yields a non-positive value and is read as no delivery, which is
// the safe reading: an attempt that cannot say it moved forward must not buy
// another one.
func (p Progress) Delivered() int64 {
	return p.Durable - p.Start
}

// Attempt performs exactly one try of a resumable transfer and reports how
// far it got. It must not retry internally: the retry seam is Resume, and an
// attempt that looped inside would hide its failures from both bounds of the
// policy.
//
// The attempt is responsible for resuming from its own durable state; Resume
// never tells it where to start, which is why the same mechanism serves
// resuming across attempts within one run and resuming across separate runs.
type Attempt func(ctx context.Context) (Progress, error)

// Retrier carries one run's retry policy plus the aggregate count of retries
// it absorbed. One instance may be shared by several concurrent transfers:
// Resume keeps all its per-transfer state in locals, and only recovered is
// mutated concurrently — it is atomic for that reason.
type Retrier struct {
	policy    RetryPolicy
	recovered atomic.Int64
}

// NewRetrier returns a Retrier bound to policy, with any bound the caller left
// unset replaced by the production default (see RetryPolicy.withDefaults).
func NewRetrier(policy RetryPolicy) *Retrier {
	return &Retrier{policy: policy.withDefaults()}
}

// withDefaults replaces every non-positive bound with its production default,
// because a zero bound does not mean "unbounded" here — it means the loop
// cannot work at all, and says so in a way that points at the wrong thing:
//
//   - MaxNoProgress at zero makes the ceiling fire on the FIRST attempt, even
//     one that delivered bytes, and report "made no progress in 0 consecutive
//     attempts" — a message that contradicts what just happened;
//   - Backoff.Steps at zero makes wait.ExponentialBackoffWithContext return
//     without running the attempt even once, so nothing is transferred and the
//     failure reads as a bare wait timeout, naming neither the transfer nor a
//     cause.
//
// Neither is a state a caller ever intends, and both are one forgotten struct
// field away. The whole Backoff is substituted rather than just its Steps: a
// Backoff missing Steps is in practice a zero value, and filling in Steps
// alone would leave Duration at zero — a retry loop that hammers the far end
// with no pause between attempts.
func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.MaxNoProgress <= 0 {
		p.MaxNoProgress = defaultMaxNoProgressAttempts
	}

	if p.Backoff.Steps <= 0 {
		p.Backoff = defaultFetchBackoff
	}

	return p
}

// Recovered reports how many retries this Retrier has absorbed across every
// transfer that used it — worth reporting once a run finishes, since a
// recovered transfer is otherwise indistinguishable from an untroubled one.
func (r *Retrier) Recovered() int64 {
	return r.recovered.Load()
}

// isFatal reports whether err ends the transfer outright: the built-in set
// first, then the policy's own extension. The order is not an optimization —
// it is what makes RetryPolicy.Fatal unable to subtract from the built-in set.
func (r *Retrier) isFatal(err error) bool {
	if IsFatalDataPlaneError(err) {
		return true
	}

	return r.policy.Fatal != nil && r.policy.Fatal(err)
}

// Resume retries attempt with bounded exponential backoff, each attempt
// continuing from the durable offset the previous one persisted, until the
// transfer completes, a fatal error occurs, ctx is cancelled, or the retry
// budget is exhausted. subject names the transfer in errors and logs (for
// example "chunk 7").
//
// WHICH FAILURES BUY ANOTHER ATTEMPT. In order, and the order is the point:
//
//  1. ctx cancellation, every member of IsFatalDataPlaneError, and anything
//     the policy's own Fatal predicate names end the loop at once, no matter
//     how many bytes the attempt delivered first. This check stands BEFORE
//     the progress check below, so a rejected credential costs one attempt
//     rather than the whole budget, and a body whose Content-Range cannot be
//     trusted is never re-read from a later offset.
//
//  2. A failure IsTransientDataPlaneError recognizes is retried.
//
//  3. A failure nobody recognizes is retried too, but only if the attempt
//     DELIVERED BYTES before it. An unknown error that arrives without any
//     delivery is fatal on the first attempt, so a genuinely broken setup
//     (a missing destination directory, a refused connection) still fails
//     immediately and loudly.
//
// Rule 3 exists because a transport can break in a way no list can name: an
// HTTP/2 session torn down mid-body surfaces as a type net/http keeps inside
// its own unexported copy of the http2 package, unreachable to errors.As from
// any other package. Delivered bytes are the observable that does not depend
// on naming the error: a connection that carried data and then stopped is by
// construction a connection worth re-opening where it stopped.
//
// The price of rule 3 is that a fatal condition this package cannot recognize
// costs a whole budget instead of a single attempt when it happens to strike
// mid-stream. Both bounds of the policy keep that price finite and noisy.
func (r *Retrier) Resume(ctx context.Context, log *slog.Logger, subject string, attempt Attempt) error {
	var (
		lastErr    error
		attempts   int
		noProgress int
	)

	// wait.Backoff.Step() mutates its receiver, so the shared policy backoff
	// must be passed BY VALUE here (ExponentialBackoffWithContext takes it
	// by value): every concurrent caller gets its own independent copy to
	// mutate, never the shared r.policy.Backoff itself.
	backoff := r.policy.Backoff

	backoffErr := wait.ExponentialBackoffWithContext(ctx, backoff, func(stepCtx context.Context) (bool, error) {
		attempts++

		progress, err := attempt(stepCtx)

		switch {
		case err == nil:
			return true, nil
		case ctx.Err() != nil:
			// Cancellation must win over classification: an aborted request
			// can surface through the HTTP transport looking like an
			// ordinary transient failure, and must not be retried.
			return false, err
		case r.isFatal(err):
			return false, err
		case !IsTransientDataPlaneError(err) && progress.Delivered() <= 0:
			// Callers wrap the errors they return through fmt.Errorf's %w, so
			// %T on err itself reports *fmt.wrapError rather than the
			// concrete error this classifier didn't recognize. Unwrap to the
			// root cause first, so this diagnostic can actually inform a
			// future addition to IsTransientDataPlaneError's allow-list. This
			// is the only place err is logged: the caller receives it back
			// unlogged.
			log.Debug("transfer failed with a non-retryable error",
				slog.String("transfer", subject),
				slog.String("error_type", fmt.Sprintf("%T", rootCause(err))))

			return false, err
		}

		if progress.Delivered() > 0 {
			noProgress = 0
		} else {
			noProgress++
		}

		if noProgress >= r.policy.MaxNoProgress {
			return false, fmt.Errorf("%s made no progress in %d consecutive attempts: %w",
				subject, noProgress, err)
		}

		lastErr = err

		r.recovered.Add(1)

		// This line is the only externally visible trace that a transfer was
		// interrupted AND recovered: a resumed transfer is otherwise
		// indistinguishable from one that never broke, which makes "did the
		// break happen, and did the client survive it?" unanswerable from a
		// run's own output. It therefore carries, and must keep carrying:
		//   - attempt, which separates one recovered break from fifteen;
		//   - resume_offset, the durable offset the next attempt continues
		//     from, which is what shows the retry resumed rather than
		//     restarted. It is in the caller's coordinates (see Progress), so
		//     it is read together with subject, not on its own.
		// It is emitted at WARN and must stay there: the commands that
		// draw live progress bars replace their run logger with one admitting
		// WARN and above, so anything below it is dropped from exactly the
		// runs where this evidence is wanted
		// (TestRetrier_LogsRecoveredBreakWhereBarsAreLive pins the level).
		//
		// wait.ExponentialBackoffWithContext mutates its OWN copy of backoff
		// (passed by value above), so this closure has no way to observe
		// whether backoff's Cap has already forced the step budget to 0 and
		// this attempt is in fact the last one the loop will make (see the
		// defaultFetchBackoff doc comment on Cap's early-termination effect).
		// Rather than approximate that with the declared (and frequently
		// wrong) Steps budget, always log here: on a genuinely terminal
		// attempt this is the only place the failure is ever reported, since
		// the error returned once the budget is exhausted (below) is not
		// logged again anywhere in this call chain.
		log.Warn("transfer interrupted, retrying from the durable offset",
			slog.String("transfer", subject),
			slog.Int("attempt", attempts),
			slog.Int64("delivered_bytes", progress.Delivered()),
			slog.Int64("resume_offset", progress.Durable),
			slog.String("error", err.Error()))

		return false, nil
	})

	switch {
	case backoffErr == nil:
		return nil
	case ctx.Err() != nil:
		return fmt.Errorf("%s: %w", subject, ctx.Err())
	case wait.Interrupted(backoffErr) && lastErr != nil:
		// attempts, not the policy's declared Steps: Cap routinely forces the
		// backoff loop to stop one or more attempts short of Steps (see
		// defaultFetchBackoff's doc comment), so Steps would misreport how
		// many attempts actually happened.
		return fmt.Errorf("%s: exhausted %d attempts on transport failures: %w",
			subject, attempts, lastErr)
	default:
		return backoffErr
	}
}

// rootCause unwraps err through every %w wrapping layer and returns the
// deepest cause, so %T on the result reports the concrete error type instead
// of the *fmt.wrapError a caller's call site introduces.
//
// Known limitation: this only follows the single-error Unwrap() error chain.
// A multi-wrap error built with fmt.Errorf("%w: %w", ...) implements
// Unwrap() []error instead, which errors.Unwrap does not see, so rootCause
// stops at such a node. exportStatusError in this package builds exactly such
// an error, but it is fatal (ErrExportUnauthorized) and therefore never
// reaches the diagnostic this helper serves.
func rootCause(err error) error {
	for {
		unwrapped := errors.Unwrap(err)
		if unwrapped == nil {
			return err
		}

		err = unwrapped
	}
}
