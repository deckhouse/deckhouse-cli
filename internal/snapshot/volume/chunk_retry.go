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
	"log/slog"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/exporter"
)

// chunkFetchBackoff bounds one chunk's in-run retries: starting at 1s and
// doubling, capped at 30s. wait.Backoff's Cap does double duty as both a
// ceiling on any single sleep AND an early-termination trigger — once a
// projected next delay would exceed Cap, the step budget is forced to 0
// immediately, ending the loop one attempt sooner than Steps alone would
// suggest. With these parameters that yields 5 attempts in the worst case
// (not 6), for a total backoff of ~30-34s added to a chunk that eventually
// succeeds — negligible against the 650-790s a single 256 MiB chunk stream
// already takes on a WAN link — while a link that keeps breaking still fails
// loudly instead of grinding for hours.
var chunkFetchBackoff = wait.Backoff{
	Steps:    6,
	Duration: 1 * time.Second,
	Factor:   2.0,
	Jitter:   0.2,
	Cap:      30 * time.Second,
}

// maxNoProgressChunkAttempts bounds attempts that re-issue the Range GET and
// advance the durable offset by zero bytes. A server that accepts the range
// and immediately closes would otherwise burn the whole backoff budget in a
// hot loop; three such attempts is proof the far end is not going to deliver.
const maxNoProgressChunkAttempts = 3

// chunkRetryPolicy bounds one chunk's retry loop: chunkRetrier.fetchChunk
// stops re-issuing the Range GET once either backoff's step budget or
// maxNoProgress consecutive zero-progress attempts is exhausted.
type chunkRetryPolicy struct {
	backoff       wait.Backoff
	maxNoProgress int
}

// defaultChunkRetryPolicy returns the production retry policy applied to
// every chunk download.
func defaultChunkRetryPolicy() chunkRetryPolicy {
	return chunkRetryPolicy{
		backoff:       chunkFetchBackoff,
		maxNoProgress: maxNoProgressChunkAttempts,
	}
}

// chunkRetrier carries one volume's retry policy plus the aggregate count of
// retries it absorbed. One instance per downloadBlockChunks call, shared by
// every chunk goroutine; only recovered is mutated concurrently, and it is
// atomic for that reason.
type chunkRetrier struct {
	policy    chunkRetryPolicy
	recovered atomic.Int64
}

// chunkProgressLedger converts fetchChunkRaw's per-attempt credits into
// strictly monotonic, never-duplicated credits for the shared onProgress
// sink. Each fetchChunkRaw attempt credits (resume prefix) + (bytes streamed
// this attempt) — i.e. the credits inside one attempt sum to an absolute
// position within the chunk. Across attempts those positions OVERLAP: attempt
// 2 re-credits the prefix attempt 1 already streamed. The ledger forwards only
// the amount by which the attempt position exceeds the highest position ever
// forwarded, so the credits it emits sum to exactly rawLen no matter how many
// attempts the chunk took.
//
// Not synchronised: one ledger belongs to one chunk, and one chunk is one
// goroutine (downloadChunk). The onProgress it forwards to is the shared,
// already-concurrency-safe sink.
type chunkProgressLedger struct {
	onProgress func(n int)
	attemptPos int64
	highWater  int64
}

// beginAttempt resets the per-attempt position counter before a new
// fetchChunkRaw attempt starts crediting from zero again.
func (l *chunkProgressLedger) beginAttempt() {
	l.attemptPos = 0
}

// credit records n additional bytes reported by the current attempt (a resume
// credit or a streamed-bytes credit) and forwards to onProgress only the
// amount by which the attempt's cumulative position exceeds every credit
// already forwarded, so a retried attempt never double-counts bytes an
// earlier attempt already reported.
func (l *chunkProgressLedger) credit(n int) {
	if n <= 0 {
		return
	}

	l.attemptPos += int64(n)

	if l.attemptPos <= l.highWater {
		return
	}

	advance := l.attemptPos - l.highWater
	l.highWater = l.attemptPos

	if l.onProgress != nil {
		l.onProgress(int(advance))
	}
}

// fetchChunk retries fetchChunkRaw with bounded exponential backoff,
// resuming each attempt from the durable offset the previous attempt
// persisted, until the chunk completes, a fatal (non-transient) error
// occurs, ctx is cancelled, or the retry budget is exhausted.
//
// This is the only retry seam for a chunk's raw download: fetchChunkRaw
// itself stays a single attempt (the resume contract several tests pin down),
// and downloadBlockChunks' errgroup is not re-entered per attempt (which
// would re-run finalizeChunkFrame/ensureChunkGeometry unnecessarily).
func (r *chunkRetrier) fetchChunk(
	ctx context.Context,
	destination *archive.RootedDestination,
	log *slog.Logger,
	fetcher *exporter.Fetcher,
	blockURL string,
	partPath string,
	chunkIdx int,
	startByte, endByte, rawLen int64,
	onProgress func(n int),
) error {
	ledger := &chunkProgressLedger{onProgress: onProgress}

	var (
		lastErr     error
		attempt     int
		noProgress  int
		lastDurable = int64(-1)
	)

	// wait.Backoff.Step() mutates its receiver, so the shared policy backoff
	// must be passed BY VALUE here (ExponentialBackoffWithContext takes it
	// by value): every concurrent chunk goroutine gets its own independent
	// copy to mutate, never the shared r.policy.backoff itself.
	backoff := r.policy.backoff

	backoffErr := wait.ExponentialBackoffWithContext(ctx, backoff, func(stepCtx context.Context) (bool, error) {
		attempt++

		ledger.beginAttempt()

		durable, err := fetchChunkRaw(stepCtx, destination, log, fetcher, blockURL, partPath,
			chunkIdx, startByte, endByte, rawLen, ledger.credit)

		switch {
		case err == nil:
			return true, nil
		case ctx.Err() != nil:
			// Cancellation must win over classification: an aborted request
			// can surface through the HTTP transport looking like an
			// ordinary transient failure, and must not be retried.
			return false, err
		case !exporter.IsTransientDataPlaneError(err):
			// fetchChunkRaw wraps every error it returns through fmt.Errorf's
			// %w, so %T on err itself always reports *fmt.wrapError and never
			// the concrete error this classifier didn't recognize. Unwrap to
			// the root cause first so this diagnostic can actually inform a
			// future addition to exporter.IsTransientDataPlaneError's
			// allow-list. This is the only place err is logged: the caller
			// receives it back unlogged.
			log.Debug("chunk fetch failed with a non-retryable error",
				slog.Int("chunk", chunkIdx),
				slog.String("error_type", fmt.Sprintf("%T", rootCause(err))))

			return false, err
		}

		if durable > lastDurable {
			noProgress = 0
		} else {
			noProgress++
		}

		lastDurable = durable

		if noProgress >= r.policy.maxNoProgress {
			return false, fmt.Errorf("chunk %d made no progress in %d consecutive attempts: %w",
				chunkIdx, noProgress, err)
		}

		lastErr = err

		r.recovered.Add(1)

		// wait.ExponentialBackoffWithContext mutates its OWN copy of backoff
		// (passed by value above), so this closure has no way to observe
		// whether backoff's Cap has already forced the step budget to 0 and
		// this attempt is in fact the last one the loop will make (see the
		// chunkFetchBackoff doc comment on Cap's early-termination effect).
		// Rather than approximate that with the declared (and frequently
		// wrong) Steps budget, always log here: on a genuinely terminal
		// attempt this is the only place the failure is ever reported, since
		// the error returned once the budget is exhausted (below) is not
		// logged again anywhere in this call chain.
		log.Warn("chunk transfer interrupted by a transient transport failure",
			slog.Int("chunk", chunkIdx),
			slog.Int("attempt", attempt),
			slog.String("error", err.Error()))

		return false, nil
	})

	switch {
	case backoffErr == nil:
		return nil
	case ctx.Err() != nil:
		return fmt.Errorf("chunk %d: %w", chunkIdx, ctx.Err())
	case wait.Interrupted(backoffErr) && lastErr != nil:
		// attempt, not the policy's declared Steps: Cap routinely forces the
		// backoff loop to stop one or more attempts short of Steps (see
		// chunkFetchBackoff's doc comment), so Steps would misreport how many
		// attempts actually happened.
		return fmt.Errorf("chunk %d: exhausted %d attempts on transient transport failures: %w",
			chunkIdx, attempt, lastErr)
	default:
		return backoffErr
	}
}

// rootCause unwraps err through every %w wrapping layer and returns the
// deepest cause, so %T on the result reports the concrete error type instead
// of the *fmt.wrapError every fetchChunkRaw call site introduces.
//
// Known limitation: this only follows the single-error Unwrap() error chain.
// A multi-wrap error built with fmt.Errorf("%w: %w", ...) implements
// Unwrap() []error instead, which errors.Unwrap does not see, so rootCause
// stops at such a node. No call site in this package produces multi-wrap
// errors today, so this is not tightened further here.
func rootCause(err error) error {
	for {
		unwrapped := errors.Unwrap(err)
		if unwrapped == nil {
			return err
		}

		err = unwrapped
	}
}
