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

	"github.com/deckhouse/deckhouse-cli/internal/dataplane"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

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

// chunkRetrier adapts the shared data-plane retry policy to one volume's
// chunk downloads: the policy owns when to try again and when to give up,
// this type owns what one attempt IS (a Range GET into a durable ".part"
// file) and how that attempt's overlapping progress credits reach the shared
// progress sink. One instance per downloadBlockChunks call, shared by every
// chunk goroutine.
type chunkRetrier struct {
	*dataplane.Retrier
}

// newChunkRetrier builds a chunk retrier over the given policy. Production
// passes dataplane.DefaultRetryPolicy(); tests pass a millisecond-scale one.
//
// It extends the shared fatal set with ErrShortChunkRead, and that is the one
// judgement this layer adds to the policy. ErrShortChunkRead means the body
// ended in a CLEAN EOF short of the range its own Content-Range promised —
// that is, the response framing itself declared the body complete while the
// header promised more. A link breaking mid-body does not produce that: Go's
// transport reports a truncated Content-Length or chunked body as
// io.ErrUnexpectedEOF, and a torn-down HTTP/2 stream as an http2 error, both
// of which the shared policy already handles. What does produce it is a
// server or proxy whose framing disagrees with its own header, and re-asking
// gets the same disagreement back.
func newChunkRetrier(policy dataplane.RetryPolicy) *chunkRetrier {
	// Extend, never replace: assigning here would silently drop whatever the
	// caller had already named, and a fatal error quietly demoted to a
	// retryable one costs a whole budget before it surfaces.
	inherited := policy.Fatal
	policy.Fatal = func(err error) bool {
		return errors.Is(err, ErrShortChunkRead) || (inherited != nil && inherited(err))
	}

	return &chunkRetrier{Retrier: dataplane.NewRetrier(policy)}
}

// fetchChunk retries fetchChunkRaw under the shared data-plane policy,
// resuming each attempt from the durable offset the previous attempt
// persisted, until the chunk completes, a fatal error occurs, ctx is
// cancelled, or the retry budget is exhausted.
//
// This is the only retry seam for a chunk's raw download: fetchChunkRaw
// itself stays a single attempt (the resume contract several tests pin down),
// and downloadBlockChunks' errgroup is not re-entered per attempt (which
// would re-run finalizeChunkFrame/ensureChunkGeometry unnecessarily).
func (r *chunkRetrier) fetchChunk(
	ctx context.Context,
	destination *archive.RootedDestination,
	log *slog.Logger,
	fetcher *dataplane.Fetcher,
	blockURL string,
	partPath string,
	chunkIdx int,
	startByte, endByte, rawLen int64,
	onProgress func(n int),
) error {
	ledger := &chunkProgressLedger{onProgress: onProgress}

	return r.Resume(ctx, log, fmt.Sprintf("chunk %d", chunkIdx),
		func(stepCtx context.Context) (dataplane.Progress, error) {
			ledger.beginAttempt()

			return fetchChunkRaw(stepCtx, destination, log, fetcher, blockURL, partPath,
				chunkIdx, startByte, endByte, rawLen, ledger.credit)
		})
}
