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

package snapimport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/deckhouse/deckhouse-cli/internal/dataplane"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// This file holds everything this package does about a transfer that BREAKS:
// the policy the retries run under, the sentinels that say what kind of failure
// a PUT met, and the two transfers that turn "the request failed" into "here is
// where the importer stands, try again from there".
//
// The three PUT loops it serves — raw block, compressed block, filesystem entry
// — differ in how a request body is produced and in what their importer says on
// a HEAD, and nothing here tries to hide that. What is shared, and has to be, is
// the retry policy: one policy for every transfer this CLI makes, living in
// internal/dataplane, consumed here and by the download side alike.

// errUploadTransportFailure marks a PUT whose HTTP round trip did not complete
// — the link broke, or the response could not be read — as opposed to one the
// importer answered and refused, and as opposed to one this client could not
// feed (see requestBodyReport.sourceEndedShort, which keeps a request body that
// ran dry out of this category).
//
// It is one of the two failures that send the transfers below to ask the
// importer where it stands — errConflictOffsetUnknown is the other. That
// question is only worth asking when the answer can be trusted to be about the
// transport: an importer that answered "no" already told us everything a probe
// would.
var errUploadTransportFailure = errors.New("upload request did not complete")

// errConflictOffsetUnknown marks a 409 that named no X-Expected-Offset.
//
// Both importers send that answer in more than one situation, and the
// situations do not agree with each other, so the answer on its own is not a
// verdict — it is a question. doBlockChunk and doFileChunk therefore refuse to
// interpret it and hand it up marked, and the transfers below resolve it by
// asking. What the answer can mean, per importer, is written out at
// blockUploadTransfer.ask and fileUploadTransfer.ask.
var errConflictOffsetUnknown = errors.New("importer refused the chunk without naming an offset")

// errConflictBudgetExhausted marks the failure of a conflict tracker: the
// importer kept directing this client to offsets it had already been sent to,
// or demanded more replay than a transfer is allowed to spend obeying it.
//
// It is one of the judgements this package adds to the shared policy's fatal
// set (resumeUpload holds the whole list), and it is added for the reason
// RetryPolicy.Fatal exists: a server-directed
// offset cycle is a disagreement, not a broken link, and asking again gets the
// same disagreement back. Without it the failure would be an error the
// classifier cannot name, arriving after an attempt that HAD moved the offset
// around — which the progress rule reads as worth another try, so a cycle would
// cost the whole retry budget and then be reported as exhaustion rather than as
// the cycle it was. The trackers are also per-attempt, so each retry would hand
// the importer a fresh allowance of conflicts to spend.
var errConflictBudgetExhausted = errors.New("importer directed too many upload repositions")

// errUploadIncomplete reports a transfer that ended without an error and
// without every declared byte. It should be unreachable; it exists so that a
// future change letting a PUT loop fall out early fails loudly here instead of
// handing a half-written destination to the finalisation step, which has
// nothing of its own to check it against.
var errUploadIncomplete = errors.New("upload ended before every declared byte was accepted")

// resumeUpload runs one payload's upload under the shared policy. Every retry
// in this package goes through here, so there is exactly one place where the
// policy is chosen and exactly one place a future second policy would have to
// be added to be noticed.
//
// The policy is dataplane.DefaultRetryPolicy — the same one the download side
// runs on, taken whole and not copied — extended with the failures this package
// can identify as being about something other than the link. Those extensions
// only ADD to the shared fatal set; RetryPolicy.Fatal cannot subtract from it.
//
// It is built per call rather than kept in a package variable so there is no
// knob a test could reach in and turn, quietly weakening what production runs
// under. Concurrency is not the reason — Retrier.Resume already gives every
// caller its own copy of the backoff to spend.
func resumeUpload(ctx context.Context, log *slog.Logger, subject string, attempt dataplane.Attempt) error {
	if log == nil {
		log = slog.Default()
	}

	policy := dataplane.DefaultRetryPolicy()

	policy.Fatal = func(err error) bool {
		// Three failures this package can name as being about something other
		// than the link, each for the same reason: another attempt gets the
		// identical answer. A server-directed offset cycle is the importer
		// disagreeing with itself; an archive that changed on disk is re-read
		// just as changed; a refused identity is refused again.
		return errors.Is(err, errConflictBudgetExhausted) ||
			errors.Is(err, archive.ErrVerifiedArchiveChanged) ||
			errors.Is(err, errUploadUnauthorized)
	}

	return dataplane.NewRetrier(policy).Resume(ctx, log, subject, attempt)
}

// worthAsking reports whether err is one whose meaning has to be settled by
// asking the importer where it stands, rather than read off the failure itself.
func worthAsking(err error) bool {
	return errors.Is(err, errUploadTransportFailure) || errors.Is(err, errConflictOffsetUnknown)
}

// blockUploadTransfer is one block volume's upload across attempts: where to
// send it, what to send, and how far the IMPORTER says it has got.
//
// One instance belongs to one UploadVolumeData call and its attempts run one
// after another in that call's goroutine, so its fields need no
// synchronization. Volumes uploaded in parallel each get their own instance —
// they share nothing mutable, which is what keeps a break on one volume from
// disturbing another.
type blockUploadTransfer struct {
	httpClient   httpDoer
	url          string
	dataFile     string
	ext          string
	totalSize    int64
	payloadLimit int64
	source       blockArchiveSource
	log          *slog.Logger
	progress     *blockUploadProgress
	activate     func()
	deps         blockDecodeDependencies

	// offset is the durable prefix: how many leading bytes the IMPORTER is
	// taken to hold. It moves only on the strength of something the importer
	// said, never on this client's own count of what it pushed into a
	// connection — bytes written into one that then died may or may not have
	// reached the device, and resuming from the optimistic count leaves a hole
	// inside a destination of exactly the right length, which no length check
	// would find.
	//
	// Every point that moves it, so an audit of "where can this value come
	// from" has the whole list:
	//
	//   - a 2xx PUT naming X-Next-Offset: the offset named, which doBlockChunk
	//     has already refused unless it equals the request's exact end;
	//   - a 409 naming X-Expected-Offset: the offset named, backwards included;
	//   - a HEAD naming X-Next-Offset, after a break or an unexplained
	//     conflict: the offset named;
	//   - the HEAD that opens the transfer, before the first attempt.
	offset int64
}

// run streams the volume to the importer, continuing from the importer's own
// offset whenever the transport breaks, and returns nil only once every
// declared byte has been accepted. The caller must not finalise the import
// unless this returns nil: POST .../finished over a partial upload marks the
// DataImport complete with incomplete data.
func (t *blockUploadTransfer) run(ctx context.Context) error {
	subject := fmt.Sprintf("block upload of %s (%d bytes)", t.dataFile, t.totalSize)

	if err := resumeUpload(ctx, t.log, subject, t.attempt); err != nil {
		return err
	}

	// A tripwire, not a check, and it is worth being exact about which. The
	// loops in putBlockFromOffset cannot exit early without an error, so
	// nothing an importer does reaches this line, and removing it on its own
	// turns no test red. What it is here for is the shape of the bug it would
	// catch: a future change that lets a loop fall out early hands a
	// half-written device to finalisation.
	if t.offset < t.totalSize {
		return fmt.Errorf("%w: %d of %d bytes at %s", errUploadIncomplete, t.offset, t.totalSize, t.url)
	}

	return nil
}

// attempt performs one try of the transfer: it sends chunk after chunk from the
// durable offset until the importer holds the whole volume, and stops at the
// first failure. It never retries internally — the retry seam is Resume.
func (t *blockUploadTransfer) attempt(ctx context.Context) (dataplane.Progress, error) {
	start := t.offset

	reached, err := putBlockFromOffset(ctx, t.httpClient, t.url, t.dataFile, t.ext,
		t.totalSize, t.offset, t.payloadLimit, t.source, t.log, t.progress, t.activate, t.deps)

	t.offset = reached

	if err != nil && worthAsking(err) {
		err = t.ask(ctx, err)
	}

	return dataplane.Progress{Start: start, Durable: t.offset}, err
}

// ask answers a broken or unexplained request by finding out where the importer
// stands, rather than by reading the failure itself as a verdict.
//
// FOR THE BLOCK IMPORTER A CONFLICT WITHOUT AN OFFSET NEVER MEANS "DONE", and
// the temptation to read it that way is what this comment exists against. That
// importer names X-Expected-Offset on every disagreement it is in a position to
// describe — including the one where the device is already full, where the
// offset it names is the device's whole size. It omits the header in exactly
// one case: a previous request's handler is still draining its dead body and
// this one was refused before anything was compared. Reading that as completion
// would send finalisation at a device that is still short. (The filesystem
// importer does have a completion case for the same answer; see
// fileUploadTransfer.ask, and do not carry either reading across.)
//
// So there is nothing to read, and the client asks instead. Whenever the block
// HEAD answers at all it reports the importer's write offset — unconditionally,
// full device or empty one — which settles both questions at once: where to
// resume, and — since an importer that answers is an importer
// that is there — whether the failure was the transport. That second part is
// what makes this probe necessary rather than merely useful: the shared retry
// loop retries an error it cannot name only when the attempt delivered
// something, and an HTTP/2 session torn down mid-request is exactly such an
// error, arriving as a type net/http keeps inside its own copy of the http2
// package. A break that landed nothing would otherwise be fatal on the first
// attempt.
//
// A full device is NOT short-circuited to success here, and that is deliberate:
// returning the failure sends the loop round once more, and the next attempt
// enters putBlockFromOffset at offset == totalSize. On a COMPRESSED payload that
// is where the archive's end-of-stream verification lives — the check that its
// declared size matches what it actually decodes to — and skipping it would mean
// finalising without it. On a raw payload there is nothing there to run and the
// extra attempt only confirms the device; it is not worth a second code path to
// avoid.
//
// The price either way is one backoff, plus — if the break landed on the last
// attempt the budget allowed — a transfer that did arrive being reported as
// failed.
//
// Limit, stated rather than papered over: when the probe ITSELF fails, this
// returns the bare failure, and the retry loop then keeps it only if the
// classifier recognises it or bytes were confirmed earlier in the same attempt.
// An importer that can neither take the bytes nor say where it stands is one
// worth failing against — but a break that coincides with a momentarily
// unanswerable importer is failed against too.
func (t *blockUploadTransfer) ask(ctx context.Context, cause error) error {
	offset, probeErr := headBlockOffset(ctx, t.httpClient, t.url, t.totalSize)
	if probeErr != nil {
		t.log.Debug("could not ask the importer how far it got",
			slog.String("url", t.url),
			slog.String("error", probeErr.Error()))

		return cause
	}

	t.offset = offset
	t.progress.creditTo(offset)

	return fmt.Errorf("%w: %w", dataplane.ErrDataPlaneNotAccepted, cause)
}

// fileUploadTransfer is one filesystem entry's upload across attempts. Its
// offset obeys the same rule as blockUploadTransfer.offset — it moves only on
// something the importer said — with one importer answer more: a HEAD that
// names no offset while reporting the declared size means the entry is whole.
//
// One instance belongs to one entry, and the entries of one tar are uploaded in
// sequence, so its fields need no synchronization.
type fileUploadTransfer struct {
	client    httpDoer
	baseURL   string
	fileURL   string
	relPath   string
	totalSize int64
	// payloadLimit caps one request's body, exactly as blockUploadTransfer's does.
	// It is a field rather than the constant putFile used to reach for directly so
	// that a guard can put several chunks into a test-sized entry — which is what a
	// transfer's offset bookkeeping has to be watched across.
	payloadLimit int64
	attrs        fileAttrs
	newBody      fileBodyFactory
	progress     *fileUploadProgress
	activate     func()
	log          *slog.Logger

	offset int64
}

// run uploads the entry, continuing from the importer's own offset whenever the
// transport breaks, and returns nil only once the importer holds every declared
// byte of it.
func (t *fileUploadTransfer) run(ctx context.Context) error {
	subject := fmt.Sprintf("file upload of %s (%d bytes)", t.relPath, t.totalSize)

	if err := resumeUpload(ctx, t.log, subject, t.attempt); err != nil {
		return err
	}

	// The same tripwire as blockUploadTransfer.run, for the same reason: putFile
	// cannot return nil short of the declared size today, and this catches the
	// future change that lets it.
	if t.offset < t.totalSize {
		return fmt.Errorf("%w: %d of %d bytes at %s", errUploadIncomplete, t.offset, t.totalSize, t.fileURL)
	}

	return nil
}

func (t *fileUploadTransfer) attempt(ctx context.Context) (dataplane.Progress, error) {
	start := t.offset

	reached, err := putFile(ctx, t.client, t.baseURL, t.relPath, t.totalSize, t.offset,
		t.payloadLimit, t.attrs, t.newBody, t.progress, t.activate)

	t.offset = reached

	if err != nil && worthAsking(err) {
		err = t.ask(ctx, err)
	}

	return dataplane.Progress{Start: start, Durable: t.offset}, err
}

// ask resolves a broken or unexplained request against the filesystem
// importer's HEAD.
//
// THIS IMPORTER'S CONFLICT WITHOUT AN OFFSET HAS TWO MEANINGS, and they are
// opposite ones: the handler of a previous, broken attempt is still draining
// its dead body, or the destination file is already there at the declared size.
// Read as failure, the second rejects a transfer that arrived; read as
// completion, the first finalises over a half-written destination. The client
// therefore asks rather than guesses, and the answers below are the whole of
// what it will act on.
//
// A HEAD that NAMES an offset is a partial upload still on disk — never a
// finished one, whatever else the answer carries. A HEAD that names none while
// reporting the declared size is a finished one: this importer's destination
// file comes into being only by renaming the partial file onto it, which it
// does on the write that brings the total to the declared size. A HEAD that
// names none and reports some OTHER size is neither, and is refused: a
// destination of the wrong size is not this transfer's destination, and
// accepting it would finalise over it.
//
// A fourth answer, an importer that has never seen this file at all, reaches
// the code below as the first of those three with an offset of zero — which is
// the right handling and not a coincidence: nothing on disk and a partial
// upload of nothing are the same instruction, "send it from the start".
//
// Nothing here is keyed on WHICH request met the refusal, and it must not be:
// this importer decides a destination is complete before it looks at the offset
// at all, so a run that begins against an already-complete file meets the
// answer on its first chunk, not on its last.
//
// The limit at blockUploadTransfer.ask — a probe that itself fails leaves the
// bare failure to the classifier — holds here word for word.
func (t *fileUploadTransfer) ask(ctx context.Context, cause error) error {
	offset, done, size, probeErr := headFileOffset(ctx, t.client, t.fileURL, t.totalSize)
	if probeErr != nil {
		t.log.Debug("could not ask the importer how far it got",
			slog.String("path", t.relPath),
			slog.String("error", probeErr.Error()))

		return cause
	}

	if !done {
		t.offset = offset
		t.progress.creditTo(offset)

		return fmt.Errorf("%w: %w", dataplane.ErrDataPlaneNotAccepted, cause)
	}

	if size != t.totalSize {
		return fmt.Errorf("importer holds %s at %d bytes, not the declared %d: %w",
			t.relPath, size, t.totalSize, cause)
	}

	t.offset = t.totalSize
	t.progress.creditTo(t.totalSize)

	return nil
}
