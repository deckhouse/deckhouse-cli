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

package upload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"

	"github.com/deckhouse/deckhouse-cli/internal/data/dataimport/util"
	"github.com/deckhouse/deckhouse-cli/internal/dataplane"
	client "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

// This file carries NO build tag on purpose. Run is per-platform only because
// reading a file's owner is (syscall.Stat_t exists on unix and not on Windows);
// the transfer below touches nothing that differs between the two, and the
// copies it replaces differed only in whitespace and a parameter name. Keeping
// it in one file means the two platform files cannot drift apart, and — the part
// that matters more — the guards in this package exercise the code that ships on
// Windows too. A second copy behind //go:build windows would compile on every
// machine and be tested on none of them.

// errUploadIncomplete reports a transfer that ended without an error and without
// every declared byte. It should be unreachable; it exists so that a future
// change letting the loop fall out early fails loudly here instead of finalising
// a half-written destination (see uploader.run).
var errUploadIncomplete = errors.New("upload ended before every declared byte was accepted")

// uploadRetryPolicy is the retry policy every upload of this command runs under.
// It is a package variable for one reason only: the production backoff starts at
// one second, so a test exercising several breaks would spend nearly all its
// runtime asleep. Tests replace it and restore it; nothing else writes it.
//
// The policy itself is the shared data-plane one, unmodified. This command has
// no judgement of its own to add, and must not grow a second policy beside it.
var uploadRetryPolicy = dataplane.DefaultRetryPolicy()

// uploader is one file's upload: the bytes to send, where to send them, and how
// far the IMPORTER says it has got. One instance belongs to one upload call and
// its attempts run one after another in that call's goroutine, so its fields
// need no synchronization.
type uploader struct {
	client *client.SafeClient
	url    string
	log    *slog.Logger

	file      *os.File
	totalSize int64
	chunkSize int64

	permOctal string
	uid, gid  int

	// offset is the durable prefix: how many leading bytes the IMPORTER is
	// taken to hold. The rule it obeys is that it moves only on the strength of
	// something the importer said, never on the client's own count of what it
	// pushed into a connection — bytes sent into one that then died may or may
	// not have been written, and resuming from the optimistic count leaves a
	// hole inside a destination of exactly the right length.
	//
	// Every point that moves it, so that an audit of "where can this value come
	// from" has the whole list:
	//
	//   - a 2xx naming X-Next-Offset: the offset named;
	//   - a 2xx naming none: offset + the chunk just sent, which is this
	//     endpoint's long-standing meaning for that answer and the ordinary end
	//     of every filesystem upload, whose last chunk is acknowledged with 201
	//     and no header;
	//   - a 409 naming X-Expected-Offset: the offset named, backwards included;
	//   - a HEAD naming X-Next-Offset, after a break or an unexplained
	//     conflict: the offset named;
	//   - a HEAD naming none but reporting the declared size: the whole size,
	//     the importer being finished (see uploader.complete);
	//   - the value upload seeds it with, which is zero unless --resume asked
	//     the importer where a previous run left off.
	offset int64
}

// run streams the file to the importer, continuing from the importer's own
// offset whenever the transport breaks, and returns nil only once every declared
// byte has been accepted. The caller must not finalise the import unless this
// returns nil: finalising a partial upload marks the DataImport complete over
// incomplete data.
func (u *uploader) run(ctx context.Context) error {
	subject := fmt.Sprintf("upload of %d bytes", u.totalSize)

	if err := dataplane.NewRetrier(uploadRetryPolicy).Resume(ctx, u.log, subject, u.attempt); err != nil {
		return err
	}

	// A check, and a reachable one: the offset this compares is whatever the
	// importer last named, and the importer names it from the partial file it
	// holds at the destination path -- a size that belongs to whatever ran
	// there before, not to this upload. An importer still holding a longer
	// partial from a DIFFERENT file therefore names an offset past the end of
	// this one, the loop in attempt never runs a single iteration, and without
	// this the finalisation step would be told a transfer succeeded that never
	// sent a byte.
	if u.offset > u.totalSize {
		return fmt.Errorf("%w: the importer holds %d bytes at %s, more than the %d being uploaded, so it is mid-transfer of a different file: upload to a fresh destination",
			errUploadIncomplete, u.offset, u.url, u.totalSize)
	}

	if u.offset < u.totalSize {
		return fmt.Errorf("%w: %d of %d bytes at %s", errUploadIncomplete, u.offset, u.totalSize, u.url)
	}

	return nil
}

// attempt performs one try of the transfer: it sends chunk after chunk from the
// durable offset until the importer holds the whole file, and stops at the first
// failure. It never retries internally — the retry seam is Retrier.Resume.
func (u *uploader) attempt(ctx context.Context) (dataplane.Progress, error) {
	start := u.offset

	for u.offset < u.totalSize {
		if err := u.putChunk(ctx); err != nil {
			return dataplane.Progress{Start: start, Durable: u.offset}, err
		}
	}

	return dataplane.Progress{Start: start, Durable: u.offset}, nil
}

// putChunk sends one chunk beginning at the durable offset and folds whatever
// the importer answers back into that offset.
func (u *uploader) putChunk(ctx context.Context) error {
	offset := u.offset
	sendLen := min(u.chunkSize, u.totalSize-offset)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.url,
		io.NopCloser(io.NewSectionReader(u.file, offset, sendLen)))
	if err != nil {
		return err
	}

	req.Header.Set("X-Content-Length", strconv.FormatInt(u.totalSize, 10))
	req.Header.Set("X-Attribute-Permissions", u.permOctal)
	req.Header.Set("X-Attribute-Uid", strconv.Itoa(u.uid))
	req.Header.Set("X-Attribute-Gid", strconv.Itoa(u.gid))
	req.Header.Set("X-Offset", strconv.FormatInt(offset, 10))

	resp, err := u.client.HTTPDo(req)
	if err != nil {
		return u.transportFailure(ctx, offset, err)
	}

	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusConflict {
		return u.resolveConflict(ctx, resp, offset)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("server error at offset %d: status %d (%s)", offset, resp.StatusCode, resp.Status)
	}

	next, err := acceptedOffset(resp.Header.Get("X-Next-Offset"), offset, sendLen)
	if err != nil {
		return err
	}

	// An acceptance that leaves the offset where it was would spin this loop
	// forever against an importer that keeps acknowledging without accepting.
	// It is handed to the retry loop rather than failed outright because the
	// loop is what bounds it and says so out loud. It can only arise from an
	// X-Next-Offset naming the offset back: sendLen is at least one byte for
	// every chunk the loop sends, so the header-less reading always moves.
	if next == offset {
		return fmt.Errorf("%w: accepted the chunk at offset %d and still names offset %d",
			dataplane.ErrDataPlaneNotAccepted, offset, next)
	}

	u.offset = next

	return nil
}

// transportFailure turns a broken request into the honest answer to "how far did
// the importer actually get".
//
// It has to ask, and this is where uploading and downloading part company. A
// download counts the bytes its own destination accepted, so a broken body still
// leaves a measurable durable prefix behind. An upload has no such local
// measurement, and the only party that knows is the importer.
//
// Asking matters for more than the resume offset. The shared retry loop retries
// an error it cannot name only when the attempt delivered something, and an
// HTTP/2 session torn down mid-request is exactly such an error — so without
// this probe an upload break that landed nothing would be declared fatal on the
// first attempt, which is the defect this path exists to remove. An importer
// that still answers is therefore evidence in its own right, recorded as
// ErrDataPlaneNotAccepted.
//
// Limit, stated rather than papered over: when the probe ITSELF fails, this
// returns the bare transport error, and the retry loop then keeps it only if the
// error is one the classifier recognises or bytes were confirmed earlier in the
// same attempt. A far end that can neither take the bytes nor say where it
// stands is a far end worth failing against — but a break that coincides with a
// momentarily unanswerable importer is failed against too.
func (u *uploader) transportFailure(ctx context.Context, offset int64, cause error) error {
	state, probeErr := util.ProbeUploadState(ctx, u.client, u.url)
	if probeErr != nil {
		u.log.Debug("could not ask the importer how far it got",
			slog.Int64("offset", offset),
			slog.String("error", probeErr.Error()))

		return fmt.Errorf("upload chunk at offset %d: %w", offset, cause)
	}

	switch {
	case state.OffsetKnown:
		u.offset = state.Offset
	case u.complete(state):
		u.offset = u.totalSize
	}

	if u.offset >= u.totalSize {
		// The request that broke was the last one and the importer now holds
		// the whole file: its answer was lost, not its bytes.
		return nil
	}

	return fmt.Errorf("%w: upload chunk at offset %d: %w", dataplane.ErrDataPlaneNotAccepted, offset, cause)
}

// resolveConflict answers a 409 by finding out where the importer stands, rather
// than by reading the 409 itself as a verdict.
//
// A conflict WITH X-Expected-Offset is the importer stating its position, and
// the client adopts it — including backwards, since the importer is the
// authority on what it kept.
//
// A conflict WITHOUT the header is the trap this function exists for. Both
// importers omit the header while the handler of a previous, broken attempt is
// still draining its dead request body, and the file importer omits it once the
// file is already whole. The same answer therefore means "come back in a moment"
// and "you are finished", so treating it as either one without asking gets the
// other one wrong: read as completion it finalises an import over a
// half-written destination, read as failure it fails a transfer that arrived.
// It is asked about instead, and an importer that will not resolve the question
// buys a pause and another attempt rather than a verdict in either direction.
func (u *uploader) resolveConflict(ctx context.Context, resp *http.Response, offset int64) error {
	if expected := resp.Header.Get("X-Expected-Offset"); expected != "" {
		next, err := strconv.ParseInt(expected, 10, 64)
		if err != nil || next < 0 {
			return fmt.Errorf("invalid X-Expected-Offset at offset %d: %q", offset, expected)
		}

		if next == offset {
			return fmt.Errorf("%w: refused the chunk at offset %d and named the same offset back",
				dataplane.ErrDataPlaneNotAccepted, offset)
		}

		u.offset = next

		return nil
	}

	state, err := util.ProbeUploadState(ctx, u.client, u.url)
	if err != nil {
		return fmt.Errorf("%w: refused the chunk at offset %d without naming one, and could not be asked: %w",
			dataplane.ErrDataPlaneNotAccepted, offset, err)
	}

	switch {
	case state.OffsetKnown && state.Offset != offset:
		u.offset = state.Offset

		return nil
	case u.complete(state):
		u.offset = u.totalSize

		return nil
	default:
		return fmt.Errorf("%w: refused the chunk at offset %d without naming one",
			dataplane.ErrDataPlaneNotAccepted, offset)
	}
}

// complete reports whether a HEAD answer means the importer holds every declared
// byte.
//
// An answer that NAMES a resume offset is never read here as a finished
// transfer, and that settles the question whatever the rest of the answer looks
// like. For the filesystem importer that is the importer saying so: it names an
// offset only while a partial upload of the destination is still on disk. For
// the block importer the offset is always named, so this exit is simply always
// taken, which the paragraph below relies on.
//
// The question therefore only ever arises for an answer naming none, which the
// filesystem importer sends when the destination file is there and no partial
// upload of it is. That is precisely a finished transfer: the destination file
// comes into being only by the importer renaming the partial one onto it, which
// it does on the write that brings the total to the declared size.
//
// A destination the importer has never seen names no offset either, and is
// excluded by the size test instead: it is reported with a size of -1, which no
// declared upload size can equal.
//
// Nothing here is keyed on WHICH request met the refusal, and it must not be: an
// importer holding a finished file refuses at whatever offset the client sent,
// deciding that the file is already whole before it looks at the offset at all.
// A fresh run therefore meets this on the first of its ten default chunks, which
// is the ordinary way the last lost acknowledgement of a previous run gets
// resolved.
//
// A block destination cannot be misread by the size test below, for two reasons
// that hold independently. Its HEAD always names an offset, finished or not, so
// it never gets past the early exit above; and it reports the device's size in a
// header of its own rather than in Content-Length, so Size is not the device
// size and the comparison could not match even if it did get here. That is what
// makes a bare size comparison safe despite a block device reporting its full
// size from creation however little has been written into it.
//
// Limit, stated rather than papered over: this trusts the importer's own notion
// of a finished destination, which is that the size matches what was declared,
// not that the contents do. A destination file that already existed at exactly
// the declared size is read as finished — but the importer reads it that way
// too, refusing to write over it for the same reason, so the disagreement this
// could cause is not one between client and importer.
func (u *uploader) complete(state util.UploadState) bool {
	if state.OffsetKnown {
		return false
	}

	return state.Size == u.totalSize
}

// acceptedOffset reads the offset an accepted chunk leaves behind. An importer
// that names none is taken at its word that the whole chunk landed, which is
// what a 2xx without the header has always meant on this endpoint.
func acceptedOffset(header string, offset, sendLen int64) (int64, error) {
	if header == "" {
		return offset + sendLen, nil
	}

	next, err := strconv.ParseInt(header, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid X-Next-Offset: %s: %w", header, err)
	}

	if next < offset {
		return 0, fmt.Errorf("server returned X-Next-Offset (%d) smaller than current offset (%d)", next, offset)
	}

	return next, nil
}

// upload streams filePath to url, surviving a transport break by continuing from
// the offset the importer confirms rather than from the client's own idea of
// where it got to.
//
// resume asks the importer for a starting offset before the first chunk, which
// is how a transfer begun by a PREVIOUS run is picked up. It has nothing to do
// with surviving a break within THIS run: that happens whether or not the flag
// was given.
func upload(
	ctx context.Context,
	log *slog.Logger,
	httpClient *client.SafeClient,
	url string,
	filePath string,
	chunks int,
	permOctal string,
	uid, gid int,
	resume bool,
) error {
	if log == nil {
		log = slog.Default()
	}

	var offset int64

	if resume {
		off, err := util.CheckUploadProgress(ctx, httpClient, url)
		if err != nil {
			return err
		}

		offset = off
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return err
	}

	totalSize := fi.Size()
	if totalSize < 0 {
		return fmt.Errorf("invalid file size")
	}

	chunkSize := totalSize / int64(chunks)
	if totalSize%int64(chunks) != 0 {
		chunkSize++
	}

	u := &uploader{
		client:    httpClient,
		url:       url,
		log:       log,
		file:      file,
		totalSize: totalSize,
		chunkSize: chunkSize,
		permOctal: permOctal,
		uid:       uid,
		gid:       gid,
		offset:    offset,
	}

	return u.run(ctx)
}
