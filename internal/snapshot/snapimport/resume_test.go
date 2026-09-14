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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// These tests run under the PRODUCTION retry policy, which is the only policy
// this package has — see resumeUpload, which builds it per call precisely so
// that nothing can swap in a weaker one. The practical consequence is that a
// retry costs a real backoff (about a second for the first, doubling after),
// so every guard here is written to need as few attempts as it can: one break
// where one break is the point, three no-progress attempts where the ceiling is
// the point. A guard that needed the whole step budget would cost fifteen
// seconds and prove nothing the policy's own tests in internal/dataplane do not
// already prove.

// capturedLog is a logger plus a reader of what it has written, admitting WARN
// and above and NOTHING BELOW IT.
//
// That level is not an arbitrary choice, and a guard built on this helper is
// only as good as that choice: `d8 snapshot upload` replaces its run logger with
// exactly this level while its progress bars are live, so anything the retry
// loop emitted below WARN would be dropped from precisely the runs where the
// evidence is wanted. A capture at Debug would keep passing after such a
// regression.
type capturedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capturedLog) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.buf.Write(p)
}

func (c *capturedLog) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.buf.String()
}

func (c *capturedLog) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(c, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// assertRecoveredBreakLogged fails unless the captured output carries the one
// line that tells an interrupted-and-recovered transfer apart from one nothing
// ever interrupted, with both of the fields that make it usable: the attempt
// number, which separates one recovered break from fifteen, and the offset the
// next attempt resumed from, which is what shows the retry RESUMED rather than
// restarted.
//
// Limit worth naming: this reads one line out of a run's output, so it proves
// the evidence is produced and visible at WARN. It says nothing about what the
// command does with its logger elsewhere.
func assertRecoveredBreakLogged(t *testing.T, output string, wantOffset int64) {
	t.Helper()

	if !strings.Contains(output, "transfer interrupted") {
		t.Errorf("no retry line in the command's output at WARN:\n%s", output)

		return
	}

	if !strings.Contains(output, "attempt=1") {
		t.Errorf("retry line carries no attempt number:\n%s", output)
	}

	if want := fmt.Sprintf("resume_offset=%d", wantOffset); !strings.Contains(output, want) {
		t.Errorf("retry line does not resume from %d (want %q):\n%s", wantOffset, want, output)
	}
}

// completeFileHeadResponse builds the HEAD answer the filesystem importer gives for a
// destination that is already whole: no X-Next-Offset, and the file's size.
//
// The size travels in Response.ContentLength, which is what headFileOffset reads. A real
// response gets that field filled in by net/http's parser from the Content-Length header;
// a hand-built one does not, so both are set here and a test that set only the header
// would silently be asserting against a size of zero.
func completeFileHeadResponse(size int64) *http.Response {
	header := http.Header{}
	header.Set("Content-Length", strconv.FormatInt(size, 10))

	resp := newTestHTTPResponse(http.StatusOK, header)
	resp.ContentLength = size

	return resp
}

// blockLeafFor builds the minimal PlannedNode a raw block upload needs: no recorded
// sizes, so resolveBlockPayloadSize measures the file on disk and no cross-check fires.
func blockLeafFor(t *testing.T, payload []byte) PlannedNode {
	t.Helper()

	dataFile := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(dataFile, payload, 0o600); err != nil {
		t.Fatalf("write data.bin: %v", err)
	}

	return PlannedNode{DataFile: dataFile}
}

// TestBlockUpload_ConflictWithoutOffsetIsNotReadAsComplete is the guard against the
// single most expensive way to misread this protocol.
//
// A 409 carrying no X-Expected-Offset is, for the BLOCK importer, only ever "a previous
// request's handler is still draining its dead body" — that importer names the offset on
// every disagreement it can describe, the full device included, where it names the
// device's whole size. A client that instead read the bare conflict as "the volume is
// already there" would send POST .../finished at a device still missing most of its
// bytes, and the import would be marked complete over them. Nothing downstream checks
// that: finalisation has no measurement of its own to compare against.
//
// The importer here keeps answering exactly that way, so the client can never make
// progress, and three things have to hold at once — each asserted separately so that a
// regression says which one broke:
//
//   - the transfer fails rather than succeeding;
//   - no finalisation is sent;
//   - the failure names the volume, which is what makes it actionable when volumes are
//     uploading in parallel.
//
// Cost note: reaching the no-progress ceiling means three attempts under the production
// backoff, about three seconds. That ceiling IS the property — a client that asked
// forever would hang a run rather than fail it — so it is paid rather than avoided.
func TestBlockUpload_ConflictWithoutOffsetIsNotReadAsComplete(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("never-declare-this-done-"), 100)
	leaf := blockLeafFor(t, payload)

	var (
		mu        sync.Mutex
		puts      int
		heads     int
		finished  int
		totalSize = int64(len(payload))
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()

		switch req.Method {
		case http.MethodHead:
			heads++

			header := http.Header{}
			// The device is barely started: whatever the conflict might be taken
			// to mean, the importer's own answer says it holds nothing.
			header.Set("X-Next-Offset", "0")

			return newTestHTTPResponse(http.StatusOK, header), nil
		case http.MethodPut:
			puts++

			// A conflict with no offset named: the shape the block importer sends
			// while an earlier request's handler is still draining.
			return newTestHTTPResponse(http.StatusConflict, http.Header{}), nil
		case http.MethodPost:
			finished++

			return newTestHTTPResponse(http.StatusNoContent, http.Header{}), nil
		default:
			return newTestHTTPResponse(http.StatusMethodNotAllowed, http.Header{}), nil
		}
	})

	importer := &clusterVolumeImporter{log: discardLogger()}

	err := importer.sendVolumeData(context.Background(), doer, "https://importer.test",
		volumeModeBlock, leaf, "namespace", "data-import", nil, nil, nil)

	if err == nil {
		t.Fatal("sendVolumeData returned nil for a device the importer never accepted a byte of")
	}

	mu.Lock()
	gotPuts, gotHeads, gotFinished := puts, heads, finished
	mu.Unlock()

	if gotFinished != 0 {
		t.Errorf("finalisation POSTs = %d, want 0 (an unfinished device must never be finalised)", gotFinished)
	}

	if !strings.Contains(err.Error(), "namespace/data-import") {
		t.Errorf("error does not name the volume: %v", err)
	}

	// The client must have ASKED rather than guessed: a probe per refused request, plus
	// the one that opened the transfer. Without this the test would also pass for a
	// client that simply treated the conflict as a plain failure, which is the opposite
	// misreading and just as wrong for the filesystem importer.
	if gotHeads != gotPuts+1 {
		t.Errorf("HEAD/PUT counts = %d/%d, want one probe per refusal plus the opening probe",
			gotHeads, gotPuts)
	}

	if gotPuts == 0 {
		t.Errorf("PUTs = 0, want the transfer to have been attempted at all (total size %d)", totalSize)
	}
}

// TestBlockUpload_BreakWhoseLastRequestLandedCompletesFromTheProbe is the other side of
// the same rule: the client asks, and when the answer says the device is full it accepts
// that too.
//
// This is the ordinary shape of a break on the last request — the importer wrote every
// byte and its acknowledgement died with the connection. A client that resumed from its
// own count of what it had pushed could not tell this apart from a request that landed
// nothing; the importer can, and is asked.
//
// What is asserted beyond the success is that exactly ONE PUT was made. Re-sending the
// whole device on top of a full one is not an error the importer would report — it
// answers the second write just as happily — so only the count distinguishes "resumed"
// from "started over".
//
// The logger is the command's own (clusterVolumeImporter.log), which makes this also the
// guard that the evidence line survives the whole call chain: a run that reached the
// shared retry policy with a silenced logger would pass every other test in this file.
func TestBlockUpload_BreakWhoseLastRequestLandedCompletesFromTheProbe(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("the-answer-was-lost-not-the-bytes-"), 100)
	leaf := blockLeafFor(t, payload)
	totalSize := int64(len(payload))

	var (
		mu       sync.Mutex
		puts     int
		finished int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()

		switch req.Method {
		case http.MethodHead:
			header := http.Header{}

			// Before the write, the importer holds nothing; after it, everything.
			// The client never sees the acknowledgement that said so.
			offset := "0"
			if puts > 0 {
				offset = strconv.FormatInt(totalSize, 10)
			}

			header.Set("X-Next-Offset", offset)

			return newTestHTTPResponse(http.StatusOK, header), nil
		case http.MethodPut:
			puts++

			return nil, errors.New("http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=NO_ERROR")
		case http.MethodPost:
			finished++

			return newTestHTTPResponse(http.StatusNoContent, http.Header{}), nil
		default:
			return newTestHTTPResponse(http.StatusMethodNotAllowed, http.Header{}), nil
		}
	})

	captured := &capturedLog{}
	importer := &clusterVolumeImporter{log: captured.logger()}

	err := importer.sendVolumeData(context.Background(), doer, "https://importer.test",
		volumeModeBlock, leaf, "namespace", "data-import", nil, nil, nil)
	if err != nil {
		t.Fatalf("sendVolumeData: %v (a break after the importer had taken every byte must not fail the volume)", err)
	}

	mu.Lock()
	gotPuts, gotFinished := puts, finished
	mu.Unlock()

	if gotPuts != 1 {
		t.Errorf("PUTs = %d, want 1 (the retry must have found the device full, not re-sent it)", gotPuts)
	}

	if gotFinished != 1 {
		t.Errorf("finalisation POSTs = %d, want 1", gotFinished)
	}

	assertRecoveredBreakLogged(t, captured.String(), totalSize)
}

// fsTarWithOneEntry writes a single-entry, uncompressed filesystem tar and returns its
// path together with the entry's path inside the importer's namespace.
func fsTarWithOneEntry(t *testing.T, content []byte) (string, string) {
	t.Helper()

	return writeSingleEntryFSTar(t, "none", content), "file.bin"
}

// TestFileUpload_ConflictWithoutOffsetOnAWholeFileIsNotAFailure pins the meaning this
// answer has for the FILESYSTEM importer and does not have for the block one.
//
// That importer decides a destination is complete before it so much as looks at the
// offset, and answers the refusal without naming one. So the very same 409 that means
// "come back in a moment" on a device means "you are finished" on a file — and reading it
// as a failure would reject a transfer whose bytes all arrived, which is how a run that
// succeeded ends up reported as broken and repeated.
//
// The client asks instead, and the HEAD it asks with is the one that distinguishes the two:
// a destination present at the declared size and no partial upload beside it. Finalisation
// following is part of the assertion — a transfer read as complete that then does not
// finalise would leave the import hanging just as surely.
func TestFileUpload_ConflictWithoutOffsetOnAWholeFileIsNotAFailure(t *testing.T) {
	t.Parallel()

	content := []byte("every byte of this file is already on the importer")
	tarPath, _ := fsTarWithOneEntry(t, content)

	var (
		mu       sync.Mutex
		puts     int
		finished int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()

		switch req.Method {
		case http.MethodHead:
			header := http.Header{}

			if puts == 0 {
				// Opening probe: nothing here yet, as far as this run knows.
				return newTestHTTPResponse(http.StatusNotFound, header), nil
			}

			// No X-Next-Offset and the declared size: the destination file exists
			// and no partial upload of it does. This importer renames the partial
			// onto the destination on the write that completes it, so that state
			// is reachable only by having completed.
			return completeFileHeadResponse(int64(len(content))), nil
		case http.MethodPut:
			puts++

			return newTestHTTPResponse(http.StatusConflict, http.Header{}), nil
		case http.MethodPost:
			finished++

			return newTestHTTPResponse(http.StatusNoContent, http.Header{}), nil
		default:
			return newTestHTTPResponse(http.StatusMethodNotAllowed, http.Header{}), nil
		}
	})

	importer := &clusterVolumeImporter{log: discardLogger()}
	leaf := PlannedNode{FilesystemData: true, TarFile: tarPath}

	progressed := 0

	err := importer.sendVolumeData(context.Background(), doer, "https://importer.test",
		volumeModeFilesystem, leaf, "namespace", "data-import", nil, func(n int) { progressed += n }, nil)
	if err != nil {
		t.Fatalf("sendVolumeData: %v (a refused chunk over an already-complete file is not a failed transfer)", err)
	}

	mu.Lock()
	gotPuts, gotFinished := puts, finished
	mu.Unlock()

	if gotPuts != 1 {
		t.Errorf("PUTs = %d, want 1 (the answer resolved the transfer; nothing was left to send)", gotPuts)
	}

	if gotFinished != 1 {
		t.Errorf("finalisation POSTs = %d, want 1", gotFinished)
	}

	if progressed != len(content) {
		t.Errorf("progress = %d, want %d (a file found complete is credited in full)", progressed, len(content))
	}
}

// TestFileUpload_ConflictWithoutOffsetOnAPartialFileIsNotCompletion is the negative twin
// of the test above, and without it that one is satisfied by a client that reads every
// unexplained conflict as "done".
//
// Here the importer answers the same refusal, but its HEAD names a resume offset — a
// partial upload still sitting on disk, which is this importer saying in as many words
// that the destination is NOT there. A client that skipped the question, or asked and
// then ignored an answer that named an offset, would finalise over a file missing most of
// its bytes.
//
// Cost note: the importer never lets the transfer move, so this runs to the retry
// ceiling. That is the honest shape of the failure — "cannot proceed" rather than
// "finished" — and the ceiling is what keeps it from being "forever".
func TestFileUpload_ConflictWithoutOffsetOnAPartialFileIsNotCompletion(t *testing.T) {
	t.Parallel()

	content := bytes.Repeat([]byte("half-a-file-is-not-a-file-"), 40)
	tarPath, _ := fsTarWithOneEntry(t, content)

	var (
		mu       sync.Mutex
		puts     int
		finished int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()

		switch req.Method {
		case http.MethodHead:
			header := http.Header{}

			if puts == 0 {
				return newTestHTTPResponse(http.StatusNotFound, header), nil
			}

			// A partial upload is on disk and stuck at zero: the dead handler
			// created the temporary file and wrote nothing into it.
			header.Set("X-Next-Offset", "0")

			return newTestHTTPResponse(http.StatusOK, header), nil
		case http.MethodPut:
			puts++

			return newTestHTTPResponse(http.StatusConflict, http.Header{}), nil
		case http.MethodPost:
			finished++

			return newTestHTTPResponse(http.StatusNoContent, http.Header{}), nil
		default:
			return newTestHTTPResponse(http.StatusMethodNotAllowed, http.Header{}), nil
		}
	})

	importer := &clusterVolumeImporter{log: discardLogger()}
	leaf := PlannedNode{FilesystemData: true, TarFile: tarPath}

	progressed := 0

	err := importer.sendVolumeData(context.Background(), doer, "https://importer.test",
		volumeModeFilesystem, leaf, "namespace", "data-import", nil, func(n int) { progressed += n }, nil)
	if err == nil {
		t.Fatal("sendVolumeData returned nil for a file the importer still holds only part of")
	}

	mu.Lock()
	gotPuts, gotFinished := puts, finished
	mu.Unlock()

	if gotFinished != 0 {
		t.Errorf("finalisation POSTs = %d, want 0 (a partial destination must never be finalised)", gotFinished)
	}

	if progressed != 0 {
		t.Errorf("progress = %d, want 0 (nothing was ever accepted)", progressed)
	}

	if gotPuts == 0 {
		t.Error("PUTs = 0, want the transfer to have been attempted at all")
	}
}

// TestFileUpload_ProbeNamingAWrongSizedDestinationIsNotCompletion covers the third
// answer fileUploadTransfer.ask acts on, and the one easiest to leave out.
//
// A HEAD naming no offset says "there is a destination file and no partial upload"; it
// does not say the file is THIS transfer's. A destination of some other size is a
// different file — left behind by an earlier archive, say — and treating its mere
// existence as completion would finalise an import whose data never arrived at all.
func TestFileUpload_ProbeNamingAWrongSizedDestinationIsNotCompletion(t *testing.T) {
	t.Parallel()

	content := []byte("this file is 42 bytes; the other one is not")
	tarPath, _ := fsTarWithOneEntry(t, content)

	var (
		mu       sync.Mutex
		puts     int
		finished int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()

		switch req.Method {
		case http.MethodHead:
			if puts == 0 {
				return newTestHTTPResponse(http.StatusNotFound, http.Header{}), nil
			}

			// A destination is there, but it is not the size this transfer declared.
			return completeFileHeadResponse(int64(len(content)) + 1), nil
		case http.MethodPut:
			puts++

			return newTestHTTPResponse(http.StatusConflict, http.Header{}), nil
		case http.MethodPost:
			finished++

			return newTestHTTPResponse(http.StatusNoContent, http.Header{}), nil
		default:
			return newTestHTTPResponse(http.StatusMethodNotAllowed, http.Header{}), nil
		}
	})

	importer := &clusterVolumeImporter{log: discardLogger()}
	leaf := PlannedNode{FilesystemData: true, TarFile: tarPath}

	err := importer.sendVolumeData(context.Background(), doer, "https://importer.test",
		volumeModeFilesystem, leaf, "namespace", "data-import", nil, nil, nil)
	if err == nil {
		t.Fatal("sendVolumeData returned nil for a destination whose size is not this file's")
	}

	mu.Lock()
	gotFinished := finished
	mu.Unlock()

	if gotFinished != 0 {
		t.Errorf("finalisation POSTs = %d, want 0", gotFinished)
	}
}

// TestBlockUpload_ParallelVolumesEachSurviveTheirOwnBreak is the concurrency guard for
// the retry machinery: volumes upload side by side, and a break on one must neither
// disturb another nor be absorbed on its behalf.
//
// It is worth having because the shape of the mistake is so ordinary. The retry state a
// transfer keeps — its durable offset, its attempt count — is exactly the kind of thing
// that gets hoisted into a shared struct or a package variable on a tidying pass, and the
// result is not a crash: two volumes would quietly resume from each other's offsets,
// which produces devices of the right length with the wrong bytes in the middle. So the
// assertions are on each volume's own content and its own resume offset, not on the calls
// returning nil.
//
// LIMIT, and it is a real one. Shared retry state fails by interleaving, so no small
// deliberate defect makes this test red every run — it is a regression guard, not a proof
// of independence, and running it under -race is what would make the shared-variable
// version of the mistake visible. What IS shown red deterministically, by breaking the
// probe in blockUploadTransfer.ask, is that both volumes genuinely go through the retry
// path here rather than completing without one.
func TestBlockUpload_ParallelVolumesEachSurviveTheirOwnBreak(t *testing.T) {
	t.Parallel()

	payloads := [][]byte{
		bytes.Repeat([]byte("first-volume-bytes-"), 300),
		bytes.Repeat([]byte("second-volume-different-bytes-"), 300),
	}

	// Both volumes break, at offsets deliberately different from each other: a transfer
	// resuming from its neighbour's offset would then be sending the wrong bytes rather
	// than accidentally the right ones.
	importers := []*interruptingBlockImporter{
		{partialN: int64(len(payloads[0]) / 3)},
		{partialN: int64(len(payloads[1]) / 2)},
	}

	errs := make([]error, len(payloads))

	var wg sync.WaitGroup

	for i := range payloads {
		srv := httptest.NewServer(importers[i])
		defer srv.Close()

		leaf := blockLeafFor(t, payloads[i])
		importer := &clusterVolumeImporter{log: discardLogger()}

		wg.Add(1)

		go func() {
			defer wg.Done()

			errs[i] = importer.sendVolumeData(context.Background(), plainHTTPDoer{}, srv.URL,
				volumeModeBlock, leaf, "namespace", fmt.Sprintf("data-import-%d", i), nil, nil, nil)
		}()
	}

	wg.Wait()

	for i := range payloads {
		if errs[i] != nil {
			t.Errorf("volume %d: sendVolumeData: %v (a break on one volume must not fail it, nor its neighbour)", i, errs[i])

			continue
		}

		if got := importers[i].durablyWritten(); !bytes.Equal(got, payloads[i]) {
			t.Errorf("volume %d: importer holds %d bytes that are not its own payload (%d bytes)",
				i, len(got), len(payloads[i]))
		}

		want := []int64{0, importers[i].partialN}
		if got := importers[i].offsetsPut(); !slices.Equal(got, want) {
			t.Errorf("volume %d: PUT offsets = %v, want %v (each volume resumes from its OWN break)",
				i, got, want)
		}
	}
}

// TestCompressedBlockUpload_DiagnosesOnlyWhatItCanSee checks a claim that lives in a
// comment and would otherwise be checked by nobody: the compressed path explains a failed
// PUT as "this archive's declared size may not match its content" when — and only when —
// the request body ran out early.
//
// Both directions matter, and the pair is the point. A body that runs dry really is the
// symptom of an over-declared archive, and net/http refusing to send a short body is the
// only way that ever surfaces, so dropping the note would leave a corrupt archive
// reported as a bare transport complaint. Attaching the note to every failure is worse in
// a quieter way: a link that broke would send the next reader to audit an archive that is
// perfectly fine, and nothing in the output would contradict them.
//
// The two are told apart by requestBodyReport.sourceEndedShort, which is the only thing
// in the round trip that can distinguish a source that ended from a transport that
// stopped asking.
func TestCompressedBlockUpload_DiagnosesOnlyWhatItCanSee(t *testing.T) {
	t.Parallel()

	const declaredSizeNote = "may not match the archive's actual decompressed content"

	payload := bytes.Repeat([]byte("archive-content-"), 300)

	t.Run("a body that ran dry is named as a size mismatch", func(t *testing.T) {
		t.Parallel()

		// gzip, not zstd: a zstd archive carries mandatory per-frame content sizes, so
		// an over-declared size is caught by the geometry walk before a single PUT is
		// sent and the body never gets the chance to run dry. The compatibility codecs
		// have no such metadata, which is exactly why the note this checks exists.
		dir := t.TempDir()
		dataFile := filepath.Join(dir, "data.bin.gz")
		writeEncodedBlockFile(t, dataFile, "gzip", payload)

		imp := &fakeBlockImporter{}
		srv := httptest.NewServer(imp)

		defer srv.Close()

		// Declared larger than the archive decodes to, so the decode stream runs out
		// before the request's declared Content-Length is met.
		overDeclared := int64(len(payload) + 5000)

		err := putBlock(context.Background(), plainHTTPDoer{}, srv.URL, dataFile, ".gz", overDeclared,
			discardLogger(), nil, nil)
		if err == nil {
			t.Fatal("putBlock returned nil for an over-declared archive size")
		}

		if !strings.Contains(err.Error(), declaredSizeNote) {
			t.Errorf("failure does not point at the declared size:\n%v", err)
		}
	})

	t.Run("a broken link is not", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		dataFile := filepath.Join(dir, "data.bin.zst")
		writeEncodedBlockFile(t, dataFile, "zstd", payload)

		doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodHead {
				header := http.Header{}
				header.Set("X-Next-Offset", "0")

				return newTestHTTPResponse(http.StatusOK, header), nil
			}

			return nil, errors.New("http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=NO_ERROR")
		})

		err := putBlock(context.Background(), doer, "https://importer.test/block", dataFile, ".zst",
			int64(len(payload)), discardLogger(), nil, nil)
		if err == nil {
			t.Fatal("putBlock returned nil for a link that never carried a byte")
		}

		if strings.Contains(err.Error(), declaredSizeNote) {
			t.Errorf("a broken link was reported as an archive-size problem, sending the reader to audit a sound archive:\n%v", err)
		}
	})
}

// TestBlockUpload_BreakWhoseProbeAlsoFailsKeepsWhatWasConfirmed covers the path on which a
// block PUT loop's reported offset is load-bearing, and which the tests above leave
// mutation-silent because the probe overwrites that value on every path they reach.
//
// When the probe ITSELF fails, nothing overwrites it. The loop's own report is then the
// only record of how far the importer had confirmed this attempt, and it decides two
// things at once: where the next attempt resumes, and — because the break arrives as an
// error no classifier can name — whether there IS a next attempt, since the shared policy
// retries an unnamed failure only on the strength of bytes delivered. A loop that
// reported zero after acknowledged chunks would turn a recoverable break into an
// immediate failure, and nothing about the run would look wrong beyond it having stopped.
//
// The property is not the block path's alone, and the block path's guard does not cover
// the other: the filesystem loop reports an offset for the same reasons and was measured
// to be mutation-silent until its own twin was written. Both are needed; see
// TestFileUpload_BreakWhoseProbeAlsoFailsKeepsWhatWasConfirmed.
//
// The transfer is built directly rather than through putBlock so the chunk limit can be
// small enough for several chunks to fit in a test-sized payload; production supplies
// 32 MiB (see newBlockUploadTransfer).
func TestBlockUpload_BreakWhoseProbeAlsoFailsKeepsWhatWasConfirmed(t *testing.T) {
	t.Parallel()

	const chunkLimit = 16

	payload := bytes.Repeat([]byte("0123456789abcdef"), 3) // 48 bytes -> three chunks
	totalSize := int64(len(payload))

	var (
		mu      sync.Mutex
		offsets []int64
		heads   int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()

		if req.Method == http.MethodHead {
			heads++

			// The importer cannot be asked: it is there, but not answering for
			// this question.
			return newTestHTTPResponse(http.StatusInternalServerError, http.Header{}), nil
		}

		offset, _ := strconv.ParseInt(req.Header.Get("X-Offset"), 10, 64)
		offsets = append(offsets, offset)

		// The second chunk's request dies on the wire, once.
		if offset == chunkLimit && len(offsets) == 2 {
			return nil, errors.New("http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=NO_ERROR")
		}

		next := offset + chunkLimit

		header := http.Header{}
		header.Set("X-Next-Offset", strconv.FormatInt(next, 10))

		if next == totalSize {
			return newTestHTTPResponse(http.StatusCreated, header), nil
		}

		return newTestHTTPResponse(http.StatusNoContent, header), nil
	})

	transfer := &blockUploadTransfer{
		httpClient:   doer,
		url:          "https://importer.test/block",
		dataFile:     "data.bin",
		totalSize:    totalSize,
		payloadLimit: chunkLimit,
		source:       bytes.NewReader(payload),
		log:          discardLogger(),
		progress:     &blockUploadProgress{},
		deps:         defaultBlockDecodeDependencies(),
	}

	if err := transfer.run(context.Background()); err != nil {
		t.Fatalf("run: %v (a break after acknowledged chunks must buy another attempt even when the importer cannot be asked)", err)
	}

	mu.Lock()
	gotOffsets, gotHeads := append([]int64(nil), offsets...), heads
	mu.Unlock()

	if want := []int64{0, chunkLimit, chunkLimit, 2 * chunkLimit}; !slices.Equal(gotOffsets, want) {
		t.Errorf("PUT offsets = %v, want %v (the retry resumes from the last chunk the importer acknowledged)",
			gotOffsets, want)
	}

	if gotHeads != 1 {
		t.Errorf("HEAD count = %d, want 1 (the probe was tried once and failed)", gotHeads)
	}
}

// TestBlockUpload_AnUnanswerableImporterFailsAtOnce pins the other half of what the probe
// is for, and keeps a sentinel from asserting something it has no evidence of.
//
// ErrDataPlaneNotAccepted says the endpoint ANSWERED and the request did not complete;
// that answer is the whole of what it claims, and it is what makes a break with nothing
// delivered retryable at all. When the probe cannot get an answer there is no such
// evidence, so the failure goes back as itself — and a far end that can neither take a
// byte nor say where it stands fails on the first attempt instead of costing a retry
// budget in silence.
//
// Counting the requests is the only way to see this: both readings end in an error, and
// only the number of attempts says which one happened.
func TestBlockUpload_AnUnanswerableImporterFailsAtOnce(t *testing.T) {
	t.Parallel()

	payload := bytes.Repeat([]byte("nothing-will-arrive-"), 20)

	var (
		mu    sync.Mutex
		puts  int
		heads int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()

		if req.Method == http.MethodHead {
			heads++

			return newTestHTTPResponse(http.StatusInternalServerError, http.Header{}), nil
		}

		puts++

		return nil, errors.New("http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=NO_ERROR")
	})

	transfer := &blockUploadTransfer{
		httpClient:   doer,
		url:          "https://importer.test/block",
		dataFile:     "data.bin",
		totalSize:    int64(len(payload)),
		payloadLimit: blockPutPayloadLimit,
		source:       bytes.NewReader(payload),
		log:          discardLogger(),
		progress:     &blockUploadProgress{},
		deps:         defaultBlockDecodeDependencies(),
	}

	if err := transfer.run(context.Background()); err == nil {
		t.Fatal("run returned nil against an importer that took nothing and answered nothing")
	}

	mu.Lock()
	gotPuts, gotHeads := puts, heads
	mu.Unlock()

	if gotPuts != 1 {
		t.Errorf("PUTs = %d, want 1 (an unanswerable far end must not buy a retry budget)", gotPuts)
	}

	if gotHeads != 1 {
		t.Errorf("HEADs = %d, want 1 (asked once, and the question went unanswered)", gotHeads)
	}
}

// TestBlockUpload_RejectedIdentityAfterProgressCostsOneAttempt pins a fatal case as fatal
// AFTER the transfer has already moved, which is the only position in which it proves
// anything.
//
// Retrying is licensed here by DELIVERED BYTES, not by the error being recognized — that
// is what carries a break no classifier can name. The same rule will just as happily
// carry a refusal that is not about the link at all, and a rejected identity is the plain
// case: the importer answered, and its answer will not change.
//
// Measured rather than assumed, and the obvious guesses are both wrong. Left unnamed this
// costs neither the whole retry budget nor the no-progress ceiling: the retry resumes AT
// the refused offset, so its very first request is refused again having delivered nothing,
// and the rule "an unrecognized failure with no delivery stops here" ends the loop on the
// second attempt. Measured on this fixture by removing the name from the fatal set: three
// requests and 1.08s against two requests and no wait, with the returned error IDENTICAL
// in both runs — "server error at offset 16: status 403 (403 Forbidden): importer rejected
// the client identity".
//
// So the whole of what naming it buys is one pointless request to an endpoint that has
// already given its final answer, and the second or so of backoff in front of it. Worth
// buying anyway, because on the published path a refused identity is the FIRST thing a
// user meets — ingress terminates TLS and a certificate-based kubeconfig never reaches the
// importer — and because the shared policy already holds that a rejected credential costs
// one attempt rather than a budget; an upload that quietly disagreed would be the odd one
// out.
//
// Checked after a delivered chunk on purpose. At zero progress the rule cannot retry
// anything, so the same assertion would hold whether or not the failure were named fatal,
// and the test would be green either way.
//
// The distinguisher is the REQUEST COUNT, not the error: both readings end in the same
// error, and only the number of attempts says which one happened.
func TestBlockUpload_RejectedIdentityAfterProgressCostsOneAttempt(t *testing.T) {
	t.Parallel()

	const chunkLimit = 16

	payload := bytes.Repeat([]byte("0123456789abcdef"), 3) // 48 bytes -> three chunks

	var (
		mu   sync.Mutex
		puts int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()

		if req.Method == http.MethodHead {
			header := http.Header{}
			header.Set("X-Next-Offset", "0")

			return newTestHTTPResponse(http.StatusOK, header), nil
		}

		puts++

		// The first chunk is taken; the second meets a refused identity.
		if puts == 1 {
			header := http.Header{}
			header.Set("X-Next-Offset", strconv.Itoa(chunkLimit))

			return newTestHTTPResponse(http.StatusNoContent, header), nil
		}

		return newTestHTTPResponse(http.StatusForbidden, http.Header{}), nil
	})

	transfer := &blockUploadTransfer{
		httpClient:   doer,
		url:          "https://importer.test/block",
		dataFile:     "data.bin",
		totalSize:    int64(len(payload)),
		payloadLimit: chunkLimit,
		source:       bytes.NewReader(payload),
		log:          discardLogger(),
		progress:     &blockUploadProgress{},
		deps:         defaultBlockDecodeDependencies(),
	}

	err := transfer.run(context.Background())
	if !errors.Is(err, errUploadUnauthorized) {
		t.Fatalf("run error = %v, want one wrapping errUploadUnauthorized", err)
	}

	mu.Lock()
	gotPuts := puts
	mu.Unlock()

	if gotPuts != 2 {
		t.Errorf("PUTs = %d, want 2 (the accepted chunk and the refused one; a refused identity must not buy a retry budget)",
			gotPuts)
	}
}

// TestFileUpload_BreakWhoseProbeAlsoFailsKeepsWhatWasConfirmed is the filesystem twin of
// TestBlockUpload_BreakWhoseProbeAlsoFailsKeepsWhatWasConfirmed, and it exists because the
// property is symmetric while the coverage was not.
//
// Both transfers report an offset out of their PUT loop, and on both the report is dead
// weight everywhere except one path: a probe that itself fails leaves nothing to overwrite
// it. There it decides where the next attempt resumes AND whether there is one at all,
// since a break arrives as an error no classifier names and only delivered bytes carry it.
// The block guard covered that path; measured, the same defect planted in putFile left the
// whole package green — so a change turning a survivable break on a multi-chunk file into
// an immediate failure would have passed every gate.
//
// Several chunks have to fit in a test-sized entry for the report to have anything to say,
// which is what fileUploadTransfer.payloadLimit is for; production sets it to the same
// 32 MiB the block path uses.
func TestFileUpload_BreakWhoseProbeAlsoFailsKeepsWhatWasConfirmed(t *testing.T) {
	t.Parallel()

	const chunkLimit = 16

	payload := bytes.Repeat([]byte("0123456789abcdef"), 3) // 48 bytes -> three chunks
	totalSize := int64(len(payload))

	var (
		mu      sync.Mutex
		offsets []int64
		heads   int
	)

	doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()

		if req.Method == http.MethodHead {
			heads++

			// The importer is there, but cannot answer this question.
			return newTestHTTPResponse(http.StatusInternalServerError, http.Header{}), nil
		}

		offset, _ := strconv.ParseInt(req.Header.Get("X-Offset"), 10, 64)
		offsets = append(offsets, offset)

		if offset == chunkLimit && len(offsets) == 2 {
			return nil, errors.New("http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=NO_ERROR")
		}

		next := offset + chunkLimit

		header := http.Header{}
		header.Set("X-Next-Offset", strconv.FormatInt(next, 10))

		if next == totalSize {
			return newTestHTTPResponse(http.StatusCreated, header), nil
		}

		return newTestHTTPResponse(http.StatusNoContent, header), nil
	})

	newBody := func(_ context.Context, offset, size int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(payload[offset : offset+size])), nil
	}

	transfer := &fileUploadTransfer{
		client:       doer,
		baseURL:      "https://importer.test",
		fileURL:      "https://importer.test/" + uploadFilesSubpath + "/file.bin",
		relPath:      "file.bin",
		totalSize:    totalSize,
		payloadLimit: chunkLimit,
		newBody:      newBody,
		progress:     &fileUploadProgress{},
		log:          discardLogger(),
	}

	if err := transfer.run(context.Background()); err != nil {
		t.Fatalf("run: %v (a break after acknowledged chunks must buy another attempt even when the importer cannot be asked)", err)
	}

	mu.Lock()
	gotOffsets, gotHeads := append([]int64(nil), offsets...), heads
	mu.Unlock()

	if want := []int64{0, chunkLimit, chunkLimit, 2 * chunkLimit}; !slices.Equal(gotOffsets, want) {
		t.Errorf("PUT offsets = %v, want %v (the retry resumes from the last chunk the importer acknowledged)",
			gotOffsets, want)
	}

	if gotHeads != 1 {
		t.Errorf("HEAD count = %d, want 1 (the probe was tried once and failed)", gotHeads)
	}
}
