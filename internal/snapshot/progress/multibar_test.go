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

package progress

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mattn/go-runewidth"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// aggregateLine formats the expected "downloaded X / total Y (N/M volumes)" line
// as the non-TTY sink emits it (DirectionDownload's PastVerb), using the same
// decor.SizeB1024 formatter.
func aggregateLine(t *testing.T, prog, total int64, volDone, volTotal int) string {
	t.Helper()

	return aggregateLineWithVerb(t, DirectionDownload.PastVerb, prog, total, volDone, volTotal)
}

// aggregateLineWithVerb is aggregateLine parametrized by the direction's
// past-tense verb, so upload-direction tests can assert "uploaded ..." without
// duplicating the format string.
func aggregateLineWithVerb(t *testing.T, verb string, prog, total int64, volDone, volTotal int) string {
	t.Helper()

	return fmt.Sprintf("%s % .1f / total % .1f (%d/%d volumes)\n",
		verb, decor.SizeB1024(prog), decor.SizeB1024(total), volDone, volTotal)
}

func TestNonTTY_Fallback(t *testing.T) {
	t.Parallel()

	type streamSpec struct {
		name  string
		total int64
		incrs []int
	}

	cases := []struct {
		name      string
		streams   []streamSpec
		wantProg  int64
		wantTotal int64
	}{
		{
			name: "two streams fully transferred",
			streams: []streamSpec{
				{name: "vol-a", total: 1024, incrs: []int{512, 512}},
				{name: "vol-b", total: 2048, incrs: []int{1024, 1024}},
			},
			wantProg:  3072,
			wantTotal: 3072,
		},
		{
			name: "two streams partially transferred",
			streams: []streamSpec{
				{name: "vol-a", total: 1024, incrs: []int{512}},
				{name: "vol-b", total: 2048, incrs: []int{1024}},
			},
			wantProg:  1536,
			wantTotal: 3072,
		},
		{
			name: "single stream zero bytes",
			streams: []streamSpec{
				{name: "vol-a", total: 0, incrs: nil},
			},
			wantProg:  0,
			wantTotal: 0,
		},
		{
			name: "SetTotal updates aggregate",
			streams: []streamSpec{
				{name: "vol-a", total: 0, incrs: []int{100}},
			},
			wantProg:  100,
			wantTotal: 512,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			buf := &bytes.Buffer{}
			sink := New(buf, false, WithInterval(time.Millisecond))

			streams := make([]Stream, 0, len(tc.streams))

			for _, spec := range tc.streams {
				streams = append(streams, sink.NewStream(spec.name, spec.total))
			}

			for i, spec := range tc.streams {
				for _, n := range spec.incrs {
					streams[i].IncrBy(n)
				}
			}

			// SetTotal case: update the first stream's total after initial NewStream.
			if tc.name == "SetTotal updates aggregate" {
				streams[0].SetTotal(512)
			}

			for _, st := range streams {
				st.Done()
			}

			sink.Wait()

			got := buf.String()
			// SetVolumeTotal is never called in this test, so volTotal stays 0;
			// volDone equals the number of streams since every stream calls Done().
			want := aggregateLine(t, tc.wantProg, tc.wantTotal, len(tc.streams), 0)

			if !strings.Contains(got, want) {
				t.Errorf("output does not contain expected final line\ngot:  %q\nwant (contained): %q", got, want)
			}
		})
	}
}

// TestNonTTY_WithDirection_Upload proves WithDirection propagates through the
// public New constructor end-to-end into the non-TTY aggregate line: an upload
// direction sink emits "uploaded X / total Y (...)" instead of the default
// "downloaded ...".
func TestNonTTY_WithDirection_Upload(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := New(buf, false, WithInterval(time.Hour), WithDirection(DirectionUpload))

	st := sink.NewStream("vol-a", 1024)
	st.IncrBy(1024)
	st.Done()

	sink.Wait()

	got := buf.String()
	want := aggregateLineWithVerb(t, "uploaded", 1024, 1024, 1, 0)

	if !strings.Contains(got, want) {
		t.Errorf("output does not contain expected upload-direction line\ngot:  %q\nwant (contained): %q", got, want)
	}

	if strings.Contains(got, "downloaded") {
		t.Errorf("upload-direction output must not contain the download verb\ngot: %q", got)
	}
}

// TestNew_WithDirection_PropagatesToTTYStream proves WithDirection propagates
// through the public New constructor end-to-end into a TTY stream's rendered
// state words (via stateWord), not just when the direction is poked directly
// into a struct in a white-box test.
func TestNew_WithDirection_PropagatesToTTYStream(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := New(buf, true, WithDirection(DirectionUpload))
	defer sink.Wait()

	// total=0: production NewStream call sites (pipeline.go, snapimport/import.go)
	// always construct with total=0 and SetTotal the real size once known. mpb/v8
	// v8.7.5 latches `triggerComplete` at construction whenever total>0
	// (progress.go's makeBarState), which makes finalize's completing
	// SetTotal(total, true) call a silent no-op unless the bar's current already
	// reached total via IncrBy — total=0 avoids that landmine, matching every
	// other stateWord test in this file.
	st := sink.NewStream("vol-a", 0)

	ts, ok := st.(*ttyStream)
	if !ok {
		t.Fatal("NewStream did not return *ttyStream")
	}

	if ts.dir != DirectionUpload {
		t.Errorf("ttyStream.dir = %+v, want %+v", ts.dir, DirectionUpload)
	}

	waiting := stateWord(atomic.LoadInt32(&ts.state), atomic.LoadInt32(&ts.activated) == 1, ts.dir)
	if want := "Waiting for DataImport to be Ready"; waiting != want {
		t.Errorf("waiting word = %q, want %q", waiting, want)
	}

	st.Activate()

	active := stateWord(atomic.LoadInt32(&ts.state), atomic.LoadInt32(&ts.activated) == 1, ts.dir)
	if want := "Uploading"; active != want {
		t.Errorf("active word = %q, want %q", active, want)
	}

	st.Done()

	done := stateWord(atomic.LoadInt32(&ts.state), atomic.LoadInt32(&ts.activated) == 1, ts.dir)
	if want := "Upload complete"; done != want {
		t.Errorf("done word = %q, want %q", done, want)
	}
}

// TestNew_DefaultDirectionIsDownload pins the explicit default: a caller that
// omits WithDirection gets DirectionDownload, matching the doc comment on New
// and WithDirection. cmd/download/download.go relies on this default rather
// than passing WithDirection(DirectionDownload) explicitly.
func TestNew_DefaultDirectionIsDownload(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := New(buf, true)
	defer sink.Wait()

	// total=0: see the matching comment in
	// TestNew_WithDirection_PropagatesToTTYStream for why a nonzero total here
	// would hang the deferred sink.Wait().
	st := sink.NewStream("vol-a", 0)

	ts, ok := st.(*ttyStream)
	if !ok {
		t.Fatal("NewStream did not return *ttyStream")
	}

	if ts.dir != DirectionDownload {
		t.Errorf("ttyStream.dir = %+v, want default %+v", ts.dir, DirectionDownload)
	}

	st.Done()
}

func TestTTY_SinkIsNonNil(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := New(buf, true)

	if sink == nil {
		t.Fatal("expected non-nil Sink for tty=true")
	}

	st := sink.NewStream("test-stream", 1024)
	st.IncrBy(1024)
	st.Done()
	sink.Wait()
}

func TestLogWriter(t *testing.T) {
	t.Parallel()

	t.Run("plain sink returns stderr", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		sink := New(buf, false, WithInterval(time.Hour))
		defer sink.Wait()

		if w := sink.LogWriter(); w != os.Stderr {
			t.Errorf("plain sink LogWriter = %v, want os.Stderr", w)
		}
	})

	t.Run("tty sink returns non-nil coordinated writer", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		sink := New(buf, true)
		defer sink.Wait()

		w := sink.LogWriter()
		if w == nil {
			t.Fatal("tty sink LogWriter returned nil")
		}

		if w == os.Stderr {
			t.Error("tty sink LogWriter must not be raw os.Stderr; it must be coordinated with the bars")
		}

		if _, err := w.Write([]byte("log line above the bars\n")); err != nil {
			t.Errorf("writing to tty sink LogWriter: %v", err)
		}
	})
}

func TestNonTTY_SetTotal_AggregatesCorrectly(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	// Use a long interval so only the Wait() line is emitted.
	sink := New(buf, false, WithInterval(time.Hour))

	st := sink.NewStream("vol-x", 0)
	st.SetTotal(4096)
	st.IncrBy(4096)
	st.Done()
	sink.Wait()

	// SetVolumeTotal is never called here, so volTotal stays 0; volDone is 1 (the
	// single stream's Done() call).
	want := aggregateLine(t, 4096, 4096, 1, 0)

	if !strings.Contains(buf.String(), want) {
		t.Errorf("SetTotal not reflected in aggregate:\ngot:  %q\nwant (contained): %q", buf.String(), want)
	}
}

// TestNonTTY_SetCurrent_SeedsAndCancelsAggregate proves the plainStream side
// of SetCurrent's absolute-value contract: SetCurrent(n) applies exactly the
// delta n to the sink's shared aggregate (mirroring SetTotal's delta pattern),
// and a later SetCurrent(0) removes that contribution again without touching
// any other stream's — the downward reset pipeline.downloadBlock/downloadFS
// rely on when the resume-seed clamp zeroes an over-seeded stream before the
// real transfer re-credits it (clamp-resume-seed-to-fresh-total).
func TestNonTTY_SetCurrent_SeedsAndCancelsAggregate(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := New(buf, false, WithInterval(time.Hour))

	seeded := sink.NewStream("vol-seeded", 1024)
	other := sink.NewStream("vol-other", 512)

	other.IncrBy(200)

	// Seed vol-seeded with 300 already-committed bytes before its transfer starts.
	seeded.SetCurrent(300)

	// Cancel the seed back out right before the real transfer begins, then let
	// the real resume-skip/incremental crediting take over from zero.
	seeded.SetCurrent(0)
	seeded.IncrBy(300)
	seeded.IncrBy(724)

	seeded.Done()
	other.Done()
	sink.Wait()

	// Total credited for vol-seeded must be exactly 1024 (300 + 724), never
	// 1324 (which a double count against the cancelled seed would produce);
	// vol-other's independent 200 must be unaffected.
	want := aggregateLine(t, 1024+200, 1024+512, 2, 0)

	if !strings.Contains(buf.String(), want) {
		t.Errorf("SetCurrent seed/cancel not reflected correctly in aggregate:\ngot:  %q\nwant (contained): %q", buf.String(), want)
	}
}

func TestNonTTY_Wait_AlwaysEmitsFinalLine(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	// Use a very long interval so only Wait() emits a line.
	sink := New(buf, false, WithInterval(time.Hour))
	sink.Wait()

	// No streams and no SetVolumeTotal call: both volDone and volTotal stay 0.
	want := aggregateLine(t, 0, 0, 0, 0)

	if !strings.Contains(buf.String(), want) {
		t.Errorf("Wait() did not emit final line:\ngot:  %q\nwant (contained): %q", buf.String(), want)
	}
}

func TestNonTTY_NoMarks(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		dir      Direction
		fail     bool
		wantDone int
	}{
		{"Non-terminal output has no marks (download, success)", DirectionDownload, false, 2},
		{"Non-terminal output has no marks (download, interrupted)", DirectionDownload, true, 1},
		{"Non-terminal output has no marks (upload, success)", DirectionUpload, false, 2},
		{"Non-terminal output has no marks (upload, interrupted)", DirectionUpload, true, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			buf := &bytes.Buffer{}
			sink := New(buf, false, WithInterval(time.Hour), WithDirection(tc.dir))
			sink.SetVolumeTotal(2)

			completeTransfer(sink.NewStream("vol-a", 0))

			b := sink.NewStream("vol-b", 0)
			if tc.fail {
				interruptTransfer(b)
			} else {
				completeTransfer(b)
			}

			sink.Wait()

			out := buf.String()
			lines := strings.Split(strings.TrimRight(out, "\n"), "\n")

			wantBytes := int64(2048)
			if tc.fail {
				wantBytes = 1024 + 512
			}

			want := aggregateLineWithVerb(t, tc.dir.PastVerb, wantBytes, 2048, tc.wantDone, 2)
			if got := lines[len(lines)-1] + "\n"; got != want {
				t.Errorf("last line = %q, want %q", got, want)
			}

			if strings.ContainsAny(out, "✓✗") || containsBraille(out) {
				t.Errorf("non-terminal output must carry no marks or spinner frames\ngot: %q", out)
			}
		})
	}
}

func TestDecorateStatus(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		state int32
		stats decor.Statistics
		want  string
	}{
		{
			"waiting",
			streamStateWaiting,
			decor.Statistics{Total: 1024},
			"",
		},
		{
			"active",
			streamStateActive,
			decor.Statistics{Current: 512, Total: 1024},
			fmt.Sprintf(" % .1f / % .1f", decor.SizeB1024(512), decor.SizeB1024(1024)),
		},
		{
			"done",
			streamStateDone,
			decor.Statistics{Current: 1024, Total: 1024},
			"",
		},
		{
			"waiting_seeded_total_known",
			streamStateWaiting,
			decor.Statistics{Current: 256, Total: 1024},
			fmt.Sprintf(" % .1f / % .1f", decor.SizeB1024(256), decor.SizeB1024(1024)),
		},
		{
			"waiting_seeded_total_unknown",
			streamStateWaiting,
			decor.Statistics{Current: 256, Total: 0},
			fmt.Sprintf(" % .1f / ???", decor.SizeB1024(256)),
		},
		{
			"done_seeded_still_no_counters",
			streamStateDone,
			decor.Statistics{Current: 256, Total: 1024},
			"",
		},
		{
			"failed_seeded_still_no_counters",
			streamStateFailed,
			decor.Statistics{Current: 256, Total: 1024},
			"",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := decorateStatus(tc.state, tc.stats)
			if got != tc.want {
				t.Errorf("decorateStatus(%d, %+v) = %q, want %q", tc.state, tc.stats, got, tc.want)
			}
		})
	}
}

func TestDecorateAppend(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		state int32
		stats decor.Statistics
		want  string
	}{
		{"waiting", streamStateWaiting, decor.Statistics{Total: 1024}, ""},
		{"active_zero_total", streamStateActive, decor.Statistics{}, " 0%"},
		{"active_50pct", streamStateActive, decor.Statistics{Current: 512, Total: 1024}, " 50%"},
		{"active_100pct", streamStateActive, decor.Statistics{Current: 1024, Total: 1024}, " 100%"},
		{"done_no_percent", streamStateDone, decor.Statistics{Current: 1024, Total: 1024}, ""},
		{"waiting_seeded_pct", streamStateWaiting, decor.Statistics{Current: 512, Total: 1024}, " 50%"},
		{"waiting_seeded_total_unknown_no_pct", streamStateWaiting, decor.Statistics{Current: 512, Total: 0}, ""},
		{"done_seeded_still_no_percent", streamStateDone, decor.Statistics{Current: 512, Total: 1024}, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := decorateAppend(tc.state, tc.stats)
			if got != tc.want {
				t.Errorf("decorateAppend(%d, %+v) = %q, want %q", tc.state, tc.stats, got, tc.want)
			}
		})
	}
}

func TestStateWord(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		state     int32
		activated bool
		dir       Direction
		want      string
	}{
		{"waiting_download", streamStateWaiting, false, DirectionDownload, "Waiting for DataExport to be Ready"},
		{"active_download", streamStateActive, true, DirectionDownload, "Downloading"},
		{"done_after_activate_download", streamStateDone, true, DirectionDownload, "Download complete"},
		{"done_without_activate_download", streamStateDone, false, DirectionDownload, "Already exists"},
		{"failed_after_activate_download", streamStateFailed, true, DirectionDownload, "Interrupted"},
		{"failed_without_activate_download", streamStateFailed, false, DirectionDownload, "Interrupted"},
		{"waiting_upload", streamStateWaiting, false, DirectionUpload, "Waiting for DataImport to be Ready"},
		{"active_upload", streamStateActive, true, DirectionUpload, "Uploading"},
		{"done_after_activate_upload", streamStateDone, true, DirectionUpload, "Upload complete"},
		{"done_without_activate_upload", streamStateDone, false, DirectionUpload, "Already exists"},
		{"failed_after_activate_upload", streamStateFailed, true, DirectionUpload, "Interrupted"},
		{"failed_without_activate_upload", streamStateFailed, false, DirectionUpload, "Interrupted"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := stateWord(tc.state, tc.activated, tc.dir)
			if got != tc.want {
				t.Errorf("stateWord(%d, %t, %+v) = %q, want %q", tc.state, tc.activated, tc.dir, got, tc.want)
			}
		})
	}
}

// TestStateWordSyncWidth documents the column-alignment contract: the status-word
// column width is synced to the widest possible word ("Waiting for DataExport to
// be Ready" / "Waiting for DataImport to be Ready"), so every other state word
// fits within it and the bar/end-of-row starts at the same x. Run once per
// direction: a single sink only ever uses one Direction, so the two waiting
// phrases never have to share a synced column, but each direction's OWN column
// must not regress.
func TestStateWordSyncWidth(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		dir    Direction
		widest string
	}{
		{"download", DirectionDownload, "Waiting for DataExport to be Ready"},
		{"upload", DirectionUpload, "Waiting for DataImport to be Ready"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			words := []string{
				stateWord(streamStateWaiting, false, tc.dir),
				stateWord(streamStateActive, true, tc.dir),
				stateWord(streamStateDone, true, tc.dir),
				stateWord(streamStateDone, false, tc.dir),
			}

			for _, w := range words {
				if len(w) > len(tc.widest) {
					t.Errorf("stateWord %q wider (%d) than synced column width %q (%d)", w, len(w), tc.widest, len(tc.widest))
				}
			}

			if got := stateWord(streamStateWaiting, false, tc.dir); got != tc.widest {
				t.Errorf("expected the waiting word to be the widest %q, got %q", tc.widest, got)
			}
		})
	}
}

// TestStateWordSyncWidth_CrossDirectionIdenticalWidth pins the empirical fact the
// design relies on: "DataExport" and "DataImport" are the same rune length, so
// the waiting word — and therefore the synced status-word column width — is
// IDENTICAL across both directions (34 runes). This is what lets stateWord's doc
// comment claim the column geometry does not shift when the direction changes.
func TestStateWordSyncWidth_CrossDirectionIdenticalWidth(t *testing.T) {
	t.Parallel()

	download := stateWord(streamStateWaiting, false, DirectionDownload)
	upload := stateWord(streamStateWaiting, false, DirectionUpload)

	if len(download) != len(upload) {
		t.Errorf("waiting word width differs across directions: download=%q (%d), upload=%q (%d)",
			download, len(download), upload, len(upload))
	}
}

// TestWCSyncWidthAlignmentDirection pins the actual (counter-intuitive) alignment
// direction of mpb's width-sync decorator constants: decor.WCSyncWidthR (which
// carries the DindentRight bit) pads on the right and is therefore LEFT-aligned,
// while bare decor.WCSyncWidth (no DindentRight bit) pads on the left and is
// therefore RIGHT-aligned. This is the exact inversion that a previous bug
// mistakenly assumed backwards; internal/progress/multibar.go's name and
// stateWord decorators rely on WCSyncWidthR being LEFT-aligned, and its
// counters/percent decorators rely on WCSyncWidth being RIGHT-aligned. The
// DSyncWidth bit is deliberately never set here: it makes WC.Format block on an
// unbuffered sync-channel round-trip that requires a matching receiver, which
// would turn this into a hanging test for no benefit — alignment direction is
// fully determined by the DindentRight bit alone.
func TestWCSyncWidthAlignmentDirection(t *testing.T) {
	t.Parallel()

	t.Run("bare WCSyncWidth is right-aligned (leading padding)", func(t *testing.T) {
		t.Parallel()

		right := decor.WC{W: 10}
		right.Init()

		got, _ := right.Format("aaa")
		want := "       aaa"

		if got != want {
			t.Errorf("decor.WC{W: 10}.Format(%q) = %q, want %q", "aaa", got, want)
		}
	})

	t.Run("WCSyncWidthR is left-aligned (trailing padding)", func(t *testing.T) {
		t.Parallel()

		left := decor.WC{W: 10, C: decor.DindentRight}
		left.Init()

		got, _ := left.Format("aaa")
		want := "aaa       "

		if got != want {
			t.Errorf("decor.WC{W: 10, C: decor.DindentRight}.Format(%q) = %q, want %q", "aaa", got, want)
		}
	})

	t.Run("package constants carry the expected DindentRight bit", func(t *testing.T) {
		t.Parallel()

		if decor.WCSyncWidthR.C&decor.DindentRight == 0 {
			t.Error("decor.WCSyncWidthR must carry the DindentRight bit")
		}

		if decor.WCSyncWidth.C&decor.DindentRight != 0 {
			t.Error("decor.WCSyncWidth must NOT carry the DindentRight bit")
		}
	})
}

// TestSpinnerFrame asserts the pure frame selector cycles through
// spinnerFrames by tick % len, including wrap-around at and past the
// frame count. mpb refresh timing/terminal animation is intentionally not tested.
func TestSpinnerFrame(t *testing.T) {
	t.Parallel()

	n := uint64(len(spinnerFrames))

	cases := []struct {
		name string
		tick uint64
		want string
	}{
		{"first", 0, spinnerFrames[0]},
		{"last_before_wrap", n - 1, spinnerFrames[n-1]},
		{"wrap_to_first", n, spinnerFrames[0]},
		{"wrap_to_second", n + 1, spinnerFrames[1]},
		{"multi_wrap", 2*n + 3, spinnerFrames[3]},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := spinnerFrame(tc.tick); got != tc.want {
				t.Errorf("spinnerFrame(%d) = %q, want %q", tc.tick, got, tc.want)
			}
		})
	}
}

// TestSpinnerFrameAdvances asserts the animation is frame-by-frame deterministic:
// consecutive ticks move to the next frame across a full cycle and wrap cleanly.
func TestSpinnerFrameAdvances(t *testing.T) {
	t.Parallel()

	n := len(spinnerFrames)

	for i := 0; i < 2*n; i++ {
		got := spinnerFrame(uint64(i))
		want := spinnerFrames[i%n]

		if got != want {
			t.Errorf("tick %d: spinnerFrame = %q, want %q", i, got, want)
		}
	}
}

// TestSpinnerCell pins the status cell per state: an animated spinner frame
// while the volume is unfinished, a still mark once it settles, and the same
// display width in every state and runewidth mode.
func TestSpinnerCell(t *testing.T) {
	t.Parallel()

	const tick = uint64(3)

	cases := []struct {
		name      string
		dir       Direction
		state     int32
		activated bool
		wantWord  string
		// wantMark is the still mark of a settled row; empty means a spinner.
		wantMark string
	}{
		{"Spinner animates while volume waits for readiness", DirectionDownload, streamStateWaiting, false, "Waiting for DataExport to be Ready", ""},
		{"Spinner animates while volume data transfers", DirectionUpload, streamStateActive, true, "Uploading", ""},
		{"success: transferred volume shows check mark", DirectionDownload, streamStateDone, true, "Download complete", "✓"},
		{"success: already present volume shows check mark", DirectionDownload, streamStateDone, false, "Already exists", "✓"},
		{"error: interrupted transfer shows cross mark", DirectionDownload, streamStateFailed, true, "Interrupted", "✗"},
		{"error: volume interrupted while waiting shows cross mark", DirectionUpload, streamStateFailed, false, "Interrupted", "✗"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := stateWord(tc.state, tc.activated, tc.dir); got != tc.wantWord {
				t.Fatalf("stateWord = %q, want %q", got, tc.wantWord)
			}

			cell, next := spinnerCell(tc.state, tick), spinnerCell(tc.state, tick+1)

			if tc.wantMark == "" {
				if want := spinnerFrame(tick) + " "; cell != want {
					t.Errorf("cell = %q, want spinner frame %q", cell, want)
				}

				if cell == next {
					t.Errorf("cell did not change between ticks: %q", cell)
				}
			} else {
				if want := tc.wantMark + " "; cell != want || next != want {
					t.Errorf("cells = %q, %q, want still mark %q", cell, next, want)
				}
			}

			// A local Condition: mutating runewidth.DefaultCondition would race
			// with the other parallel tests.
			eastAsian := &runewidth.Condition{EastAsianWidth: true}

			for _, c := range []string{cell, next} {
				if w := runewidth.StringWidth(c); w != spinnerCellWidth {
					t.Errorf("width(%q) = %d, want %d", c, w, spinnerCellWidth)
				}

				if w := eastAsian.StringWidth(c); w != spinnerCellWidth {
					t.Errorf("East Asian width(%q) = %d, want %d", c, w, spinnerCellWidth)
				}
			}
		})
	}
}

// notifyWriter is a goroutine-safe buffer that signals every write, so a test
// can wait for the next mpb frame instead of sleeping.
type notifyWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	notify chan struct{}
}

func newNotifyWriter() *notifyWriter {
	return &notifyWriter{notify: make(chan struct{}, 1)}
}

func (w *notifyWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, err := w.buf.Write(p)
	w.mu.Unlock()

	select {
	case w.notify <- struct{}{}:
	default:
	}

	return n, err
}

func (w *notifyWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buf.String()
}

// TestTTYStream_SpinnerStateGating drives real ttyStreams through every state
// and checks the status cell and its rendered column.
func TestTTYStream_SpinnerStateGating(t *testing.T) {
	t.Parallel()

	t.Run("success: cell follows stream state", func(t *testing.T) {
		t.Parallel()

		sink := newTTYSink(&bytes.Buffer{}, DirectionDownload)

		st, ok := sink.NewStream("spin", 0).(*ttyStream)
		if !ok {
			t.Fatal("NewStream did not return *ttyStream")
		}

		failed, ok := sink.NewStream("failed", 0).(*ttyStream)
		if !ok {
			t.Fatal("NewStream did not return *ttyStream")
		}

		cellOf := func(s *ttyStream) string {
			return spinnerCell(atomic.LoadInt32(&s.state), atomic.AddUint64(&s.spinTick, 1))
		}

		if cell := cellOf(st); !containsBraille(cell) {
			t.Errorf("waiting stream cell = %q, want a spinner frame", cell)
		}

		st.Activate()

		if cell := cellOf(st); !containsBraille(cell) {
			t.Errorf("active stream cell = %q, want a spinner frame", cell)
		}

		st.Done()

		if cell := cellOf(st); cell != "✓ " {
			t.Errorf("done stream cell = %q, want %q", cell, "✓ ")
		}

		failed.Fail()

		if cell := cellOf(failed); cell != "✗ " {
			t.Errorf("failed stream cell = %q, want %q", cell, "✗ ")
		}

		sink.Wait()
	})

	t.Run("Status column keeps constant width", func(t *testing.T) {
		t.Parallel()

		rows := []struct{ name, word string }{
			{"waiting-volume", "Waiting for DataExport to be Ready"},
			{"act", "Downloading"},
			{"done-vol", "Download complete"},
			{"failed-volume-with-a-long-name", "Interrupted"},
		}

		w := newNotifyWriter()
		sink := newRenderSink(w, DirectionDownload)
		sink.SetVolumeTotal(len(rows))

		waiting := sink.NewStream(rows[0].name, 0)
		active := sink.NewStream(rows[1].name, 0)
		completeTransfer(sink.NewStream(rows[2].name, 0))
		sink.NewStream(rows[3].name, 0).Fail()

		active.Activate()
		active.SetTotal(1024)
		active.IncrBy(512)

		t.Cleanup(func() {
			waiting.Fail()
			active.Fail()
			sink.Wait()
		})

		// wordColumns returns the display column of each row's state word in the
		// newest frame showing all four states at once.
		wordColumns := func() ([]int, bool) {
			frames := renderedFrames(w.String())

		frameLoop:
			for i := len(frames) - 1; i >= 0; i-- {
				cols := make([]int, 0, len(rows))

				for _, r := range rows {
					row, ok := findRow(frames[i], r.name)
					idx := strings.Index(row, r.word)

					if !ok || idx < 0 {
						continue frameLoop
					}

					cols = append(cols, runewidth.StringWidth(row[:idx]))
				}

				return cols, true
			}

			return nil, false
		}

		deadline := time.After(5 * time.Second)

		for {
			if cols, ok := wordColumns(); ok {
				for i, c := range cols {
					if c != cols[0] {
						t.Errorf("state word of %q starts at column %d, want %d (as %q)", rows[i].name, c, cols[0], rows[0].name)
					}
				}

				return
			}

			select {
			case <-w.notify:
			case <-deadline:
				t.Fatalf("no frame showed all four states within 5s\nraw: %q", w.String())
			}
		}
	})
}

func TestStateBarFiller(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		state     int32
		wantEmpty bool
	}{
		{"waiting_empty", streamStateWaiting, true},
		{"active_bar", streamStateActive, false},
		{"done_empty", streamStateDone, true},
		{"failed_empty", streamStateFailed, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			state := tc.state
			filler := stateBarFiller{
				state: &state,
				inner: mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]").Build(),
			}

			var buf bytes.Buffer

			stats := decor.Statistics{Current: 5, Total: 10, AvailableWidth: ttyBarWidth, RequestedWidth: ttyBarWidth}
			if err := filler.Fill(&buf, stats); err != nil {
				t.Fatalf("Fill returned error: %v", err)
			}

			got := buf.String()
			if tc.wantEmpty {
				if got != "" {
					t.Errorf("state %d: Fill wrote %q, want empty", tc.state, got)
				}

				return
			}

			if !strings.Contains(got, "[") || !strings.Contains(got, "]") {
				t.Errorf("state %d: Fill = %q, want a bracketed bar containing '[' and ']'", tc.state, got)
			}
		})
	}
}

// TestNameCell asserts the leaf name is rendered in full with a single trailing
// separator space and is NEVER truncated with a '…'. Names far longer than the
// old fixed 24-rune column must pass through verbatim (decor.WCSyncWidth sizes
// the column to the widest name at render time).
func TestNameCell(t *testing.T) {
	t.Parallel()

	longName := "nss-child-2b8d1e2b97271demovmdisk-1c2f0cb1b1ad-very-long-leaf-name"

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", " "},
		{"short_ascii", "ab", "ab "},
		{"long_unbounded", longName, longName + " "},
		{"rune_name", "абвгд", "абвгд "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := nameCell(tc.input)
			if got != tc.want {
				t.Errorf("nameCell(%q) = %q, want %q", tc.input, got, tc.want)
			}

			if strings.ContainsRune(got, '…') {
				t.Errorf("nameCell(%q) = %q contains an ellipsis; names must never be truncated", tc.input, got)
			}
		})
	}
}

// TestTTYSink_StateMachine drives the waiting→active→done and waiting→done (resume
// skip) transitions through the real ttyStream and asserts the activated flag and
// final state words, then drains the sink. With the summary header removed there
// are no aggregate counters to observe, so the state is read from stateWord.
func TestTTYSink_StateMachine(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := newTTYSink(buf, DirectionDownload)

	s1, ok := sink.NewStream("stream-1", 0).(*ttyStream)
	if !ok {
		t.Fatal("NewStream did not return *ttyStream")
	}

	s2, ok := sink.NewStream("stream-2", 0).(*ttyStream)
	if !ok {
		t.Fatal("NewStream did not return *ttyStream")
	}

	wordOf := func(s *ttyStream) string {
		return stateWord(atomic.LoadInt32(&s.state), atomic.LoadInt32(&s.activated) == 1, DirectionDownload)
	}

	if got := wordOf(s1); got != "Waiting for DataExport to be Ready" {
		t.Errorf("fresh stream word = %q, want Waiting for DataExport to be Ready", got)
	}

	// waiting → active → done: a real download ends as "Download complete".
	s1.Activate()

	if got := wordOf(s1); got != "Downloading" {
		t.Errorf("activated stream word = %q, want Downloading", got)
	}

	s1.Done()

	if got := wordOf(s1); got != "Download complete" {
		t.Errorf("downloaded stream word = %q, want Download complete", got)
	}

	// waiting → done without Activate: a resume skip ends as "Already exists".
	s2.Done()

	if got := wordOf(s2); got != "Already exists" {
		t.Errorf("resume-skipped stream word = %q, want Already exists", got)
	}

	sink.Wait()
}

// TestTTYStream_ActivateIdempotent verifies that a duplicate Activate keeps the
// stream active and the activated flag set (exactly-once state transition), so a
// re-issued Activate never regresses the row word.
func TestTTYStream_ActivateIdempotent(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := newTTYSink(buf, DirectionDownload)

	st, ok := sink.NewStream("dup", 0).(*ttyStream)
	if !ok {
		t.Fatal("NewStream did not return *ttyStream")
	}

	st.Activate()
	st.Activate()

	if got := atomic.LoadInt32(&st.state); got != streamStateActive {
		t.Errorf("state after double Activate = %d, want active(%d)", got, streamStateActive)
	}

	if got := atomic.LoadInt32(&st.activated); got != 1 {
		t.Errorf("activated after double Activate = %d, want 1", got)
	}

	st.Done()
	sink.Wait()
}

// TestTTYStream_SetCurrent_SeedsAndCancels proves the ttyStream side of
// SetCurrent's absolute-value contract: SetCurrent(n) sets the underlying mpb
// bar's current value directly (observable via stats.Current fed into
// decorateStatus/decorateAppend), and a later SetCurrent(0) resets it again
// cleanly — the downward correction the resume-seed clamp applies before the
// real transfer's incremental crediting proceeds from zero
// (clamp-resume-seed-to-fresh-total). This exercises the actual *Bar.SetCurrent
// call (not just the pure decorator functions TestDecorateStatus/
// TestDecorateAppend already cover), so a regression in the mpb-facing plumbing
// itself is caught.
func TestTTYStream_SetCurrent_SeedsAndCancels(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := newTTYSink(buf, DirectionDownload)

	st, ok := sink.NewStream("seed", 0).(*ttyStream)
	if !ok {
		t.Fatal("NewStream did not return *ttyStream")
	}

	// Seed while still waiting, before Activate — mirrors precreateStreams
	// seeding a fresh stream immediately after creation.
	st.SetCurrent(256)

	if got := st.bar.Current(); got != 256 {
		t.Errorf("Current after seed = %d, want 256", got)
	}

	// Reset to zero, mirroring the resume-seed clamp's downward correction in
	// downloadBlock/downloadFS (SetCurrent(0) when the seed exceeds the fresh total).
	st.SetCurrent(0)

	if got := st.bar.Current(); got != 0 {
		t.Errorf("Current after cancel = %d, want 0", got)
	}

	// The real transfer's own incremental crediting proceeds from the
	// cancelled baseline exactly as if no seed had ever been applied.
	st.Activate()
	st.SetTotal(256)
	st.IncrBy(256)

	if got := st.bar.Current(); got != 256 {
		t.Errorf("Current after real crediting = %d, want 256 (no double count against the cancelled seed)", got)
	}

	st.Done()
	sink.Wait()
}

// TestTTYSink_NoSummaryHeader guards the summary-cleanup decision: a TTY run must
// never reintroduce the OLD removed header wording ("preparing exports"/"exports
// ready") or the old "waiting…" counter. It deliberately does NOT ban the NEW
// "volumes downloaded" summary bar text — that bar is a distinct, later
// product decision (see
// TestVolumeCounterLabel and the TTY/non-TTY count-once tests below for its
// dedicated coverage). mpb terminal frames are not asserted here; we only assert
// the rendered output never contains the removed header strings.
func TestTTYSink_NoSummaryHeader(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := New(buf, true)

	// Mirror the pipeline's usage: streams are always registered with total=0 and
	// the real total is supplied later via SetTotal. (A bar created with total>0
	// enables mpb's trigger-complete, after which Done's SetTotal(_, true) is a
	// no-op — the resume-skip stream would never complete and Wait would hang.)
	s1 := sink.NewStream("layer-a", 0)
	s2 := sink.NewStream("layer-b", 0)

	s1.Activate()
	s1.SetTotal(1024)
	s1.IncrBy(1024)
	s1.Done()

	// s2 is a resume skip: Done without Activate.
	s2.Done()

	sink.Wait()

	out := buf.String()

	for _, banned := range []string{"preparing exports", "exports ready", "waiting\u2026"} {
		if strings.Contains(out, banned) {
			t.Errorf("TTY output must not contain removed header text %q\ngot: %q", banned, out)
		}
	}
}

// TestVolumeCounterLabel pins the exact text of the "N/M volumes <verb>"
// summary bar as a pure function, independent of any mpb rendering.
func TestVolumeCounterLabel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		done  int
		total int
		verb  string
		want  string
	}{
		{name: "no volumes in scope renders nothing", done: 0, total: 0, verb: "downloaded", want: ""},
		{name: "none done yet", done: 0, total: 4, verb: "downloaded", want: " 0/4 volumes downloaded"},
		{name: "partially done", done: 2, total: 4, verb: "downloaded", want: " 2/4 volumes downloaded"},
		{name: "fully done", done: 4, total: 4, verb: "downloaded", want: " 4/4 volumes downloaded"},
		{name: "Download summary says downloaded", done: 1, total: 2, verb: DirectionDownload.PastVerb, want: " 1/2 volumes downloaded"},
		{name: "Upload summary says uploaded", done: 1, total: 2, verb: DirectionUpload.PastVerb, want: " 1/2 volumes uploaded"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := volumeCounterLabel(tc.done, tc.total, tc.verb); got != tc.want {
				t.Errorf("volumeCounterLabel(%d, %d, %q) = %q, want %q", tc.done, tc.total, tc.verb, got, tc.want)
			}
		})
	}
}

func TestSummaryCell(t *testing.T) {
	t.Parallel()

	const tick = uint64(4)

	frame := spinnerFrame(tick)

	cases := []struct {
		name      string
		done      int
		total     int
		completed bool
		verb      string
		want      string
	}{
		{"success: no volumes in scope renders nothing", 0, 0, false, "downloaded", ""},
		{"success: no volumes in scope renders nothing after completion", 0, 0, true, "downloaded", ""},
		{"success: live counter shows spinner", 1, 3, false, "downloaded", " 1/3 volumes downloaded " + frame},
		{"success: live counter with all done still spins", 3, 3, false, "downloaded", " 3/3 volumes downloaded " + frame},
		{"success: all volumes done shows check mark", 3, 3, true, "downloaded", " 3/3 volumes downloaded ✓"},
		{"success: single volume done shows check mark", 1, 1, true, "uploaded", " 1/1 volumes uploaded ✓"},
		{"error: some volumes not done shows cross mark", 2, 3, true, "downloaded", " 2/3 volumes downloaded ✗"},
		{"error: no volumes done shows cross mark", 0, 2, true, "uploaded", " 0/2 volumes uploaded ✗"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := summaryCell(tc.done, tc.total, tc.completed, tick, tc.verb)
			if got != tc.want {
				t.Errorf("summaryCell(%d, %d, %t, %d, %q) = %q, want %q",
					tc.done, tc.total, tc.completed, tick, tc.verb, got, tc.want)
			}
		})
	}
}

// TestTTYSink_VolumeCounter_CountsOnce proves that the TTY sink's N/M
// volume-counter counts BOTH a real download (Activate then Done) and a resume
// skip (Done without Activate) exactly once each, and that a duplicate Done()
// call does not double-count. Per this package's testing philosophy, mpb
// terminal frames are not asserted (writing to a bytes.Buffer instead of a real
// terminal means mpb's renderer never flushes a frame, so the buffer stays
// empty even on a passing run — see TestTTYSink_NoSummaryHeader); instead this
// test asserts the sink's own observable counter state directly, and also
// exercises volumeCounterLabel with the resulting (done, total) pair to pin
// what WOULD be rendered.
func TestTTYSink_VolumeCounter_CountsOnce(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := New(buf, true)

	ts, ok := sink.(*ttySink)
	if !ok {
		t.Fatal("New(_, true) did not return *ttySink")
	}

	ts.SetVolumeTotal(2)

	// Streams are registered with total=0, mirroring the pipeline: a bar created
	// with total>0 enables mpb's trigger-complete, after which Done's
	// SetTotal(_, true) is a no-op and a resume-skip stream would never settle,
	// hanging Wait (see TestTTYSink_NoSummaryHeader).
	a := sink.NewStream("stream-a", 0)
	b := sink.NewStream("stream-b", 0)

	// Stream A: a real download (waiting -> active -> done).
	a.Activate()
	a.SetTotal(1024)
	a.IncrBy(1024)
	a.Done()

	// Duplicate Done on A must not double-count.
	a.Done()

	// Stream B: a resume skip (waiting -> done without Activate).
	b.Done()

	sink.Wait()

	got := ts.volDone.Load()
	if got != 2 {
		t.Errorf("volDone = %d, want 2 (one real download + one resume skip, duplicate Done not counted)", got)
	}

	wantLabel := " 2/2 volumes downloaded"
	if label := volumeCounterLabel(int(got), int(ts.volTotal.Load()), ts.dir.PastVerb); label != wantLabel {
		t.Errorf("volumeCounterLabel(%d, %d, %q) = %q, want %q", got, ts.volTotal.Load(), ts.dir.PastVerb, label, wantLabel)
	}
}

// TestTTYSink_Fail_ExcludesFromVolumeCounter proves that Fail() (unlike Done())
// does not count a stream toward "N/M volumes downloaded", renders the
// "Interrupted" state word, and still unblocks Wait() so a failed stream never
// hangs the run alongside successfully completed ones.
func TestTTYSink_Fail_ExcludesFromVolumeCounter(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := New(buf, true)

	ts, ok := sink.(*ttySink)
	if !ok {
		t.Fatal("New(_, true) did not return *ttySink")
	}

	ts.SetVolumeTotal(2)

	a := sink.NewStream("stream-a", 0)
	b := sink.NewStream("stream-b", 0)

	// Stream A: activated then interrupted mid-transfer.
	a.Activate()
	a.SetTotal(1024)
	a.IncrBy(512)
	a.Fail()

	// Stream B: completes normally.
	b.Done()

	sink.Wait()

	got := ts.volDone.Load()
	if got != 1 {
		t.Errorf("volDone = %d, want 1 (failed stream must not be counted)", got)
	}

	streamA, ok := a.(*ttyStream)
	if !ok {
		t.Fatal("NewStream did not return *ttyStream")
	}

	state := atomic.LoadInt32(&streamA.state)
	if state != streamStateFailed {
		t.Errorf("stream A state = %d, want streamStateFailed (%d)", state, streamStateFailed)
	}

	activated := atomic.LoadInt32(&streamA.activated) == 1
	if word := stateWord(state, activated, DirectionDownload); word != "Interrupted" {
		t.Errorf("stateWord for failed stream = %q, want %q", word, "Interrupted")
	}
}

// TestTTYSink_Fail_FromWaiting proves that Fail() may be called directly from
// the waiting state (before Activate — e.g. the stream's DataExport never
// became Ready) without panicking and without counting toward volDone.
func TestTTYSink_Fail_FromWaiting(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := New(buf, true)

	ts, ok := sink.(*ttySink)
	if !ok {
		t.Fatal("New(_, true) did not return *ttySink")
	}

	ts.SetVolumeTotal(1)

	a := sink.NewStream("stream-a", 0)
	a.Fail()

	sink.Wait()

	if got := ts.volDone.Load(); got != 0 {
		t.Errorf("volDone = %d, want 0 (stream failed from waiting, never counted)", got)
	}
}

// TestNonTTY_Fail_ExcludesFromVolumeCounter mirrors
// TestTTYSink_Fail_ExcludesFromVolumeCounter for the non-TTY fallback sink: the
// final aggregate line's "(N/M volumes)" suffix must count only the Done()
// stream, not the Fail()ed one.
func TestNonTTY_Fail_ExcludesFromVolumeCounter(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := New(buf, false, WithInterval(time.Hour))

	sink.SetVolumeTotal(2)

	a := sink.NewStream("vol-a", 100)
	b := sink.NewStream("vol-b", 200)

	a.IncrBy(100)
	a.Done()

	b.IncrBy(200)
	b.Fail()

	sink.Wait()

	got := buf.String()
	want := aggregateLine(t, 300, 300, 1, 2)

	if !strings.Contains(got, want) {
		t.Errorf("output does not contain expected volume-counter line\ngot:  %q\nwant (contained): %q", got, want)
	}
}

// TestStream_DoneThenFail_FirstOutcomeWins pins the documented misuse
// behaviour: whichever terminal method is called FIRST decides whether the
// stream counts, and the second call is a no-op in both directions and on
// both sinks — no panic, no double count.
func TestStream_DoneThenFail_FirstOutcomeWins(t *testing.T) {
	t.Parallel()

	t.Run("TTY: Done then Fail keeps the Done outcome", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		sink := New(buf, true)

		ts, ok := sink.(*ttySink)
		if !ok {
			t.Fatal("New(_, true) did not return *ttySink")
		}

		ts.SetVolumeTotal(1)

		a := sink.NewStream("stream-a", 0)
		a.Done()
		a.Fail()

		sink.Wait()

		if got := ts.volDone.Load(); got != 1 {
			t.Errorf("volDone = %d, want 1 (Done fired first, Fail must be a no-op)", got)
		}
	})

	t.Run("TTY: Fail then Done keeps the Fail outcome", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		sink := New(buf, true)

		ts, ok := sink.(*ttySink)
		if !ok {
			t.Fatal("New(_, true) did not return *ttySink")
		}

		ts.SetVolumeTotal(1)

		a := sink.NewStream("stream-a", 0)
		a.Fail()
		a.Done()

		sink.Wait()

		if got := ts.volDone.Load(); got != 0 {
			t.Errorf("volDone = %d, want 0 (Fail fired first, Done must be a no-op)", got)
		}
	})

	t.Run("non-TTY: Done then Fail keeps the Done outcome", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		sink := New(buf, false, WithInterval(time.Hour))
		sink.SetVolumeTotal(1)

		a := sink.NewStream("vol-a", 100)
		a.IncrBy(100)
		a.Done()
		a.Fail()

		sink.Wait()

		got := buf.String()
		want := aggregateLine(t, 100, 100, 1, 1)

		if !strings.Contains(got, want) {
			t.Errorf("output does not contain expected line\ngot:  %q\nwant (contained): %q", got, want)
		}
	})

	t.Run("non-TTY: Fail then Done keeps the Fail outcome", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		sink := New(buf, false, WithInterval(time.Hour))
		sink.SetVolumeTotal(1)

		a := sink.NewStream("vol-a", 100)
		a.IncrBy(100)
		a.Fail()
		a.Done()

		sink.Wait()

		got := buf.String()
		want := aggregateLine(t, 100, 100, 0, 1)

		if !strings.Contains(got, want) {
			t.Errorf("output does not contain expected line\ngot:  %q\nwant (contained): %q", got, want)
		}
	})
}

// TestNonTTY_VolumeCounter_FormatAndCountOnce asserts the non-TTY aggregate
// line's "(N/M volumes)" suffix uses SetVolumeTotal for M, counts each stream's
// Done() exactly once for N even when Done is called twice on one stream, and
// matches the aggregateLine helper's format.
func TestNonTTY_VolumeCounter_FormatAndCountOnce(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	// Long interval so only Wait() emits, keeping the assertion deterministic.
	sink := New(buf, false, WithInterval(time.Hour))

	sink.SetVolumeTotal(3)

	s1 := sink.NewStream("vol-a", 100)
	s2 := sink.NewStream("vol-b", 200)
	s3 := sink.NewStream("vol-c", 300)

	s1.IncrBy(100)
	s2.IncrBy(200)
	s3.IncrBy(300)

	s1.Done()
	// Duplicate Done on s1 must not push the count past 3.
	s1.Done()
	s2.Done()
	s3.Done()

	sink.Wait()

	got := buf.String()
	want := aggregateLine(t, 600, 600, 3, 3)

	if !strings.Contains(got, want) {
		t.Errorf("output does not contain expected volume-counter line\ngot:  %q\nwant (contained): %q", got, want)
	}

	if strings.Count(got, "(4/3 volumes)") != 0 {
		t.Errorf("duplicate Done() overcounted volumes:\ngot: %q", got)
	}
}

// TestVolumeCounter_ZeroVolumes covers the empty-selection / manifest-only-tree
// edge case: SetVolumeTotal(0) with no streams. The TTY sink must render no
// "volumes downloaded" text at all (volumeCounterLabel returns "" for total==0),
// and the non-TTY sink's final line must show "(0/0 volumes)".
func TestVolumeCounter_ZeroVolumes(t *testing.T) {
	t.Parallel()

	t.Run("tty renders no volume-counter text", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		sink := New(buf, true)
		sink.SetVolumeTotal(0)
		sink.Wait()

		if strings.Contains(buf.String(), "volumes downloaded") {
			t.Errorf("TTY output must not render volume-counter text when total==0\ngot: %q", buf.String())
		}
	})

	t.Run("non-tty shows 0/0 volumes", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		sink := New(buf, false, WithInterval(time.Hour))
		sink.SetVolumeTotal(0)
		sink.Wait()

		want := aggregateLine(t, 0, 0, 0, 0)

		if !strings.Contains(buf.String(), want) {
			t.Errorf("non-TTY output does not contain expected zero-volume line\ngot:  %q\nwant (contained): %q", buf.String(), want)
		}
	})
}

// summaryLines extracts every rendered line of the volume-counter summary bar
// from a raw mpb output buffer, in render order. Each render cycle rewrites
// the previous frame in place (mpb interleaves cursor-reset escape sequences
// between frames), so splitting on newlines and filtering for the marker text
// (e.g. "volumes downloaded") recovers exactly one summary line per frame
// regardless of the surrounding control bytes.
func summaryLines(raw, marker string) []string {
	lines := strings.Split(raw, "\n")
	out := make([]string, 0, len(lines))

	for _, line := range lines {
		if strings.Contains(line, marker) {
			out = append(out, line)
		}
	}

	return out
}

// summaryLineGap returns the number of space runes between the end of the
// marker counter text and the next non-space rune on the same rendered line
// (the spinner frame or the outcome mark).
func summaryLineGap(t *testing.T, line, marker string) int {
	t.Helper()

	idx := strings.Index(line, marker)
	if idx < 0 {
		t.Fatalf("line %q does not contain %q", line, marker)
	}

	rest := line[idx+len(marker):]

	gap := 0
	for _, r := range rest {
		if r != ' ' {
			break
		}

		gap++
	}

	return gap
}

// TestTTYSink_SummaryBarSpinnerAdjacency pins that the summary spinner, and the
// mark replacing it, sit right after the "N/M volumes <verb>" text on every
// rendered frame instead of drifting across the terminal.
func TestTTYSink_SummaryBarSpinnerAdjacency(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		dir  Direction
	}{
		{"success: download summary", DirectionDownload},
		{"success: upload summary", DirectionUpload},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			marker := "volumes " + tc.dir.PastVerb

			buf := &bytes.Buffer{}
			sink := newRenderSink(buf, tc.dir)
			sink.SetVolumeTotal(2)

			completeTransfer(sink.NewStream("stream-a", 0))

			// stream-b is a resume skip: Done without Activate.
			sink.NewStream("stream-b", 0).Done()

			sink.Wait()

			lines := summaryLines(buf.String(), marker)
			if len(lines) == 0 {
				t.Fatal("no rendered volume-counter summary lines captured")
			}

			for i, line := range lines {
				if gap := summaryLineGap(t, line, marker); gap > 1 {
					t.Errorf("frame %d: gap between %q and spinner = %d, want <= 1\nline: %q", i, marker, gap, line)
				}
			}

			last := lines[len(lines)-1]
			if want := "2/2 " + marker; !strings.Contains(last, want) {
				t.Errorf("final rendered summary line = %q, want to contain %q", last, want)
			}
		})
	}
}

// frameSeparator is the cursor-up + erase-down sequence mpb writes before
// redrawing a frame in place.
var frameSeparator = regexp.MustCompile("\x1b\\[\\d+A\x1b\\[J")

// newRenderSink builds a ttySink whose frames really reach w: mpb renders to a
// non-terminal writer only with WithAutoRefresh.
func newRenderSink(w io.Writer, dir Direction) *ttySink {
	p := mpb.New(mpb.WithOutput(w), mpb.WithAutoRefresh(), mpb.WithRefreshRate(5*time.Millisecond))

	return &ttySink{p: p, dir: dir}
}

// renderedFrames splits raw mpb output into frames, each a slice of rows.
func renderedFrames(raw string) [][]string {
	chunks := frameSeparator.Split(raw, -1)
	frames := make([][]string, 0, len(chunks))

	for _, chunk := range chunks {
		if chunk = strings.TrimRight(chunk, "\n"); chunk != "" {
			frames = append(frames, strings.Split(chunk, "\n"))
		}
	}

	return frames
}

// lastFrame returns the rows of the final frame left on screen.
func lastFrame(t *testing.T, raw string) []string {
	t.Helper()

	frames := renderedFrames(raw)
	if len(frames) == 0 {
		t.Fatalf("no rendered frames captured\nraw: %q", raw)
	}

	return frames[len(frames)-1]
}

// findRow returns the row of frame whose first field is name.
func findRow(frame []string, name string) (string, bool) {
	for _, row := range frame {
		if fields := strings.Fields(row); len(fields) > 0 && fields[0] == name {
			return row, true
		}
	}

	return "", false
}

// findSummaryRow returns the row of frame carrying the "volumes <verb>" counter.
func findSummaryRow(frame []string) (string, bool) {
	for _, row := range frame {
		if strings.Contains(row, "volumes "+DirectionDownload.PastVerb) || strings.Contains(row, "volumes "+DirectionUpload.PastVerb) {
			return row, true
		}
	}

	return "", false
}

// statusGlyph returns the status-column content of a volume row: the field
// right after the name, or the first state word when the column is blank.
func statusGlyph(row string) string {
	fields := strings.Fields(row)
	if len(fields) < 2 {
		return ""
	}

	return fields[1]
}

func containsBraille(s string) bool {
	return strings.ContainsFunc(s, func(r rune) bool { return r >= 0x2800 && r <= 0x28FF })
}

// completeTransfer drives s through a full real transfer: waiting → active → done.
func completeTransfer(s Stream) {
	s.Activate()
	s.SetTotal(1024)
	s.IncrBy(1024)
	s.Done()
}

// interruptTransfer drives s into the active state and fails it mid-copy.
func interruptTransfer(s Stream) {
	s.Activate()
	s.SetTotal(1024)
	s.IncrBy(512)
	s.Fail()
}

func TestTTYSink_SummaryMark(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		volumes int
		drive   func(streams []Stream)
		want    string
	}{
		{
			name:    "Summary shows check mark when all volumes completed",
			volumes: 3,
			drive: func(streams []Stream) {
				for _, s := range streams {
					completeTransfer(s)
				}
			},
			want: " 3/3 volumes downloaded ✓",
		},
		{
			name:    "Summary shows check mark when all volumes already present",
			volumes: 2,
			drive: func(streams []Stream) {
				for _, s := range streams {
					s.Done()
				}
			},
			want: " 2/2 volumes downloaded ✓",
		},
		{
			name:    "Summary shows cross mark when a volume failed",
			volumes: 3,
			drive: func(streams []Stream) {
				completeTransfer(streams[0])
				completeTransfer(streams[1])
				interruptTransfer(streams[2])
			},
			want: " 2/3 volumes downloaded ✗",
		},
		{
			name:    "Summary shows cross mark after interrupt",
			volumes: 3,
			drive: func(streams []Stream) {
				completeTransfer(streams[0])

				streams[1].Activate()
				streams[1].SetTotal(1024)
				streams[1].IncrBy(256)

				// Mirrors the pipeline's post-cancel sweep over every stream.
				for _, s := range streams {
					s.Fail()
				}
			},
			want: " 1/3 volumes downloaded ✗",
		},
		{
			name:    "Summary mark follows volume count, not exit status",
			volumes: 2,
			drive: func(streams []Stream) {
				completeTransfer(streams[0])
				streams[1].Done()
			},
			want: " 2/2 volumes downloaded ✓",
		},
		{
			name:    "error: volume failed while waiting gives cross mark",
			volumes: 2,
			drive: func(streams []Stream) {
				completeTransfer(streams[0])
				streams[1].Fail()
			},
			want: " 1/2 volumes downloaded ✗",
		},
		{
			name:    "error: Done after Fail keeps cross mark",
			volumes: 1,
			drive: func(streams []Stream) {
				streams[0].Fail()
				streams[0].Done()
			},
			want: " 0/1 volumes downloaded ✗",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			buf := &bytes.Buffer{}
			sink := newRenderSink(buf, DirectionDownload)
			sink.SetVolumeTotal(tc.volumes)

			streams := make([]Stream, 0, tc.volumes)
			for i := range tc.volumes {
				streams = append(streams, sink.NewStream(fmt.Sprintf("vol-%d", i), 0))
			}

			tc.drive(streams)
			sink.Wait()

			frame := lastFrame(t, buf.String())

			row, ok := findSummaryRow(frame)
			if !ok {
				t.Fatalf("final frame has no summary row\nframe: %q", frame)
			}

			if strings.TrimRight(row, " ") != tc.want {
				t.Errorf("final summary row = %q, want %q", row, tc.want)
			}

			if containsBraille(strings.Join(frame, "\n")) {
				t.Errorf("final frame still shows a spinner glyph\nframe: %q", frame)
			}
		})
	}
}

func TestTTYSink_VolumeRowMark(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		dir      Direction
		drive    func(s Stream)
		wantMark string
		wantWord string
	}{
		{"Downloaded volume shows check mark", DirectionDownload, completeTransfer, "✓", "Download complete"},
		{"Uploaded volume shows check mark", DirectionUpload, completeTransfer, "✓", "Upload complete"},
		{"Already present volume shows check mark", DirectionDownload, func(s Stream) { s.Done() }, "✓", "Already exists"},
		{"Volume interrupted during transfer shows cross mark", DirectionDownload, interruptTransfer, "✗", "Interrupted"},
		{"Volume interrupted while waiting shows cross mark", DirectionUpload, func(s Stream) { s.Fail() }, "✗", "Interrupted"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			buf := &bytes.Buffer{}
			sink := newRenderSink(buf, tc.dir)
			sink.SetVolumeTotal(1)

			tc.drive(sink.NewStream("vol-a", 0))
			sink.Wait()

			frame := lastFrame(t, buf.String())

			row, ok := findRow(frame, "vol-a")
			if !ok {
				t.Fatalf("final frame has no row for vol-a\nframe: %q", frame)
			}

			if got := statusGlyph(row); got != tc.wantMark {
				t.Errorf("status column = %q, want %q\nrow: %q", got, tc.wantMark, row)
			}

			if !strings.Contains(row, tc.wantWord) {
				t.Errorf("row %q does not contain state %q", row, tc.wantWord)
			}
		})
	}
}

func TestTTYSink_SummaryVerb(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		dir       Direction
		want      string
		forbidden string
	}{
		{"Download summary says downloaded", DirectionDownload, "1/2 volumes downloaded", "uploaded"},
		{"Upload summary says uploaded", DirectionUpload, "1/2 volumes uploaded", "downloaded"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			buf := &bytes.Buffer{}
			sink := newRenderSink(buf, tc.dir)
			sink.SetVolumeTotal(2)

			completeTransfer(sink.NewStream("vol-a", 0))
			sink.NewStream("vol-b", 0).Fail()
			sink.Wait()

			row, ok := findSummaryRow(lastFrame(t, buf.String()))
			if !ok {
				t.Fatalf("final frame has no summary row\nraw: %q", buf.String())
			}

			if !strings.Contains(row, tc.want) {
				t.Errorf("summary row = %q, want to contain %q", row, tc.want)
			}

			if strings.Contains(buf.String(), tc.forbidden) {
				t.Errorf("output must not contain %q\nraw: %q", tc.forbidden, buf.String())
			}
		})
	}
}

func TestTTYSink_SummaryAboveVolumeRows(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		volumes int
	}{
		{"Summary line is rendered above volume rows (one volume)", 1},
		{"Summary line is rendered above volume rows (volumes finish in reverse order)", 4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			buf := &bytes.Buffer{}
			sink := newRenderSink(buf, DirectionDownload)
			sink.SetVolumeTotal(tc.volumes)

			streams := make([]Stream, 0, tc.volumes)
			for i := range tc.volumes {
				streams = append(streams, sink.NewStream(fmt.Sprintf("vol-%d", i), 0))
			}

			for i := len(streams) - 1; i >= 0; i-- {
				if i%2 == 0 {
					completeTransfer(streams[i])
				} else {
					streams[i].Done()
				}
			}

			sink.Wait()

			frames := renderedFrames(buf.String())
			if len(frames) == 0 {
				t.Fatalf("no rendered frames captured\nraw: %q", buf.String())
			}

			for i, frame := range frames {
				summaryIdx := slices.IndexFunc(frame, func(row string) bool {
					return strings.Contains(row, "volumes downloaded")
				})

				for j, row := range frame {
					if fields := strings.Fields(row); len(fields) == 0 || !strings.HasPrefix(fields[0], "vol-") {
						continue
					}

					if summaryIdx < 0 || j < summaryIdx {
						t.Errorf("frame %d: volume row %d %q precedes the summary row (index %d)\nframe: %q", i, j, row, summaryIdx, frame)
					}
				}
			}

			if last := frames[len(frames)-1]; !strings.Contains(last[0], "volumes downloaded") {
				t.Errorf("final frame starts with %q, want the summary row\nframe: %q", last[0], last)
			}
		})
	}
}

// TestTTYSink_Wait_UnfinalizedStreamBlocksUntilSettled pins the exact mechanism
// behind the "double Ctrl-C" deadlock (2026-07-07 review): a pre-created mpb bar
// only unblocks Progress.Wait() once Done or Fail settles it — SetTotal(_, true)
// is the ONLY thing that completes a bar (verified against the pinned mpb/v8
// v8.7.5 source; see the progress-finalize-streams-on-early-error-paths task
// design_refs). A stream that is created and never terminally settled therefore
// blocks Wait() forever. This test proves the hang exists at this layer — which
// is exactly why pipeline.Run needs its own defensive sweep, since this package
// cannot make Wait() self-heal — and that calling Fail() on the dangling stream
// immediately unblocks it, the same mechanism the sweep relies on.
func TestTTYSink_Wait_UnfinalizedStreamBlocksUntilSettled(t *testing.T) {
	t.Parallel()

	sink := New(io.Discard, true)

	settled := sink.NewStream("settled", 0)
	dangling := sink.NewStream("dangling", 0)

	settled.Done()

	waitDone := make(chan struct{})

	go func() {
		sink.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("Wait() returned before the dangling stream was settled; expected it to block")
	case <-time.After(200 * time.Millisecond):
		// Expected: Wait() is still blocked on the unfinalized stream.
	}

	// Settling the dangling stream (the sweep's exact mechanism) must unblock Wait().
	dangling.Fail()

	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait() did not return within 5s after the dangling stream was Failed")
	}
}

// TestTTYSink_FailSweep_UnblocksWait mirrors pipeline.Run's post-g.Wait()
// defensive sweep: after some streams settle normally, an unconditional Fail()
// call over every remaining stream must unblock Wait() promptly, proving the
// sweep is a safe, general fix for any early-return path that leaves a stream
// dangling — not just the specific paths pipeline.go happens to guard today.
func TestTTYSink_FailSweep_UnblocksWait(t *testing.T) {
	t.Parallel()

	sink := New(io.Discard, true)

	settled := sink.NewStream("settled", 0)
	settled.Done()

	dangling := []Stream{
		sink.NewStream("leaf-1", 0),
		sink.NewStream("leaf-2", 0),
	}

	waitDone := make(chan struct{})

	go func() {
		sink.Wait()
		close(waitDone)
	}()

	// The sweep: Fail every stream unconditionally. Already-Done streams (like
	// "settled") are unaffected by a later Fail — first terminal call wins — so
	// a real caller can safely sweep the whole map, not just the dangling subset.
	go func() {
		for _, s := range dangling {
			s.Fail()
		}
	}()

	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("sink.Wait() did not return after the Fail sweep settled every remaining stream")
	}
}
