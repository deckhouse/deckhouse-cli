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
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// aggregateLine formats the expected "downloaded X / total Y" line as the
// non-TTY sink emits it, using the same decor.SizeB1024 formatter.
func aggregateLine(t *testing.T, prog, total int64) string {
	t.Helper()

	return fmt.Sprintf("downloaded % .1f / total % .1f\n",
		decor.SizeB1024(prog), decor.SizeB1024(total))
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
			want := aggregateLine(t, tc.wantProg, tc.wantTotal)

			if !strings.Contains(got, want) {
				t.Errorf("output does not contain expected final line\ngot:  %q\nwant (contained): %q", got, want)
			}
		})
	}
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

	want := aggregateLine(t, 4096, 4096)

	if !strings.Contains(buf.String(), want) {
		t.Errorf("SetTotal not reflected in aggregate:\ngot:  %q\nwant (contained): %q", buf.String(), want)
	}
}

func TestNonTTY_Wait_AlwaysEmitsFinalLine(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	// Use a very long interval so only Wait() emits a line.
	sink := New(buf, false, WithInterval(time.Hour))
	sink.Wait()

	want := aggregateLine(t, 0, 0)

	if !strings.Contains(buf.String(), want) {
		t.Errorf("Wait() did not emit final line:\ngot:  %q\nwant (contained): %q", buf.String(), want)
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
		want      string
	}{
		{"waiting", streamStateWaiting, false, "Waiting for DataExport to be Ready"},
		{"active", streamStateActive, true, "Downloading"},
		{"done_after_activate", streamStateDone, true, "Download complete"},
		{"done_without_activate", streamStateDone, false, "Already exists"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := stateWord(tc.state, tc.activated)
			if got != tc.want {
				t.Errorf("stateWord(%d, %t) = %q, want %q", tc.state, tc.activated, got, tc.want)
			}
		})
	}
}

// TestStateWordSyncWidth documents the column-alignment contract: the status-word
// column width is synced to the widest possible word ("Waiting for DataExport to
// be Ready"), so every other state word fits within it and the bar/end-of-row
// starts at the same x.
func TestStateWordSyncWidth(t *testing.T) {
	t.Parallel()

	widest := "Waiting for DataExport to be Ready"

	words := []string{
		stateWord(streamStateWaiting, false),
		stateWord(streamStateActive, true),
		stateWord(streamStateDone, true),
		stateWord(streamStateDone, false),
	}

	for _, w := range words {
		if len(w) > len(widest) {
			t.Errorf("stateWord %q wider (%d) than synced column width %q (%d)", w, len(w), widest, len(widest))
		}
	}

	if stateWord(streamStateWaiting, false) != widest {
		t.Errorf("expected the waiting word to be the widest %q, got %q", widest, stateWord(streamStateWaiting, false))
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
// waitingSpinnerFrames by tick % len, including wrap-around at and past the
// frame count. mpb refresh timing/terminal animation is intentionally not tested.
func TestSpinnerFrame(t *testing.T) {
	t.Parallel()

	n := uint64(len(waitingSpinnerFrames))

	cases := []struct {
		name string
		tick uint64
		want string
	}{
		{"first", 0, waitingSpinnerFrames[0]},
		{"last_before_wrap", n - 1, waitingSpinnerFrames[n-1]},
		{"wrap_to_first", n, waitingSpinnerFrames[0]},
		{"wrap_to_second", n + 1, waitingSpinnerFrames[1]},
		{"multi_wrap", 2*n + 3, waitingSpinnerFrames[3]},
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

	n := len(waitingSpinnerFrames)

	for i := 0; i < 2*n; i++ {
		got := spinnerFrame(uint64(i))
		want := waitingSpinnerFrames[i%n]

		if got != want {
			t.Errorf("tick %d: spinnerFrame = %q, want %q", i, got, want)
		}
	}
}

// TestSpinnerCell asserts state gating and constant width: a real glyph cell
// (frame + trailing space) only in the waiting state, a same-width blank in the
// active and done states, and an identical display (rune) width across all three.
func TestSpinnerCell(t *testing.T) {
	t.Parallel()

	const tick = uint64(3)

	waitingCell := spinnerCell(streamStateWaiting, tick)
	activeCell := spinnerCell(streamStateActive, tick)
	doneCell := spinnerCell(streamStateDone, tick)

	if want := spinnerFrame(tick) + " "; waitingCell != want {
		t.Errorf("waiting cell = %q, want %q", waitingCell, want)
	}

	if strings.TrimSpace(waitingCell) == "" {
		t.Errorf("waiting cell %q must contain a non-blank glyph", waitingCell)
	}

	if activeCell != "  " {
		t.Errorf("active cell = %q, want two-space blank", activeCell)
	}

	if doneCell != "  " {
		t.Errorf("done cell = %q, want two-space blank", doneCell)
	}

	// Constant display width is what keeps the columns to the right from shifting
	// when the spinner appears (waiting) or disappears (active/done).
	wWidth := utf8.RuneCountInString(waitingCell)
	if wWidth != spinnerCellWidth {
		t.Errorf("waiting cell width = %d, want %d", wWidth, spinnerCellWidth)
	}

	if aWidth := utf8.RuneCountInString(activeCell); aWidth != wWidth {
		t.Errorf("active cell width = %d, want %d (== waiting)", aWidth, wWidth)
	}

	if dWidth := utf8.RuneCountInString(doneCell); dWidth != wWidth {
		t.Errorf("done cell width = %d, want %d (== waiting)", dWidth, wWidth)
	}
}

// TestTTYStream_SpinnerStateGating drives a real ttyStream through
// waiting → active → done and asserts the spinner cell is non-blank only while
// waiting and blank (constant width) once the row becomes active or done.
func TestTTYStream_SpinnerStateGating(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := newTTYSink(buf)

	st, ok := sink.NewStream("spin", 0).(*ttyStream)
	if !ok {
		t.Fatal("NewStream did not return *ttyStream")
	}

	cellNow := func() string {
		return spinnerCell(atomic.LoadInt32(&st.state), atomic.AddUint64(&st.spinTick, 1))
	}

	if cell := cellNow(); strings.TrimSpace(cell) == "" {
		t.Errorf("fresh (waiting) stream spinner cell = %q, want a non-blank glyph", cell)
	}

	st.Activate()

	if cell := cellNow(); cell != "  " {
		t.Errorf("active stream spinner cell = %q, want two-space blank", cell)
	}

	st.Done()

	if cell := cellNow(); cell != "  " {
		t.Errorf("done stream spinner cell = %q, want two-space blank", cell)
	}

	sink.Wait()
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
	sink := newTTYSink(buf)

	s1, ok := sink.NewStream("stream-1", 0).(*ttyStream)
	if !ok {
		t.Fatal("NewStream did not return *ttyStream")
	}

	s2, ok := sink.NewStream("stream-2", 0).(*ttyStream)
	if !ok {
		t.Fatal("NewStream did not return *ttyStream")
	}

	wordOf := func(s *ttyStream) string {
		return stateWord(atomic.LoadInt32(&s.state), atomic.LoadInt32(&s.activated) == 1)
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
	sink := newTTYSink(buf)

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

// TestTTYSink_NoSummaryHeader guards the summary-cleanup decision: a TTY run must
// emit only per-leaf docker-pull rows and never the old animated aggregate header
// text. mpb terminal frames are not asserted; we only assert the rendered output
// never contains the removed header strings (or the old "waiting…" counter).
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
