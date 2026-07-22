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
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// Sink is a multi-bar progress container for concurrent byte-stream transfers.
// NewStream creates a per-stream progress handle; Wait drains all rendering
// and emits a final aggregate line (non-TTY) or waits for bar completion (TTY).
type Sink interface {
	// NewStream registers a named stream with a known total byte count.
	// A total of 0 is allowed when the size is not yet known; call SetTotal later.
	NewStream(name string, total int64) Stream

	// SetVolumeTotal sets M, the total number of volume streams this run will
	// download, for the live "N/M volumes downloaded" counter.
	SetVolumeTotal(n int)

	// Wait blocks until all streams have finished and flushes remaining output.
	Wait()

	// LogWriter returns an io.Writer that is safe to use for log output while
	// the sink is active. For the TTY sink it returns a writer whose writes are
	// coordinated with the mpb renderer, so log lines print cleanly above the
	// live bars instead of corrupting their cursor accounting (which otherwise
	// makes the bar re-print as multiple blocks). For the plain (non-TTY) sink
	// it returns os.Stderr, preserving the existing logging behaviour.
	LogWriter() io.Writer
}

// Stream is a per-stream progress handle returned by Sink.NewStream.
type Stream interface {
	// IncrBy advances the stream's byte counter by n.
	IncrBy(n int)

	// SetTotal updates the stream's expected total byte count.
	SetTotal(total int64)

	// SetCurrent sets the stream's current byte counter to an absolute value.
	// It exists for the pipeline's resume-seed clamp: when a resume seed
	// credited from stale on-disk state (a chunk geometry about to be purged,
	// or a stale sizes sidecar) exceeds the fresh authoritative total — a changed
	// --chunk-size or a volume shrunk between runs — downloadBlock/downloadFS
	// call SetCurrent(0) to reset the displayed value to 0 BEFORE lowering the
	// total, so the bar never renders current > total. It is an absolute
	// DOWNWARD correction only; ordinary forward progress is reported with
	// IncrBy, never with SetCurrent.
	SetCurrent(current int64)

	// Activate transitions the stream from waiting to downloading state.
	// For the TTY sink it flips the bar from "waiting for export…" to the live
	// byte-counter display. For the plain (non-TTY) sink it is a no-op.
	// Must be called exactly once after the DataExport becomes ready, before
	// byte transfer begins.
	Activate()

	// Done marks the stream as successfully complete and counts it toward the
	// "N/M volumes downloaded" completion total. If Fail was already called on
	// this stream, Done is a no-op (the first terminal call wins; see Fail).
	Done()

	// Fail marks the stream as terminated WITHOUT completing successfully (the
	// underlying transfer was cancelled or errored, or the stream's DataExport
	// never became ready). Unlike Done, a failed stream is excluded from the
	// "N/M volumes downloaded" / "(N/M volumes)" completion counters, so M (the
	// total) stays correct but a failed stream never counts as one of the N
	// completions. Fail still unblocks Wait() the same way Done does, and may be
	// called from the waiting state (before Activate) or the active state.
	// If Done was already called on this stream, Fail is a no-op (the first
	// terminal call wins; see Done).
	Fail()
}

// Direction supplies the direction-specific wording rendered by stateWord and
// the non-TTY aggregate line. It deliberately covers ONLY the words that
// differ between a download and an upload; "Already exists" (resume skip)
// and "Interrupted" (Fail) are direction-independent and stay hard-coded in
// stateWord regardless of Direction.
type Direction struct {
	// ActiveWord is the state word shown while a stream is actively
	// transferring, e.g. "Downloading" or "Uploading".
	ActiveWord string
	// DoneWord is the state word shown for a stream that finished after
	// Activate was called, e.g. "Download complete" or "Upload complete".
	DoneWord string
	// WaitResource names the resource a waiting stream is blocked on
	// ("DataExport" or "DataImport"), interpolated into the
	// direction-independent "Waiting for %s to be Ready" template.
	WaitResource string
	// PastVerb is the lower-case past-tense verb used in the non-TTY
	// aggregate line, e.g. "downloaded" or "uploaded".
	PastVerb string
}

// DirectionDownload is the default direction: docker-pull-style download
// wording, used by `d8 snapshot download`.
var DirectionDownload = Direction{
	ActiveWord:   "Downloading",
	DoneWord:     "Download complete",
	WaitResource: "DataExport",
	PastVerb:     "downloaded",
}

// DirectionUpload is the direction used by `d8 snapshot upload`.
var DirectionUpload = Direction{
	ActiveWord:   "Uploading",
	DoneWord:     "Upload complete",
	WaitResource: "DataImport",
	PastVerb:     "uploaded",
}

// Option configures the progress Sink constructor.
type Option func(*sinkConfig)

type sinkConfig struct {
	interval  time.Duration
	direction Direction
}

// WithInterval sets the periodic reporting interval for the non-TTY fallback sink.
// Default is 2 seconds.
func WithInterval(d time.Duration) Option {
	return func(c *sinkConfig) {
		c.interval = d
	}
}

// WithDirection sets the direction-specific wording (see Direction) rendered
// by the returned Sink. Default is DirectionDownload, so a caller downloading
// may omit this option entirely.
func WithDirection(d Direction) Option {
	return func(c *sinkConfig) {
		c.direction = d
	}
}

// New constructs a Sink. When tty is true it returns an mpb/v8-backed multi-bar
// renderer writing to w with one docker-pull-style row per stream (no aggregate
// summary header). When tty is false it returns a plain-log fallback that writes
// "downloaded X / total Y" aggregate lines (humanised via decor.SizeB1024) to w
// on a periodic interval and always emits a final deterministic line on Wait().
// The rendered wording (both TTY state words and the non-TTY past-tense verb)
// follows cfg.direction, which defaults to DirectionDownload; pass
// WithDirection(DirectionUpload) for an upload-flavored Sink.
func New(w io.Writer, tty bool, opts ...Option) Sink {
	cfg := sinkConfig{interval: 2 * time.Second, direction: DirectionDownload}

	for _, o := range opts {
		o(&cfg)
	}

	if tty {
		return newTTYSink(w, cfg.direction)
	}

	return newPlainSink(w, cfg.interval, cfg.direction)
}

// ── TTY sink (mpb/v8-backed) ──────────────────────────────────────────────────

// ttyBarWidth is a fixed, compact bar width (in characters) modelled on
// `docker pull`. A fixed width keeps the bar from spanning the whole terminal
// and stops it reflowing/jittering when the terminal is resized. Only the
// drawn [====>   ] portion is fixed; the name and counter decorators add their
// own (also fixed) widths around it.
const ttyBarWidth = 28

// stream state constants stored atomically in ttyStream.state.
// The machine is one-way: waiting → active → done/failed, or waiting →
// done/failed directly (resume skip / early failure before Activate).
const (
	streamStateWaiting = int32(0)
	streamStateActive  = int32(1)
	streamStateDone    = int32(2)
	streamStateFailed  = int32(3)
)

type ttySink struct {
	p *mpb.Progress
	// dir is the direction-specific wording (see Direction) rendered by every
	// stream this sink creates.
	dir Direction
	// summaryOnce guards one-time creation of the bottom-pinned volume-counter bar.
	summaryOnce sync.Once
	summaryBar  *mpb.Bar
	// volTotal/volDone back the "N/M volumes downloaded" counter: volTotal is set
	// once via SetVolumeTotal, volDone is incremented exactly once per stream by
	// ttyStream.Done (see the SwapInt32 gate there).
	volTotal atomic.Int64
	volDone  atomic.Int64
}

func newTTYSink(w io.Writer, dir Direction) *ttySink {
	p := mpb.New(mpb.WithOutput(w))

	return &ttySink{p: p, dir: dir}
}

// SetVolumeTotal sets M for the bottom "N/M volumes downloaded" summary bar.
func (s *ttySink) SetVolumeTotal(n int) {
	s.volTotal.Store(int64(n))
}

// NewStream adds a named per-stream bar. The bar starts in the waiting state and
// switches to the live byte-counter display after Activate() is called. There is
// no aggregate summary header among the per-leaf rows: `docker pull` shows only
// per-layer rows. A separate bottom-pinned volume-counter bar (see summaryOnce)
// reports overall N/M completion below every per-leaf row.
func (s *ttySink) NewStream(name string, total int64) Stream {
	// Create the bottom-pinned volume-counter bar the first time any stream is
	// registered. mpb.BarPriority(math.MinInt) pins it below every per-leaf row
	// regardless of registration order (verified empirically against mpb/v8
	// v8.7.5: greater priority renders at the top; math.MinInt is the smallest
	// possible priority, so this bar always sinks to the bottom).
	//
	// The spinner filler is built directly (not via AddSpinner, which hardcodes
	// the default center-positioned style) and given an explicit narrow
	// mpb.BarWidth, mirroring the per-leaf bar's own BarWidth(ttyBarWidth) below.
	// Without a requested width, mpb/v8 v8.7.5's sFiller.Fill computes
	// width = internal.CheckRequestedWidth(stat.RequestedWidth, stat.AvailableWidth),
	// and CheckRequestedWidth returns AvailableWidth whenever requested < 1 — so an
	// unconfigured spinner claims ALL remaining terminal width. The default
	// (center) position then pads AvailableWidth/2 blanks BEFORE the glyph,
	// stranding it far to the right of the " N/M volumes downloaded" label on any
	// reasonably wide terminal (the reported bug). PositionLeft() plus a
	// spinnerCellWidth-sized bar glues the glyph immediately after the label,
	// matching the per-row waiting-spinner cell it echoes.
	s.summaryOnce.Do(func() {
		bar, err := s.p.Add(0,
			mpb.SpinnerStyle().PositionLeft().Build(),
			mpb.BarWidth(spinnerCellWidth),
			mpb.BarPriority(math.MinInt),
			mpb.PrependDecorators(
				decor.Any(func(_ decor.Statistics) string {
					return volumeCounterLabel(int(s.volDone.Load()), int(s.volTotal.Load()))
				}),
			),
		)
		if err != nil {
			// Add only fails once the container has been shut down by Wait();
			// NewStream after Wait is a misuse of the Sink contract (unreachable
			// in the download pipeline, which registers every stream before Wait).
			panic(fmt.Sprintf("progress: creating volume-counter summary bar: %v", err))
		}

		s.summaryBar = bar
	})

	ts := &ttyStream{sink: s, total: total, dir: s.dir}

	// Render the row as a docker-pull layer line: the bar is drawn ONLY while the
	// stream is active. The state-aware filler wraps a growing-arrow BarStyle and
	// emits nothing in the waiting/done states, so no [bar] occupies the row then.
	filler := stateBarFiller{
		state: &ts.state,
		inner: mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]").Build(),
	}

	// Layout: name → spinner → stateWord → [bar] → counters → percent. Every row
	// uses the SAME decorator chain and widths in every state — only the rendered
	// content changes — so a row never shifts horizontally as it transitions
	// waiting → active → done. Column geometry:
	//   - name: full leaf name, never truncated; a width-synced cell (WCSyncWidthR)
	//     auto-sizes the column to the longest name across all rows. nameCell appends
	//     one trailing space so even the widest (unpadded) row keeps a clean gap
	//     before the spinner.
	//   - spinner: fixed-width (spinnerCellWidth) animated cell, non-blank only while
	//     waiting; a same-width blank in active/done so the column never shifts.
	//   - stateWord: left-aligned width-synced cell (WCSyncWidthR); the widest word
	//     sets one shared width across all rows, so the bar / end-of-row begins at
	//     the same x in every state.
	//   - counters/percent: right-aligned width-synced cells (WCSyncWidth) so the
	//     active rows' numbers form one uniform right-hand column.
	//
	// mpb's "R" suffix is counter-intuitive: WCSyncWidthR sets the DindentRight bit,
	// which makes WC.Init() use runewidth.FillRight (padding appended on the right) —
	// text glued to the LEFT, i.e. left-aligned. Bare WCSyncWidth has no DindentRight,
	// so WC.Init() uses runewidth.FillLeft (padding prepended on the left) — text
	// glued to the RIGHT, i.e. right-aligned. So WCSyncWidthR is LEFT-aligned and
	// WCSyncWidth is RIGHT-aligned; do not re-invert these.
	bar, err := s.p.Add(
		total,
		filler,
		mpb.BarWidth(ttyBarWidth),
		mpb.PrependDecorators(
			decor.Name(nameCell(name), decor.WCSyncWidthR),
			// Waiting spinner: a fixed-width animated cell shown only while the row
			// is waiting. mpb calls this once per refresh; the atomic add advances
			// the frame each refresh so the glyph spins. WC{W: spinnerCellWidth}
			// reserves the same width in every state (blank in active/done), so no
			// other column shifts when the spinner appears or disappears.
			decor.Any(func(_ decor.Statistics) string {
				return spinnerCell(atomic.LoadInt32(&ts.state), atomic.AddUint64(&ts.spinTick, 1))
			}, decor.WC{W: spinnerCellWidth}),
			decor.Any(func(_ decor.Statistics) string {
				return " " + stateWord(atomic.LoadInt32(&ts.state), atomic.LoadInt32(&ts.activated) == 1, ts.dir)
			}, decor.WCSyncWidthR),
		),
		mpb.AppendDecorators(
			decor.Any(func(stats decor.Statistics) string {
				return decorateStatus(atomic.LoadInt32(&ts.state), stats)
			}, decor.WCSyncWidth),
			decor.Any(func(stats decor.Statistics) string {
				return decorateAppend(atomic.LoadInt32(&ts.state), stats)
			}, decor.WCSyncWidth),
		),
	)
	if err != nil {
		// Add only fails once the container has been shut down by Wait(); calling
		// NewStream after Wait is a misuse of the Sink contract (unreachable in the
		// download pipeline, which registers every stream before Wait).
		panic(fmt.Sprintf("progress: registering stream %q after Wait: %v", name, err))
	}

	ts.bar = bar

	return ts
}

// Wait completes the volume-counter summary bar and then drains the mpb renderer
// once all per-stream bars have finished. By the time Wait is called every
// per-leaf stream has called Done, so the settled "M/M volumes downloaded" line
// renders before the container drains.
func (s *ttySink) Wait() {
	if s.summaryBar != nil {
		s.summaryBar.SetTotal(1, true)
	}

	s.p.Wait()
}

// LogWriter returns the mpb container itself, which implements io.Writer by
// funnelling writes through the same render loop that draws the bars. Writes
// therefore appear cleanly above the live bars without corrupting them. After
// Wait has been called the container is shut down and writes are dropped, so
// callers must emit post-completion log lines through a different writer.
func (s *ttySink) LogWriter() io.Writer {
	return s.p
}

type ttyStream struct {
	bar *mpb.Bar
	// sink is a back-reference to the owning ttySink, used only to bump volDone
	// on completion for the bottom volume-counter bar. Never nil in production
	// (set by ttySink.NewStream); tests that construct a bare *ttyStream leave it
	// nil, which Done() guards against.
	sink *ttySink
	// dir is this stream's direction wording, copied from the owning ttySink at
	// NewStream time (mirrors how state is carried per-stream rather than
	// re-read from sink on every render).
	dir   Direction
	mu    sync.Mutex
	total int64
	state int32 // atomic: streamStateWaiting / streamStateActive / streamStateDone
	// activated records whether Activate was ever called (atomic 0/1). It
	// distinguishes a real download (waiting → active → done, "Download complete")
	// from a resume skip (waiting → done without Activate, "Already exists") in
	// stateWord without adding a new Stream interface method.
	activated int32
	// spinTick is the waiting-spinner frame counter. mpb invokes the spinner
	// decorator once per refresh; each invocation does an atomic add so the
	// frame advances per refresh and the waiting glyph animates.
	spinTick uint64
}

func (s *ttyStream) IncrBy(n int) {
	s.bar.IncrBy(n)
}

func (s *ttyStream) SetTotal(total int64) {
	s.mu.Lock()
	s.total = total
	s.mu.Unlock()

	s.bar.SetTotal(total, false)
}

// SetCurrent sets the bar's current byte counter to an absolute value. See the
// Stream interface doc comment for why this exists (the resume-seed clamp
// downward correction, never ordinary incremental reporting).
func (s *ttyStream) SetCurrent(current int64) {
	s.bar.SetCurrent(current)
}

// Activate transitions the stream from waiting to downloading and records that
// the stream was activated (so a finished stream renders "Download complete"
// rather than the resume-skip word "Already exists"). Subsequent calls after the
// first are no-ops on the state (CAS ensures exactly-once semantics).
func (s *ttyStream) Activate() {
	atomic.StoreInt32(&s.activated, 1)
	atomic.CompareAndSwapInt32(&s.state, streamStateWaiting, streamStateActive)
}

// Done marks the stream as successfully complete and triggers bar completion in
// mpb, counting it toward the volume-counter. See finalize for the
// first-terminal-call-wins semantics shared with Fail.
func (s *ttyStream) Done() {
	s.finalize(streamStateDone, true)
}

// Fail marks the stream as terminated without completing successfully and
// triggers bar completion in mpb, WITHOUT counting it toward the
// volume-counter (see finalize). It may be called directly from the waiting
// state (e.g. the DataExport never became ready) or from the active state
// (e.g. the transfer was cancelled mid-copy).
func (s *ttyStream) Fail() {
	s.finalize(streamStateFailed, false)
}

// finalize atomically transitions the stream to a terminal state (done or
// failed) and settles the mpb bar. Only the FIRST terminal call takes effect:
// once the state is already streamStateDone or streamStateFailed, a later
// Done()/Fail() call (a misuse no call site triggers, since each registers
// exactly one terminal method) is a no-op, so a stream is never double-counted
// or re-settled regardless of call order.
func (s *ttyStream) finalize(target int32, countCompletion bool) {
	for {
		prev := atomic.LoadInt32(&s.state)
		if prev == streamStateDone || prev == streamStateFailed {
			return
		}

		if atomic.CompareAndSwapInt32(&s.state, prev, target) {
			break
		}
	}

	if countCompletion && s.sink != nil {
		s.sink.volDone.Add(1)
	}

	s.mu.Lock()
	total := s.total
	s.mu.Unlock()

	s.bar.SetTotal(total, true)
}

// ── State-aware bar filler ────────────────────────────────────────────────────

// stateBarFiller renders the wrapped bar ONLY while the stream is in the active
// state. In the waiting and done states it writes nothing, so no [bar] occupies
// the row — matching `docker pull`, where a layer shows a bar only while it is
// actually transferring (waiting rows show "Waiting", finished rows show
// "Download complete"/"Already exists" with no residual bar).
type stateBarFiller struct {
	state *int32 // points at the owning ttyStream.state (read atomically)
	inner mpb.BarFiller
}

// Fill delegates to the inner growing-arrow filler only in the active state and
// writes nothing otherwise. It is unit-assertable independently of mpb rendering:
// empty output when waiting/done, a bracketed bar containing '[' and ']' when active.
func (f stateBarFiller) Fill(w io.Writer, stat decor.Statistics) error {
	if atomic.LoadInt32(f.state) != streamStateActive {
		return nil
	}

	return f.inner.Fill(w, stat)
}

// ── Waiting spinner ───────────────────────────────────────────────────────────

// spinnerCellWidth is the fixed display-rune width of the waiting-spinner cell:
// one braille glyph plus a trailing space while waiting, or two blanks otherwise.
// Keeping it constant means the spinner column never shifts the columns to its
// right when the glyph appears (waiting) or disappears (active/done).
const spinnerCellWidth = 2

// waitingSpinnerFrames is the 10-frame braille spinner cycled while a row waits
// for its DataExport, matching the familiar docker-pull-style motion.
var waitingSpinnerFrames = []string{
	"\u280b", "\u2819", "\u2839", "\u2838", "\u283c",
	"\u2834", "\u2826", "\u2827", "\u2807", "\u280f",
}

// spinnerFrame returns the braille glyph for the given tick, cycling through
// waitingSpinnerFrames by tick modulo the frame count. It is pure and
// deterministic so the animation can be unit-asserted without mpb rendering.
func spinnerFrame(tick uint64) string {
	return waitingSpinnerFrames[tick%uint64(len(waitingSpinnerFrames))]
}

// spinnerCell returns the fixed-width waiting-spinner cell for a stream's state.
// While waiting it returns the current braille glyph plus a trailing space (an
// animated indicator); in the active and done states it returns a same-width
// blank ("  "), so the spinner is visible only while waiting and the column
// width stays constant across every state. It is pure and unit-assertable.
func spinnerCell(state int32, tick uint64) string {
	if state == streamStateWaiting {
		return spinnerFrame(tick) + " "
	}

	return "  "
}

// ── Decorator pure functions ──────────────────────────────────────────────────

// stateWord returns the docker-pull status word for a stream's current state.
// dir supplies the direction-specific words (Direction); activated distinguishes
// a finished real transfer from a resume skip:
//
//   - waiting: "Waiting for <dir.WaitResource> to be Ready" (the row is blocked
//     until its DataExport/DataImport becomes Ready; the descriptive phrase
//     tells the user WHAT is being waited on and that it is the readiness of
//     that resource).
//   - active: dir.ActiveWord ("Downloading"/"Uploading").
//   - done after Activate: dir.DoneWord ("Download complete"/"Upload complete").
//   - done without Activate (resume skip): "Already exists" — DIRECTION-INDEPENDENT,
//     unchanged for both directions.
//   - failed (Fail called, from waiting or active): "Interrupted" —
//     DIRECTION-INDEPENDENT, pinned to match the CLI's Ctrl-C/SIGINT
//     cancellation use case for both directions.
//
// "Waiting for DataExport to be Ready" and "Waiting for DataImport to be Ready"
// are both 34 runes (verified empirically: "DataExport"/"DataImport" are the
// same length), so either is the widest word for its direction and sets the
// WCSyncWidth status-word column width identically; every other word fits
// within it and rows do not shift horizontally as the state changes. A single
// sink only ever uses one Direction, so the two waiting phrases never appear
// side-by-side in the same synced column.
func stateWord(state int32, activated bool, dir Direction) string {
	switch state {
	case streamStateActive:
		return dir.ActiveWord
	case streamStateDone:
		if activated {
			return dir.DoneWord
		}

		return "Already exists"
	case streamStateFailed:
		return "Interrupted"
	default:
		return fmt.Sprintf("Waiting for %s to be Ready", dir.WaitResource)
	}
}

// decorateStatus returns the byte-counter append text for a stream bar.
// Counters render in the active state ("<current> / <total>" in
// human-readable binary units) exactly as before, AND ALSO in the waiting
// state when the stream was seeded with already-committed on-disk bytes
// (Current > 0) — the pipeline's seedStreamFromDisk credits those bytes via
// IncrBy before the transfer starts — so a resumed volume's row shows its
// real committed bytes immediately, before the DataExport becomes ready. A
// seeded row whose total is not yet known (a
// filesystem volume before its listing returns) shows "<current> / ???"
// instead of a bogus zero total. Done/failed rows still show no counters
// (docker-pull parity: a finished layer shows only its status word). It is a
// pure function used directly in unit tests without any mpb rendering.
func decorateStatus(state int32, stats decor.Statistics) string {
	switch {
	case state == streamStateActive:
		return fmt.Sprintf(" % .1f / % .1f", decor.SizeB1024(stats.Current), decor.SizeB1024(stats.Total))
	case state == streamStateWaiting && stats.Current > 0 && stats.Total > 0:
		return fmt.Sprintf(" % .1f / % .1f", decor.SizeB1024(stats.Current), decor.SizeB1024(stats.Total))
	case state == streamStateWaiting && stats.Current > 0:
		return fmt.Sprintf(" % .1f / ???", decor.SizeB1024(stats.Current))
	default:
		return ""
	}
}

// decorateAppend returns the percentage append text for a stream bar. The
// percent renders in the active state exactly as before, AND ALSO in the
// waiting state once a seeded stream's total is known (see decorateStatus).
// A seeded row whose total is still unknown shows no percentage (there is
// nothing to divide by yet); waiting rows show " 0%" is deliberately NOT used
// there, since an un-seeded waiting row (Current == 0) must keep showing no
// percentage at all (docker-pull parity). It is a pure function used directly
// in unit tests.
func decorateAppend(state int32, stats decor.Statistics) string {
	switch {
	case state == streamStateActive && stats.Total <= 0:
		return " 0%"
	case state == streamStateActive:
		return fmt.Sprintf(" %.0f%%", float64(stats.Current)/float64(stats.Total)*100)
	case state == streamStateWaiting && stats.Current > 0 && stats.Total > 0:
		return fmt.Sprintf(" %.0f%%", float64(stats.Current)/float64(stats.Total)*100)
	default:
		return ""
	}
}

// volumeCounterLabel returns the text for the bottom volume-counter summary bar.
// It is a pure function so the label can be unit-tested without any mpb
// rendering. total==0 means no volumes are in scope (e.g. a manifest-only
// selection), so nothing is rendered.
func volumeCounterLabel(done, total int) string {
	if total == 0 {
		return ""
	}

	return fmt.Sprintf(" %d/%d volumes downloaded", done, total)
}

// nameCell renders the FULL leaf name with NO truncation, followed by a single
// trailing separator space. Combined with decor.WCSyncWidth the name column
// auto-sizes to the longest name across all rows, so every name prints in full
// and shorter rows are padded to align. The trailing space guarantees a clean
// gap before the spinner cell even on the widest row (which receives no sync
// padding of its own).
func nameCell(name string) string {
	return name + " "
}

// ── Non-TTY (plain-log) sink ──────────────────────────────────────────────────

type plainSink struct {
	w        io.Writer
	interval time.Duration
	// dir is the direction-specific wording (see Direction) rendered by emit's
	// aggregate line. Stored at sink level, not per-stream: unlike the TTY sink,
	// the plain sink has no per-stream rendering (plainStream renders nothing;
	// only emit produces text), so a plainStream.dir field would be dead state.
	dir      Direction
	mu       sync.Mutex
	progress int64
	total    int64
	// volTotal/volDone back the "(N/M volumes)" suffix on the aggregate line: M is
	// set once via SetVolumeTotal, N is incremented once per stream by
	// plainStream.Done. Both are protected by mu (no separate lock/timer added).
	volTotal int64
	volDone  int64
	stop     chan struct{}
	stopped  chan struct{}
}

func newPlainSink(w io.Writer, interval time.Duration, dir Direction) *plainSink {
	s := &plainSink{
		w:        w,
		interval: interval,
		dir:      dir,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}

	go s.tick()

	return s
}

func (s *plainSink) tick() {
	defer close(s.stopped)

	t := time.NewTicker(s.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			s.emit()
		case <-s.stop:
			return
		}
	}
}

// emit writes one "downloaded X / total Y (N/M volumes)" (or "uploaded ..."
// for DirectionUpload) aggregate line to the output writer. Using
// fmt.Fprintf to an io.Writer; write errors are intentionally ignored for
// progress output.
func (s *plainSink) emit() {
	s.mu.Lock()
	prog := s.progress
	tot := s.total
	volDone := s.volDone
	volTotal := s.volTotal
	s.mu.Unlock()

	fmt.Fprintf(s.w, "%s % .1f / total % .1f (%d/%d volumes)\n",
		s.dir.PastVerb, decor.SizeB1024(prog), decor.SizeB1024(tot), volDone, volTotal)
}

// SetVolumeTotal sets M for the "(N/M volumes)" suffix on the aggregate line.
func (s *plainSink) SetVolumeTotal(n int) {
	s.mu.Lock()
	s.volTotal = int64(n)
	s.mu.Unlock()
}

// NewStream registers an additional stream and adds its total to the aggregate.
func (s *plainSink) NewStream(_ string, total int64) Stream {
	s.mu.Lock()
	s.total += total
	s.mu.Unlock()

	return &plainStream{sink: s, total: total}
}

// Wait stops the periodic goroutine and emits a final deterministic aggregate line.
func (s *plainSink) Wait() {
	close(s.stop)
	<-s.stopped

	s.emit()
}

// LogWriter returns os.Stderr for the plain sink: the non-TTY path performs no
// cursor manipulation, so logs do not corrupt the periodic progress lines and
// logging behaviour is unchanged from before.
func (s *plainSink) LogWriter() io.Writer {
	return os.Stderr
}

type plainStream struct {
	sink    *plainSink
	mu      sync.Mutex
	total   int64
	current int64
	// settled gates Done()/Fail() to exactly one terminal transition per stream:
	// the FIRST call (whichever method) wins, mirroring ttyStream.finalize. A
	// duplicate Done() call must not double-count, and Done() after Fail() (or
	// vice versa) must not count a stream the other outcome already settled.
	settled bool
}

func (s *plainStream) IncrBy(n int) {
	s.mu.Lock()
	s.current += int64(n)
	s.mu.Unlock()

	s.sink.mu.Lock()
	s.sink.progress += int64(n)
	s.sink.mu.Unlock()
}

// SetCurrent sets this stream's own current byte counter to an absolute value
// and applies the resulting delta to the sink's shared aggregate — mirroring
// SetTotal's delta pattern below — since the non-TTY sink's "downloaded X /
// total Y" line sums every stream's progress into one shared counter. See the
// Stream interface doc comment for why this exists (the resume-seed clamp
// downward correction, never ordinary incremental reporting).
func (s *plainStream) SetCurrent(current int64) {
	s.mu.Lock()
	delta := current - s.current
	s.current = current
	s.mu.Unlock()

	if delta == 0 {
		return
	}

	s.sink.mu.Lock()
	s.sink.progress += delta
	s.sink.mu.Unlock()
}

func (s *plainStream) SetTotal(total int64) {
	s.mu.Lock()
	delta := total - s.total
	s.total = total
	s.mu.Unlock()

	if delta == 0 {
		return
	}

	s.sink.mu.Lock()
	s.sink.total += delta
	s.sink.mu.Unlock()
}

// Activate is a no-op for the plain sink; the non-TTY path has no bar state.
func (s *plainStream) Activate() {}

// Done increments the sink's completed-volume counter (N) for the "(N/M volumes)"
// aggregate-line suffix, exactly once per stream (a duplicate Done() call, or a
// Done() after Fail() already settled the stream, is a no-op on the counter);
// byte progress itself is tracked entirely via IncrBy.
func (s *plainStream) Done() {
	s.finalize(true)
}

// Fail marks the stream as settled WITHOUT incrementing the sink's
// completed-volume counter, so a cancelled/errored stream is excluded from the
// "(N/M volumes)" suffix's N (M, the total, is unaffected). It may be called
// from any point in the stream's lifecycle.
func (s *plainStream) Fail() {
	s.finalize(false)
}

// finalize settles the stream at most once: the FIRST Done()/Fail() call wins
// and later calls (in either order) are no-ops, so a stream is never
// double-counted and a Fail-then-Done (or Done-then-Fail) sequence keeps
// whichever outcome happened first.
func (s *plainStream) finalize(countCompletion bool) {
	s.mu.Lock()
	alreadySettled := s.settled
	s.settled = true
	s.mu.Unlock()

	if alreadySettled || !countCompletion {
		return
	}

	s.sink.mu.Lock()
	s.sink.volDone++
	s.sink.mu.Unlock()
}
