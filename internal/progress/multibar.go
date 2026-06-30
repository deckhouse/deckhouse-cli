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
	"os"
	"strings"
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

	// Activate transitions the stream from waiting to downloading state.
	// For the TTY sink it flips the bar from "waiting for export…" to the live
	// byte-counter display. For the plain (non-TTY) sink it is a no-op.
	// Must be called exactly once after the DataExport becomes ready, before
	// byte transfer begins.
	Activate()

	// Done marks the stream as complete.
	Done()
}

// Option configures the progress Sink constructor.
type Option func(*sinkConfig)

type sinkConfig struct {
	interval time.Duration
}

// WithInterval sets the periodic reporting interval for the non-TTY fallback sink.
// Default is 2 seconds.
func WithInterval(d time.Duration) Option {
	return func(c *sinkConfig) {
		c.interval = d
	}
}

// New constructs a Sink. When tty is true it returns an mpb/v8-backed multi-bar
// renderer writing to w with one docker-pull-style row per stream (no aggregate
// summary header). When tty is false it returns a plain-log fallback that writes
// "downloaded X / total Y" aggregate lines (humanised via decor.SizeB1024) to w
// on a periodic interval and always emits a final deterministic line on Wait().
func New(w io.Writer, tty bool, opts ...Option) Sink {
	cfg := sinkConfig{interval: 2 * time.Second}

	for _, o := range opts {
		o(&cfg)
	}

	if tty {
		return newTTYSink(w)
	}

	return newPlainSink(w, cfg.interval)
}

// ── TTY sink (mpb/v8-backed) ──────────────────────────────────────────────────

// ttyBarWidth is a fixed, compact bar width (in characters) modelled on
// `docker pull`. A fixed width keeps the bar from spanning the whole terminal
// and stops it reflowing/jittering when the terminal is resized. Only the
// drawn [====>   ] portion is fixed; the name and counter decorators add their
// own (also fixed) widths around it.
const ttyBarWidth = 28

// ttyNameWidth is the fixed display-rune width of the leaf-name column.
// Leaf names longer than this are truncated with a trailing '…'; shorter names
// are right-padded with spaces so all bars align in neat docker-pull-like columns.
const ttyNameWidth = 24

// stream state constants stored atomically in ttyStream.state.
// The machine is one-way: waiting → active → done, or waiting → done (resume skip).
const (
	streamStateWaiting = int32(0)
	streamStateActive  = int32(1)
	streamStateDone    = int32(2)
)

type ttySink struct {
	p *mpb.Progress
}

func newTTYSink(w io.Writer) *ttySink {
	p := mpb.New(mpb.WithOutput(w))

	return &ttySink{p: p}
}

// NewStream adds a named per-stream bar. The bar starts in the waiting state and
// switches to the live byte-counter display after Activate() is called. There is
// no aggregate summary header: `docker pull` shows only per-layer rows, so the
// TTY display is the set of per-leaf docker-pull rows and nothing else.
func (s *ttySink) NewStream(name string, total int64) Stream {
	ts := &ttyStream{total: total}

	// Render the row as a docker-pull layer line: the bar is drawn ONLY while the
	// stream is active. The state-aware filler wraps a growing-arrow BarStyle and
	// emits nothing in the waiting/done states, so no [bar] occupies the row then.
	filler := stateBarFiller{
		state: &ts.state,
		inner: mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]").Build(),
	}

	// Layout: name → stateWord → [bar] → counters → percent. Every row uses the
	// SAME decorator chain and widths in every state — only the rendered content
	// changes — so a row never shifts horizontally as it transitions
	// waiting → active → done. Column geometry:
	//   - name: fixed-width left-aligned cell (truncateName pads/truncates to ttyNameWidth).
	//   - stateWord: left-aligned width-synced cell (WCSyncWidth); the widest word
	//     ("Waiting for DataExport") sets one shared width across all rows, so the bar /
	//     end-of-row begins at the same x in every state.
	//   - counters/percent: right-aligned width-synced cells (WCSyncWidthR) so the
	//     active rows' numbers form one uniform right-hand column.
	bar, err := s.p.Add(
		total,
		filler,
		mpb.BarWidth(ttyBarWidth),
		mpb.PrependDecorators(
			decor.Name(truncateName(name, ttyNameWidth), decor.WC{W: ttyNameWidth}),
			decor.Any(func(_ decor.Statistics) string {
				return " " + stateWord(atomic.LoadInt32(&ts.state), atomic.LoadInt32(&ts.activated) == 1)
			}, decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Any(func(stats decor.Statistics) string {
				return decorateStatus(atomic.LoadInt32(&ts.state), stats)
			}, decor.WCSyncWidthR),
			decor.Any(func(stats decor.Statistics) string {
				return decorateAppend(atomic.LoadInt32(&ts.state), stats)
			}, decor.WCSyncWidthR),
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

// Wait drains the mpb renderer once all per-stream bars have finished. By the
// time Wait is called every per-leaf stream has called Done.
func (s *ttySink) Wait() {
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
	bar   *mpb.Bar
	mu    sync.Mutex
	total int64
	state int32 // atomic: streamStateWaiting / streamStateActive / streamStateDone
	// activated records whether Activate was ever called (atomic 0/1). It
	// distinguishes a real download (waiting → active → done, "Download complete")
	// from a resume skip (waiting → done without Activate, "Already exists") in
	// stateWord without adding a new Stream interface method.
	activated int32
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

// Activate transitions the stream from waiting to downloading and records that
// the stream was activated (so a finished stream renders "Download complete"
// rather than the resume-skip word "Already exists"). Subsequent calls after the
// first are no-ops on the state (CAS ensures exactly-once semantics).
func (s *ttyStream) Activate() {
	atomic.StoreInt32(&s.activated, 1)
	atomic.CompareAndSwapInt32(&s.state, streamStateWaiting, streamStateActive)
}

// Done marks the stream as complete and triggers bar completion in mpb.
func (s *ttyStream) Done() {
	atomic.StoreInt32(&s.state, streamStateDone)

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

// ── Decorator pure functions ──────────────────────────────────────────────────

// stateWord returns the docker-pull status word for a stream's current state.
// The activated flag distinguishes a finished real download from a resume skip:
//
//   - waiting: "Waiting for DataExport" (the row is blocked until its DataExport
//     is provisioned; the descriptive phrase tells the user WHAT is being waited on).
//   - active: "Downloading".
//   - done after Activate: "Download complete".
//   - done without Activate (resume skip): "Already exists".
//
// "Waiting for DataExport" is the widest word, so it sets the WCSyncWidth
// status-word column width; every other word fits within it and rows do not
// shift horizontally as the state changes.
func stateWord(state int32, activated bool) string {
	switch state {
	case streamStateActive:
		return "Downloading"
	case streamStateDone:
		if activated {
			return "Download complete"
		}

		return "Already exists"
	default:
		return "Waiting for DataExport"
	}
}

// decorateStatus returns the byte-counter append text for a stream bar. Counters
// render ONLY in the active state ("<current> / <total>" in human-readable binary
// units); waiting and done rows show no counters (docker-pull parity). It is a pure
// function used directly in unit tests without any mpb rendering.
func decorateStatus(state int32, stats decor.Statistics) string {
	if state != streamStateActive {
		return ""
	}

	return fmt.Sprintf(" % .1f / % .1f", decor.SizeB1024(stats.Current), decor.SizeB1024(stats.Total))
}

// decorateAppend returns the percentage append text for a stream bar. The percent
// renders ONLY in the active state; waiting and done rows show no percentage. It is
// a pure function used directly in unit tests.
func decorateAppend(state int32, stats decor.Statistics) string {
	if state != streamStateActive {
		return ""
	}

	if stats.Total <= 0 {
		return " 0%"
	}

	return fmt.Sprintf(" %.0f%%", float64(stats.Current)/float64(stats.Total)*100)
}

// truncateName left-aligns name in a fixed-width column of exactly width display
// runes. Names shorter than width are right-padded with spaces. Names longer than
// width are truncated with a trailing '…' so the total display length is always
// exactly width. The measurement is rune-aware (Unicode code points, not bytes).
func truncateName(name string, width int) string {
	runes := []rune(name)
	if len(runes) <= width {
		return string(runes) + strings.Repeat(" ", width-len(runes))
	}

	return string(runes[:width-1]) + "…"
}

// ── Non-TTY (plain-log) sink ──────────────────────────────────────────────────

type plainSink struct {
	w        io.Writer
	interval time.Duration
	mu       sync.Mutex
	progress int64
	total    int64
	stop     chan struct{}
	stopped  chan struct{}
}

func newPlainSink(w io.Writer, interval time.Duration) *plainSink {
	s := &plainSink{
		w:        w,
		interval: interval,
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

// emit writes one "downloaded X / total Y" aggregate line to the output writer.
// Using fmt.Fprintf to an io.Writer; write errors are intentionally ignored for
// progress output.
func (s *plainSink) emit() {
	s.mu.Lock()
	prog := s.progress
	tot := s.total
	s.mu.Unlock()

	fmt.Fprintf(s.w, "downloaded % .1f / total % .1f\n", decor.SizeB1024(prog), decor.SizeB1024(tot))
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
	sink  *plainSink
	mu    sync.Mutex
	total int64
}

func (s *plainStream) IncrBy(n int) {
	s.sink.mu.Lock()
	s.sink.progress += int64(n)
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

// Done is a no-op for the plain sink; progress is tracked entirely via IncrBy.
func (s *plainStream) Done() {}
