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
	"sync"
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
}

// Stream is a per-stream progress handle returned by Sink.NewStream.
type Stream interface {
	// IncrBy advances the stream's byte counter by n.
	IncrBy(n int)

	// SetTotal updates the stream's expected total byte count.
	SetTotal(total int64)

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
// renderer writing to w with per-stream bars and one overall aggregate bar.
// When tty is false it returns a plain-log fallback that writes
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

type ttySink struct {
	p        *mpb.Progress
	agg      *mpb.Bar
	mu       sync.Mutex
	aggTotal int64
}

func newTTYSink(w io.Writer) *ttySink {
	p := mpb.New(mpb.WithOutput(w))

	agg := p.AddBar(
		0,
		mpb.PrependDecorators(
			decor.Name("total", decor.WC{W: 7}),
			decor.Counters(decor.SizeB1024(0), " %.1f / %.1f"),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
		),
		mpb.BarPriority(0),
	)

	return &ttySink{p: p, agg: agg}
}

// NewStream adds a named per-stream bar and updates the overall aggregate total.
func (s *ttySink) NewStream(name string, total int64) Stream {
	bar := s.p.AddBar(
		total,
		mpb.PrependDecorators(
			decor.Name(name, decor.WC{W: 20}),
			decor.Counters(decor.SizeB1024(0), " %.1f / %.1f"),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
			decor.AverageSpeed(decor.SizeB1024(0), " %.1f "),
			decor.AverageETA(decor.ET_STYLE_GO),
		),
	)

	s.mu.Lock()
	s.aggTotal += total
	s.agg.SetTotal(s.aggTotal, false)
	s.mu.Unlock()

	return &ttyStream{sink: s, bar: bar, total: total}
}

// Wait forces the aggregate bar to complete and drains the mpb renderer.
func (s *ttySink) Wait() {
	s.mu.Lock()
	total := s.aggTotal
	s.mu.Unlock()

	s.agg.SetTotal(total, true)
	s.p.Wait()
}

type ttyStream struct {
	sink  *ttySink
	bar   *mpb.Bar
	mu    sync.Mutex
	total int64
}

func (s *ttyStream) IncrBy(n int) {
	s.bar.IncrBy(n)
	s.sink.agg.IncrBy(n)
}

func (s *ttyStream) SetTotal(total int64) {
	s.mu.Lock()
	delta := total - s.total
	s.total = total
	s.mu.Unlock()

	s.bar.SetTotal(total, false)

	if delta != 0 {
		s.sink.mu.Lock()
		s.sink.aggTotal += delta
		newTotal := s.sink.aggTotal
		s.sink.mu.Unlock()

		s.sink.agg.SetTotal(newTotal, false)
	}
}

func (s *ttyStream) Done() {
	s.mu.Lock()
	total := s.total
	s.mu.Unlock()

	s.bar.SetTotal(total, true)
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

	fmt.Fprintf(s.w, "downloaded % .1f / total % .1f\n", decor.SizeB1024(prog), decor.SizeB1024(tot)) //nolint:errcheck
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

// Done is a no-op for the plain sink; progress is tracked entirely via IncrBy.
func (s *plainStream) Done() {}
