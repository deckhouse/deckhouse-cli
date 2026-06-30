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
	"testing"
	"time"

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

func TestSummaryLabel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		ready int
		total int
		want  string
	}{
		{0, 0, ""},
		{0, 3, " preparing exports (0/3 ready)"},
		{1, 3, " preparing exports (1/3 ready)"},
		{2, 3, " preparing exports (2/3 ready)"},
		{3, 3, " exports ready (3/3)"},
		{5, 3, " exports ready (3/3)"},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("ready_%d_of_%d", tc.ready, tc.total), func(t *testing.T) {
			t.Parallel()

			got := summaryLabel(tc.ready, tc.total)
			if got != tc.want {
				t.Errorf("summaryLabel(%d, %d) = %q, want %q", tc.ready, tc.total, got, tc.want)
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
			" waiting\u2026",
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
			fmt.Sprintf(" % .1f / % .1f", decor.SizeB1024(1024), decor.SizeB1024(1024)),
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
		{"done_100pct", streamStateDone, decor.Statistics{Current: 1024, Total: 1024}, " 100%"},
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

func TestTruncateName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		width int
		want  string
	}{
		{"empty", "", 4, "    "},
		{"shorter", "ab", 4, "ab  "},
		{"exact", "abcd", 4, "abcd"},
		{"one_over", "abcde", 4, "abc…"},
		{"many_over", "abcdefgh", 4, "abc…"},
		{"rune_shorter", "аб", 4, "аб  "},
		{"rune_truncate", "абвгд", 4, "абв…"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := truncateName(tc.input, tc.width)
			if got != tc.want {
				t.Errorf("truncateName(%q, %d) = %q, want %q", tc.input, tc.width, got, tc.want)
			}
		})
	}
}

// TestTTYSink_ReadyCounts verifies the readyCount / totalCount aggregation and the
// waiting→active→done state machine via the observable sink counters.
func TestTTYSink_ReadyCounts(t *testing.T) {
	t.Parallel()

	buf := &bytes.Buffer{}
	sink := newTTYSink(buf)

	s1 := sink.NewStream("stream-1", 0)
	s2 := sink.NewStream("stream-2", 0)
	s3 := sink.NewStream("stream-3", 0)

	if got := sink.totalCount.Load(); got != 3 {
		t.Fatalf("totalCount after 3 NewStream = %d, want 3", got)
	}

	if got := sink.readyCount.Load(); got != 0 {
		t.Fatalf("readyCount before any transition = %d, want 0", got)
	}

	// waiting → active: readyCount increments exactly once.
	s1.Activate()

	if got := sink.readyCount.Load(); got != 1 {
		t.Errorf("readyCount after Activate = %d, want 1", got)
	}

	// Duplicate Activate is a no-op (CAS guarantees exactly-once semantics).
	s1.Activate()

	if got := sink.readyCount.Load(); got != 1 {
		t.Errorf("readyCount after duplicate Activate = %d, want 1", got)
	}

	// waiting → done via Done(): readyCount increments.
	s2.Done()

	if got := sink.readyCount.Load(); got != 2 {
		t.Errorf("readyCount after Done(waiting) = %d, want 2", got)
	}

	// active → done: Activate then Done must not double-increment readyCount.
	s3.Activate()

	if got := sink.readyCount.Load(); got != 3 {
		t.Errorf("readyCount after s3.Activate = %d, want 3", got)
	}

	s3.Done()

	if got := sink.readyCount.Load(); got != 3 {
		t.Errorf("readyCount after s3.Done (was active) = %d, want 3", got)
	}

	// Finish s1 to let sink.Wait() drain all bars.
	s1.Done()

	if got := sink.readyCount.Load(); got != 3 {
		t.Errorf("readyCount after s1.Done (was active) = %d, want 3", got)
	}

	sink.Wait()
}
