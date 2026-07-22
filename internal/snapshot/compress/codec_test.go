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

package compress_test

import (
	"bytes"
	stdgzip "compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

func TestNew_UnknownCodec(t *testing.T) {
	t.Helper()

	_, err := compress.New("bogus", 0)
	if !errors.Is(err, compress.ErrUnknownCodec) {
		t.Fatalf("expected ErrUnknownCodec; got: %v", err)
	}
}

func TestNew_Zstd(t *testing.T) {
	t.Helper()

	c, err := compress.New("zstd", 0)
	if err != nil {
		t.Fatalf("New(zstd): %v", err)
	}

	if c.Name() != "zstd" {
		t.Errorf("Name() = %q; want %q", c.Name(), "zstd")
	}

	if c.Ext() != ".zst" {
		t.Errorf("Ext() = %q; want %q", c.Ext(), ".zst")
	}
}

func TestNew_None(t *testing.T) {
	t.Helper()

	c, err := compress.New("none", 0)
	if err != nil {
		t.Fatalf("New(none): %v", err)
	}

	if c.Name() != "none" {
		t.Errorf("Name() = %q; want %q", c.Name(), "none")
	}

	if c.Ext() != "" {
		t.Errorf("Ext() = %q; want empty string", c.Ext())
	}
}

func TestNames_ContainsAllCodecs(t *testing.T) {
	t.Helper()

	names := compress.Names()
	found := make(map[string]bool, len(names))

	for _, n := range names {
		found[n] = true
	}

	for _, want := range []string{"zstd", "gzip", "lz4", "none"} {
		if !found[want] {
			t.Errorf("Names() does not contain %q; got %v", want, names)
		}
	}
}

func TestNames_Sorted(t *testing.T) {
	t.Helper()

	names := compress.Names()

	for i := 1; i < len(names); i++ {
		if names[i] < names[i-1] {
			t.Errorf("Names() not sorted at index %d: %q > %q", i, names[i-1], names[i])
		}
	}
}

func TestUserSelectableNames_IsExactlyNoneAndZstd(t *testing.T) {
	t.Helper()

	names := compress.UserSelectableNames()

	want := map[string]bool{"none": true, "zstd": true}
	if len(names) != len(want) {
		t.Fatalf("UserSelectableNames() = %v; want exactly %v", names, want)
	}

	for _, n := range names {
		if !want[n] {
			t.Errorf("UserSelectableNames() contains unexpected codec %q; got %v", n, names)
		}
	}
}

// TestUserSelectableNames_ReturnedSliceIsACopy asserts mutating the returned
// slice cannot perturb the package's allow-list for subsequent callers.
func TestUserSelectableNames_ReturnedSliceIsACopy(t *testing.T) {
	t.Helper()

	names := compress.UserSelectableNames()
	if len(names) == 0 {
		t.Fatal("UserSelectableNames() returned an empty slice")
	}

	names[0] = "tampered"

	again := compress.UserSelectableNames()
	if again[0] == "tampered" {
		t.Fatal("mutating a returned slice affected a subsequent UserSelectableNames() call")
	}
}

func TestIsUserSelectable(t *testing.T) {
	t.Helper()

	cases := []struct {
		name string
		want bool
	}{
		{"none", true},
		{"zstd", true},
		{"gzip", false},
		{"lz4", false},
		{"bogus-codec", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := compress.IsUserSelectable(tc.name)
			if got != tc.want {
				t.Errorf("IsUserSelectable(%q) = %v; want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestDefaultCodecName_IsZstd(t *testing.T) {
	t.Helper()

	if compress.DefaultCodecName != "zstd" {
		t.Errorf("DefaultCodecName = %q; want %q", compress.DefaultCodecName, "zstd")
	}

	c, err := compress.New(compress.DefaultCodecName, 0)
	if err != nil {
		t.Fatalf("New(DefaultCodecName): %v", err)
	}

	if c.Name() != "zstd" {
		t.Errorf("default codec Name() = %q; want %q", c.Name(), "zstd")
	}
}

func TestCodec_ZstdEncodeFrame_RoundTrip(t *testing.T) {
	t.Helper()

	c, err := compress.New("zstd", 0)
	if err != nil {
		t.Fatalf("New(zstd): %v", err)
	}

	src := bytes.Repeat([]byte("hello zstd codec "), 100)

	frame, err := c.EncodeFrame(src)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}

	defer dec.Close()

	got, err := dec.DecodeAll(frame, nil)
	if err != nil {
		t.Fatalf("DecodeAll: %v", err)
	}

	if !bytes.Equal(got, src) {
		t.Errorf("zstd round-trip mismatch: len got=%d want=%d", len(got), len(src))
	}
}

func TestCodec_ZstdEncodeFrame_ConcatenatedFrames(t *testing.T) {
	// Frames produced by EncodeFrame must concatenate into a valid multi-frame zstd stream.
	t.Helper()

	c, err := compress.New("zstd", 0)
	if err != nil {
		t.Fatalf("New(zstd): %v", err)
	}

	chunks := [][]byte{
		bytes.Repeat([]byte("alpha"), 100),
		bytes.Repeat([]byte("beta-"), 100),
		bytes.Repeat([]byte("gamma"), 100),
	}

	var frames []byte

	var plain []byte

	for _, ch := range chunks {
		frame, encErr := c.EncodeFrame(ch)
		if encErr != nil {
			t.Fatalf("EncodeFrame: %v", encErr)
		}

		frames = append(frames, frame...)
		plain = append(plain, ch...)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}

	defer dec.Close()

	got, err := dec.DecodeAll(frames, nil)
	if err != nil {
		t.Fatalf("DecodeAll concatenated frames: %v", err)
	}

	if !bytes.Equal(got, plain) {
		t.Errorf("concatenated frames mismatch: len got=%d want=%d", len(got), len(plain))
	}
}

func TestCodec_NoneEncodeFrame_Passthrough(t *testing.T) {
	t.Helper()

	c, err := compress.New("none", 0)
	if err != nil {
		t.Fatalf("New(none): %v", err)
	}

	src := []byte("hello none codec — raw passthrough")

	out, err := c.EncodeFrame(src)
	if err != nil {
		t.Fatalf("EncodeFrame(none): %v", err)
	}

	if !bytes.Equal(out, src) {
		t.Errorf("none passthrough mismatch: got %q want %q", out, src)
	}
}

func TestCodec_NoneEncodeFrame_IsolatedCopy(t *testing.T) {
	// none codec must return a copy so mutating the input does not affect the output.
	t.Helper()

	c, err := compress.New("none", 0)
	if err != nil {
		t.Fatalf("New(none): %v", err)
	}

	src := []byte("copy isolation test")

	out, err := c.EncodeFrame(src)
	if err != nil {
		t.Fatalf("EncodeFrame(none): %v", err)
	}

	src[0] = 'X'

	if out[0] == 'X' {
		t.Error("none EncodeFrame returned alias to input; mutation of src visible in output")
	}
}

func TestNew_Gzip(t *testing.T) {
	t.Helper()

	c, err := compress.New("gzip", 0)
	if err != nil {
		t.Fatalf("New(gzip): %v", err)
	}

	if c.Name() != "gzip" {
		t.Errorf("Name() = %q; want %q", c.Name(), "gzip")
	}

	if c.Ext() != ".gz" {
		t.Errorf("Ext() = %q; want %q", c.Ext(), ".gz")
	}
}

func TestNew_Lz4(t *testing.T) {
	t.Helper()

	c, err := compress.New("lz4", 0)
	if err != nil {
		t.Fatalf("New(lz4): %v", err)
	}

	if c.Name() != "lz4" {
		t.Errorf("Name() = %q; want %q", c.Name(), "lz4")
	}

	if c.Ext() != ".lz4" {
		t.Errorf("Ext() = %q; want %q", c.Ext(), ".lz4")
	}
}

func TestCodec_GzipEncodeFrame_RoundTrip(t *testing.T) {
	t.Helper()

	c, err := compress.New("gzip", 0)
	if err != nil {
		t.Fatalf("New(gzip): %v", err)
	}

	src := bytes.Repeat([]byte("hello gzip codec "), 100)

	frame, err := c.EncodeFrame(src)
	if err != nil {
		t.Fatalf("EncodeFrame(gzip): %v", err)
	}

	r, err := stdgzip.NewReader(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}

	defer func() { _ = r.Close() }()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("io.ReadAll(gzip): %v", err)
	}

	if !bytes.Equal(got, src) {
		t.Errorf("gzip round-trip mismatch: len got=%d want=%d", len(got), len(src))
	}
}

func TestCodec_GzipEncodeFrame_ConcatenatedFrames(t *testing.T) {
	// Concatenated gzip streams must decode to the concatenation of their inputs.
	t.Helper()

	c, err := compress.New("gzip", 0)
	if err != nil {
		t.Fatalf("New(gzip): %v", err)
	}

	chunks := [][]byte{
		bytes.Repeat([]byte("alpha"), 100),
		bytes.Repeat([]byte("beta-"), 100),
	}

	var frames []byte

	var plain []byte

	for _, ch := range chunks {
		frame, encErr := c.EncodeFrame(ch)
		if encErr != nil {
			t.Fatalf("EncodeFrame(gzip): %v", encErr)
		}

		frames = append(frames, frame...)
		plain = append(plain, ch...)
	}

	var got []byte

	rd := bytes.NewReader(frames)

	for rd.Len() > 0 {
		r, openErr := stdgzip.NewReader(rd)
		if openErr != nil {
			t.Fatalf("gzip.NewReader: %v", openErr)
		}

		chunk, readErr := io.ReadAll(r)
		if readErr != nil {
			t.Fatalf("io.ReadAll: %v", readErr)
		}

		_ = r.Close()

		got = append(got, chunk...)
	}

	if !bytes.Equal(got, plain) {
		t.Errorf("gzip concatenated frames mismatch: len got=%d want=%d", len(got), len(plain))
	}
}

func TestCodec_Lz4EncodeFrame_RoundTrip(t *testing.T) {
	t.Helper()

	c, err := compress.New("lz4", 0)
	if err != nil {
		t.Fatalf("New(lz4): %v", err)
	}

	src := bytes.Repeat([]byte("hello lz4 codec "), 100)

	frame, err := c.EncodeFrame(src)
	if err != nil {
		t.Fatalf("EncodeFrame(lz4): %v", err)
	}

	r := lz4.NewReader(bytes.NewReader(frame))

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("lz4 decode: %v", err)
	}

	if !bytes.Equal(got, src) {
		t.Errorf("lz4 round-trip mismatch: len got=%d want=%d", len(got), len(src))
	}
}

func TestCodec_Lz4EncodeFrame_ConcatenatedFrames(t *testing.T) {
	// Concatenated LZ4 frames decode to the concatenation of their inputs.
	// pierrec/lz4/v4 returns EOF after each frame; a per-frame reader loop is required
	// (the same pattern used in block_merge when decompressing a merged .lz4 file).
	t.Helper()

	c, err := compress.New("lz4", 0)
	if err != nil {
		t.Fatalf("New(lz4): %v", err)
	}

	chunks := [][]byte{
		bytes.Repeat([]byte("alpha"), 100),
		bytes.Repeat([]byte("beta-"), 100),
	}

	var frames []byte

	var plain []byte

	for _, ch := range chunks {
		frame, encErr := c.EncodeFrame(ch)
		if encErr != nil {
			t.Fatalf("EncodeFrame(lz4): %v", encErr)
		}

		frames = append(frames, frame...)
		plain = append(plain, ch...)
	}

	underlying := bytes.NewReader(frames)

	var got []byte

	for underlying.Len() > 0 {
		r := lz4.NewReader(underlying)

		chunk, readErr := io.ReadAll(r)
		if readErr != nil {
			t.Fatalf("lz4 decode frame: %v", readErr)
		}

		got = append(got, chunk...)
	}

	if !bytes.Equal(got, plain) {
		t.Errorf("lz4 concatenated frames mismatch: len got=%d want=%d", len(got), len(plain))
	}
}

func TestCodec_ZstdEncodeStream_RoundTrip(t *testing.T) {
	t.Helper()

	c, err := compress.New("zstd", 0)
	if err != nil {
		t.Fatalf("New(zstd): %v", err)
	}

	src := bytes.Repeat([]byte("hello zstd stream "), 200)

	var buf bytes.Buffer

	if err := c.EncodeStream(&buf, bytes.NewReader(src)); err != nil {
		t.Fatalf("EncodeStream(zstd): %v", err)
	}

	dec, decErr := zstd.NewReader(nil)
	if decErr != nil {
		t.Fatalf("zstd.NewReader: %v", decErr)
	}

	defer dec.Close()

	got, decErr := dec.DecodeAll(buf.Bytes(), nil)
	if decErr != nil {
		t.Fatalf("zstd DecodeAll: %v", decErr)
	}

	if !bytes.Equal(got, src) {
		t.Errorf("zstd EncodeStream round-trip mismatch: len got=%d want=%d", len(got), len(src))
	}
}

func TestCodec_GzipEncodeStream_RoundTrip(t *testing.T) {
	t.Helper()

	c, err := compress.New("gzip", 0)
	if err != nil {
		t.Fatalf("New(gzip): %v", err)
	}

	src := bytes.Repeat([]byte("hello gzip stream "), 200)

	var buf bytes.Buffer

	if err := c.EncodeStream(&buf, bytes.NewReader(src)); err != nil {
		t.Fatalf("EncodeStream(gzip): %v", err)
	}

	r, openErr := stdgzip.NewReader(&buf)
	if openErr != nil {
		t.Fatalf("gzip.NewReader: %v", openErr)
	}

	defer func() { _ = r.Close() }()

	got, readErr := io.ReadAll(r)
	if readErr != nil {
		t.Fatalf("gzip ReadAll: %v", readErr)
	}

	if !bytes.Equal(got, src) {
		t.Errorf("gzip EncodeStream round-trip mismatch: len got=%d want=%d", len(got), len(src))
	}
}

func TestCodec_Lz4EncodeStream_RoundTrip(t *testing.T) {
	t.Helper()

	c, err := compress.New("lz4", 0)
	if err != nil {
		t.Fatalf("New(lz4): %v", err)
	}

	src := bytes.Repeat([]byte("hello lz4 stream "), 200)

	var buf bytes.Buffer

	if err := c.EncodeStream(&buf, bytes.NewReader(src)); err != nil {
		t.Fatalf("EncodeStream(lz4): %v", err)
	}

	got, readErr := io.ReadAll(lz4.NewReader(&buf))
	if readErr != nil {
		t.Fatalf("lz4 ReadAll: %v", readErr)
	}

	if !bytes.Equal(got, src) {
		t.Errorf("lz4 EncodeStream round-trip mismatch: len got=%d want=%d", len(got), len(src))
	}
}

func TestCodec_NoneEncodeStream_Passthrough(t *testing.T) {
	t.Helper()

	c, err := compress.New("none", 0)
	if err != nil {
		t.Fatalf("New(none): %v", err)
	}

	src := []byte("none stream passthrough — byte-identical")

	var buf bytes.Buffer

	if err := c.EncodeStream(&buf, bytes.NewReader(src)); err != nil {
		t.Fatalf("EncodeStream(none): %v", err)
	}

	if !bytes.Equal(buf.Bytes(), src) {
		t.Errorf("none EncodeStream passthrough mismatch: got %q want %q", buf.Bytes(), src)
	}
}

// TestCodec_EncodeFrameStream_MatchesEncodeFrame is the empirical proof the
// EncodeFrameStream contract demands: for every registered codec, streaming
// a chunk's raw bytes from a real on-disk file (the same shape as a
// downloadChunk ".part" file) through EncodeFrameStream must produce the
// exact same bytes as EncodeFrame(rawBytes) — including zstd, whose
// implementation satisfies this by delegating to EncodeFrame internally
// rather than by matching the library's independent streaming API (see
// zstd.go's EncodeFrameStream doc comment for why the two diverge).
func TestCodec_EncodeFrameStream_MatchesEncodeFrame(t *testing.T) {
	t.Helper()

	for _, name := range compress.Names() {
		t.Run(name, func(t *testing.T) {
			c, err := compress.New(name, 0)
			if err != nil {
				t.Fatalf("New(%s): %v", name, err)
			}

			src := bytes.Repeat([]byte("stream-encode-from-part payload, "), 50000)

			frame, err := c.EncodeFrame(src)
			if err != nil {
				t.Fatalf("EncodeFrame(%s): %v", name, err)
			}

			partPath := filepath.Join(t.TempDir(), "chunk_00000.part")
			if err := os.WriteFile(partPath, src, 0o644); err != nil {
				t.Fatalf("write part file: %v", err)
			}

			f, err := os.Open(partPath)
			if err != nil {
				t.Fatalf("open part file: %v", err)
			}

			defer func() { _ = f.Close() }()

			var buf bytes.Buffer

			if err := c.EncodeFrameStream(&buf, f, int64(len(src))); err != nil {
				t.Fatalf("EncodeFrameStream(%s): %v", name, err)
			}

			if !bytes.Equal(buf.Bytes(), frame) {
				t.Errorf("%s: EncodeFrameStream(.part) != EncodeFrame(raw): stream_len=%d frame_len=%d",
					name, buf.Len(), len(frame))
			}
		})
	}
}

// maxReadTracker records the largest single buffer length any caller ever
// requested via Read, hiding whatever io.WriterTo/io.ReaderFrom fast path
// the wrapped reader might otherwise offer io.Copy — so the recorded
// maximum reflects the actual buffering strategy of the code under test,
// not an unrelated stdlib optimization.
type maxReadTracker struct {
	r       io.Reader
	maxRead int
}

func (m *maxReadTracker) Read(p []byte) (int, error) {
	if len(p) > m.maxRead {
		m.maxRead = len(p)
	}

	// io.EOF must pass through unwrapped and unaltered: io.Copy's loop
	// compares it with == io.EOF, not errors.Is, so wrapping it here would
	// turn a normal end-of-stream signal into a hard read error.
	return m.r.Read(p)
}

// plainWriter exposes only io.Writer, hiding any io.ReaderFrom the wrapped
// writer might implement (e.g. *bytes.Buffer), so io.Copy cannot bypass its
// default fixed-size buffer copy loop when writing into it.
type plainWriter struct {
	w io.Writer
}

func (p *plainWriter) Write(b []byte) (int, error) {
	n, err := p.w.Write(b)
	if err != nil {
		return n, fmt.Errorf("plain write: %w", err)
	}

	return n, nil
}

// TestCodec_EncodeFrameStream_MemoryBound proves the actual buffering
// behavior behind the EncodeFrameStream contract, per the "verified
// empirically, not from a doc comment" code-style rule: none/gzip/lz4 must
// never request a single Read() anywhere near the full chunk size (they
// stream through their own small internal buffer), while zstd is expected
// to request exactly the full size in one call — the documented, bounded
// fallback (see zstd.go).
func TestCodec_EncodeFrameStream_MemoryBound(t *testing.T) {
	t.Helper()

	const size = 64 * 1024 * 1024 // dwarfs any codec's own internal buffer
	// smallBufferCeiling sits above lz4's default 4 MiB block size (the
	// largest internal buffer among the streaming codecs) yet far below
	// size and the package's 16 MiB chunk-size floor, so it cleanly
	// separates "streams through its own buffer" from "read the whole
	// chunk".
	const smallBufferCeiling = 8 * 1024 * 1024

	src := bytes.Repeat([]byte{0xAB}, size)

	cases := []struct {
		name           string
		wantFullBuffer bool
	}{
		{"none", false},
		{"gzip", false},
		{"lz4", false},
		{"zstd", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := compress.New(tc.name, 0)
			if err != nil {
				t.Fatalf("New(%s): %v", tc.name, err)
			}

			tracker := &maxReadTracker{r: bytes.NewReader(src)}

			var buf bytes.Buffer

			if err := c.EncodeFrameStream(&plainWriter{&buf}, tracker, size); err != nil {
				t.Fatalf("EncodeFrameStream(%s): %v", tc.name, err)
			}

			gotFullBuffer := tracker.maxRead >= smallBufferCeiling
			if gotFullBuffer != tc.wantFullBuffer {
				t.Errorf("%s: max single Read() request was %d bytes (full-buffer=%v); want full-buffer=%v",
					tc.name, tracker.maxRead, gotFullBuffer, tc.wantFullBuffer)
			}
		})
	}
}
