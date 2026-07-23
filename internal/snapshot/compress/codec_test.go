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
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

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

func TestCodec_EncodeFrameStream_ProducesEquivalentFrame(t *testing.T) {
	t.Parallel()

	for _, name := range compress.Names() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

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

			t.Cleanup(func() {
				if err := f.Close(); err != nil {
					t.Errorf("close part file: %v", err)
				}
			})

			var buf bytes.Buffer

			if err := c.EncodeFrameStream(&buf, f, int64(len(src))); err != nil {
				t.Fatalf("EncodeFrameStream(%s): %v", name, err)
			}

			if name != "zstd" {
				if !bytes.Equal(buf.Bytes(), frame) {
					t.Errorf("%s: EncodeFrameStream(.part) != EncodeFrame(raw): stream_len=%d frame_len=%d",
						name, buf.Len(), len(frame))
				}

				return
			}

			dec, err := zstd.NewReader(nil)
			if err != nil {
				t.Fatalf("zstd.NewReader: %v", err)
			}

			t.Cleanup(dec.Close)

			got, err := dec.DecodeAll(buf.Bytes(), nil)
			if err != nil {
				t.Fatalf("DecodeAll streamed zstd frame: %v", err)
			}

			if !bytes.Equal(got, src) {
				t.Errorf("zstd streamed frame decoded %d bytes, want %d", len(got), len(src))
			}
		})
	}
}

func TestCodec_ZstdEncodeFrameStream_FrameMetadata(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		size int
	}{
		{"small", 123},
		{"multiple_blocks", 512 * 1024},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, err := compress.New("zstd", int(compress.LevelBetter))
			if err != nil {
				t.Fatalf("New(zstd): %v", err)
			}

			src := bytes.Repeat([]byte("zstd-frame-metadata"), tc.size/len("zstd-frame-metadata")+1)
			src = src[:tc.size]

			var frame bytes.Buffer

			if err := c.EncodeFrameStream(&frame, bytes.NewReader(src), int64(len(src))); err != nil {
				t.Fatalf("EncodeFrameStream: %v", err)
			}

			var header zstd.Header
			if err := header.Decode(frame.Bytes()); err != nil {
				t.Fatalf("decode frame header: %v", err)
			}

			if !header.HasFCS {
				t.Error("streamed frame header has no content size")
			}

			if header.FrameContentSize != uint64(len(src)) {
				t.Errorf("frame content size = %d, want %d", header.FrameContentSize, len(src))
			}

			if !header.HasCheckSum {
				t.Error("streamed frame header has no checksum")
			}

			dec, err := zstd.NewReader(nil)
			if err != nil {
				t.Fatalf("zstd.NewReader: %v", err)
			}

			t.Cleanup(dec.Close)

			got, err := dec.DecodeAll(frame.Bytes(), nil)
			if err != nil {
				t.Fatalf("DecodeAll: %v", err)
			}

			if !bytes.Equal(got, src) {
				t.Errorf("decoded %d bytes, want %d", len(got), len(src))
			}

			corrupt := bytes.Clone(frame.Bytes())
			corrupt[len(corrupt)-1] ^= 0xFF

			if _, err := dec.DecodeAll(corrupt, nil); err == nil {
				t.Error("DecodeAll accepted a corrupted frame checksum")
			}
		})
	}
}

var (
	errOversizedRead = errors.New("read buffer exceeds cap")
	errSourceRead    = errors.New("source read failed")
	errDestination   = errors.New("destination write failed")
)

type generatedReader struct {
	remaining int64
	maxRead   int
}

func (r *generatedReader) Read(p []byte) (int, error) {
	if r.maxRead > 0 && len(p) > r.maxRead {
		return 0, errOversizedRead
	}

	if r.remaining == 0 {
		return 0, io.EOF
	}

	n := min(int64(len(p)), r.remaining)
	for i := range int(n) {
		p[i] = byte(i*31 + 7)
	}

	r.remaining -= n

	return int(n), nil
}

type terminalErrorReader struct {
	err error
}

func (r terminalErrorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func TestCodec_ZstdEncodeFrameStream_ExactInputAndErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		dst         io.Writer
		src         io.Reader
		size        int64
		wantIs      error
		wantContain []string
	}{
		{
			name:        "negative size",
			dst:         io.Discard,
			src:         bytes.NewReader(nil),
			size:        -1,
			wantContain: []string{"non-positive content size"},
		},
		{
			name:        "zero size",
			dst:         io.Discard,
			src:         bytes.NewReader(nil),
			size:        0,
			wantContain: []string{"non-positive content size"},
		},
		{
			name:        "short source closes writer",
			dst:         io.Discard,
			src:         bytes.NewReader([]byte("short")),
			size:        10,
			wantIs:      io.EOF,
			wantContain: []string{"copy exactly 10 frame bytes", "close frame writer"},
		},
		{
			name:        "extra source byte",
			dst:         io.Discard,
			src:         bytes.NewReader([]byte("extra")),
			size:        4,
			wantContain: []string{"beyond declared frame size 4"},
		},
		{
			name:        "copy error closes writer",
			dst:         io.Discard,
			src:         io.MultiReader(bytes.NewReader([]byte("abc")), terminalErrorReader{err: errSourceRead}),
			size:        10,
			wantIs:      errSourceRead,
			wantContain: []string{"copy exactly 10 frame bytes", "close frame writer"},
		},
		{
			name:        "trailing probe error",
			dst:         io.Discard,
			src:         io.MultiReader(bytes.NewReader([]byte("abc")), terminalErrorReader{err: errSourceRead}),
			size:        3,
			wantIs:      errSourceRead,
			wantContain: []string{"verify source ends at declared frame size 3"},
		},
		{
			name:        "close error",
			dst:         failingWriter{err: errDestination},
			src:         bytes.NewReader([]byte("abc")),
			size:        3,
			wantIs:      errDestination,
			wantContain: []string{"close frame writer"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, err := compress.New("zstd", 0)
			if err != nil {
				t.Fatalf("New(zstd): %v", err)
			}

			err = c.EncodeFrameStream(tc.dst, tc.src, tc.size)
			if err == nil {
				t.Fatal("EncodeFrameStream returned nil error")
			}

			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Errorf("EncodeFrameStream error %v does not wrap %v", err, tc.wantIs)
			}

			for _, text := range tc.wantContain {
				if !strings.Contains(err.Error(), text) {
					t.Errorf("EncodeFrameStream error %q does not contain %q", err, text)
				}
			}
		})
	}
}

func TestCodec_ZstdEncodeFrameStream_CapsSourceReads(t *testing.T) {
	t.Parallel()

	const (
		size       = 16 * 1024 * 1024
		maxReadCap = 1 * 1024 * 1024
	)

	c, err := compress.New("zstd", 0)
	if err != nil {
		t.Fatalf("New(zstd): %v", err)
	}

	src := &generatedReader{remaining: size, maxRead: maxReadCap}

	if err := c.EncodeFrameStream(io.Discard, src, size); err != nil {
		t.Fatalf("EncodeFrameStream: %v", err)
	}

	if src.remaining != 0 {
		t.Errorf("source has %d bytes remaining, want 0", src.remaining)
	}
}

const (
	zstdAllocSizeEnv = "D8_SNAPSHOT_TEST_ZSTD_ALLOC_SIZE"
	zstdAllocFileEnv = "D8_SNAPSHOT_TEST_ZSTD_ALLOC_FILE"
)

func TestCodec_ZstdEncodeFrameStream_AllocationSlope(t *testing.T) {
	t.Parallel()

	if rawSize := os.Getenv(zstdAllocSizeEnv); rawSize != "" {
		size, err := strconv.ParseInt(rawSize, 10, 64)
		if err != nil {
			t.Fatalf("parse child allocation size: %v", err)
		}

		c, err := compress.New("zstd", 0)
		if err != nil {
			t.Fatalf("New(zstd): %v", err)
		}

		runtime.GC()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		if err := c.EncodeFrameStream(io.Discard, &generatedReader{remaining: size}, size); err != nil {
			t.Fatalf("EncodeFrameStream: %v", err)
		}

		var after runtime.MemStats
		runtime.ReadMemStats(&after)

		totalAllocated := after.TotalAlloc - before.TotalAlloc
		resultPath := os.Getenv(zstdAllocFileEnv)

		if err := os.WriteFile(resultPath, []byte(strconv.FormatUint(totalAllocated, 10)), 0o600); err != nil {
			t.Fatalf("write child allocation result: %v", err)
		}

		return
	}

	measure := func(size int64) uint64 {
		t.Helper()

		resultPath := filepath.Join(t.TempDir(), "total-alloc")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		t.Cleanup(cancel)

		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCodec_ZstdEncodeFrameStream_AllocationSlope$", "-test.count=1")
		cmd.Env = append(os.Environ(),
			zstdAllocSizeEnv+"="+strconv.FormatInt(size, 10),
			zstdAllocFileEnv+"="+resultPath,
		)

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("allocation child for %d bytes: %v\n%s", size, err, output)
		}

		rawResult, err := os.ReadFile(resultPath)
		if err != nil {
			t.Fatalf("read allocation child result: %v", err)
		}

		totalAllocated, err := strconv.ParseUint(string(rawResult), 10, 64)
		if err != nil {
			t.Fatalf("parse allocation child result %q: %v", rawResult, err)
		}

		return totalAllocated
	}

	const (
		smallSize       = 2 * 1024 * 1024
		largeSize       = 32 * 1024 * 1024
		maxPayloadSlope = 8 * 1024 * 1024
	)

	smallAllocated := measure(smallSize)
	largeAllocated := measure(largeSize)

	t.Logf("total allocations: %d-byte frame=%d, %d-byte frame=%d",
		smallSize, smallAllocated, largeSize, largeAllocated)

	if largeAllocated > smallAllocated+maxPayloadSlope {
		t.Errorf(
			"total allocation grew by %d bytes from a %d-byte to %d-byte frame; want <= %d",
			largeAllocated-smallAllocated,
			smallSize,
			largeSize,
			maxPayloadSlope,
		)
	}
}

func TestCodec_ZstdEncodeFrameStream_ConcurrentCalls(t *testing.T) {
	t.Parallel()

	c, err := compress.New("zstd", 0)
	if err != nil {
		t.Fatalf("New(zstd): %v", err)
	}

	for i := range 8 {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()

			src := bytes.Repeat([]byte{byte(i + 1)}, 512*1024+i)

			var frame bytes.Buffer

			if err := c.EncodeFrameStream(&frame, bytes.NewReader(src), int64(len(src))); err != nil {
				t.Fatalf("EncodeFrameStream: %v", err)
			}

			dec, err := zstd.NewReader(nil)
			if err != nil {
				t.Fatalf("zstd.NewReader: %v", err)
			}

			t.Cleanup(dec.Close)

			got, err := dec.DecodeAll(frame.Bytes(), nil)
			if err != nil {
				t.Fatalf("DecodeAll: %v", err)
			}

			if !bytes.Equal(got, src) {
				t.Errorf("decoded %d bytes, want %d", len(got), len(src))
			}
		})
	}
}
