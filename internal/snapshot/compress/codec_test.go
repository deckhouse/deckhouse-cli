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
	"io"
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
