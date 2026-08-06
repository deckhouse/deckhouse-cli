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
	"errors"
	"io"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

// decodeCases maps a codec extension (as accepted by compress.NewReader) to the
// codec name (as accepted by compress.New) that produces frames for it.
var decodeCases = []struct {
	name      string
	ext       string
	codecName string
}{
	{name: "zstd", ext: ".zst", codecName: "zstd"},
	{name: "gzip", ext: ".gz", codecName: "gzip"},
	{name: "lz4", ext: ".lz4", codecName: "lz4"},
	{name: "none", ext: "", codecName: "none"},
}

func TestNewReader_SingleFrameRoundTrip(t *testing.T) {
	t.Helper()

	for _, tc := range decodeCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()

			c, err := compress.New(tc.codecName, 0)
			if err != nil {
				t.Fatalf("New(%s): %v", tc.codecName, err)
			}

			src := bytes.Repeat([]byte("hello streaming decode reader "), 200)

			frame, err := c.EncodeFrame(src)
			if err != nil {
				t.Fatalf("EncodeFrame: %v", err)
			}

			r, err := compress.NewReader(tc.ext, bytes.NewReader(frame))
			if err != nil {
				t.Fatalf("NewReader(%q): %v", tc.ext, err)
			}

			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}

			if err := r.Close(); err != nil {
				t.Errorf("Close on a fully-drained stream returned an error: %v", err)
			}

			if !bytes.Equal(got, src) {
				t.Errorf("round-trip mismatch: len got=%d want=%d", len(got), len(src))
			}
		})
	}
}

func TestNewReader_ConcatenatedFramesRoundTrip(t *testing.T) {
	// Block-volume files concatenate one independent codec frame per chunk;
	// NewReader must decode all of them in a single Read/io.Copy pass with no
	// manual per-frame loop in the caller.
	t.Helper()

	for _, tc := range decodeCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Helper()

			c, err := compress.New(tc.codecName, 0)
			if err != nil {
				t.Fatalf("New(%s): %v", tc.codecName, err)
			}

			chunks := [][]byte{
				bytes.Repeat([]byte("alpha-chunk-"), 100),
				bytes.Repeat([]byte("beta--chunk-"), 100),
				bytes.Repeat([]byte("gamma-chunk-"), 100),
			}

			var frames []byte

			var plain []byte

			for _, chunk := range chunks {
				frame, encErr := c.EncodeFrame(chunk)
				if encErr != nil {
					t.Fatalf("EncodeFrame: %v", encErr)
				}

				frames = append(frames, frame...)
				plain = append(plain, chunk...)
			}

			r, err := compress.NewReader(tc.ext, bytes.NewReader(frames))
			if err != nil {
				t.Fatalf("NewReader(%q): %v", tc.ext, err)
			}

			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("ReadAll concatenated frames: %v", err)
			}

			if err := r.Close(); err != nil {
				t.Errorf("Close on a fully-drained stream returned an error: %v", err)
			}

			if !bytes.Equal(got, plain) {
				t.Errorf("concatenated frames mismatch: len got=%d want=%d", len(got), len(plain))
			}
		})
	}
}

func TestNewReader_UnknownExtension(t *testing.T) {
	t.Helper()

	r, err := compress.NewReader(".xz", bytes.NewReader(nil))
	if !errors.Is(err, compress.ErrUnknownCodec) {
		t.Fatalf("expected ErrUnknownCodec; got: %v", err)
	}

	if r != nil {
		t.Errorf("expected a nil reader on error; got %v", r)
	}
}

func TestNewReader_NonePassthroughCloseDoesNotConsumeSource(t *testing.T) {
	// The "" (none) reader must not close or otherwise consume src beyond what
	// the caller itself read: src is owned by the caller, not the decode reader.
	t.Helper()

	data := []byte("hello none passthrough — the caller owns this reader")
	src := bytes.NewReader(data)

	r, err := compress.NewReader("", src)
	if err != nil {
		t.Fatalf("NewReader(\"\"): %v", err)
	}

	partial := make([]byte, 5)

	n, err := io.ReadFull(r, partial)
	if err != nil {
		t.Fatalf("partial read: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	remaining, err := io.ReadAll(src)
	if err != nil {
		t.Fatalf("reading remainder directly from src: %v", err)
	}

	if !bytes.Equal(remaining, data[n:]) {
		t.Errorf("Close consumed bytes beyond the caller's own reads: remaining=%q want=%q", remaining, data[n:])
	}
}

// TestNewReader_LZ4ReadPastEndReturnsCleanEOF exercises the lz4 frame-swap
// reader's boundary specifically: after all concatenated frames are drained,
// further Read calls must return io.EOF (not hang, not an error), and it must
// be returned exactly once per call rather than repeating stale bytes.
func TestNewReader_LZ4ReadPastEndReturnsCleanEOF(t *testing.T) {
	t.Helper()

	c, err := compress.New("lz4", 0)
	if err != nil {
		t.Fatalf("New(lz4): %v", err)
	}

	chunks := [][]byte{
		bytes.Repeat([]byte("first-frame-"), 50),
		bytes.Repeat([]byte("second-frame-"), 50),
		bytes.Repeat([]byte("third-frame-"), 50),
	}

	var frames []byte

	var plain []byte

	for _, chunk := range chunks {
		frame, encErr := c.EncodeFrame(chunk)
		if encErr != nil {
			t.Fatalf("EncodeFrame: %v", encErr)
		}

		frames = append(frames, frame...)
		plain = append(plain, chunk...)
	}

	r, err := compress.NewReader(".lz4", bytes.NewReader(frames))
	if err != nil {
		t.Fatalf("NewReader(.lz4): %v", err)
	}

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if !bytes.Equal(got, plain) {
		t.Fatalf("round-trip mismatch: len got=%d want=%d", len(got), len(plain))
	}

	buf := make([]byte, 16)

	n, err := r.Read(buf)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read past end: got n=%d err=%v; want n=0 err=io.EOF", n, err)
	}

	n, err = r.Read(buf)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("second Read past end: got n=%d err=%v; want n=0 err=io.EOF (must not hang or error)", n, err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestNewReader_LZ4TruncatedFrameErrors covers the corrupt/truncated-input
// path: a frame cut short must surface a decode error, not a silent partial
// read reported as io.EOF.
func TestNewReader_LZ4TruncatedFrameErrors(t *testing.T) {
	t.Helper()

	c, err := compress.New("lz4", 0)
	if err != nil {
		t.Fatalf("New(lz4): %v", err)
	}

	src := bytes.Repeat([]byte("truncate me please, this needs to be long enough to span blocks "), 200)

	frame, err := c.EncodeFrame(src)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	truncated := frame[:len(frame)/2]

	r, err := compress.NewReader(".lz4", bytes.NewReader(truncated))
	if err != nil {
		t.Fatalf("NewReader(.lz4): %v", err)
	}

	_, err = io.ReadAll(r)
	if err == nil {
		t.Fatal("expected an error decoding a truncated lz4 frame; got nil")
	}

	if errors.Is(err, io.EOF) {
		t.Errorf("truncated input must not be reported as a clean io.EOF: %v", err)
	}
}
