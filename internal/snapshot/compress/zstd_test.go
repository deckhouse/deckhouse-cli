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
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

// decodeAll decodes a complete zstd stream or concatenated frames into raw bytes.
func decodeAll(t *testing.T, data []byte) []byte {
	t.Helper()

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}
	defer dec.Close()

	out, err := dec.DecodeAll(data, nil)
	if err != nil {
		t.Fatalf("DecodeAll: %v", err)
	}

	return out
}

// mustNewEncoder creates an Encoder or fails the test.
func mustNewEncoder(t *testing.T, level compress.Level) *compress.Encoder {
	t.Helper()

	enc, err := compress.NewEncoder(level)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}

	return enc
}

func TestEncodeStream_roundTrip(t *testing.T) {
	t.Helper()

	enc := mustNewEncoder(t, compress.LevelDefault)
	src := []byte("hello, zstd stream round-trip test data — some content to compress")

	var buf bytes.Buffer

	if err := enc.EncodeStream(&buf, bytes.NewReader(src)); err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}

	got := decodeAll(t, buf.Bytes())
	if !bytes.Equal(got, src) {
		t.Errorf("round-trip mismatch: got %q; want %q", got, src)
	}
}

func TestEncodeStream_emptySource(t *testing.T) {
	t.Helper()

	enc := mustNewEncoder(t, compress.LevelDefault)

	var buf bytes.Buffer

	if err := enc.EncodeStream(&buf, bytes.NewReader(nil)); err != nil {
		t.Fatalf("EncodeStream empty: %v", err)
	}

	got := decodeAll(t, buf.Bytes())
	if len(got) != 0 {
		t.Errorf("expected empty output; got %d bytes", len(got))
	}
}

func TestEncodeFrame_roundTrip(t *testing.T) {
	t.Helper()

	enc := mustNewEncoder(t, compress.LevelDefault)
	src := []byte("single independent frame round-trip")

	frame, err := enc.EncodeFrame(src)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	got := decodeAll(t, frame)
	if !bytes.Equal(got, src) {
		t.Errorf("round-trip mismatch: got %q; want %q", got, src)
	}
}

func TestEncodeFrame_concatenatedFramesDecode(t *testing.T) {
	// Encode three independent chunks, concatenate the frames, and verify that
	// the concatenated stream decodes to the original concatenation.
	t.Helper()

	enc := mustNewEncoder(t, compress.LevelDefault)

	chunks := [][]byte{
		bytes.Repeat([]byte("alpha"), 100),
		bytes.Repeat([]byte("beta-"), 100),
		bytes.Repeat([]byte("gamma"), 100),
	}

	var frames []byte
	var plain []byte

	for _, c := range chunks {
		frame, err := enc.EncodeFrame(c)
		if err != nil {
			t.Fatalf("EncodeFrame: %v", err)
		}

		frames = append(frames, frame...)
		plain = append(plain, c...)
	}

	got := decodeAll(t, frames)
	if !bytes.Equal(got, plain) {
		t.Errorf("concatenated frame mismatch: got len=%d; want len=%d", len(got), len(plain))
	}
}

func TestEncodeStream_corruptedDataDecodeFails(t *testing.T) {
	t.Helper()

	enc := mustNewEncoder(t, compress.LevelDefault)
	// Use a payload large enough that corrupting the middle does not hit the header.
	src := bytes.Repeat([]byte("abcdefgh"), 200)

	var buf bytes.Buffer

	if err := enc.EncodeStream(&buf, bytes.NewReader(src)); err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}

	corrupt := bytes.Clone(buf.Bytes())
	mid := len(corrupt) / 2

	for i := mid; i < mid+32 && i < len(corrupt); i++ {
		corrupt[i] ^= 0xFF
	}

	dec, err := zstd.NewReader(bytes.NewReader(corrupt))
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}
	defer dec.Close()

	_, readErr := io.ReadAll(dec)
	if readErr == nil {
		t.Error("expected decode error for corrupted stream; got nil")
	}
}

func TestEncodeFrame_corruptedDecodeFails(t *testing.T) {
	t.Helper()

	enc := mustNewEncoder(t, compress.LevelDefault)
	src := bytes.Repeat([]byte("abcdefgh"), 200)

	frame, err := enc.EncodeFrame(src)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	corrupt := bytes.Clone(frame)
	mid := len(corrupt) / 2

	for i := mid; i < mid+32 && i < len(corrupt); i++ {
		corrupt[i] ^= 0xFF
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd.NewReader(nil): %v", err)
	}
	defer dec.Close()

	_, decErr := dec.DecodeAll(corrupt, nil)
	if decErr == nil {
		t.Error("expected decode error for corrupted frame; got nil")
	}
}

func TestNewEncoder_allLevels(t *testing.T) {
	t.Helper()

	levels := []compress.Level{
		compress.LevelFastest,
		compress.LevelDefault,
		compress.LevelBetter,
		compress.LevelBest,
	}

	src := []byte("test payload for all levels")

	for _, level := range levels {
		enc, err := compress.NewEncoder(level)
		if err != nil {
			t.Errorf("NewEncoder(level=%d): %v", level, err)
			continue
		}

		var buf bytes.Buffer

		if err := enc.EncodeStream(&buf, bytes.NewReader(src)); err != nil {
			t.Errorf("EncodeStream(level=%d): %v", level, err)
			continue
		}

		got := decodeAll(t, buf.Bytes())
		if !bytes.Equal(got, src) {
			t.Errorf("level=%d: decoded mismatch", level)
		}
	}
}
