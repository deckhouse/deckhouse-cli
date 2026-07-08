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

package compress

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Level is the zstd compression level.
// Re-exported as a type alias so callers need not import the underlying library.
type Level = zstd.EncoderLevel

// Predefined compression levels.
const (
	LevelFastest = zstd.SpeedFastest
	LevelDefault = zstd.SpeedDefault
	LevelBetter  = zstd.SpeedBetterCompression
	LevelBest    = zstd.SpeedBestCompression
)

// Encoder compresses data using the zstd algorithm with CRC always enabled.
// Concurrent calls to EncodeFrame are safe. EncodeStream creates a new
// internal writer per call and is also safe to call concurrently.
type Encoder struct {
	level Level
	// enc is used for EncodeAll (EncodeFrame). EncodeAll is documented as safe
	// for concurrent use across goroutines.
	enc *zstd.Encoder
}

// NewEncoder creates a new Encoder at the given compression level.
// CRC is always enabled to allow integrity checking at decode time.
func NewEncoder(level Level) (*Encoder, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(true), zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, fmt.Errorf("creating zstd encoder: %w", err)
	}

	return &Encoder{level: level, enc: enc}, nil
}

// EncodeStream compresses src into a single self-contained zstd stream and
// writes it to dst. The stream carries a CRC checksum.
// Use this for block volume chunks written into the staging directory.
func (e *Encoder) EncodeStream(dst io.Writer, src io.Reader) error {
	w, err := zstd.NewWriter(dst, zstd.WithEncoderCRC(true), zstd.WithEncoderLevel(e.level))
	if err != nil {
		return fmt.Errorf("creating zstd stream writer: %w", err)
	}

	if _, err := io.Copy(w, src); err != nil {
		_ = w.Close()
		return fmt.Errorf("compressing stream: %w", err)
	}

	return w.Close()
}

// EncodeFrame compresses src into a single independent zstd frame and returns
// the compressed bytes. Multiple frames produced by EncodeFrame can be
// concatenated; the result is a valid multi-frame zstd stream that decodes
// to the concatenation of all original inputs.
// Use this to encode individual block-volume chunks before merging them
// into data.bin.zst.
func (e *Encoder) EncodeFrame(src []byte) ([]byte, error) {
	return e.enc.EncodeAll(src, nil), nil
}

// EncodeFrameStream reads exactly size bytes from src and delegates to
// EncodeFrame. zstd's public streaming Write/Close API does NOT reproduce
// EncodeAll's output byte-for-byte once the input exceeds one zstd block
// (128 KiB, the format's own hard maximum): EncodeAll's multi-block path and
// the streaming writer's multi-block path track repeat-offset match history
// differently internally, so the encoded bytes diverge even for identical
// input and identical encoder options — proven empirically in
// codec_test.go, including with a real *os.File source and the encoder's
// own content-size reset. Since every chunk this package ever encodes is at
// least 16 MiB (see volume.DefaultChunkSize and the CLI's --chunk-size
// floor), the "fits in one 128 KiB block" fast path (which IS byte-for-byte
// identical to EncodeAll, being the same code path) never applies here.
//
// This is the "codec genuinely cannot stream-encode to an identical frame"
// case documented on the Codec.EncodeFrameStream contract: peak memory for
// this call is bounded by size, not by a fixed streaming window, so callers
// MUST keep an upper bound on size (see the CLI's --chunk-size maximum).
func (z *zstdCodec) EncodeFrameStream(dst io.Writer, src io.Reader, size int64) error {
	raw := make([]byte, size)

	if _, err := io.ReadFull(src, raw); err != nil {
		return fmt.Errorf("zstd: read %d bytes for frame: %w", size, err)
	}

	frame, err := z.EncodeFrame(raw)
	if err != nil {
		return err
	}

	if _, err := dst.Write(frame); err != nil {
		return fmt.Errorf("zstd: write frame: %w", err)
	}

	return nil
}

// zstdCodec wraps Encoder to implement the Codec interface.
type zstdCodec struct {
	enc *Encoder
}

// newZstdCodec constructs a zstdCodec at the given level.
// level == 0 uses LevelDefault; any other positive value is forwarded directly.
func newZstdCodec(level int) (Codec, error) {
	l := LevelDefault
	if level > 0 {
		l = Level(level)
	}

	enc, err := NewEncoder(l)
	if err != nil {
		return nil, err
	}

	return &zstdCodec{enc: enc}, nil
}

func (*zstdCodec) Name() string { return "zstd" }

func (*zstdCodec) Ext() string { return ".zst" }

func (z *zstdCodec) EncodeFrame(src []byte) ([]byte, error) {
	return z.enc.EncodeFrame(src)
}

func (z *zstdCodec) EncodeStream(dst io.Writer, src io.Reader) error {
	return z.enc.EncodeStream(dst, src)
}
