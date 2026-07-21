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
	"bufio"
	"errors"
	"fmt"
	"io"

	kgzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// NewReader returns a streaming decompressing io.ReadCloser for src, selecting
// the codec by ext (the file-extension convention used by Codec.Ext: ".zst",
// ".gz", ".lz4", or "" for no compression). It is the decode-side counterpart
// to New/Codec's Encode* methods.
//
// All four readers decode a concatenation of independent codec frames/members
// written by EncodeFrame in one continuous Read stream — no per-frame loop is
// needed by the caller, matching how block-volume chunks are stored. ".lz4" is
// the odd one out internally: pierrec's lz4.Reader does not auto-continue past
// one frame's end marker into a concatenated next frame, so NewReader wraps it
// in a stateful frame-swap reader (see lz4FrameReader) that reproduces that
// behavior; ".zst" and ".gz" already auto-concatenate in the underlying library.
//
// Callers MUST call Close on the returned io.ReadCloser once done reading:
// the zstd and gzip decoders hold internal buffers (and, for zstd, background
// goroutines) that are only released on Close.
//
// NewReader returns an error wrapping ErrUnknownCodec for any ext it does not
// recognize.
func NewReader(ext string, src io.Reader) (io.ReadCloser, error) {
	switch ext {
	case ".zst":
		zr, err := zstd.NewReader(src)
		if err != nil {
			return nil, fmt.Errorf("open zstd reader: %w", err)
		}

		return &zstdReadCloser{dec: zr}, nil
	case ".gz":
		gr, err := kgzip.NewReader(src)
		if err != nil {
			return nil, fmt.Errorf("open gzip reader: %w", err)
		}

		return gr, nil
	case ".lz4":
		br, ok := src.(*bufio.Reader)
		if !ok {
			br = bufio.NewReader(src)
		}

		return &lz4FrameReader{br: br}, nil
	case "":
		// No codec: src already carries raw bytes. The caller owns src, so
		// Close here must be a no-op rather than closing/consuming it.
		return io.NopCloser(src), nil
	default:
		return nil, fmt.Errorf("%w: extension %q (valid: .zst, .gz, .lz4, \"\")", ErrUnknownCodec, ext)
	}
}

// lz4FrameReader decodes a concatenation of independent lz4 frames from a
// shared bufio.Reader, relocating the per-frame-loop technique that
// snapimport.decompressLZ4Frames uses for a Writer sink into an io.Reader
// shape: lz4.Reader consumes exactly one frame's bytes and leaves the rest
// buffered in br, so a fresh lz4.NewReader(br) per frame picks up where the
// previous one left off.
type lz4FrameReader struct {
	br  *bufio.Reader
	cur *lz4.Reader
}

// Read decodes bytes from the current lz4 frame, transparently advancing to
// the next concatenated frame (or returning io.EOF once none remain) when the
// current frame is exhausted.
func (r *lz4FrameReader) Read(p []byte) (int, error) {
	for {
		if r.cur == nil {
			if _, err := r.br.Peek(1); err != nil {
				if errors.Is(err, io.EOF) {
					return 0, io.EOF
				}

				return 0, fmt.Errorf("peek lz4 source: %w", err)
			}

			r.cur = lz4.NewReader(r.br)
		}

		n, err := r.cur.Read(p)
		if errors.Is(err, io.EOF) {
			r.cur = nil

			if n > 0 {
				return n, nil
			}
			// This frame yielded no bytes on its terminal read; loop to peek
			// for the next frame (or true end-of-stream) without surfacing a
			// spurious EOF to the caller mid-stream.
			continue
		}

		if err != nil {
			return n, fmt.Errorf("decode lz4 frame: %w", err)
		}

		return n, nil
	}
}

// Close is a no-op: pierrec's lz4.Reader has no Close method to release, and
// br wraps a caller-owned src that this reader does not own.
func (r *lz4FrameReader) Close() error {
	return nil
}

// zstdReadCloser adapts *zstd.Decoder to io.ReadCloser: zstd.Decoder.Close
// returns no error, but the internal buffers and goroutines it holds are only
// released once Close is called, so the adapter still calls it and reports no
// error to satisfy io.Closer.
type zstdReadCloser struct {
	dec *zstd.Decoder
}

// Read reads decompressed bytes from the underlying zstd decoder.
func (z *zstdReadCloser) Read(p []byte) (int, error) {
	return z.dec.Read(p)
}

// Close releases the zstd decoder's internal resources. It always returns nil,
// matching zstd.Decoder.Close's own signature.
func (z *zstdReadCloser) Close() error {
	z.dec.Close()

	return nil
}
