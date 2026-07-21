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

	kgzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

// NewReader returns a streaming decompressing io.ReadCloser for src, selecting
// the codec by ext (the file-extension convention used by Codec.Ext: ".zst",
// ".gz", or "" for no compression). It is the decode-side counterpart to
// New/Codec's Encode* methods.
//
// Both the ".zst" and ".gz" readers decode a concatenation of independent
// codec frames/members written by EncodeFrame in one continuous Read stream —
// no per-frame loop is needed by the caller, matching how block-volume chunks
// are stored (see the compress-decode-reader-lz4 follow-on task for lz4, whose
// reader does NOT auto-concatenate frames and needs a stateful frame-swap
// implementation instead).
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
	case "":
		// No codec: src already carries raw bytes. The caller owns src, so
		// Close here must be a no-op rather than closing/consuming it.
		return io.NopCloser(src), nil
	default:
		return nil, fmt.Errorf("%w: extension %q (valid: .zst, .gz, \"\")", ErrUnknownCodec, ext)
	}
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
