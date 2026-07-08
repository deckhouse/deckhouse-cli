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

// Package compress provides pluggable compression codecs used for both block-volume
// chunk frames (EncodeFrame) and per-file filesystem-volume entries (EncodeStream).
// EncodeFrame produces independent frames that may be concatenated into a valid
// multi-frame stream.  EncodeStream writes one self-contained compressed stream per
// call and is the path used for per-file FS compression inside data.tar.
package compress

import (
	"errors"
	"fmt"
	"io"
	"slices"
)

// Codec compresses data for snapshot volume payloads.
//
// EncodeFrame is used for block-volume chunks: each call produces one independent
// frame; concatenated frames form a valid multi-frame stream decodable by standard
// tools (e.g. zstd, gunzip, lz4 -d).
//
// EncodeStream is used for per-file FS compression: it streams src into dst as one
// self-contained compressed stream.  Each call is safe to invoke concurrently.
// For codec=none, EncodeStream is a byte-identical passthrough.
//
// EncodeFrameStream produces the SAME independent frame EncodeFrame would for the
// raw bytes src yields, but reads them from src rather than requiring the whole
// chunk as a []byte up front. It is the finalize-time path for a block chunk whose
// raw bytes already live in a durable file on disk (see volume.downloadChunk):
// callers pass the open file plus its exact known length so an implementation that
// cannot avoid buffering can size its allocation once instead of growing it.
type Codec interface {
	// Name returns the codec identifier, e.g. "zstd", "none".
	Name() string

	// Ext returns the file-extension suffix including the leading dot, e.g. ".zst".
	// Returns "" for no compression so the output file carries no extension.
	Ext() string

	// EncodeFrame compresses src into a single self-contained frame.
	// Multiple frames may be concatenated to form a valid multi-frame stream.
	EncodeFrame(src []byte) ([]byte, error)

	// EncodeStream compresses src into dst as one self-contained stream.
	// It is safe to call concurrently from multiple goroutines.
	EncodeStream(dst io.Writer, src io.Reader) error

	// EncodeFrameStream reads exactly size bytes from src and writes the resulting
	// independent frame to dst — byte-identical to EncodeFrame(rawBytes) for the
	// same rawBytes, verified in codec_test.go for every registered codec.
	// Implementations that can genuinely stream bound their memory to their own
	// internal buffer, independent of size; see the zstd implementation's doc
	// comment for the one documented exception.
	EncodeFrameStream(dst io.Writer, src io.Reader, size int64) error
}

// ErrUnknownCodec is returned by New when the requested codec name is not registered.
var ErrUnknownCodec = errors.New("unknown codec")

// DefaultCodecName is the codec used when no --volume-compression flag is provided.
const DefaultCodecName = "zstd"

type codecFactory func(level int) (Codec, error)

// codecRegistry maps codec names to factory functions.
var codecRegistry = map[string]codecFactory{
	"zstd": newZstdCodec,
	"gzip": newGzipCodec,
	"lz4":  newLz4Codec,
	"none": func(_ int) (Codec, error) { return noneCodec{}, nil },
}

// New constructs the named codec at the requested compression level.
// level is passed to codecs that support it; use 0 for the codec default.
// Unknown names return ErrUnknownCodec.
func New(name string, level int) (Codec, error) {
	factory, ok := codecRegistry[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q (valid: %v)", ErrUnknownCodec, name, Names())
	}

	return factory(level)
}

// Names returns the registered codec names in sorted order.
func Names() []string {
	names := make([]string, 0, len(codecRegistry))
	for n := range codecRegistry {
		names = append(names, n)
	}

	slices.Sort(names)

	return names
}
