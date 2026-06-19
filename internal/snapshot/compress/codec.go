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

// Package compress provides pluggable compression codecs for block-volume frames.
// FS volumes always use plain uncompressed data.tar; the Codec abstraction applies
// only to block volumes.
package compress

import (
	"errors"
	"fmt"
	"slices"
)

// Codec compresses raw bytes into independent frames suitable for block-volume chunks.
// Multiple frames produced by the same codec may be concatenated; the resulting
// byte sequence must be decodable as a single multi-frame stream
// (e.g. zstd multi-frame, raw passthrough for none).
type Codec interface {
	// Name returns the codec identifier, e.g. "zstd", "none".
	Name() string

	// Ext returns the file-extension suffix including the leading dot, e.g. ".zst".
	// Returns "" for no compression so the output file is named data.bin.
	Ext() string

	// EncodeFrame compresses src into a single self-contained frame.
	// Multiple frames may be concatenated to form a valid multi-frame stream.
	EncodeFrame(src []byte) ([]byte, error)
}

// ErrUnknownCodec is returned by New when the requested codec name is not registered.
var ErrUnknownCodec = errors.New("unknown codec")

// DefaultCodecName is the codec used when no --volume-compression flag is provided.
const DefaultCodecName = "zstd"

type codecFactory func(level int) (Codec, error)

// codecRegistry maps codec names to factory functions.
// New codecs (gzip, lz4) are added here when their files are introduced.
var codecRegistry = map[string]codecFactory{
	"zstd": newZstdCodec,
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
