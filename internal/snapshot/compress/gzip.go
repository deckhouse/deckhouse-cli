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
	"bytes"
	"fmt"
	"io"

	kgzip "github.com/klauspost/compress/gzip"
)

// gzipCodec compresses block-volume frames using gzip encoding.
// Each EncodeFrame call produces a complete, independent gzip stream.
// Concatenated gzip streams decompress to the concatenation of their inputs.
type gzipCodec struct {
	level int
}

func newGzipCodec(level int) (Codec, error) {
	if level == 0 {
		level = kgzip.DefaultCompression
	}

	return &gzipCodec{level: level}, nil
}

func (*gzipCodec) Name() string { return "gzip" }

func (*gzipCodec) Ext() string { return ".gz" }

// EncodeStream compresses src into dst as one complete gzip stream.
func (g *gzipCodec) EncodeStream(dst io.Writer, src io.Reader) error {
	w, err := kgzip.NewWriterLevel(dst, g.level)
	if err != nil {
		return fmt.Errorf("gzip: new writer: %w", err)
	}

	if _, err := io.Copy(w, src); err != nil {
		_ = w.Close()
		return fmt.Errorf("gzip: copy: %w", err)
	}

	return w.Close()
}

// EncodeFrame compresses src into one complete gzip stream.
func (g *gzipCodec) EncodeFrame(src []byte) ([]byte, error) {
	var buf bytes.Buffer

	w, err := kgzip.NewWriterLevel(&buf, g.level)
	if err != nil {
		return nil, fmt.Errorf("gzip: new writer: %w", err)
	}

	_, err = w.Write(src)
	if err != nil {
		_ = w.Close()

		return nil, fmt.Errorf("gzip: write: %w", err)
	}

	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("gzip: close: %w", err)
	}

	return buf.Bytes(), nil
}
