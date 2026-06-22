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

	"github.com/pierrec/lz4/v4"
)

// lz4Codec compresses block-volume frames using the LZ4 frame format.
// Each EncodeFrame call produces a complete, independent LZ4 frame.
// Concatenated LZ4 frames decompress to the concatenation of their inputs.
type lz4Codec struct {
	level lz4.CompressionLevel
}

func newLz4Codec(level int) (Codec, error) {
	return &lz4Codec{level: lz4.CompressionLevel(level)}, nil
}

func (*lz4Codec) Name() string { return "lz4" }

func (*lz4Codec) Ext() string { return ".lz4" }

// EncodeStream compresses src into dst as one complete LZ4 frame.
func (l *lz4Codec) EncodeStream(dst io.Writer, src io.Reader) error {
	w := lz4.NewWriter(dst)

	if err := w.Apply(lz4.CompressionLevelOption(l.level)); err != nil {
		return fmt.Errorf("lz4: apply options: %w", err)
	}

	if _, err := io.Copy(w, src); err != nil {
		_ = w.Close()
		return fmt.Errorf("lz4: copy: %w", err)
	}

	return w.Close()
}

// EncodeFrame compresses src into one complete LZ4 frame.
func (l *lz4Codec) EncodeFrame(src []byte) ([]byte, error) {
	var buf bytes.Buffer

	w := lz4.NewWriter(&buf)

	err := w.Apply(lz4.CompressionLevelOption(l.level))
	if err != nil {
		return nil, fmt.Errorf("lz4: apply options: %w", err)
	}

	_, err = w.Write(src)
	if err != nil {
		_ = w.Close()

		return nil, fmt.Errorf("lz4: write: %w", err)
	}

	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("lz4: close: %w", err)
	}

	return buf.Bytes(), nil
}
