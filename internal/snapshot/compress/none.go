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
)

// noneCodec passes raw bytes through without compression.
// Ext returns "" so the output file is named data.bin with no extension suffix.
type noneCodec struct{}

func (noneCodec) Name() string { return "none" }

func (noneCodec) Ext() string { return "" }

// EncodeFrame returns a copy of src unmodified.
// A copy is returned so callers may not alias or mutate the input through the output.
func (noneCodec) EncodeFrame(src []byte) ([]byte, error) {
	return bytes.Clone(src), nil
}

// EncodeStream copies src to dst byte-for-byte (passthrough, no compression).
func (noneCodec) EncodeStream(dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, src)
	if err != nil {
		return fmt.Errorf("none: passthrough: %w", err)
	}

	return nil
}
