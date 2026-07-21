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

package snapimport

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
)

// TestDecompressInto_RoundTrip verifies that data files written by the download path (one
// independent codec frame per chunk, concatenated) decompress back to the original bytes
// for every codec, including the raw (no-extension) case. decompressInto is still used by
// the filesystem per-entry path (fs.go's decompressEntryToTemp); the block path no longer
// calls it (see putBlockCompressed in volume.go), but the function itself is unchanged.
func TestDecompressInto_RoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("the quick brown fox\n"), 5000)

	cases := []struct {
		codec string
		ext   string
	}{
		{"zstd", ".zst"},
		{"gzip", ".gz"},
		{"lz4", ".lz4"},
		{"none", ""},
	}

	for _, tc := range cases {
		t.Run(tc.codec, func(t *testing.T) {
			codec, err := compress.New(tc.codec, 0)
			if err != nil {
				t.Fatalf("compress.New(%q): %v", tc.codec, err)
			}

			// Two frames concatenated, mirroring multi-chunk block output.
			var buf bytes.Buffer

			for _, part := range [][]byte{payload[:len(payload)/2], payload[len(payload)/2:]} {
				frame, encErr := codec.EncodeFrame(part)
				if encErr != nil {
					t.Fatalf("EncodeFrame: %v", encErr)
				}

				buf.Write(frame)
			}

			dir := t.TempDir()
			srcPath := filepath.Join(dir, "data.bin"+tc.ext)

			if err := os.WriteFile(srcPath, buf.Bytes(), 0o600); err != nil {
				t.Fatalf("write src: %v", err)
			}

			src, err := os.Open(srcPath)
			if err != nil {
				t.Fatalf("open src: %v", err)
			}
			defer src.Close()

			var out bytes.Buffer

			if err := decompressInto(&out, src, tc.ext); err != nil {
				t.Fatalf("decompressInto: %v", err)
			}

			if !bytes.Equal(out.Bytes(), payload) {
				t.Errorf("decompressed bytes differ from original (len got=%d want=%d)", out.Len(), len(payload))
			}
		})
	}
}
