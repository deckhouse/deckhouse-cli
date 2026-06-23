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

// TestDecompressToTemp_RoundTrip verifies that data files written by the download path
// (one independent codec frame per chunk, concatenated) decompress back to the original
// bytes for every codec, including the raw (no-extension) case.
func TestDecompressToTemp_RoundTrip(t *testing.T) {
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

			tmpPath, size, err := decompressToTemp(srcPath, dir)
			if err != nil {
				t.Fatalf("decompressToTemp: %v", err)
			}
			defer os.Remove(tmpPath)

			got, err := os.ReadFile(tmpPath)
			if err != nil {
				t.Fatalf("read decompressed: %v", err)
			}

			if size != int64(len(got)) {
				t.Errorf("size mismatch: reported %d, file %d", size, len(got))
			}

			if !bytes.Equal(got, payload) {
				t.Errorf("decompressed bytes differ from original (len got=%d want=%d)", len(got), len(payload))
			}
		})
	}
}

// TestResolveBlockSource_RawUsedInPlace verifies a raw data.bin is streamed in place (no
// second on-disk copy), while a compressed file is decompressed into a removable temp file.
func TestResolveBlockSource_RawUsedInPlace(t *testing.T) {
	dir := t.TempDir()
	rawPath := filepath.Join(dir, "data.bin")
	payload := []byte("raw-block-bytes")

	if err := os.WriteFile(rawPath, payload, 0o600); err != nil {
		t.Fatalf("write raw: %v", err)
	}

	path, size, cleanup, err := resolveBlockSource(rawPath)
	if err != nil {
		t.Fatalf("resolveBlockSource(raw): %v", err)
	}

	if path != rawPath {
		t.Errorf("raw source path = %q, want the archive file %q (no temp copy)", path, rawPath)
	}

	if size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", size, len(payload))
	}

	cleanup()

	if _, statErr := os.Stat(rawPath); statErr != nil {
		t.Errorf("raw archive file must survive cleanup: %v", statErr)
	}
}

func TestResolveBlockSource_CompressedDecompressedToTemp(t *testing.T) {
	dir := t.TempDir()
	payload := bytes.Repeat([]byte("abc"), 1000)

	codec, err := compress.New("zstd", 0)
	if err != nil {
		t.Fatalf("compress.New: %v", err)
	}

	frame, err := codec.EncodeFrame(payload)
	if err != nil {
		t.Fatalf("EncodeFrame: %v", err)
	}

	srcPath := filepath.Join(dir, "data.bin.zst")
	if err := os.WriteFile(srcPath, frame, 0o600); err != nil {
		t.Fatalf("write src: %v", err)
	}

	path, size, cleanup, err := resolveBlockSource(srcPath)
	if err != nil {
		t.Fatalf("resolveBlockSource(compressed): %v", err)
	}

	if path == srcPath {
		t.Error("compressed source must be decompressed into a temp file, not used in place")
	}

	if size != int64(len(payload)) {
		t.Errorf("decompressed size = %d, want %d", size, len(payload))
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read temp: %v", err)
	}

	if !bytes.Equal(got, payload) {
		t.Errorf("decompressed temp bytes differ from original")
	}

	cleanup()

	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Errorf("temp file must be removed by cleanup, stat err = %v", statErr)
	}
}
