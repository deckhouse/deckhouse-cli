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

package volume_test

import (
	gotar "archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/compress"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// writeZstdBlockPayload encodes payload as a single content-size-bearing zstd frame and
// writes it to <nodeDir>/data.bin.zst, returning the on-disk (compressed) byte length.
func writeZstdBlockPayload(t *testing.T, nodeDir string, payload []byte) int64 {
	t.Helper()

	codec, err := compress.New("zstd", 0)
	if err != nil {
		t.Fatalf("compress.New(zstd): %v", err)
	}

	var buf bytes.Buffer
	if err := codec.EncodeFrameStream(&buf, bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatalf("EncodeFrameStream: %v", err)
	}

	path := filepath.Join(nodeDir, archive.DataBlockName(".zst"))
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	return int64(buf.Len())
}

// writeRawBlockPayload writes payload verbatim to <nodeDir>/data.bin (codec "none").
func writeRawBlockPayload(t *testing.T, nodeDir string, payload []byte) {
	t.Helper()

	path := filepath.Join(nodeDir, archive.DataBlockName(""))
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// writeFSPayload writes a single-entry data.tar (codec "none") with rawSize == len(content),
// returning the on-disk tar byte length.
func writeFSPayload(t *testing.T, nodeDir string, content []byte) int64 {
	t.Helper()

	tarPath := filepath.Join(nodeDir, archive.FsTarName)
	tarBytes := buildSingleEntryDataTar(t, content)

	if err := os.WriteFile(tarPath, tarBytes, 0o600); err != nil {
		t.Fatalf("write %s: %v", tarPath, err)
	}

	return int64(len(tarBytes))
}

func TestMeasurePayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T, nodeDir string) (wantRaw, wantStored int64)
		wantMode string
		wantErr  bool
	}{
		{
			name: "success: block zstd payload",
			setup: func(t *testing.T, nodeDir string) (int64, int64) {
				payload := bytes.Repeat([]byte("block-zstd-payload-bytes-"), 200)
				stored := writeZstdBlockPayload(t, nodeDir, payload)

				return int64(len(payload)), stored
			},
			wantMode: "block",
		},
		{
			name: "success: block raw (none) payload",
			setup: func(t *testing.T, nodeDir string) (int64, int64) {
				payload := []byte("raw block payload bytes")
				writeRawBlockPayload(t, nodeDir, payload)

				return int64(len(payload)), int64(len(payload))
			},
			wantMode: "block",
		},
		{
			name: "success: empty block payload is legitimate, not an error",
			setup: func(t *testing.T, nodeDir string) (int64, int64) {
				writeRawBlockPayload(t, nodeDir, nil)

				return 0, 0
			},
			wantMode: "block",
		},
		{
			name: "success: filesystem tar payload",
			setup: func(t *testing.T, nodeDir string) (int64, int64) {
				content := []byte("filesystem entry content bytes")
				stored := writeFSPayload(t, nodeDir, content)

				return int64(len(content)), stored
			},
			wantMode: "filesystem",
		},
		{
			name: "success: node with no payload (aggregator) returns zero values, no error",
			setup: func(*testing.T, string) (int64, int64) {
				return 0, 0
			},
			wantMode: "",
		},
		{
			name: "error: corrupted block payload (truncated zstd frame)",
			setup: func(t *testing.T, nodeDir string) (int64, int64) {
				path := filepath.Join(nodeDir, archive.DataBlockName(".zst"))
				if err := os.WriteFile(path, []byte("not a valid zstd frame"), 0o600); err != nil {
					t.Fatalf("write corrupt payload: %v", err)
				}

				return 0, 0
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nodeDir := t.TempDir()
			wantRaw, wantStored := tt.setup(t, nodeDir)

			got, err := volume.MeasurePayload(context.Background(), nil, nodeDir)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("MeasurePayload: %v", err)
			}

			if got.RawBytes != wantRaw {
				t.Errorf("RawBytes = %d, want %d", got.RawBytes, wantRaw)
			}

			if got.StoredBytes != wantStored {
				t.Errorf("StoredBytes = %d, want %d", got.StoredBytes, wantStored)
			}

			if got.Mode != tt.wantMode {
				t.Errorf("Mode = %q, want %q", got.Mode, tt.wantMode)
			}
		})
	}
}

// buildSingleEntryDataTar builds a minimal data.tar with one regular "none"-codec entry
// carrying the required D8 PAX metadata (matching archive.FSMetadata's contract).
func buildSingleEntryDataTar(t *testing.T, content []byte) []byte {
	t.Helper()

	metadata, err := archive.NewFSMetadata("none", "file.bin", int64(len(content)))
	if err != nil {
		t.Fatalf("NewFSMetadata: %v", err)
	}

	storedPath, err := metadata.StoredPath()
	if err != nil {
		t.Fatalf("StoredPath: %v", err)
	}

	var buf bytes.Buffer

	tw := gotar.NewWriter(&buf)

	hdr := &gotar.Header{
		Format:     gotar.FormatPAX,
		Typeflag:   gotar.TypeReg,
		Name:       storedPath,
		Mode:       0o600,
		Size:       int64(len(content)),
		PAXRecords: metadata.PAXRecords(),
	}

	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}

	if _, err := tw.Write(content); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	return buf.Bytes()
}
