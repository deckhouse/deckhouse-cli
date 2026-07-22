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

package archive_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

func TestBlockChunkIndexPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dataFilePath string
		want         string
	}{
		{"data.bin.zst", "data.bin.zst.chunkidx"},
		{"data.bin.lz4", "data.bin.lz4.chunkidx"},
		{"data.bin.gz", "data.bin.gz.chunkidx"},
		{"data.bin", "data.bin.chunkidx"},
		{filepath.Join("data", "pvc-1.bin.zst"), filepath.Join("data", "pvc-1.bin.zst.chunkidx")},
	}

	for _, tc := range tests {
		t.Run(tc.dataFilePath, func(t *testing.T) {
			t.Parallel()

			got := archive.BlockChunkIndexPath(tc.dataFilePath)
			if got != tc.want {
				t.Errorf("BlockChunkIndexPath(%q) = %q; want %q", tc.dataFilePath, got, tc.want)
			}
		})
	}
}

func TestBlockChunkIndex_WriteReadRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		idx  archive.BlockChunkIndex
	}{
		{
			name: "multiple chunks",
			idx: archive.BlockChunkIndex{
				ChunkSize:            256 * 1024 * 1024,
				TotalSize:            10 * 1024 * 1024 * 1024,
				CompressedChunkSizes: []int64{1000, 2000, 1500, 999},
			},
		},
		{
			name: "single chunk",
			idx: archive.BlockChunkIndex{
				ChunkSize:            1024,
				TotalSize:            100,
				CompressedChunkSizes: []int64{42},
			},
		},
		{
			name: "zero total size, no chunks",
			idx: archive.BlockChunkIndex{
				ChunkSize:            1024,
				TotalSize:            0,
				CompressedChunkSizes: []int64{},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dataFilePath := filepath.Join(t.TempDir(), "data.bin.zst")

			if err := archive.WriteBlockChunkIndex(dataFilePath, tc.idx); err != nil {
				t.Fatalf("WriteBlockChunkIndex: %v", err)
			}

			got, found, err := archive.ReadBlockChunkIndex(dataFilePath)
			if err != nil {
				t.Fatalf("ReadBlockChunkIndex: %v", err)
			}

			if !found {
				t.Fatal("ReadBlockChunkIndex found = false; want true after WriteBlockChunkIndex")
			}

			// Treat a nil and an empty slice as equal — JSON round-tripping an
			// empty slice through encoding/json can decode into either, and both
			// mean the same thing here ("no chunks recorded").
			if len(got.CompressedChunkSizes) == 0 && len(tc.idx.CompressedChunkSizes) == 0 {
				got.CompressedChunkSizes = tc.idx.CompressedChunkSizes
			}

			if !reflect.DeepEqual(got, tc.idx) {
				t.Errorf("ReadBlockChunkIndex = %+v; want %+v", got, tc.idx)
			}
		})
	}
}

func TestBlockChunkIndex_MissingFileIsNotAnError(t *testing.T) {
	t.Parallel()

	dataFilePath := filepath.Join(t.TempDir(), "data.bin.zst")

	idx, found, err := archive.ReadBlockChunkIndex(dataFilePath)
	if err != nil {
		t.Fatalf("ReadBlockChunkIndex on a missing sidecar: %v", err)
	}

	if found {
		t.Error("found = true; want false when the sidecar does not exist")
	}

	if !reflect.DeepEqual(idx, archive.BlockChunkIndex{}) {
		t.Errorf("idx = %+v; want the zero value when not found", idx)
	}
}

// TestBlockChunkIndex_CorruptFileIsAnError pins the sentinel a caller relies
// on to route an unparseable sidecar to the degrade-not-fail path (fall back
// to byte-zero resume) instead of a hard abort: the error MUST satisfy
// errors.Is(err, archive.ErrCorruptBlockChunkIndex), not just be non-nil.
func TestBlockChunkIndex_CorruptFileIsAnError(t *testing.T) {
	t.Parallel()

	dataFilePath := filepath.Join(t.TempDir(), "data.bin.zst")

	if err := os.WriteFile(archive.BlockChunkIndexPath(dataFilePath), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, found, err := archive.ReadBlockChunkIndex(dataFilePath)
	if err == nil {
		t.Fatal("expected an error reading a corrupt chunk index, got nil")
	}

	if !errors.Is(err, archive.ErrCorruptBlockChunkIndex) {
		t.Errorf("err = %v; want it to satisfy errors.Is(err, archive.ErrCorruptBlockChunkIndex)", err)
	}

	if found {
		t.Error("found = true; want false on a read error")
	}
}
