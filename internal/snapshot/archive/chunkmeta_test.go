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
	"testing"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

func TestChunkMeta_WriteReadRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		meta archive.ChunkMeta
	}{
		{name: "typical geometry", meta: archive.ChunkMeta{ChunkSize: 256 * 1024 * 1024, TotalSize: 10 * 1024 * 1024 * 1024}},
		{name: "chunk size larger than total size", meta: archive.ChunkMeta{ChunkSize: 1024, TotalSize: 100}},
		{name: "zero total size", meta: archive.ChunkMeta{ChunkSize: 1024, TotalSize: 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()

			if err := archive.WriteChunkMeta(dir, tc.meta); err != nil {
				t.Fatalf("WriteChunkMeta: %v", err)
			}

			got, found, err := archive.ReadChunkMeta(dir)
			if err != nil {
				t.Fatalf("ReadChunkMeta: %v", err)
			}

			if !found {
				t.Fatal("ReadChunkMeta found = false; want true after WriteChunkMeta")
			}

			if got != tc.meta {
				t.Errorf("ReadChunkMeta = %+v; want %+v", got, tc.meta)
			}
		})
	}
}

func TestChunkMeta_MissingFileIsNotAnError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	meta, found, err := archive.ReadChunkMeta(dir)
	if err != nil {
		t.Fatalf("ReadChunkMeta on a dir with no chunks.meta: %v", err)
	}

	if found {
		t.Error("found = true; want false when chunks.meta does not exist")
	}

	if meta != (archive.ChunkMeta{}) {
		t.Errorf("meta = %+v; want the zero value when not found", meta)
	}
}

func TestChunkMeta_MissingDirIsNotAnError(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "does-not-exist")

	_, found, err := archive.ReadChunkMeta(dir)
	if err != nil {
		t.Fatalf("ReadChunkMeta on a non-existent dir: %v", err)
	}

	if found {
		t.Error("found = true; want false when the chunk dir itself does not exist")
	}
}

// TestChunkMeta_CorruptFileIsAnError pins the sentinel ensureChunkGeometry
// relies on to route an unparseable sidecar to the purge-and-recreate path
// instead of a hard abort: the error MUST satisfy
// errors.Is(err, archive.ErrCorruptChunkMeta), not just be non-nil.
func TestChunkMeta_CorruptFileIsAnError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	path := filepath.Join(dir, archive.ChunkMetaFileName)
	if err := os.WriteFile(path, []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, found, err := archive.ReadChunkMeta(dir)
	if err == nil {
		t.Fatal("expected an error reading a corrupt chunks.meta, got nil")
	}

	if !errors.Is(err, archive.ErrCorruptChunkMeta) {
		t.Errorf("err = %v; want it to satisfy errors.Is(err, archive.ErrCorruptChunkMeta)", err)
	}

	if found {
		t.Error("found = true; want false on a read error")
	}
}

// TestChunkMeta_DetectsMismatch pins the exact comparison the geometry guard
// (volume.ensureChunkGeometry) relies on: a resumed run's chunkSize/totalSize
// must be compared for an EXACT match against the previously recorded
// geometry — any difference, in either field, must be detectable so the
// caller can discard stale chunks rather than silently reuse a byte range
// computed under different arithmetic.
func TestChunkMeta_DetectsMismatch(t *testing.T) {
	t.Parallel()

	original := archive.ChunkMeta{ChunkSize: 100, TotalSize: 1000}

	tests := []struct {
		name    string
		current archive.ChunkMeta
		match   bool
	}{
		{name: "identical geometry matches", current: archive.ChunkMeta{ChunkSize: 100, TotalSize: 1000}, match: true},
		{name: "smaller chunk size does not match", current: archive.ChunkMeta{ChunkSize: 50, TotalSize: 1000}, match: false},
		{name: "larger chunk size does not match", current: archive.ChunkMeta{ChunkSize: 200, TotalSize: 1000}, match: false},
		{name: "different total size does not match", current: archive.ChunkMeta{ChunkSize: 100, TotalSize: 2000}, match: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()

			if err := archive.WriteChunkMeta(dir, original); err != nil {
				t.Fatalf("WriteChunkMeta: %v", err)
			}

			got, found, err := archive.ReadChunkMeta(dir)
			if err != nil {
				t.Fatalf("ReadChunkMeta: %v", err)
			}

			if !found {
				t.Fatal("found = false; want true")
			}

			match := got.ChunkSize == tc.current.ChunkSize && got.TotalSize == tc.current.TotalSize
			if match != tc.match {
				t.Errorf("match against %+v = %v; want %v (stored %+v)", tc.current, match, tc.match, got)
			}
		})
	}
}
