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

package volume

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
)

// ErrMissingChunk is returned by MergeBlockChunks when one or more expected
// chunk files are absent, preventing a gap-free merge.
var ErrMissingChunk = errors.New("block chunk missing")

// MergeBlockChunks concatenates all chunk_%05d.zst files from the temporary
// chunk directory (nodeDir/data.img.zst.d/) into nodeDir/data.img.zst in
// strict ascending-index order.
//
// Pre-conditions (enforced):
//   - All chunks 0 .. ceil(totalSize/chunkSize)-1 must be present.
//   - If any chunk is missing, ErrMissingChunk is returned and no output is written.
//
// Post-conditions on success:
//   - nodeDir/data.img.zst is a fully durable (fsynced) multi-frame zstd stream.
//   - The chunk directory and all its contents are removed.
//
// chunkSize ≤ 0 falls back to DefaultChunkSize.
func MergeBlockChunks(nodeDir string, totalSize, chunkSize int64) error {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}

	numChunks := int((totalSize + chunkSize - 1) / chunkSize)
	chunkDir := filepath.Join(nodeDir, archive.BlockChunksDirName)
	finalPath := filepath.Join(nodeDir, archive.DataBlockName)

	// Verify all chunks are present before writing anything.
	for i := range numChunks {
		p := filepath.Join(chunkDir, archive.ChunkFileName(i))
		if _, err := os.Stat(p); os.IsNotExist(err) {
			return fmt.Errorf("chunk %d (%s): %w", i, p, ErrMissingChunk)
		} else if err != nil {
			return fmt.Errorf("stat chunk %d: %w", i, err)
		}
	}

	// Concatenate chunks in order into data.img.zst via AtomicWriter.
	aw, err := archive.NewAtomicWriter(finalPath)
	if err != nil {
		return fmt.Errorf("open atomic writer for %s: %w", finalPath, err)
	}

	for i := range numChunks {
		p := filepath.Join(chunkDir, archive.ChunkFileName(i))

		if err := copyFile(aw, p); err != nil {
			aw.Abort()
			return fmt.Errorf("copy chunk %d into merged file: %w", i, err)
		}
	}

	if err := aw.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", finalPath, err)
	}

	// Remove the temporary chunk directory after successful commit.
	if err := os.RemoveAll(chunkDir); err != nil {
		return fmt.Errorf("remove chunk dir %s: %w", chunkDir, err)
	}

	return nil
}

// copyFile copies the contents of src into dst.
func copyFile(dst io.Writer, src string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(dst, f); err != nil {
		return err
	}

	return nil
}
