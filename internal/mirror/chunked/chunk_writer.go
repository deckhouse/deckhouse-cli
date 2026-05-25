/*
Copyright 2024 Flant JSC

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

package chunked

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// tmpChunkSuffix is appended to in-flight chunk files so an aborted run leaves
// behind a clearly-named partial file that can be cleaned up later, rather
// than a half-written .chunk that looks like a finished artifact.
const tmpChunkSuffix = ".tmp"

type FileWriter struct {
	chunkSize  int64
	chunkIndex int

	workingDir   string
	baseFileName string
	activeChunk  *os.File

	// writtenChunks tracks paths of all chunk files we have created so far
	// (relative to workingDir, with the .tmp suffix still attached).  On
	// Finalize they are renamed to drop the suffix; on Cleanup they are
	// removed.  Used to guarantee no half-finished artifacts survive a
	// failed/interrupted run.
	writtenChunks []string
}

func NewChunkedFileWriter(chunkSize int64, dirPath, baseFileName string) *FileWriter {
	return &FileWriter{
		chunkSize:    chunkSize,
		workingDir:   filepath.Clean(dirPath),
		baseFileName: baseFileName,
	}
}

func (c *FileWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if c.activeChunk == nil {
		if err := c.swapActiveChunk(); err != nil {
			return 0, fmt.Errorf("Create first chunk: %w", err)
		}
	}

	chunkStat, err := c.activeChunk.Stat()
	if err != nil {
		return 0, fmt.Errorf("Read chunk size: %w", err)
	}

	buf := bytes.NewBuffer(p)
	bytesWritten := 0

	for {
		for c.chunkSize-chunkStat.Size() > 0 {
			s := buf.Next(512 * 1024)
			if len(s) == 0 {
				return bytesWritten, nil
			}

			written, err := c.activeChunk.Write(s)
			bytesWritten += written

			if err != nil {
				return 0, fmt.Errorf("Write to chunk: %w", err)
			}

			chunkStat, err = c.activeChunk.Stat()
			if err != nil {
				return 0, fmt.Errorf("Read chunk size: %w", err)
			}
		}

		if err = c.swapActiveChunk(); err != nil {
			return 0, fmt.Errorf("Swap active chunk: %w", err)
		}

		chunkStat, err = c.activeChunk.Stat()
		if err != nil {
			return 0, fmt.Errorf("Read chunk size: %w", err)
		}
	}
}

// Close flushes and closes the currently-active chunk. It does NOT promote
// the .tmp chunk paths to their final names: callers must invoke Finalize for
// that, or Cleanup to discard everything.  This split lets the caller decide
// after-the-fact whether the produced bundle is valid (e.g. only after Pack
// returned without error) and avoids the previous behavior where a Ctrl+C
// during packing left behind ready-looking .chunk files.
func (c *FileWriter) Close() error {
	return c.closeActiveChunk()
}

// Finalize promotes every successfully-written chunk from its temporary path
// (<base>.NNNN.chunk.tmp) to the final path (<base>.NNNN.chunk).  Must be
// called only after Close and only on a successful pack.
func (c *FileWriter) Finalize() error {
	var errs []error

	for _, tmpRelPath := range c.writtenChunks {
		tmp := filepath.Join(c.workingDir, tmpRelPath)

		final := filepath.Join(c.workingDir, finalChunkName(tmpRelPath))
		if err := os.Rename(tmp, final); err != nil {
			errs = append(errs, fmt.Errorf("rename %s -> %s: %w", tmp, final, err))
		}
	}

	c.writtenChunks = nil

	return errors.Join(errs...)
}

// Cleanup removes any temporary chunk files this writer has created. Safe to
// call multiple times. Use this when the operation failed or was cancelled so
// no partial artifacts leak into the user's bundle directory.
func (c *FileWriter) Cleanup() {
	if c.activeChunk != nil {
		_ = c.activeChunk.Close()
		c.activeChunk = nil
	}

	for _, tmpRelPath := range c.writtenChunks {
		_ = os.Remove(filepath.Join(c.workingDir, tmpRelPath))
	}

	c.writtenChunks = nil
}

func (c *FileWriter) swapActiveChunk() error {
	if c.activeChunk != nil {
		if err := c.closeActiveChunk(); err != nil {
			return fmt.Errorf("Close active chunk file: %w", err)
		}

		c.chunkIndex++
	}

	tmpName := fmt.Sprintf("%s.%04d.chunk%s", c.baseFileName, c.chunkIndex, tmpChunkSuffix)

	newChunk, err := os.Create(filepath.Join(c.workingDir, tmpName))
	if err != nil {
		return fmt.Errorf("Create new chunk file: %w", err)
	}

	c.activeChunk = newChunk
	c.writtenChunks = append(c.writtenChunks, tmpName)

	return nil
}

func (c *FileWriter) closeActiveChunk() error {
	if c.activeChunk != nil {
		if err := c.activeChunk.Sync(); err != nil {
			return fmt.Errorf("Flush chunk: %w", err)
		}

		if err := c.activeChunk.Close(); err != nil {
			return fmt.Errorf("Close chunk: %w", err)
		}

		c.activeChunk = nil
	}

	return nil
}

// finalChunkName strips the temporary suffix from a chunk file name.
func finalChunkName(tmpName string) string {
	if len(tmpName) > len(tmpChunkSuffix) && tmpName[len(tmpName)-len(tmpChunkSuffix):] == tmpChunkSuffix {
		return tmpName[:len(tmpName)-len(tmpChunkSuffix)]
	}

	return tmpName
}
