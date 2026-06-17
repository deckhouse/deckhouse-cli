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

// Package archive: deterministic naming + crash-safe file I/O for the snapshot output tree.
package archive

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// AtomicWriter writes data to "<finalPath>.tmp" and, on Commit, fsyncs the
// data, renames to the final path, then fsyncs the parent directory.
// A file at its final (non-.tmp) name is always fully durable.
// Call Abort to remove the temporary file when an error occurs.
type AtomicWriter struct {
	finalPath string
	tmpPath   string
	f         *os.File
}

// NewAtomicWriter creates (or truncates) "<path>.tmp" and returns a writer
// ready to receive data. The caller must call either Commit or Abort.
func NewAtomicWriter(path string) (*AtomicWriter, error) {
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", tmpPath, err)
	}

	return &AtomicWriter{finalPath: path, tmpPath: tmpPath, f: f}, nil
}

// Write implements io.Writer.
func (w *AtomicWriter) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

// Commit fsyncs and closes the temporary file, atomically renames it to the
// final path, then fsyncs the parent directory for durability.
// After Commit the AtomicWriter must not be used again.
func (w *AtomicWriter) Commit() error {
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		return fmt.Errorf("syncing %s: %w", w.tmpPath, err)
	}

	if err := w.f.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", w.tmpPath, err)
	}

	if err := os.Rename(w.tmpPath, w.finalPath); err != nil {
		return fmt.Errorf("renaming %s to %s: %w", w.tmpPath, w.finalPath, err)
	}

	return syncDir(filepath.Dir(w.finalPath))
}

// Abort closes and removes the temporary file. Safe to call even if Write
// returned an error. Errors from close/remove are intentionally suppressed
// because the caller's original error takes precedence.
func (w *AtomicWriter) Abort() {
	_ = w.f.Close()
	_ = os.Remove(w.tmpPath)
}

// WriteFileAtomic copies r into path using an AtomicWriter.
// On any error the temporary file is removed and the final path is never created.
func WriteFileAtomic(path string, r io.Reader) error {
	aw, err := NewAtomicWriter(path)
	if err != nil {
		return err
	}

	if _, err := io.Copy(aw, r); err != nil {
		aw.Abort()
		return fmt.Errorf("writing %s: %w", path, err)
	}

	return aw.Commit()
}

// EnsureDir creates path and all parents (MkdirAll) and then fsyncs the
// directory so the creation is durable before any files are written into it.
func EnsureDir(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("creating dir %s: %w", path, err)
	}

	return syncDir(path)
}

// syncDir opens path as a directory and calls Sync to flush the entry to stable
// storage. This makes preceding renames and creates visible after a power loss.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening dir %s for sync: %w", path, err)
	}

	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("syncing dir %s: %w", path, err)
	}

	return d.Close()
}
