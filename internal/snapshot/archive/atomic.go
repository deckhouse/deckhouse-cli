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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// AtomicWriter writes data to "<finalPath>.tmp" and, on Commit, fsyncs the
// data, renames to the final path, then fsyncs the parent directory.
// A parent-directory sync failure can leave the final file published but with
// durability unconfirmed; CommitError exposes that state to callers.
// Call Abort to remove the temporary file when an error occurs.
type AtomicWriter struct {
	finalPath string
	tmpPath   string
	f         *os.File
	ops       atomicCommitOps
}

type atomicCommitOps struct {
	syncTemp  func(*os.File) error
	closeTemp func(*os.File) error
	rename    func(string, string) error
	syncDir   func(string) error
}

// PublicationState describes whether an AtomicWriter commit error happened
// before or after the final path became visible.
type PublicationState uint8

const (
	// PublicationUnpublished means rename did not complete.
	PublicationUnpublished PublicationState = iota
	// PublicationPublished means rename completed but parent-directory
	// durability is not yet confirmed.
	PublicationPublished
)

// CommitError preserves the commit operation's original cause and records
// whether the final path was published before that operation failed.
type CommitError struct {
	state PublicationState
	err   error
}

// Error implements error.
func (e *CommitError) Error() string {
	return e.err.Error()
}

// Unwrap exposes the original commit failure for errors.Is/errors.As.
func (e *CommitError) Unwrap() error {
	return e.err
}

// PublicationState returns the final-path state at the failure boundary.
func (e *CommitError) PublicationState() PublicationState {
	return e.state
}

// CommitPublicationState returns the publication state carried by err. Errors
// that did not originate from an AtomicWriter commit are treated as
// PublicationUnpublished.
func CommitPublicationState(err error) PublicationState {
	var commitErr *CommitError
	if errors.As(err, &commitErr) {
		return commitErr.PublicationState()
	}

	return PublicationUnpublished
}

type directorySyncHookKey struct{}

// DirectorySyncHook wraps a parent-directory sync operation. Calling next
// performs the real sync. The hook is scoped to a context so deterministic
// operation injection does not affect concurrent writers.
type DirectorySyncHook func(path string, next func() error) error

// WithDirectorySyncHook returns a context that applies hook to
// AtomicWriter.CommitContext and ConfirmFileDurability parent-directory syncs.
func WithDirectorySyncHook(ctx context.Context, hook DirectorySyncHook) context.Context {
	if hook == nil {
		return ctx
	}

	return context.WithValue(ctx, directorySyncHookKey{}, hook)
}

// NewAtomicWriter creates (or truncates) "<path>.tmp" and returns a writer
// ready to receive data. The caller must call either Commit or Abort.
func NewAtomicWriter(path string) (*AtomicWriter, error) {
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", tmpPath, err)
	}

	return &AtomicWriter{
		finalPath: path,
		tmpPath:   tmpPath,
		f:         f,
		ops: atomicCommitOps{
			syncTemp:  (*os.File).Sync,
			closeTemp: (*os.File).Close,
			rename:    os.Rename,
			syncDir:   syncDir,
		},
	}, nil
}

// Write implements io.Writer.
func (w *AtomicWriter) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

// OpenTempReader opens the unpublished temporary file for validation. The
// caller must close the returned reader before calling Commit or Abort.
func (w *AtomicWriter) OpenTempReader() (io.ReadCloser, error) {
	f, err := os.Open(w.tmpPath)
	if err != nil {
		return nil, fmt.Errorf("opening %s for validation: %w", w.tmpPath, err)
	}

	return f, nil
}

// Commit is CommitContext with a non-cancellable context.
// After Commit the AtomicWriter must not be used again.
func (w *AtomicWriter) Commit() error {
	return w.CommitContext(context.Background())
}

// CommitContext fsyncs and closes the temporary file, checks cancellation,
// atomically renames it to the final path, then fsyncs the parent directory.
//
// Publication begins at the cancellation checkpoint immediately before Rename.
// Cancellation observed before that point removes the temporary file and leaves
// the final path unchanged. Once the checkpoint succeeds, cancellation no
// longer changes the result: Rename and parent-directory sync determine the
// return value, so CommitContext never reports pre-publication cancellation
// after publishing.
func (w *AtomicWriter) CommitContext(ctx context.Context) error {
	if err := w.ops.syncTemp(w.f); err != nil {
		w.Abort()

		return newCommitError(PublicationUnpublished, fmt.Errorf("syncing %s: %w", w.tmpPath, err))
	}

	if err := w.ops.closeTemp(w.f); err != nil {
		w.Abort()

		return newCommitError(PublicationUnpublished, fmt.Errorf("closing %s: %w", w.tmpPath, err))
	}

	if err := ctx.Err(); err != nil {
		w.Abort()

		return newCommitError(
			PublicationUnpublished,
			fmt.Errorf("committing %s cancelled before publication: %w", w.finalPath, err),
		)
	}

	if err := w.ops.rename(w.tmpPath, w.finalPath); err != nil {
		w.Abort()

		return newCommitError(
			PublicationUnpublished,
			fmt.Errorf("renaming %s to %s: %w", w.tmpPath, w.finalPath, err),
		)
	}

	if err := runDirectorySync(ctx, filepath.Dir(w.finalPath), w.ops.syncDir); err != nil {
		return newCommitError(
			PublicationPublished,
			fmt.Errorf("syncing parent directory for %s: %w", w.finalPath, err),
		)
	}

	return nil
}

func newCommitError(state PublicationState, err error) error {
	return &CommitError{state: state, err: err}
}

func runDirectorySync(ctx context.Context, path string, syncFn func(string) error) error {
	hook, _ := ctx.Value(directorySyncHookKey{}).(DirectorySyncHook)
	if hook == nil {
		return syncFn(path)
	}

	return hook(path, func() error {
		return syncFn(path)
	})
}

// ConfirmFileDurability fsyncs path's parent directory before an already
// published final file is trusted. Cancellation observed before the sync
// prevents it from starting; once it starts, the sync result takes precedence.
func ConfirmFileDurability(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return newCommitError(
			PublicationPublished,
			fmt.Errorf("confirming durability for %s cancelled before sync: %w", path, err),
		)
	}

	if err := runDirectorySync(ctx, filepath.Dir(path), syncDir); err != nil {
		return newCommitError(
			PublicationPublished,
			fmt.Errorf("syncing parent directory for published file %s: %w", path, err),
		)
	}

	return nil
}

// Abort closes and removes the temporary file. Safe to call even if Write
// returned an error. Errors from close/remove are intentionally suppressed
// because the caller's original error takes precedence.
func (w *AtomicWriter) Abort() {
	_ = w.f.Close()
	_ = os.Remove(w.tmpPath)
}

// WriteFileAtomic copies r into path using an AtomicWriter. Pre-publication
// errors remove the temporary file and leave the old final unchanged. A
// PublicationPublished error means the complete final file is visible but its
// parent-directory durability remains unconfirmed.
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
