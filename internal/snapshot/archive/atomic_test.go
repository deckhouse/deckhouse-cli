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

package archive

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// errAfterReader returns data bytes then returns errTrigger on the next Read.
type errAfterReader struct {
	data []byte
	pos  int
	err  error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, r.err
	}

	n := copy(p, r.data[r.pos:])
	r.pos += n

	return n, nil
}

func TestWriteFileAtomic_success(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "out.txt")
	content := []byte("hello atomic world")

	err := WriteFileAtomic(path, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}

	// The final file must exist with correct content.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: got %q; want %q", got, content)
	}

	// No leftover .tmp on success.
	_, err = os.Stat(path + ".tmp")
	if !errors.Is(err, os.ErrNotExist) {
		t.Error("expected .tmp to be absent after successful WriteFileAtomic")
	}
}

func TestWriteFileAtomic_errorLeavesNoFinalFile(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "out.txt")

	// Reader that delivers some bytes then fails — simulates a mid-write error
	// before any rename could happen.
	r := &errAfterReader{
		data: []byte("partial data"),
		err:  errors.New("simulated read failure"),
	}

	err := WriteFileAtomic(path, r)
	if err == nil {
		t.Fatal("expected error from WriteFileAtomic, got nil")
	}

	// Final file must not exist.
	_, statErr := os.Stat(path)
	if !errors.Is(statErr, os.ErrNotExist) {
		t.Error("final file must not exist after a write error")
	}

	// The .tmp should also be cleaned up by Abort.
	_, statErr = os.Stat(path + ".tmp")
	if !errors.Is(statErr, os.ErrNotExist) {
		t.Error(".tmp should be removed by Abort")
	}
}

func TestAtomicWriter_commitProducesCorrectContent(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")

	aw, err := NewAtomicWriter(path)
	if err != nil {
		t.Fatalf("NewAtomicWriter: %v", err)
	}

	chunks := [][]byte{[]byte("chunk1"), []byte("chunk2"), []byte("chunk3")}

	for _, c := range chunks {
		if _, err := aw.Write(c); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	if err := aw.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	want := []byte("chunk1chunk2chunk3")

	if !bytes.Equal(got, want) {
		t.Errorf("content mismatch: got %q; want %q", got, want)
	}

	_, err = os.Stat(path + ".tmp")
	if !errors.Is(err, os.ErrNotExist) {
		t.Error(".tmp must be absent after Commit")
	}
}

func TestAtomicWriter_CommitContextCancelsAfterSyncBeforePublication(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.bin")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	aw, err := NewAtomicWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := aw.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	aw.ops.syncTemp = func(f *os.File) error {
		close(syncStarted)
		<-releaseSync

		return f.Sync()
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- aw.CommitContext(ctx)
	}()

	<-syncStarted
	cancel()
	close(releaseSync)

	err = <-result
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CommitContext error = %v, want context.Canceled", err)
	}

	if got := CommitPublicationState(err); got != PublicationUnpublished {
		t.Fatalf("publication state = %v, want unpublished", got)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, []byte("old")) {
		t.Fatalf("final bytes = %q, want unchanged old bytes", got)
	}

	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary file survived cancellation: %v", err)
	}
}

func TestAtomicWriter_CommitContextPreservesOperationErrors(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AtomicWriter, context.CancelFunc, error)
		state     PublicationState
	}{
		{
			name: "SyncErrorDuringCancellation",
			configure: func(aw *AtomicWriter, cancel context.CancelFunc, sentinel error) {
				aw.ops.syncTemp = func(*os.File) error {
					cancel()

					return sentinel
				}
			},
		},
		{
			name: "CloseError",
			configure: func(aw *AtomicWriter, _ context.CancelFunc, sentinel error) {
				aw.ops.closeTemp = func(*os.File) error {
					return sentinel
				}
			},
		},
		{
			name: "RenameError",
			configure: func(aw *AtomicWriter, _ context.CancelFunc, sentinel error) {
				aw.ops.rename = func(string, string) error {
					return sentinel
				}
			},
		},
		{
			name: "ParentSyncErrorAfterPublication",
			configure: func(aw *AtomicWriter, cancel context.CancelFunc, sentinel error) {
				aw.ops.syncDir = func(string) error {
					cancel()

					return sentinel
				}
			},
			state: PublicationPublished,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.bin")
			aw, err := NewAtomicWriter(path)
			if err != nil {
				t.Fatal(err)
			}

			if _, err := aw.Write([]byte("content")); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			sentinel := errors.New("commit operation sentinel")
			tt.configure(aw, cancel, sentinel)

			err = aw.CommitContext(ctx)
			if !errors.Is(err, sentinel) {
				t.Fatalf("CommitContext error = %v, want operation sentinel", err)
			}

			if errors.Is(err, context.Canceled) {
				t.Fatalf("operation error was replaced by cancellation: %v", err)
			}

			if got := CommitPublicationState(err); got != tt.state {
				t.Fatalf("publication state = %v, want %v", got, tt.state)
			}

			_, statErr := os.Stat(path)
			if tt.state == PublicationPublished && statErr != nil {
				t.Fatalf("published final file missing: %v", statErr)
			}

			if tt.state == PublicationUnpublished && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("final file published before failed operation: %v", statErr)
			}

			if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temporary file survived failed commit: %v", err)
			}
		})
	}
}

func TestAtomicWriter_PrePublicationFailuresPreserveOldFinal(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AtomicWriter, error)
	}{
		{
			name: "Sync",
			configure: func(aw *AtomicWriter, sentinel error) {
				aw.ops.syncTemp = func(*os.File) error {
					return sentinel
				}
			},
		},
		{
			name: "Close",
			configure: func(aw *AtomicWriter, sentinel error) {
				aw.ops.closeTemp = func(*os.File) error {
					return sentinel
				}
			},
		},
		{
			name: "Rename",
			configure: func(aw *AtomicWriter, sentinel error) {
				aw.ops.rename = func(string, string) error {
					return sentinel
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.bin")
			if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
				t.Fatal(err)
			}

			aw, err := NewAtomicWriter(path)
			if err != nil {
				t.Fatal(err)
			}

			if _, err := aw.Write([]byte("new")); err != nil {
				t.Fatal(err)
			}

			sentinel := errors.New("pre-publication operation sentinel")
			tt.configure(aw, sentinel)

			err = aw.CommitContext(context.Background())
			if !errors.Is(err, sentinel) {
				t.Fatalf("CommitContext error = %v, want sentinel", err)
			}

			if got := CommitPublicationState(err); got != PublicationUnpublished {
				t.Fatalf("publication state = %v, want unpublished", got)
			}

			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(got, []byte("old")) {
				t.Fatalf("final bytes = %q, want unchanged old bytes", got)
			}

			if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temporary file survived pre-publication failure: %v", err)
			}
		})
	}
}

func TestConfirmFileDurability_RetriesPublishedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(path, []byte("published"), 0o644); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("directory sync sentinel")
	calls := 0
	ctx := WithDirectorySyncHook(context.Background(), func(_ string, next func() error) error {
		calls++
		if calls == 1 {
			return sentinel
		}

		return next()
	})

	err := ConfirmFileDurability(ctx, path)
	if !errors.Is(err, sentinel) {
		t.Fatalf("ConfirmFileDurability error = %v, want sentinel", err)
	}

	if got := CommitPublicationState(err); got != PublicationPublished {
		t.Fatalf("publication state = %v, want published", got)
	}

	if err := ConfirmFileDurability(ctx, path); err != nil {
		t.Fatalf("ConfirmFileDurability retry: %v", err)
	}

	if calls != 2 {
		t.Fatalf("directory sync calls = %d, want 2", calls)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, []byte("published")) {
		t.Fatalf("final bytes = %q, want unchanged published bytes", got)
	}
}

func TestAtomicWriter_CommitContextIgnoresCancellationAfterPublicationBegins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.bin")
	aw, err := NewAtomicWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := aw.Write([]byte("content")); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	rename := aw.ops.rename
	aw.ops.rename = func(oldPath, newPath string) error {
		if err := rename(oldPath, newPath); err != nil {
			return err
		}

		cancel()

		return nil
	}

	if err := aw.CommitContext(ctx); err != nil {
		t.Fatalf("CommitContext after publication began: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, []byte("content")) {
		t.Fatalf("final bytes = %q, want content", got)
	}
}

func TestAtomicWriter_abortLeavesNoFinalFile(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "aborted.bin")

	aw, err := NewAtomicWriter(path)
	if err != nil {
		t.Fatalf("NewAtomicWriter: %v", err)
	}

	if _, err := aw.Write([]byte("some data")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	aw.Abort()

	_, err = os.Stat(path)
	if !errors.Is(err, os.ErrNotExist) {
		t.Error("final file must not exist after Abort")
	}

	_, err = os.Stat(path + ".tmp")
	if !errors.Is(err, os.ErrNotExist) {
		t.Error(".tmp must be absent after Abort")
	}
}

func TestAtomicWriter_openTempReaderKeepsFinalInvisible(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "validated.bin")
	content := []byte("validate before publish")

	aw, err := NewAtomicWriter(path)
	if err != nil {
		t.Fatalf("NewAtomicWriter: %v", err)
	}

	if _, err := aw.Write(content); err != nil {
		t.Fatalf("Write: %v", err)
	}

	reader, err := aw.OpenTempReader()
	if err != nil {
		t.Fatalf("OpenTempReader: %v", err)
	}

	got, readErr := io.ReadAll(reader)
	closeErr := reader.Close()

	if readErr != nil {
		t.Fatalf("ReadAll: %v", readErr)
	}

	if closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}

	if !bytes.Equal(got, content) {
		t.Fatalf("temporary content = %q; want %q", got, content)
	}

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("final file must remain invisible before Commit, Stat error: %v", err)
	}

	if err := aw.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Fatalf("committed content = %q; want %q", got, content)
	}
}

func TestWriteFileAtomic_emptyReader(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "empty.txt")

	err := WriteFileAtomic(path, bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("WriteFileAtomic empty: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if info.Size() != 0 {
		t.Errorf("expected empty file, got size %d", info.Size())
	}
}

func TestEnsureDir_createsAndIsDurable(t *testing.T) {
	t.Helper()

	base := t.TempDir()
	nested := filepath.Join(base, "a", "b", "c")

	if err := EnsureDir(nested); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}

	info, err := os.Stat(nested)
	if err != nil {
		t.Fatalf("Stat after EnsureDir: %v", err)
	}

	if !info.IsDir() {
		t.Error("expected a directory")
	}
}

func TestEnsureDir_idempotent(t *testing.T) {
	t.Helper()

	dir := t.TempDir()

	if err := EnsureDir(dir); err != nil {
		t.Fatalf("first EnsureDir: %v", err)
	}

	if err := EnsureDir(dir); err != nil {
		t.Fatalf("second EnsureDir (idempotent): %v", err)
	}
}

func TestWriteFileAtomic_parentDirSynced(t *testing.T) {
	// Smoke test: WriteFileAtomic into a newly created dir does not error,
	// confirming that parent-dir fsync succeeds in a temp filesystem.
	t.Helper()

	base := t.TempDir()
	sub := filepath.Join(base, "sub")

	if err := EnsureDir(sub); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}

	path := filepath.Join(sub, "file.txt")

	err := WriteFileAtomic(path, bytes.NewReader([]byte("durable")))
	if err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
}

// Ensure AtomicWriter satisfies io.Writer at compile time.
var _ io.Writer = (*AtomicWriter)(nil)
