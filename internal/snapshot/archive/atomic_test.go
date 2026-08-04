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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
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

func TestAtomicWriter_UnsupportedReplacementIsUnpublished(t *testing.T) {
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

	ctx, cancel := context.WithCancel(context.Background())
	aw.ops.rename = func(string, string) error {
		cancel()

		return ErrAtomicReplaceUnsupported
	}

	err = aw.CommitContext(ctx)
	if !errors.Is(err, ErrAtomicReplaceUnsupported) {
		t.Fatalf("CommitContext error = %v, want unsupported replacement", err)
	}

	if errors.Is(err, context.Canceled) {
		t.Fatalf("post-checkpoint cancellation replaced publication error: %v", err)
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
		t.Fatalf("temporary file survived unsupported replacement: %v", err)
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

func TestRootedDestinationEnsureDir_reconfirmsVisibleDeepAncestry(t *testing.T) {
	t.Helper()

	base := t.TempDir()
	destination, err := OpenRootedDestination(base, nil)
	if err != nil {
		t.Fatalf("OpenRootedDestination: %v", err)
	}
	defer func() { _ = destination.Close() }()

	const depth = 64

	components := make([]string, 0, depth)
	for index := range depth {
		components = append(components, fmt.Sprintf("d%02d", index))
	}

	nested := filepath.Join(append([]string{base}, components...)...)
	failure := errors.New("ancestor sync failure")
	failAt := filepath.Join(append([]string{base}, components[:depth-1]...)...)

	var (
		attempt   int
		events    [][]string
		nextCalls int
	)

	destination.SetDirectorySyncHook(func(path string, next func() error) error {
		relative, relErr := filepath.Rel(base, path)
		if relErr != nil {
			return relErr
		}

		events[attempt] = append(events[attempt], relative)
		if attempt < 2 && filepath.Clean(path) == filepath.Clean(failAt) {
			return failure
		}

		if attempt == 2 && filepath.Clean(path) == filepath.Clean(failAt) {
			return context.Canceled
		}

		nextCalls++

		return next()
	})

	for attempt = 0; attempt < 3; attempt++ {
		events = append(events, nil)

		err = destination.EnsureDir(nested)
		if attempt < 2 && !errors.Is(err, failure) {
			t.Fatalf("attempt %d: expected sync failure, got %v", attempt, err)
		}

		if attempt == 2 && !errors.Is(err, context.Canceled) {
			t.Fatalf("attempt %d: expected cancellation, got %v", attempt, err)
		}

		info, statErr := os.Stat(nested)
		if statErr != nil {
			t.Fatalf("attempt %d: stat visible nested directory: %v", attempt, statErr)
		}

		if !info.IsDir() {
			t.Fatalf("attempt %d: nested path is not a directory", attempt)
		}

		if len(events[attempt]) != depth+1 {
			t.Fatalf("attempt %d: got %d confirmations before failure, want %d",
				attempt, len(events[attempt]), depth+1)
		}

		if events[attempt][0] != filepath.Join(components...) {
			t.Fatalf("attempt %d: first confirmation = %q, want leaf %q",
				attempt, events[attempt][0], filepath.Join(components...))
		}

		wantFailed := filepath.Join(components[:depth-1]...)
		if events[attempt][len(events[attempt])-1] != wantFailed {
			t.Fatalf("attempt %d: last confirmation = %q, want failed ancestor %q",
				attempt, events[attempt][len(events[attempt])-1], wantFailed)
		}
	}

	attempt = 3
	events = append(events, nil)

	if err := destination.EnsureDir(nested); err != nil {
		t.Fatalf("successful retry: %v", err)
	}

	if len(events[attempt]) != depth+1 {
		t.Fatalf("successful retry confirmations = %d, want %d", len(events[attempt]), depth+1)
	}

	if events[attempt][0] != filepath.Join(components...) {
		t.Fatalf("successful retry first confirmation = %q, want leaf %q",
			events[attempt][0], filepath.Join(components...))
	}

	wantLast := filepath.Join(components[:depth-1]...)
	if events[attempt][len(events[attempt])-1] != wantLast {
		t.Fatalf("successful retry last confirmation = %q, want leaf parent %q",
			events[attempt][len(events[attempt])-1], wantLast)
	}

	wantNextCalls := 4*depth + 1
	if nextCalls != wantNextCalls {
		t.Fatalf("platform confirmation calls = %d, want %d", nextCalls, wantNextCalls)
	}
}

func TestRootedDestinationEnsureDir_retriesEveryVisibleEntry(t *testing.T) {
	t.Helper()

	components := []string{"a", "b", "c", "d"}

	for failedEntry := range components {
		t.Run(fmt.Sprintf("entry-%d", failedEntry), func(t *testing.T) {
			base := t.TempDir()
			destination, err := OpenRootedDestination(base, nil)
			if err != nil {
				t.Fatalf("OpenRootedDestination: %v", err)
			}
			defer func() { _ = destination.Close() }()

			nested := filepath.Join(append([]string{base}, components...)...)
			containingComponents := components[:failedEntry]
			failAt := filepath.Join(append([]string{base}, containingComponents...)...)
			failure := fmt.Errorf("sync entry %d: %w", failedEntry, io.ErrUnexpectedEOF)

			destination.SetDirectorySyncHook(func(path string, next func() error) error {
				if filepath.Clean(path) == filepath.Clean(failAt) {
					return failure
				}

				return next()
			})

			err = destination.EnsureDir(nested)
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("expected entry sync failure, got %v", err)
			}

			if _, err := os.Stat(nested); err != nil {
				t.Fatalf("nested directory was not left visible: %v", err)
			}

			confirmations := 0
			destination.SetDirectorySyncHook(func(_ string, next func() error) error {
				confirmations++

				return next()
			})

			if err := destination.EnsureDir(nested); err != nil {
				t.Fatalf("retry visible ancestry: %v", err)
			}

			if confirmations != len(components)+1 {
				t.Fatalf("retry confirmations = %d, want %d", confirmations, len(components)+1)
			}
		})
	}
}

func TestRootedDestinationEnsureDir_rootReplacementDuringConfirmation(t *testing.T) {
	t.Helper()

	base := t.TempDir()
	output := filepath.Join(base, "output")
	pinned := filepath.Join(base, "pinned")
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatalf("Mkdir output: %v", err)
	}

	var replaced bool

	destination, err := OpenRootedDestination(output, func(phase MutationPhase, _ string) {
		if phase != MutationSync || replaced {
			return
		}

		replaced = true
		if renameErr := os.Rename(output, pinned); renameErr != nil {
			t.Fatalf("replace rooted destination: %v", renameErr)
		}

		if mkdirErr := os.Mkdir(output, 0o755); mkdirErr != nil {
			t.Fatalf("recreate rooted destination: %v", mkdirErr)
		}
	})
	if err != nil {
		t.Fatalf("OpenRootedDestination: %v", err)
	}
	defer func() { _ = destination.Close() }()

	var bindingLoss error
	destination.SetBindingLossHandler(func(err error) {
		bindingLoss = err
	})

	nested := filepath.Join(output, "a", "b")
	err = destination.EnsureDir(nested)
	if !errors.Is(err, ErrNonRegularArchiveArtifact) {
		t.Fatalf("expected binding loss, got %v", err)
	}

	if !errors.Is(bindingLoss, ErrNonRegularArchiveArtifact) {
		t.Fatalf("binding-loss handler got %v", bindingLoss)
	}

	if _, err := os.Stat(filepath.Join(pinned, "a", "b")); err != nil {
		t.Fatalf("pinned tree lost created ancestry: %v", err)
	}

	entries, err := os.ReadDir(output)
	if err != nil {
		t.Fatalf("ReadDir replacement: %v", err)
	}

	if len(entries) != 0 {
		t.Fatalf("replacement tree was mutated: %v", entries)
	}
}

func TestRootedDestinationDeepOperationsBoundDescriptors(t *testing.T) {
	t.Helper()

	base := t.TempDir()
	targetLength := 3500
	if runtime.GOOS == "darwin" {
		targetLength = 700
	}

	components := make([]string, 0, targetLength/2)
	for length := 1; length < targetLength; length += 2 {
		components = append(components, "d")
	}

	deepRelative := filepath.Join(components...)
	deep := filepath.Join(base, deepRelative)

	var (
		mu      sync.Mutex
		current int
		peak    int
	)

	destination, err := OpenRootedDestination(base, nil)
	if err != nil {
		t.Fatalf("OpenRootedDestination: %v", err)
	}
	defer func() { _ = destination.Close() }()

	destination.setDescriptorObserver(func(delta int) {
		mu.Lock()
		current += delta
		peak = max(peak, current)
		mu.Unlock()
	})

	if err := destination.EnsureDir(deep); err != nil {
		t.Fatalf("EnsureDir near path limit: %v", err)
	}

	path := filepath.Join(deep, "payload")
	file, err := destination.CreateExclusive(path, 0o644)
	if err != nil {
		t.Fatalf("CreateExclusive: %v", err)
	}

	if _, err := file.WriteString("payload"); err != nil {
		_ = file.Close()

		t.Fatalf("write payload: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("close payload: %v", err)
	}

	if _, err := destination.Stat(path); err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if data, err := destination.ReadFile(path); err != nil || string(data) != "payload" {
		t.Fatalf("ReadFile = %q, %v; want payload", data, err)
	}

	renamed := filepath.Join(deep, "renamed")
	if err := destination.Rename(path, renamed); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	if err := destination.SyncParent(renamed); err != nil {
		t.Fatalf("SyncParent: %v", err)
	}

	mu.Lock()
	descriptorPeak := peak
	descriptorCurrent := current
	mu.Unlock()

	if descriptorPeak > 8 {
		t.Fatalf("descriptor peak = %d across %d components, want at most 8",
			descriptorPeak, len(components))
	}

	if descriptorCurrent != 0 {
		t.Fatalf("descriptor balance after deep operations = %d, want 0", descriptorCurrent)
	}

	const workers = 24

	start := make(chan struct{})
	results := make(chan error, workers)

	var group sync.WaitGroup
	for range workers {
		group.Add(1)

		go func() {
			defer group.Done()
			<-start

			for range 4 {
				if _, err := destination.Stat(renamed); err != nil {
					results <- err

					return
				}
			}

			results <- nil
		}()
	}

	close(start)
	group.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Fatalf("concurrent deep Stat: %v", err)
		}
	}

	mu.Lock()
	concurrentPeak := peak
	concurrentCurrent := current
	mu.Unlock()

	if concurrentPeak > workers*5+8 {
		t.Fatalf("concurrent descriptor peak = %d with %d workers", concurrentPeak, workers)
	}

	if concurrentCurrent != 0 {
		t.Fatalf("descriptor balance after concurrent operations = %d, want 0", concurrentCurrent)
	}
}

func TestRootedDestinationRemoveAllBoundedAndCancellable(t *testing.T) {
	t.Helper()

	base := t.TempDir()
	root := filepath.Join(base, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatalf("create root: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	opens := 0
	destination, err := OpenRootedDestination(root, func(phase MutationPhase, _ string) {
		if phase != MutationOpen {
			return
		}

		opens++
		if opens == 64 {
			cancel()
		}
	})
	if err != nil {
		t.Fatalf("OpenRootedDestination: %v", err)
	}

	var (
		mu                 sync.Mutex
		currentDescriptors int
		peakDescriptors    int
	)
	destination.setDescriptorObserver(func(delta int) {
		mu.Lock()
		currentDescriptors += delta
		peakDescriptors = max(peakDescriptors, currentDescriptors)
		mu.Unlock()
	})

	components := make([]string, 128)
	for index := range components {
		components[index] = fmt.Sprintf("d%03d", index)
	}

	deep := filepath.Join(append([]string{root, "cleanup"}, components...)...)
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatalf("create cleanup tree: %v", err)
	}

	destination.SetTraversalContext(ctx)
	err = destination.RemoveAll(filepath.Join(root, "cleanup"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RemoveAll cancellation error = %v, want context.Canceled", err)
	}

	mu.Lock()
	afterCancellation := currentDescriptors
	mu.Unlock()
	if afterCancellation != 0 {
		t.Fatalf("descriptor balance after cancellation = %d, want 0", afterCancellation)
	}

	destination.SetTraversalContext(context.Background())
	if err := destination.RemoveAll(filepath.Join(root, "cleanup")); err != nil {
		t.Fatalf("RemoveAll retry: %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, "cleanup")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup tree survived retry: %v", err)
	}

	mu.Lock()
	afterCleanup := currentDescriptors
	cleanupPeak := peakDescriptors
	mu.Unlock()
	if afterCleanup != 0 {
		t.Fatalf("descriptor balance after cleanup = %d, want 0", afterCleanup)
	}

	if cleanupPeak > 5 {
		t.Fatalf("cleanup descriptor peak = %d across %d levels, want at most 5",
			cleanupPeak, len(components)+1)
	}

	if err := destination.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("remove closed root: %v", err)
	}
}

// Ensure AtomicWriter satisfies io.Writer at compile time.
var _ io.Writer = (*AtomicWriter)(nil)
