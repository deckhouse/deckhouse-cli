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
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/windows"
)

func TestMoveFileDurably_UsesWriteThroughCreate(t *testing.T) {
	sentinel := errors.New("move sentinel")
	var oldPath, newPath string
	var flags uint32

	err := moveFileDurably(
		"old.tmp",
		"new.bin",
		func(string) (bool, error) {
			return false, nil
		},
		func(oldPathPtr, newPathPtr *uint16, moveFlags uint32) error {
			oldPath = windows.UTF16PtrToString(oldPathPtr)
			newPath = windows.UTF16PtrToString(newPathPtr)
			flags = moveFlags

			return sentinel
		},
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("moveFileDurably error = %v, want sentinel", err)
	}

	if oldPath != "old.tmp" || newPath != "new.bin" {
		t.Fatalf("move paths = %q -> %q, want old.tmp -> new.bin", oldPath, newPath)
	}

	wantFlags := uint32(windows.MOVEFILE_WRITE_THROUGH)
	if flags != wantFlags {
		t.Fatalf("move flags = %#x, want %#x", flags, wantFlags)
	}
}

func TestMoveFileDurably_ReplacementContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		exists    pathExistsFunc
		moveError error
		wantError error
		wantCause error
		wantMove  bool
	}{
		{
			name: "existing destination fails before move",
			exists: func(string) (bool, error) {
				return true, nil
			},
			wantError: ErrAtomicReplaceUnsupported,
		},
		{
			name: "destination wins create race",
			exists: func(string) (bool, error) {
				return false, nil
			},
			moveError: windows.ERROR_FILE_EXISTS,
			wantError: ErrAtomicReplaceUnsupported,
			wantCause: windows.ERROR_FILE_EXISTS,
			wantMove:  true,
		},
		{
			name: "sharing failure preserves cause",
			exists: func(string) (bool, error) {
				return false, nil
			},
			moveError: windows.ERROR_SHARING_VIOLATION,
			wantError: windows.ERROR_SHARING_VIOLATION,
			wantMove:  true,
		},
		{
			name: "destination probe failure preserves cause",
			exists: func(string) (bool, error) {
				return false, windows.ERROR_ACCESS_DENIED
			},
			wantError: windows.ERROR_ACCESS_DENIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			moveCalls := 0
			err := moveFileDurably(
				"old.tmp",
				"new.bin",
				tt.exists,
				func(*uint16, *uint16, uint32) error {
					moveCalls++

					return tt.moveError
				},
			)
			if !errors.Is(err, tt.wantError) {
				t.Fatalf("moveFileDurably error = %v, want %v", err, tt.wantError)
			}

			if tt.wantCause != nil && !errors.Is(err, tt.wantCause) {
				t.Fatalf("moveFileDurably error = %v, want cause %v", err, tt.wantCause)
			}

			wantCalls := 0
			if tt.wantMove {
				wantCalls = 1
			}

			if moveCalls != wantCalls {
				t.Fatalf("move calls = %d, want %d", moveCalls, wantCalls)
			}
		})
	}
}

func TestAtomicWriter_WindowsCreate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.bin")
	aw, err := NewAtomicWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	want := []byte("new durable content")
	if _, err := aw.Write(want); err != nil {
		t.Fatal(err)
	}

	if err := aw.CommitContext(context.Background()); err != nil {
		t.Fatalf("CommitContext: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("final bytes = %q, want %q", got, want)
	}

	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary file survived commit: %v", err)
	}

	if err := ConfirmFileDurability(context.Background(), path); err != nil {
		t.Fatalf("ConfirmFileDurability: %v", err)
	}
}

func TestAtomicWriter_WindowsConcurrentReaderPreservesOldFinal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}

	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := windows.CloseHandle(handle); err != nil {
			t.Errorf("CloseHandle: %v", err)
		}
	}()

	aw, err := NewAtomicWriter(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := aw.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}

	err = aw.CommitContext(context.Background())
	if !errors.Is(err, ErrAtomicReplaceUnsupported) {
		t.Fatalf("CommitContext error = %v, want unsupported replacement", err)
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
		t.Fatalf("temporary file survived failed publication: %v", err)
	}
}

func TestAtomicWriter_WindowsReplacementHasNoMissingOrPartialWindow(t *testing.T) {
	const readerCount = 4

	path := filepath.Join(t.TempDir(), "data.bin")
	oldContent := []byte("complete old final")
	if err := os.WriteFile(path, oldContent, 0o644); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	done := make(chan struct{})
	readerErrors := make(chan error, readerCount)

	var readersReady sync.WaitGroup
	readersReady.Add(readerCount)

	var readers sync.WaitGroup
	readers.Add(readerCount)

	for range readerCount {
		go func() {
			defer readers.Done()
			<-start

			ready := false
			defer func() {
				if !ready {
					readersReady.Done()
				}
			}()

			firstRead := true
			for {
				content, err := os.ReadFile(path)
				if err != nil {
					readerErrors <- fmt.Errorf("reading final: %w", err)

					return
				}

				if !bytes.Equal(content, oldContent) {
					readerErrors <- fmt.Errorf("observed final content %q", content)

					return
				}

				if firstRead {
					readersReady.Done()
					firstRead = false
					ready = true
				}

				select {
				case <-done:
					return
				default:
				}
			}
		}()
	}

	close(start)
	readersReady.Wait()

	writerErr := func() error {
		for range 100 {
			aw, err := NewAtomicWriter(path)
			if err != nil {
				return err
			}

			if _, err := aw.Write([]byte("new content")); err != nil {
				return err
			}

			err = aw.CommitContext(context.Background())
			if !errors.Is(err, ErrAtomicReplaceUnsupported) {
				return fmt.Errorf("CommitContext error = %w, want unsupported replacement", err)
			}

			if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("temporary file survived failed replacement: %w", err)
			}
		}

		return nil
	}()

	close(done)
	readers.Wait()
	close(readerErrors)

	for err := range readerErrors {
		t.Error(err)
	}

	if writerErr != nil {
		t.Fatal(writerErr)
	}
}

func TestConfirmFileDurability_WindowsIsSingleWriteThroughConfirmation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.bin")
	if err := os.WriteFile(path, []byte("published"), 0o644); err != nil {
		t.Fatal(err)
	}

	calls := 0
	ctx := WithDirectorySyncHook(context.Background(), func(_ string, next func() error) error {
		calls++

		return next()
	})

	if err := ConfirmFileDurability(ctx, path); err != nil {
		t.Fatal(err)
	}

	if calls != 1 {
		t.Fatalf("durability confirmations = %d, want 1", calls)
	}
}

func TestEnsureDir_WindowsUsesMkdirAllContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "c")

	if err := EnsureDir(path); err != nil {
		t.Fatalf("EnsureDir: %v", err)
	}

	if err := EnsureDir(path); err != nil {
		t.Fatalf("idempotent EnsureDir: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if !info.IsDir() {
		t.Fatalf("%s is not a directory", path)
	}
}
