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
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestMoveFileDurably_UsesWriteThroughReplacement(t *testing.T) {
	sentinel := errors.New("move sentinel")
	var oldPath, newPath string
	var flags uint32

	err := moveFileDurably("old.tmp", "new.bin", func(oldPathPtr, newPathPtr *uint16, moveFlags uint32) error {
		oldPath = windows.UTF16PtrToString(oldPathPtr)
		newPath = windows.UTF16PtrToString(newPathPtr)
		flags = moveFlags

		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("moveFileDurably error = %v, want sentinel", err)
	}

	if oldPath != "old.tmp" || newPath != "new.bin" {
		t.Fatalf("move paths = %q -> %q, want old.tmp -> new.bin", oldPath, newPath)
	}

	wantFlags := uint32(windows.MOVEFILE_REPLACE_EXISTING | windows.MOVEFILE_WRITE_THROUGH)
	if flags != wantFlags {
		t.Fatalf("move flags = %#x, want %#x", flags, wantFlags)
	}
}

func TestAtomicWriter_WindowsCreateAndReplace(t *testing.T) {
	tests := []struct {
		name       string
		oldContent []byte
	}{
		{name: "Create"},
		{name: "Replace", oldContent: []byte("old")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.bin")
			if tt.oldContent != nil {
				if err := os.WriteFile(path, tt.oldContent, 0o644); err != nil {
					t.Fatal(err)
				}
			}

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
		})
	}
}

func TestAtomicWriter_WindowsPublicationFailurePreservesOldFinal(t *testing.T) {
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
	if !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		t.Fatalf("CommitContext error = %v, want sharing violation", err)
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
