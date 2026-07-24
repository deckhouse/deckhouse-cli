//go:build windows

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
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestWindowsArchiveLockDeniesRootAndEntryReplacementWhileHeld(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "archive")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("create archive root: %v", err)
	}

	lock, err := AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("acquire archive writer: %v", err)
	}

	lockPath := filepath.Join(root, archiveLockFileName)
	if err := os.Rename(lockPath, lockPath+".replacement"); err == nil {
		t.Fatal("renamed a lock entry whose handle denies delete sharing")
	}

	if err := os.Rename(root, root+".replacement"); err == nil {
		t.Fatal("renamed an archive root whose pinned handle denies delete sharing")
	}

	if err := lock.Verify(); err != nil {
		t.Fatalf("verify retained Windows lock binding: %v", err)
	}

	if err := lock.Unlock(); err != nil {
		t.Fatalf("release archive writer: %v", err)
	}

	if err := os.Rename(lockPath, lockPath+".released"); err != nil {
		t.Fatalf("rename lock entry after release: %v", err)
	}

	if err := os.Rename(root, root+".released"); err != nil {
		t.Fatalf("rename archive root after release: %v", err)
	}
}

func TestWindowsArchiveLockKeepsDomainAcrossAncestorReplacement(t *testing.T) {
	base := t.TempDir()
	ancestor := filepath.Join(base, "ancestor")
	root := filepath.Join(ancestor, "parent", "archive")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("create nested archive root: %v", err)
	}

	reader, err := AcquireReadLock(root)
	if err != nil {
		t.Fatalf("acquire archive reader: %v", err)
	}

	displaced := ancestor + ".displaced"
	if err := os.Rename(ancestor, displaced); err != nil {
		if verifyErr := reader.Verify(); verifyErr != nil {
			t.Fatalf("verify retained lock after denied ancestor rename: %v", verifyErr)
		}

		if unlockErr := reader.Unlock(); unlockErr != nil {
			t.Fatalf("release reader after denied ancestor rename: %v", unlockErr)
		}

		return
	}

	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("create second same-path archive tree: %v", err)
	}

	writer, err := AcquireWriteLock(root)
	if writer != nil {
		_ = writer.Unlock()
	}

	if !errors.Is(err, ErrArchiveLocked) {
		t.Fatalf("same-path replacement writer error = %v, want ErrArchiveLocked", err)
	}

	if err := reader.Unlock(); err != nil {
		t.Fatalf("release displaced reader: %v", err)
	}

	writer, err = AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("acquire replacement writer after release: %v", err)
	}
	defer func() { _ = writer.Unlock() }()
}

func TestWindowsArchiveLockRejectsReparseEntry(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T, path string)
	}{
		{
			name: "symlink",
			build: func(t *testing.T, path string) {
				t.Helper()

				target := filepath.Join(t.TempDir(), "target")
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatalf("write lock target: %v", err)
				}

				if err := os.Symlink(target, path); err != nil {
					t.Skipf("Windows symlinks are unavailable: %v", err)
				}
			},
		},
		{
			name: "junction",
			build: func(t *testing.T, path string) {
				t.Helper()

				output, err := exec.Command("cmd.exe", "/c", "mklink", "/J", path, t.TempDir()).CombinedOutput()
				if err != nil {
					t.Fatalf("create lock junction: %v: %s", err, output)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			tc.build(t, filepath.Join(root, archiveLockFileName))

			lock, err := AcquireWriteLock(root)
			if lock != nil {
				_ = lock.Unlock()
			}

			if !errors.Is(err, ErrNonRegularArchiveArtifact) {
				t.Fatalf("AcquireWriteLock error = %v, want ErrNonRegularArchiveArtifact", err)
			}
		})
	}
}
