//go:build linux || darwin || freebsd || openbsd

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
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestArchiveLockRejectsUnixNonRegularEntriesPromptly(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T, path string)
	}{
		{
			name: "symlink",
			build: func(t *testing.T, path string) {
				t.Helper()

				if err := os.Symlink(filepath.Join(t.TempDir(), "target"), path); err != nil {
					t.Fatalf("create lock symlink: %v", err)
				}
			},
		},
		{
			name: "fifo",
			build: func(t *testing.T, path string) {
				t.Helper()

				if err := unix.Mkfifo(path, 0o600); err != nil {
					t.Fatalf("create lock FIFO: %v", err)
				}
			},
		},
		{
			name: "unix socket",
			build: func(t *testing.T, path string) {
				t.Helper()

				listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				if err != nil {
					t.Fatalf("create lock socket: %v", err)
				}

				t.Cleanup(func() { _ = listener.Close() })
			},
		},
		{
			name: "directory",
			build: func(t *testing.T, path string) {
				t.Helper()

				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatalf("create lock directory: %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root, err := os.MkdirTemp("", "d8-lock-")
			if err != nil {
				t.Fatalf("create short lock fixture root: %v", err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(root) })

			tc.build(t, filepath.Join(root, archiveLockFileName))

			result := make(chan error, 1)
			go func() {
				lock, lockErr := AcquireWriteLock(root)
				if lock != nil {
					_ = lock.Unlock()
				}

				result <- lockErr
			}()

			select {
			case err := <-result:
				if !errors.Is(err, ErrNonRegularArchiveArtifact) {
					t.Fatalf("AcquireWriteLock error = %v, want ErrNonRegularArchiveArtifact", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("AcquireWriteLock blocked on a non-regular lock entry")
			}
		})
	}
}

func TestArchiveLockRejectsExistingDeviceBeforeOpen(t *testing.T) {
	const devicePath = "/dev/null"

	info, err := os.Lstat(devicePath)
	if err != nil {
		t.Fatalf("inspect required device fixture %s: %v", devicePath, err)
	}

	if info.Mode()&os.ModeDevice == 0 {
		t.Fatalf("device fixture %s has mode %s", devicePath, info.Mode())
	}

	source, err := OpenRootedSource(filepath.Dir(devicePath))
	if err != nil {
		t.Fatalf("open device parent as rooted source: %v", err)
	}
	defer func() { _ = source.Close() }()

	file, err := openArchiveLockAt(source.dir, filepath.Base(devicePath), devicePath)
	if file != nil {
		_ = file.Close()
	}

	if !errors.Is(err, ErrNonRegularArchiveArtifact) {
		t.Fatalf("openArchiveLockAt error = %v, want ErrNonRegularArchiveArtifact", err)
	}
}

func TestArchiveLockEntryReplacementFailsClosedAndKeepsRootDomain(t *testing.T) {
	root := t.TempDir()
	reader, err := AcquireReadLock(root)
	if err != nil {
		t.Fatalf("acquire reader: %v", err)
	}

	lockPath := filepath.Join(root, archiveLockFileName)
	displaced := lockPath + ".displaced"
	if err := os.Rename(lockPath, displaced); err != nil {
		t.Fatalf("rename held lock entry: %v", err)
	}

	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatalf("create replacement lock entry: %v", err)
	}

	if err := reader.Verify(); !errors.Is(err, ErrArchiveLockChanged) {
		t.Fatalf("Verify error = %v, want ErrArchiveLockChanged", err)
	}

	writer, err := AcquireWriteLock(root)
	if writer != nil {
		_ = writer.Unlock()
	}

	if !errors.Is(err, ErrArchiveLocked) {
		t.Fatalf("replacement-entry writer error = %v, want ErrArchiveLocked", err)
	}

	runArchiveLockHelper(t, root, "write", false)

	if err := reader.Unlock(); err != nil {
		t.Fatalf("release displaced reader: %v", err)
	}

	writer, err = AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("acquire replacement entry after release: %v", err)
	}
	defer func() { _ = writer.Unlock() }()
}

func TestArchiveLockRootReplacementFailsClosed(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "archive")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("create archive root: %v", err)
	}

	reader, err := AcquireReadLock(root)
	if err != nil {
		t.Fatalf("acquire reader: %v", err)
	}

	displaced := root + ".displaced"
	if err := os.Rename(root, displaced); err != nil {
		t.Fatalf("rename held archive root: %v", err)
	}

	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("create replacement archive root: %v", err)
	}

	if err := reader.Verify(); !errors.Is(err, ErrNonRegularArchiveArtifact) {
		t.Fatalf("Verify error = %v, want ErrNonRegularArchiveArtifact", err)
	}

	writer, err := AcquireWriteLock(root)
	if writer != nil {
		_ = writer.Unlock()
	}

	if !errors.Is(err, ErrArchiveLocked) {
		t.Fatalf("replacement-root writer error = %v, want ErrArchiveLocked", err)
	}

	runArchiveLockHelper(t, root, "write", false)

	if err := reader.Unlock(); err != nil {
		t.Fatalf("release displaced reader: %v", err)
	}

	writer, err = AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("acquire replacement root after release: %v", err)
	}
	defer func() { _ = writer.Unlock() }()
}
