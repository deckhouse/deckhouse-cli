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
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const archiveLockHelperMode = "D8_ARCHIVE_LOCK_HELPER_MODE"

func TestArchiveLockCompatibility(t *testing.T) {
	t.Run("readers coexist and exclude writer", func(t *testing.T) {
		root := t.TempDir()

		first, err := AcquireReadLock(root)
		if err != nil {
			t.Fatalf("acquire first reader: %v", err)
		}
		defer func() { _ = first.Unlock() }()

		second, err := AcquireReadLock(root)
		if err != nil {
			t.Fatalf("acquire second reader: %v", err)
		}
		defer func() { _ = second.Unlock() }()

		_, err = AcquireWriteLock(root)
		if !errors.Is(err, ErrArchiveLocked) {
			t.Fatalf("writer error = %v, want ErrArchiveLocked", err)
		}
	})

	t.Run("writer excludes reader and releases", func(t *testing.T) {
		root := t.TempDir()

		writer, err := AcquireWriteLock(root)
		if err != nil {
			t.Fatalf("acquire writer: %v", err)
		}

		_, err = AcquireReadLock(root)
		if !errors.Is(err, ErrArchiveLocked) {
			t.Fatalf("reader error = %v, want ErrArchiveLocked", err)
		}

		if err := writer.Unlock(); err != nil {
			t.Fatalf("release writer: %v", err)
		}

		reader, err := AcquireReadLock(root)
		if err != nil {
			t.Fatalf("acquire reader after release: %v", err)
		}
		defer func() { _ = reader.Unlock() }()
	})
}

func TestArchiveLockCancellationAndCleanup(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	lock, err := AcquireWriteLockContext(ctx, root)
	if lock != nil {
		_ = lock.Unlock()
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AcquireWriteLockContext error = %v, want context.Canceled", err)
	}

	lock, err = AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("acquire after cancellation: %v", err)
	}

	if err := lock.Unlock(); err != nil {
		t.Fatalf("release after cancellation: %v", err)
	}

	lock, err = AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("reacquire after release: %v", err)
	}
	defer func() { _ = lock.Unlock() }()
}

func TestArchiveLockSingleBindingPerRootedSource(t *testing.T) {
	source, err := OpenRootedSource(t.TempDir())
	if err != nil {
		t.Fatalf("open rooted source: %v", err)
	}
	defer func() { _ = source.Close() }()

	first, err := AcquireRootedReadLock(context.Background(), source)
	if err != nil {
		t.Fatalf("acquire first rooted reader: %v", err)
	}

	second, err := AcquireRootedReadLock(context.Background(), source)
	if second != nil {
		_ = second.Unlock()
	}

	if err == nil {
		t.Fatal("second lock on one rooted source unexpectedly succeeded")
	}

	if err := first.Verify(); err != nil {
		t.Fatalf("failed second acquisition disturbed first lock: %v", err)
	}

	if err := first.Unlock(); err != nil {
		t.Fatalf("release first rooted reader: %v", err)
	}

	second, err = AcquireRootedReadLock(context.Background(), source)
	if err != nil {
		t.Fatalf("reacquire rooted reader after release: %v", err)
	}
	defer func() { _ = second.Unlock() }()
}

func TestArchiveLockStaleRegularEntryIsReusable(t *testing.T) {
	root := t.TempDir()
	lockPath := filepath.Join(root, archiveLockFileName)
	if err := os.WriteFile(lockPath, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale lock entry: %v", err)
	}

	lock, err := AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("acquire stale lock entry: %v", err)
	}

	if err := lock.Unlock(); err != nil {
		t.Fatalf("release stale lock entry: %v", err)
	}

	info, err := os.Stat(lockPath)
	if err != nil {
		t.Fatalf("stat retained lock entry: %v", err)
	}

	if !info.Mode().IsRegular() {
		t.Fatalf("retained lock mode = %s, want regular", info.Mode())
	}
}

func TestArchiveLockCrossProcessCompatibilityAndCrashRelease(t *testing.T) {
	root := t.TempDir()

	reader, err := AcquireReadLock(root)
	if err != nil {
		t.Fatalf("acquire parent reader: %v", err)
	}

	runArchiveLockHelper(t, root, "read", true)
	runArchiveLockHelper(t, root, "write", false)

	if err := reader.Unlock(); err != nil {
		t.Fatalf("release parent reader: %v", err)
	}

	writer, err := AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("acquire parent writer: %v", err)
	}

	runArchiveLockHelper(t, root, "read", false)
	runArchiveLockHelper(t, root, "write", false)

	if err := writer.Unlock(); err != nil {
		t.Fatalf("release parent writer: %v", err)
	}

	runArchiveLockHelper(t, root, "write-crash", true)

	writer, err = AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("acquire after abrupt child exit: %v", err)
	}
	defer func() { _ = writer.Unlock() }()
}

func TestArchiveLockProcessHelper(t *testing.T) {
	mode := os.Getenv(archiveLockHelperMode)
	if mode == "" {
		return
	}

	root := os.Getenv("D8_ARCHIVE_LOCK_HELPER_ROOT")

	var (
		lock *Lock
		err  error
	)

	switch mode {
	case "read":
		lock, err = AcquireReadLock(root)
	case "write", "write-crash":
		lock, err = AcquireWriteLock(root)
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}

	if err != nil {
		t.Fatalf("acquire %s lock: %v", mode, err)
	}

	if mode == "write-crash" {
		os.Exit(0)
	}

	if err := lock.Unlock(); err != nil {
		t.Fatalf("release %s lock: %v", mode, err)
	}
}

func runArchiveLockHelper(t *testing.T, root, mode string, wantSuccess bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestArchiveLockProcessHelper$")
	cmd.Env = append(os.Environ(),
		archiveLockHelperMode+"="+mode,
		"D8_ARCHIVE_LOCK_HELPER_ROOT="+root,
	)

	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("archive lock helper %s did not return promptly: %v", mode, ctx.Err())
	}

	if wantSuccess && err != nil {
		t.Fatalf("archive lock helper %s failed: %v\n%s", mode, err, output)
	}

	if !wantSuccess && err == nil {
		t.Fatalf("archive lock helper %s unexpectedly acquired incompatible lock\n%s", mode, output)
	}

	if !wantSuccess && !strings.Contains(string(output), ErrArchiveLocked.Error()) {
		t.Fatalf("archive lock helper %s output = %q, want %q", mode, output, ErrArchiveLocked)
	}
}
