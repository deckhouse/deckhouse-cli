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
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const archiveLockHelperMode = "D8_ARCHIVE_LOCK_HELPER_MODE"

type archiveLockHolderProcess struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stderr *bytes.Buffer
	once   sync.Once
}

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

func TestArchiveLockSiblingDomainsAreIndependent(t *testing.T) {
	parent := t.TempDir()
	firstRoot := filepath.Join(parent, "first")
	secondRoot := filepath.Join(parent, "second")

	for _, root := range []string{firstRoot, secondRoot} {
		if err := os.Mkdir(root, 0o700); err != nil {
			t.Fatalf("create sibling archive root %s: %v", root, err)
		}
	}

	t.Run("writers coexist", func(t *testing.T) {
		first, err := AcquireWriteLock(firstRoot)
		if err != nil {
			t.Fatalf("acquire first sibling writer: %v", err)
		}
		defer func() { _ = first.Unlock() }()

		second, err := AcquireWriteLock(secondRoot)
		if err != nil {
			t.Fatalf("acquire second sibling writer: %v", err)
		}
		defer func() { _ = second.Unlock() }()

		runArchiveLockHelper(t, secondRoot, "write", false)
	})

	t.Run("reader and unrelated writer coexist", func(t *testing.T) {
		reader, err := AcquireReadLock(firstRoot)
		if err != nil {
			t.Fatalf("acquire first sibling reader: %v", err)
		}
		defer func() { _ = reader.Unlock() }()

		writer, err := AcquireWriteLock(secondRoot)
		if err != nil {
			t.Fatalf("acquire unrelated sibling writer: %v", err)
		}

		if err := writer.Unlock(); err != nil {
			t.Fatalf("release unrelated sibling writer: %v", err)
		}

		runArchiveLockHelper(t, secondRoot, "write", true)
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

func TestArchiveWriteLockRootDurabilityFailureIsRetryable(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "output")
	sentinel := errors.New("root durability sentinel")

	lock, err := acquireLockWithRootEnsurer(
		context.Background(),
		root,
		true,
		func(_ context.Context, path string) (os.FileInfo, error) {
			if mkdirErr := os.MkdirAll(path, 0o755); mkdirErr != nil {
				return nil, mkdirErr
			}

			return nil, sentinel
		},
	)
	if lock != nil {
		_ = lock.Unlock()

		t.Fatal("write lock returned after root durability failure")
	}

	if !errors.Is(err, sentinel) {
		t.Fatalf("write lock error = %v, want root durability sentinel", err)
	}

	if _, statErr := os.Stat(root); statErr != nil {
		t.Fatalf("visible root must remain recoverable: %v", statErr)
	}

	if _, statErr := os.Stat(filepath.Join(root, archiveLockFileName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("lock entry exists after root durability failure: %v", statErr)
	}

	lock, err = AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("retry write lock after root durability failure: %v", err)
	}

	if err := lock.Unlock(); err != nil {
		t.Fatalf("release retried write lock: %v", err)
	}
}

func TestArchiveWriteLockReconfirmsExistingRoot(t *testing.T) {
	root := t.TempDir()
	ensureCalls := 0

	lock, err := acquireLockWithRootEnsurer(
		context.Background(),
		root,
		true,
		func(ctx context.Context, path string) (os.FileInfo, error) {
			ensureCalls++

			return ensureWriteLockRoot(ctx, path)
		},
	)
	if err != nil {
		t.Fatalf("acquire existing write root: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	if ensureCalls != 1 {
		t.Fatalf("root durability confirmations = %d, want 1", ensureCalls)
	}
}

func TestArchiveWriteLockRejectsRootReplacementAfterConfirmation(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "output")
	displaced := filepath.Join(parent, "displaced")

	lock, err := acquireLockWithRootEnsurer(
		context.Background(),
		root,
		true,
		func(_ context.Context, path string) (os.FileInfo, error) {
			if mkdirErr := os.Mkdir(path, 0o755); mkdirErr != nil {
				return nil, mkdirErr
			}

			info, statErr := os.Stat(path)
			if statErr != nil {
				return nil, statErr
			}

			if renameErr := os.Rename(path, displaced); renameErr != nil {
				return nil, renameErr
			}

			if mkdirErr := os.Mkdir(path, 0o755); mkdirErr != nil {
				return nil, mkdirErr
			}

			return info, nil
		},
	)
	if lock != nil {
		_ = lock.Unlock()

		t.Fatal("write lock returned after confirmed root replacement")
	}

	if !errors.Is(err, ErrArchiveLockChanged) {
		t.Fatalf("write lock error = %v, want ErrArchiveLockChanged", err)
	}

	if _, statErr := os.Stat(filepath.Join(root, archiveLockFileName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("lock entry exists after root replacement: %v", statErr)
	}
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
	case "read", "read-hold":
		lock, err = AcquireReadLock(root)
	case "write", "write-crash", "write-hold":
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

	if strings.HasSuffix(mode, "-hold") {
		if _, err := os.Stdout.WriteString("ready\n"); err != nil {
			t.Fatalf("publish helper readiness: %v", err)
		}

		if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
			t.Fatalf("wait for helper release: %v", err)
		}
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

func startArchiveLockHolder(t *testing.T, root, mode string) *archiveLockHolderProcess {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestArchiveLockProcessHelper$")
	cmd.Env = append(os.Environ(),
		archiveLockHelperMode+"="+mode+"-hold",
		"D8_ARCHIVE_LOCK_HELPER_ROOT="+root,
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("open archive lock helper stdin: %v", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()

		t.Fatalf("open archive lock helper stdout: %v", err)
	}

	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()

		t.Fatalf("start archive lock helper: %v", err)
	}

	ready, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		_ = stdin.Close()
		_ = cmd.Wait()

		t.Fatalf("wait for archive lock helper readiness: %v\n%s", err, stderr)
	}

	if ready != "ready\n" {
		_ = stdin.Close()
		_ = cmd.Wait()

		t.Fatalf("archive lock helper readiness = %q, want ready", ready)
	}

	holder := &archiveLockHolderProcess{
		t:      t,
		cmd:    cmd,
		stdin:  stdin,
		stderr: stderr,
	}
	t.Cleanup(holder.Release)

	return holder
}

func (p *archiveLockHolderProcess) Release() {
	p.t.Helper()

	p.once.Do(func() {
		if err := p.stdin.Close(); err != nil {
			p.t.Errorf("release archive lock helper: %v", err)
		}

		if err := p.cmd.Wait(); err != nil {
			p.t.Errorf("archive lock holder failed: %v\n%s", err, p.stderr)
		}
	})
}
