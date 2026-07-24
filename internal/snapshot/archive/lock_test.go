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

func TestArchiveWriteLockCancellationAfterDurabilityPreservesCause(t *testing.T) {
	root := filepath.Join(t.TempDir(), "output")
	ctx, cancel := context.WithCancel(context.Background())
	ctx = WithWriteLockBoundaryHook(ctx, func(boundary WriteLockBoundary) {
		if boundary == WriteLockBoundaryAfterDurability {
			cancel()
		}
	})

	lock, err := AcquireWriteLockContext(ctx, root)
	if lock != nil {
		_ = lock.Unlock()

		t.Fatal("write lock returned after cancellation at durability handoff")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("write lock error = %v, want context.Canceled", err)
	}

	if _, statErr := os.Stat(filepath.Join(root, archiveLockFileName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("lock entry exists after cancellation at durability handoff: %v", statErr)
	}

	lock, err = AcquireWriteLock(root)
	if err != nil {
		t.Fatalf("retry write lock after cancellation: %v", err)
	}

	if err := lock.Unlock(); err != nil {
		t.Fatalf("release retried write lock: %v", err)
	}
}

func TestArchiveWriteLockRootDurabilityFailureIsRetryable(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "output")
	sentinel := errors.New("root durability sentinel")

	lock, err := acquireLockWithRootEnsurer(
		context.Background(),
		root,
		true,
		func(_ context.Context, path string) (*durableWriteRoot, error) {
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
		func(ctx context.Context, path string) (*durableWriteRoot, error) {
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

func TestArchiveWriteLockRejectsDurableChainReplacement(t *testing.T) {
	boundaries := []struct {
		name  string
		value WriteLockBoundary
	}{
		{name: "after durability", value: WriteLockBoundaryAfterDurability},
		{name: "before rooted lock", value: WriteLockBoundaryBeforeRootLock},
	}

	replacements := []struct {
		name   string
		target func(ancestor, parent, root string) string
		tail   string
	}{
		{
			name: "root",
			target: func(_, _, root string) string {
				return root
			},
		},
		{
			name: "immediate parent",
			target: func(_, parent, _ string) string {
				return parent
			},
			tail: "output",
		},
		{
			name: "higher ancestor",
			target: func(ancestor, _, _ string) string {
				return ancestor
			},
			tail: filepath.Join("parent", "output"),
		},
	}

	for _, boundary := range boundaries {
		t.Run(boundary.name, func(t *testing.T) {
			for _, replacement := range replacements {
				t.Run(replacement.name, func(t *testing.T) {
					base := t.TempDir()
					ancestor := filepath.Join(base, "ancestor")
					parent := filepath.Join(ancestor, "parent")
					root := filepath.Join(parent, "output")
					displaced := filepath.Join(base, "displaced")
					replaced := false

					ctx := WithWriteLockBoundaryHook(context.Background(), func(current WriteLockBoundary) {
						if current != boundary.value || replaced {
							return
						}

						replaced = true
						target := replacement.target(ancestor, parent, root)
						if err := os.Rename(target, displaced); err != nil {
							t.Fatalf("displace %s: %v", replacement.name, err)
						}

						if err := os.MkdirAll(filepath.Join(target, replacement.tail), 0o755); err != nil {
							t.Fatalf("create replacement %s: %v", replacement.name, err)
						}
					})

					lock, err := AcquireWriteLockContext(ctx, root)
					if lock != nil {
						_ = lock.Unlock()

						t.Fatalf("write lock returned after %s replacement", replacement.name)
					}

					if !replaced {
						t.Fatalf("%s replacement hook was not reached", replacement.name)
					}

					if !errors.Is(err, ErrArchiveLockChanged) {
						t.Fatalf("write lock error = %v, want ErrArchiveLockChanged", err)
					}

					lockPath := filepath.Join(root, archiveLockFileName)
					if _, statErr := os.Stat(lockPath); !errors.Is(statErr, os.ErrNotExist) {
						t.Fatalf("lock entry exists after %s replacement: %v", replacement.name, statErr)
					}
				})
			}
		})
	}
}

func TestArchiveWriteLockReconfirmsDurableChainAfterABA(t *testing.T) {
	boundaries := []WriteLockBoundary{
		WriteLockBoundaryAfterDurability,
		WriteLockBoundaryBeforeRootLock,
	}

	replacements := []struct {
		name   string
		target func(ancestor, parent, root string) string
		tail   string
	}{
		{
			name: "root",
			target: func(_, _, root string) string {
				return root
			},
		},
		{
			name: "immediate parent",
			target: func(_, parent, _ string) string {
				return parent
			},
			tail: "output",
		},
		{
			name: "higher ancestor",
			target: func(ancestor, _, _ string) string {
				return ancestor
			},
			tail: filepath.Join("parent", "output"),
		},
	}

	for _, boundary := range boundaries {
		t.Run(boundaryName(boundary), func(t *testing.T) {
			for _, replacement := range replacements {
				t.Run(replacement.name, func(t *testing.T) {
					base := t.TempDir()
					ancestor := filepath.Join(base, "ancestor")
					parent := filepath.Join(ancestor, "parent")
					root := filepath.Join(parent, "output")
					if err := os.MkdirAll(root, 0o755); err != nil {
						t.Fatalf("create archive root: %v", err)
					}

					displaced := filepath.Join(base, "displaced")
					abaComplete := false
					reconfirmed := false

					ctx := WithWriteLockBoundaryHook(context.Background(), func(current WriteLockBoundary) {
						if current == WriteLockBoundaryBeforeDurabilityReconfirmation && abaComplete {
							reconfirmed = true

							return
						}

						if current != boundary || abaComplete {
							return
						}

						target := replacement.target(ancestor, parent, root)
						replaceSyncRestoreDirectory(t, target, displaced, replacement.tail)
						abaComplete = true
					})

					lock, err := AcquireWriteLockContext(ctx, root)
					if err != nil {
						t.Fatalf("acquire write lock after reconfirmed %s ABA: %v", replacement.name, err)
					}

					if !abaComplete {
						t.Fatalf("%s ABA hook was not reached", replacement.name)
					}

					if !reconfirmed {
						t.Fatalf("%s ABA was not followed by durability reconfirmation", replacement.name)
					}

					if err := lock.Verify(); err != nil {
						t.Fatalf("verify write lock after reconfirmed %s ABA: %v", replacement.name, err)
					}

					replaceSyncRestoreDirectory(t,
						replacement.target(ancestor, parent, root), displaced, replacement.tail)
					if err := lock.Verify(); err != nil {
						t.Fatalf("retain write lock after reconfirmed %s ABA: %v", replacement.name, err)
					}

					destination, err := NewLockedRootedDestination(lock, nil)
					if err != nil {
						t.Fatalf("open rooted destination after reconfirmed %s ABA: %v", replacement.name, err)
					}

					if destination.source != lock.source {
						t.Fatalf("rooted destination changed the locked source after %s ABA", replacement.name)
					}

					if err := destination.Close(); err != nil {
						t.Fatalf("close rooted destination after %s ABA: %v", replacement.name, err)
					}

					if err := lock.Unlock(); err != nil {
						t.Fatalf("release write lock after %s ABA: %v", replacement.name, err)
					}
				})
			}
		})
	}
}

func TestArchiveWriteLockRejectsSymlinkAliasReplacementAfterConfirmation(t *testing.T) {
	for _, boundary := range []WriteLockBoundary{
		WriteLockBoundaryAfterDurability,
		WriteLockBoundaryBeforeRootLock,
	} {
		t.Run(boundaryName(boundary), func(t *testing.T) {
			base := t.TempDir()
			first := filepath.Join(base, "first")
			second := filepath.Join(base, "second")
			alias := filepath.Join(base, "alias")

			for _, target := range []string{first, second} {
				if err := os.MkdirAll(filepath.Join(target, "output"), 0o755); err != nil {
					t.Fatalf("create alias target: %v", err)
				}
			}

			if err := os.Symlink(first, alias); err != nil {
				t.Fatalf("create archive alias: %v", err)
			}

			replaced := false
			ctx := WithWriteLockBoundaryHook(context.Background(), func(current WriteLockBoundary) {
				if current != boundary || replaced {
					return
				}

				replaced = true
				if err := os.Remove(alias); err != nil {
					t.Fatalf("remove archive alias: %v", err)
				}

				if err := os.Symlink(second, alias); err != nil {
					t.Fatalf("replace archive alias: %v", err)
				}
			})

			root := filepath.Join(alias, "output")
			lock, err := AcquireWriteLockContext(ctx, root)
			if lock != nil {
				_ = lock.Unlock()

				t.Fatal("write lock returned after symlink alias replacement")
			}

			if !errors.Is(err, ErrArchiveLockChanged) {
				t.Fatalf("write lock error = %v, want ErrArchiveLockChanged", err)
			}

			if _, statErr := os.Stat(filepath.Join(root, archiveLockFileName)); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("lock entry exists after symlink alias replacement: %v", statErr)
			}
		})
	}
}

func TestArchiveWriteLockReconfirmsSymlinkAliasAfterABA(t *testing.T) {
	for _, boundary := range []WriteLockBoundary{
		WriteLockBoundaryAfterDurability,
		WriteLockBoundaryBeforeRootLock,
	} {
		t.Run(boundaryName(boundary), func(t *testing.T) {
			base := t.TempDir()
			target := filepath.Join(base, "target")
			alias := filepath.Join(base, "alias")
			displaced := filepath.Join(base, "displaced")
			root := filepath.Join(alias, "output")
			if err := os.MkdirAll(filepath.Join(target, "output"), 0o755); err != nil {
				t.Fatalf("create alias target: %v", err)
			}

			if err := os.Symlink(target, alias); err != nil {
				t.Fatalf("create archive alias: %v", err)
			}

			abaComplete := false
			reconfirmed := false

			ctx := WithWriteLockBoundaryHook(context.Background(), func(current WriteLockBoundary) {
				if current == WriteLockBoundaryBeforeDurabilityReconfirmation && abaComplete {
					reconfirmed = true

					return
				}

				if current != boundary || abaComplete {
					return
				}

				if err := os.Rename(alias, displaced); err != nil {
					t.Fatalf("displace archive alias: %v", err)
				}

				if err := os.Symlink(target, alias); err != nil {
					t.Fatalf("create replacement archive alias: %v", err)
				}

				if err := syncDir(base); err != nil {
					t.Fatalf("sync replacement archive alias: %v", err)
				}

				if err := os.Remove(alias); err != nil {
					t.Fatalf("remove replacement archive alias: %v", err)
				}

				if err := os.Rename(displaced, alias); err != nil {
					t.Fatalf("restore archive alias: %v", err)
				}

				abaComplete = true
			})

			lock, err := AcquireWriteLockContext(ctx, root)
			if err != nil {
				t.Fatalf("acquire write lock after reconfirmed alias ABA: %v", err)
			}
			defer func() { _ = lock.Unlock() }()

			if !abaComplete {
				t.Fatal("archive alias ABA hook was not reached")
			}

			if !reconfirmed {
				t.Fatal("archive alias ABA was not followed by durability reconfirmation")
			}

			if err := lock.Verify(); err != nil {
				t.Fatalf("verify write lock after reconfirmed alias ABA: %v", err)
			}
		})
	}
}

func replaceSyncRestoreDirectory(t *testing.T, target, displaced, tail string) {
	t.Helper()

	if err := os.Rename(target, displaced); err != nil {
		t.Fatalf("displace confirmed directory %s: %v", target, err)
	}

	if err := EnsureDir(filepath.Join(target, tail)); err != nil {
		t.Fatalf("durably create replacement directory %s: %v", target, err)
	}

	if err := os.RemoveAll(target); err != nil {
		t.Fatalf("remove replacement directory %s: %v", target, err)
	}

	if err := os.Rename(displaced, target); err != nil {
		t.Fatalf("restore confirmed directory %s: %v", target, err)
	}
}

func boundaryName(boundary WriteLockBoundary) string {
	switch boundary {
	case WriteLockBoundaryAfterDurability:
		return "after durability"
	case WriteLockBoundaryBeforeRootLock:
		return "before rooted lock"
	case WriteLockBoundaryBeforeDurabilityReconfirmation:
		return "before durability reconfirmation"
	default:
		return "unknown"
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
