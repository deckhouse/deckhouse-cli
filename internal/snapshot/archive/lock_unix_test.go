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
	"runtime"
	"strings"
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

func TestArchiveLockExternalDomainReplacementFailsClosed(t *testing.T) {
	root := t.TempDir()
	reader, err := AcquireReadLock(root)
	if err != nil {
		t.Fatalf("acquire reader: %v", err)
	}

	domainPath, err := archiveLockDomainPath(root)
	if err != nil {
		t.Fatalf("resolve external lock domain: %v", err)
	}

	if err := os.Remove(domainPath); err != nil {
		t.Fatalf("unlink held external lock record: %v", err)
	}

	if err := os.WriteFile(domainPath, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("create replacement external lock record: %v", err)
	}

	if err := reader.Verify(); !errors.Is(err, ErrArchiveLockChanged) {
		t.Fatalf("Verify error = %v, want ErrArchiveLockChanged", err)
	}

	writer, err := AcquireWriteLock(root)
	if writer != nil {
		_ = writer.Unlock()
	}

	if !errors.Is(err, ErrArchiveLocked) {
		t.Fatalf("replacement-domain writer error = %v, want ErrArchiveLocked", err)
	}

	runArchiveLockHelper(t, root, "write", false)

	if err := reader.Unlock(); err != nil {
		t.Fatalf("release reader: %v", err)
	}

	runArchiveLockHelper(t, root, "write", true)
}

func TestArchiveLockAliasAndReplacementShareStableProcessDomain(t *testing.T) {
	base := t.TempDir()
	ancestor := filepath.Join(base, "ancestor")
	root := filepath.Join(ancestor, "archive")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("create archive root: %v", err)
	}

	aliasParent := filepath.Join(base, "alias")
	if err := os.Symlink(ancestor, aliasParent); err != nil {
		t.Fatalf("create archive-root alias: %v", err)
	}
	aliasRoot := filepath.Join(aliasParent, "archive")

	holder := startArchiveLockHolder(t, root, "read")
	runArchiveLockHelper(t, aliasRoot, "write", false)

	domainPath, err := archiveLockDomainPath(aliasRoot)
	if err != nil {
		t.Fatalf("resolve aliased external lock domain: %v", err)
	}

	if err := os.Remove(domainPath); err != nil {
		t.Fatalf("unlink held external lock record: %v", err)
	}

	if err := os.WriteFile(domainPath, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("create replacement external lock record: %v", err)
	}

	displaced := ancestor + ".displaced"
	if err := os.Rename(ancestor, displaced); err != nil {
		t.Fatalf("replace archive ancestor: %v", err)
	}

	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("create replacement archive tree: %v", err)
	}

	runArchiveLockHelper(t, aliasRoot, "write", false)

	holder.Release()
	runArchiveLockHelper(t, aliasRoot, "write", true)
}

func TestArchiveLockStaleExternalDomainRecoversAcrossProcess(t *testing.T) {
	root := t.TempDir()
	domainPath, err := archiveLockDomainPath(root)
	if err != nil {
		t.Fatalf("resolve external lock domain: %v", err)
	}

	if err := os.WriteFile(domainPath, []byte("stale malformed record"), 0o600); err != nil {
		t.Fatalf("write stale external lock record: %v", err)
	}

	runArchiveLockHelper(t, root, "write", true)
	runArchiveLockHelper(t, root, "read", true)
}

func TestArchiveLockRejectsDomainLocatorIdentityCollision(t *testing.T) {
	root := t.TempDir()
	domainPath, err := archiveLockDomainPath(root)
	if err != nil {
		t.Fatalf("resolve external lock domain: %v", err)
	}

	collisionRecord, err := encodeArchiveLockDomain("/different/archive", "different-root")
	if err != nil {
		t.Fatalf("encode colliding lock-domain record: %v", err)
	}

	if err := os.WriteFile(domainPath, collisionRecord, 0o600); err != nil {
		t.Fatalf("write colliding lock-domain record: %v", err)
	}

	lock, err := AcquireWriteLock(root)
	if lock != nil {
		_ = lock.Unlock()
	}

	if !errors.Is(err, ErrNonRegularArchiveArtifact) {
		t.Fatalf("colliding lock-domain error = %v, want ErrNonRegularArchiveArtifact", err)
	}
}

func TestDarwinArchiveLockTmpAliasesShareProcessDomain(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin /tmp alias is platform-specific")
	}

	privateRoot, err := os.MkdirTemp("/private/tmp", "d8-archive-lock-alias-")
	if err != nil {
		t.Fatalf("create private tmp archive root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(privateRoot) })

	publicRoot := strings.Replace(privateRoot, "/private/tmp/", "/tmp/", 1)
	privateDomain, err := archiveLockDomainPath(privateRoot)
	if err != nil {
		t.Fatalf("resolve private tmp lock domain: %v", err)
	}

	publicDomain, err := archiveLockDomainPath(publicRoot)
	if err != nil {
		t.Fatalf("resolve public tmp lock domain: %v", err)
	}

	if privateDomain != publicDomain {
		t.Fatalf("tmp alias domains differ: private=%s public=%s", privateDomain, publicDomain)
	}

	holder := startArchiveLockHolder(t, privateRoot, "read")
	runArchiveLockHelper(t, publicRoot, "write", false)
	holder.Release()
	runArchiveLockHelper(t, publicRoot, "write", true)
}

func TestArchiveLockCarrierReleasesPartialAcquisition(t *testing.T) {
	const (
		freeKey    = int64(0x6d31f001)
		blockedKey = int64(0x6d31f002)
	)

	blocker := registerTestArchiveCarrierAnchor(t, []int64{blockedKey})
	locked, err := tryArchiveCarrierLock(blocker, false)
	if err != nil {
		t.Fatalf("acquire blocking carrier range: %v", err)
	}

	if !locked {
		t.Fatal("blocking carrier range was unexpectedly busy")
	}

	contender := registerTestArchiveCarrierAnchor(t, []int64{freeKey, blockedKey})
	locked, err = tryArchiveCarrierLock(contender, true)
	if err != nil {
		t.Fatalf("attempt partial carrier acquisition: %v", err)
	}

	if locked {
		t.Fatal("partial carrier acquisition unexpectedly succeeded")
	}

	archiveLockCarrier.Lock()
	_, leaked := archiveLockCarrier.ranges[freeKey]
	archiveLockCarrier.Unlock()
	if leaked {
		t.Fatal("failed carrier acquisition leaked its first range")
	}

	if err := unlockArchiveCarrierLock(blocker); err != nil {
		t.Fatalf("release blocking carrier range: %v", err)
	}

	archiveLockCarrier.Lock()
	carrierOpen := archiveLockCarrier.file != nil
	archiveLockCarrier.Unlock()
	if carrierOpen {
		t.Fatal("last carrier range release retained the carrier descriptor")
	}
}

func registerTestArchiveCarrierAnchor(t *testing.T, lockKeys []int64) *os.File {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "carrier-anchor-")
	if err != nil {
		t.Fatalf("create carrier anchor: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })

	archiveLockCarrier.Lock()
	if archiveLockCarrier.anchors == nil {
		archiveLockCarrier.anchors = make(map[*os.File]*archiveLockAnchorState)
	}

	archiveLockCarrier.anchors[file] = &archiveLockAnchorState{lockKeys: lockKeys}
	archiveLockCarrier.Unlock()
	t.Cleanup(func() {
		archiveLockCarrier.Lock()
		delete(archiveLockCarrier.anchors, file)
		archiveLockCarrier.Unlock()
	})

	return file
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

func TestArchiveLockAncestorReplacementKeepsSamePathDomain(t *testing.T) {
	tests := []struct {
		name            string
		replacedElement string
	}{
		{
			name:            "immediate parent",
			replacedElement: "parent",
		},
		{
			name:            "higher ancestor",
			replacedElement: "ancestor",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			ancestor := filepath.Join(base, "ancestor")
			parent := filepath.Join(ancestor, "parent")
			root := filepath.Join(parent, "archive")

			if err := os.MkdirAll(root, 0o700); err != nil {
				t.Fatalf("create nested archive root: %v", err)
			}

			reader, err := AcquireReadLock(root)
			if err != nil {
				t.Fatalf("acquire reader: %v", err)
			}

			replaced := parent
			if tc.replacedElement == "ancestor" {
				replaced = ancestor
			}

			if err := os.Rename(replaced, replaced+".displaced"); err != nil {
				t.Fatalf("rename held archive %s: %v", tc.replacedElement, err)
			}

			if err := os.MkdirAll(root, 0o700); err != nil {
				t.Fatalf("create second same-path archive tree: %v", err)
			}

			if err := reader.Verify(); !errors.Is(err, ErrNonRegularArchiveArtifact) {
				t.Fatalf("Verify error = %v, want ErrNonRegularArchiveArtifact", err)
			}

			writer, err := AcquireWriteLock(root)
			if writer != nil {
				_ = writer.Unlock()
			}

			if !errors.Is(err, ErrArchiveLocked) {
				t.Fatalf("same-path replacement writer error = %v, want ErrArchiveLocked", err)
			}

			secondReader, err := AcquireReadLock(root)
			if secondReader != nil {
				_ = secondReader.Unlock()
			}

			if !errors.Is(err, ErrArchiveLocked) {
				t.Fatalf("same-path replacement reader error = %v, want ErrArchiveLocked", err)
			}

			runArchiveLockHelper(t, root, "write", false)

			if err := reader.Unlock(); err != nil {
				t.Fatalf("release displaced reader: %v", err)
			}

			writer, err = AcquireWriteLock(root)
			if err != nil {
				t.Fatalf("acquire replacement writer after release: %v", err)
			}
			defer func() { _ = writer.Unlock() }()
		})
	}
}

func TestArchiveLockRejectsMaliciousDomainEntries(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T, path string)
	}{
		{
			name: "symlink",
			build: func(t *testing.T, path string) {
				t.Helper()

				if err := os.Symlink(filepath.Join(t.TempDir(), "target"), path); err != nil {
					t.Fatalf("create domain symlink: %v", err)
				}
			},
		},
		{
			name: "fifo",
			build: func(t *testing.T, path string) {
				t.Helper()

				if err := unix.Mkfifo(path, 0o600); err != nil {
					t.Fatalf("create domain FIFO: %v", err)
				}
			},
		},
		{
			name: "directory",
			build: func(t *testing.T, path string) {
				t.Helper()

				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatalf("create domain directory: %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			domainPath, err := archiveLockDomainPath(root)
			if err != nil {
				t.Fatalf("resolve archive lock domain: %v", err)
			}

			if err := os.Remove(domainPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("remove prior archive lock domain: %v", err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(domainPath) })

			tc.build(t, domainPath)

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
				t.Fatal("AcquireWriteLock blocked on a malicious domain entry")
			}
		})
	}
}

func TestArchiveLockRepairsMalformedRegularDomainWhenUnheld(t *testing.T) {
	root := t.TempDir()
	domainPath, err := archiveLockDomainPath(root)
	if err != nil {
		t.Fatalf("resolve archive lock domain: %v", err)
	}

	if err := os.WriteFile(domainPath, []byte("malformed stale state"), 0o600); err != nil {
		t.Fatalf("write malformed regular domain: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(domainPath) })

	reader, err := AcquireReadLock(root)
	if err != nil {
		t.Fatalf("acquire reader with stale regular domain: %v", err)
	}
	defer func() { _ = reader.Unlock() }()
}
