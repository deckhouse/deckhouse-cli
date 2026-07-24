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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	archiveLockFileName        = ".d8-snapshot-download.lock"
	archiveLockDomainMaxRecord = 64 * 1024
)

var archiveLockDomainMagic = [8]byte{'D', '8', 'A', 'L', 'K', '0', '0', '1'}

// ErrArchiveLocked is returned when an incompatible archive reader or writer owns the lock.
var (
	ErrArchiveLocked      = errors.New("snapshot archive is locked")
	ErrArchiveLockChanged = errors.New("snapshot archive lock binding changed")
)

// Lock is a held cooperative archive lock bound to one stable path-domain record, pinned root,
// and descriptor-relative lock inode. Callers must Unlock it. A lock acquired from an existing
// RootedSource does not close that source; a path-based acquisition owns and closes its
// internally opened source.
type Lock struct {
	mu         sync.Mutex
	source     *RootedSource
	anchor     *os.File
	file       *os.File
	path       string
	ownsSource bool
	bound      bool
	held       bool
}

// AcquireReadLock takes a non-blocking shared lock. Multiple uploads may coexist, while an
// upload and a download writer exclude one another.
func AcquireReadLock(root string) (*Lock, error) {
	return AcquireReadLockContext(context.Background(), root)
}

// AcquireWriteLock takes a non-blocking exclusive lock used by archive writers.
func AcquireWriteLock(root string) (*Lock, error) {
	return AcquireWriteLockContext(context.Background(), root)
}

// AcquireReadLockContext is AcquireReadLock with cancellation propagated through acquisition.
func AcquireReadLockContext(ctx context.Context, root string) (*Lock, error) {
	return acquireLock(ctx, root, false)
}

// AcquireWriteLockContext is AcquireWriteLock with cancellation propagated through acquisition.
func AcquireWriteLockContext(ctx context.Context, root string) (*Lock, error) {
	return acquireLock(ctx, root, true)
}

// AcquireRootedReadLock takes a shared lock through source's already-pinned root descriptor.
// The source and lock become one namespace-bound view until Unlock returns.
func AcquireRootedReadLock(ctx context.Context, source *RootedSource) (*Lock, error) {
	if source == nil {
		return nil, errors.New("acquire rooted archive read lock: source is nil")
	}

	return acquireSourceLock(ctx, source, false, false)
}

func acquireLock(ctx context.Context, root string, exclusive bool) (*Lock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if exclusive {
		if err := os.MkdirAll(root, 0o755); err != nil {
			return nil, fmt.Errorf("create archive root %s: %w", root, err)
		}
	}

	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve archive root %s: %w", root, err)
	}

	source, err := OpenRootedSource(absolute)
	if err != nil {
		return nil, err
	}

	lock, err := acquireSourceLock(ctx, source, exclusive, false)
	if err != nil {
		return nil, errors.Join(err, wrapArchiveLockCloseError("archive root", source.Close()))
	}

	lock.ownsSource = true

	return lock, nil
}

func acquireSourceLock(
	ctx context.Context,
	source *RootedSource,
	exclusive bool,
	ownsSource bool,
) (*Lock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := source.verifyNamespaceCurrent(); err != nil {
		return nil, err
	}

	if err := source.claimLock(); err != nil {
		return nil, err
	}

	bound := false

	defer func() {
		if !bound {
			source.unbindLock()
		}
	}()

	// Every process acquires the archive-specific external domain before the pinned root and
	// in-root entry. The domain records both the full normalized path and root file identity;
	// a replacement root may take it over only after every holder of the old identity exits.
	anchor, err := openArchiveLockAnchor(source)
	if err != nil {
		return nil, fmt.Errorf("open archive namespace lock anchor for %s: %w", source.path, err)
	}

	anchorLocked, err := tryArchiveAnchorLock(anchor, source, exclusive)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("lock archive namespace anchor for %s: %w", source.path, err),
			wrapArchiveLockCloseError("archive namespace anchor", closeArchiveLockAnchor(anchor)),
		)
	}

	if !anchorLocked {
		return nil, errors.Join(
			fmt.Errorf("%w: %s", ErrArchiveLocked, source.path),
			wrapArchiveLockCloseError("archive namespace anchor", closeArchiveLockAnchor(anchor)),
		)
	}

	rootLocked, err := tryArchiveRootLock(source.dir, exclusive)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("lock pinned archive root %s: %w", source.path, err),
			wrapArchiveLockCloseError("archive namespace lock", unlockArchiveAnchorLock(anchor)),
			wrapArchiveLockCloseError("archive namespace anchor", closeArchiveLockAnchor(anchor)),
		)
	}

	if !rootLocked {
		return nil, errors.Join(
			fmt.Errorf("%w: %s", ErrArchiveLocked, source.path),
			wrapArchiveLockCloseError("archive namespace lock", unlockArchiveAnchorLock(anchor)),
			wrapArchiveLockCloseError("archive namespace anchor", closeArchiveLockAnchor(anchor)),
		)
	}

	lockPath := filepath.Join(source.path, archiveLockFileName)

	file, err := openArchiveLockAt(source.dir, archiveLockFileName, lockPath)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("open rooted archive lock %s: %w", lockPath, err),
			wrapArchiveLockCloseError("archive root lock", unlockArchiveRootLock(source.dir)),
			wrapArchiveLockCloseError("archive namespace lock", unlockArchiveAnchorLock(anchor)),
			wrapArchiveLockCloseError("archive namespace anchor", closeArchiveLockAnchor(anchor)),
		)
	}

	fileLocked, err := tryArchiveFileLock(file, exclusive)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("lock rooted archive entry %s: %w", lockPath, err),
			wrapArchiveLockCloseError(lockPath, file.Close()),
			wrapArchiveLockCloseError("archive root lock", unlockArchiveRootLock(source.dir)),
			wrapArchiveLockCloseError("archive namespace lock", unlockArchiveAnchorLock(anchor)),
			wrapArchiveLockCloseError("archive namespace anchor", closeArchiveLockAnchor(anchor)),
		)
	}

	if !fileLocked {
		return nil, errors.Join(
			fmt.Errorf("%w: %s", ErrArchiveLocked, source.path),
			wrapArchiveLockCloseError(lockPath, file.Close()),
			wrapArchiveLockCloseError("archive root lock", unlockArchiveRootLock(source.dir)),
			wrapArchiveLockCloseError("archive namespace lock", unlockArchiveAnchorLock(anchor)),
			wrapArchiveLockCloseError("archive namespace anchor", closeArchiveLockAnchor(anchor)),
		)
	}

	lock := &Lock{
		source:     source,
		anchor:     anchor,
		file:       file,
		path:       lockPath,
		ownsSource: ownsSource,
		held:       true,
	}

	if err := source.bindLock(lock.verifyLockEntry); err != nil {
		return nil, errors.Join(err, lock.Unlock())
	}

	lock.bound = true
	bound = true

	if err := lock.Verify(); err != nil {
		return nil, errors.Join(err, lock.Unlock())
	}

	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, lock.Unlock())
	}

	return lock, nil
}

// Verify proves that the external domain, current root name, and in-root lock entry still
// identify the pinned handles.
func (l *Lock) Verify() error {
	if l == nil {
		return errors.New("verify archive lock: lock is nil")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.held || l.source == nil || l.file == nil {
		return errors.New("verify archive lock: lock is not held")
	}

	if err := l.source.verifyNamespaceCurrent(); err != nil {
		return fmt.Errorf("verify locked archive root: %w", err)
	}

	if err := verifyArchiveLockAnchor(l.anchor); err != nil {
		return fmt.Errorf("verify archive lock domain: %w", err)
	}

	return l.verifyLockEntryLocked()
}

func verifyArchiveLockAnchor(anchor *os.File) error {
	if anchor == nil {
		return fmt.Errorf("external archive lock domain is absent: %w", ErrArchiveLockChanged)
	}

	expectedInfo, err := anchor.Stat()
	if err != nil {
		return fmt.Errorf("inspect held archive lock domain %s: %w", anchor.Name(), err)
	}

	currentInfo, err := os.Lstat(anchor.Name())
	if err != nil {
		return fmt.Errorf("inspect current archive lock domain %s: %w",
			anchor.Name(), errors.Join(ErrArchiveLockChanged, err))
	}

	if !currentInfo.Mode().IsRegular() || !os.SameFile(expectedInfo, currentInfo) {
		return fmt.Errorf("%s no longer names the held archive lock domain: %w",
			anchor.Name(), errors.Join(ErrArchiveLockChanged, ErrNonRegularArchiveArtifact))
	}

	return nil
}

func (l *Lock) verifyLockEntry() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.held {
		return fmt.Errorf("%w: lock is no longer held", ErrArchiveLockChanged)
	}

	return l.verifyLockEntryLocked()
}

func (l *Lock) verifyLockEntryLocked() error {
	current, err := openArchiveRegularAt(l.source.dir, archiveLockFileName, l.path)
	if err != nil {
		return fmt.Errorf("verify current archive lock %s: %w",
			l.path, errors.Join(ErrArchiveLockChanged, err))
	}

	defer func() { _ = current.Close() }()

	expectedInfo, err := l.file.Stat()
	if err != nil {
		return fmt.Errorf("inspect pinned archive lock %s: %w", l.path, err)
	}

	currentInfo, err := current.Stat()
	if err != nil {
		return fmt.Errorf("inspect current archive lock %s: %w", l.path, err)
	}

	if !os.SameFile(expectedInfo, currentInfo) {
		return fmt.Errorf("%s no longer names the locked file: %w",
			l.path, errors.Join(ErrArchiveLockChanged, ErrNonRegularArchiveArtifact))
	}

	return nil
}

// Unlock releases the lock entry, then the root lock, and closes every owned handle. It is
// idempotent. The harmless regular lock file remains in the archive for future processes.
func (l *Lock) Unlock() error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.held {
		return nil
	}

	l.held = false
	if l.bound {
		l.source.unbindLock()
		l.bound = false
	}

	unlockFileErr := unlockArchiveFileLock(l.file)
	closeFileErr := l.file.Close()
	unlockRootErr := unlockArchiveRootLock(l.source.dir)
	unlockAnchorErr := unlockArchiveAnchorLock(l.anchor)
	closeAnchorErr := closeArchiveLockAnchor(l.anchor)

	var closeSourceErr error
	if l.ownsSource {
		closeSourceErr = l.source.Close()
	}

	l.file = nil
	l.anchor = nil
	l.source = nil

	return errors.Join(
		wrapArchiveLockCloseError("archive lock entry", unlockFileErr),
		wrapArchiveLockCloseError("archive lock entry", closeFileErr),
		wrapArchiveLockCloseError("archive root lock", unlockRootErr),
		wrapArchiveLockCloseError("archive namespace lock", unlockAnchorErr),
		wrapArchiveLockCloseError("archive namespace anchor", closeAnchorErr),
		wrapArchiveLockCloseError("archive root", closeSourceErr),
	)
}

func wrapArchiveLockCloseError(resource string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("release %s: %w", resource, err)
}

type archiveLockDomainBinding struct {
	valid      bool
	pathEquals bool
	rootEquals bool
}

func bindArchiveLockDomain(
	anchor *os.File,
	source *RootedSource,
	exclusive bool,
	tryLock func(bool) (bool, error),
	unlock func() error,
) (bool, error) {
	locked, err := tryLock(exclusive)
	if err != nil || !locked {
		return locked, err
	}

	binding, err := inspectArchiveLockDomain(anchor, source)
	if err != nil {
		return false, errors.Join(err, unlock())
	}

	if binding.valid && !binding.pathEquals {
		return false, errors.Join(
			fmt.Errorf("archive lock domain locator collision for %s: %w",
				source.path, ErrNonRegularArchiveArtifact),
			unlock(),
		)
	}

	if binding.valid && binding.rootEquals {
		return true, nil
	}

	if exclusive {
		if err := writeArchiveLockDomain(anchor, source); err != nil {
			return false, errors.Join(err, unlock())
		}

		return true, nil
	}

	if err := unlock(); err != nil {
		return false, err
	}

	locked, err = tryLock(true)
	if err != nil {
		return false, err
	}

	if !locked {
		return retryArchiveLockDomainReader(anchor, source, tryLock, unlock)
	}

	binding, err = inspectArchiveLockDomain(anchor, source)
	if err != nil {
		return false, errors.Join(err, unlock())
	}

	if binding.valid && !binding.pathEquals {
		return false, errors.Join(
			fmt.Errorf("archive lock domain locator collision for %s: %w",
				source.path, ErrNonRegularArchiveArtifact),
			unlock(),
		)
	}

	if !binding.valid || !binding.rootEquals {
		if err := writeArchiveLockDomain(anchor, source); err != nil {
			return false, errors.Join(err, unlock())
		}
	}

	if err := unlock(); err != nil {
		return false, err
	}

	return tryLock(false)
}

func retryArchiveLockDomainReader(
	anchor *os.File,
	source *RootedSource,
	tryLock func(bool) (bool, error),
	unlock func() error,
) (bool, error) {
	locked, err := tryLock(false)
	if err != nil || !locked {
		return locked, err
	}

	binding, err := inspectArchiveLockDomain(anchor, source)
	if err != nil {
		return false, errors.Join(err, unlock())
	}

	if !binding.valid || !binding.pathEquals || !binding.rootEquals {
		return false, errors.Join(
			fmt.Errorf("%w: %s", ErrArchiveLocked, source.path),
			unlock(),
		)
	}

	return true, nil
}

func inspectArchiveLockDomain(anchor *os.File, source *RootedSource) (archiveLockDomainBinding, error) {
	expectedPath, expectedRootID, err := archiveLockDomainIdentity(source)
	if err != nil {
		return archiveLockDomainBinding{}, err
	}

	info, err := anchor.Stat()
	if err != nil {
		return archiveLockDomainBinding{}, fmt.Errorf("inspect archive lock domain: %w", err)
	}

	size := info.Size()
	if size <= 0 || size > archiveLockDomainMaxRecord {
		return archiveLockDomainBinding{}, nil
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(io.NewSectionReader(anchor, 0, size), data); err != nil {
		return archiveLockDomainBinding{}, fmt.Errorf("read archive lock domain: %w", err)
	}

	path, rootID, valid := decodeArchiveLockDomain(data)
	if !valid {
		return archiveLockDomainBinding{}, nil
	}

	return archiveLockDomainBinding{
		valid:      true,
		pathEquals: path == expectedPath,
		rootEquals: path == expectedPath && rootID == expectedRootID,
	}, nil
}

func writeArchiveLockDomain(anchor *os.File, source *RootedSource) error {
	path, rootID, err := archiveLockDomainIdentity(source)
	if err != nil {
		return err
	}

	record, err := encodeArchiveLockDomain(path, rootID)
	if err != nil {
		return err
	}

	if err := anchor.Truncate(0); err != nil {
		return fmt.Errorf("truncate archive lock domain: %w", err)
	}

	if _, err := anchor.WriteAt(record, 0); err != nil {
		return fmt.Errorf("write archive lock domain: %w", err)
	}

	if err := anchor.Sync(); err != nil {
		return fmt.Errorf("sync archive lock domain: %w", err)
	}

	return nil
}

func encodeArchiveLockDomain(path, rootID string) ([]byte, error) {
	recordSize := len(archiveLockDomainMagic) + 8 + len(path) + len(rootID)
	if recordSize > archiveLockDomainMaxRecord {
		return nil, fmt.Errorf("archive lock domain identity is too large: %w", ErrNonRegularArchiveArtifact)
	}

	record := bytes.NewBuffer(make([]byte, 0, recordSize))
	record.Write(archiveLockDomainMagic[:])

	if err := binary.Write(record, binary.LittleEndian, uint32(len(path))); err != nil {
		return nil, fmt.Errorf("encode archive lock domain path length: %w", err)
	}

	if err := binary.Write(record, binary.LittleEndian, uint32(len(rootID))); err != nil {
		return nil, fmt.Errorf("encode archive lock domain root length: %w", err)
	}

	record.WriteString(path)
	record.WriteString(rootID)

	return record.Bytes(), nil
}

func decodeArchiveLockDomain(record []byte) (string, string, bool) {
	const headerSize = 16

	if len(record) < headerSize || !bytes.Equal(record[:8], archiveLockDomainMagic[:]) {
		return "", "", false
	}

	pathLength := int(binary.LittleEndian.Uint32(record[8:12]))
	rootLength := int(binary.LittleEndian.Uint32(record[12:16]))

	if pathLength < 1 || rootLength < 1 || pathLength+rootLength != len(record)-headerSize {
		return "", "", false
	}

	pathEnd := headerSize + pathLength

	return string(record[headerSize:pathEnd]), string(record[pathEnd:]), true
}
