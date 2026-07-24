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
	"reflect"
	"strings"
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
	mu                 sync.Mutex
	source             *RootedSource
	anchor             *os.File
	file               *os.File
	path               string
	verifyRootIdentity func() error
	ownsSource         bool
	bound              bool
	held               bool
}

// WriteLockBoundary identifies a deterministic archive write-lock acquisition handoff.
type WriteLockBoundary uint8

const (
	// WriteLockBoundaryAfterDurability runs after the directory-creation confirmation pass
	// and before the pinned root and ancestry receive their handoff reconfirmation.
	WriteLockBoundaryAfterDurability WriteLockBoundary = iota + 1
	// WriteLockBoundaryBeforeRootLock runs immediately before the rooted lock sequence starts.
	WriteLockBoundaryBeforeRootLock
	// WriteLockBoundaryBeforeDurabilityReconfirmation runs after an acquisition handoff and
	// before the current exact directory chain is durably confirmed again.
	WriteLockBoundaryBeforeDurabilityReconfirmation
)

// WriteLockBoundaryHook runs at deterministic write-lock handoffs. It supports adversarial
// replacement tests through the real acquisition path; ordinary callers do not install one.
type WriteLockBoundaryHook func(WriteLockBoundary)

type writeLockBoundaryHookKey struct{}

// WriteLockReconfirmationPhase identifies one descriptor-bound durability operation.
type WriteLockReconfirmationPhase uint8

const (
	// WriteLockReconfirmationBeforeOpen runs before opening an exact child directory.
	WriteLockReconfirmationBeforeOpen WriteLockReconfirmationPhase = iota + 1
	// WriteLockReconfirmationBeforeSync runs before syncing an exact directory descriptor.
	WriteLockReconfirmationBeforeSync
	// WriteLockReconfirmationAfterSync runs after syncing the directory that contains path
	// and before its namespace generation is accepted.
	WriteLockReconfirmationAfterSync
	// WriteLockReconfirmationBeforeClose runs before closing an exact directory descriptor.
	WriteLockReconfirmationBeforeClose
)

// WriteLockReconfirmationHook runs at descriptor-bound durability operations. Returning an
// error injects that operation's failure for deterministic production-path tests.
type WriteLockReconfirmationHook func(WriteLockReconfirmationPhase, string) error

type writeLockReconfirmationHookKey struct{}

type durablePathEntry struct {
	path          string
	info          os.FileInfo
	parentPath    string
	parentInfo    os.FileInfo
	symlinkTarget string
}

type durableWriteRoot struct {
	source            *RootedSource
	durableRootPath   string
	requestedRootPath string
	durablePath       []durablePathEntry
	requestedPath     []durablePathEntry
}

// WithWriteLockBoundaryHook returns a context that invokes hook during write-lock acquisition.
func WithWriteLockBoundaryHook(ctx context.Context, hook WriteLockBoundaryHook) context.Context {
	return context.WithValue(ctx, writeLockBoundaryHookKey{}, hook)
}

// WithWriteLockReconfirmationHook returns a context that invokes hook while exact directory
// descriptors are opened, synced, generation-checked, and closed.
func WithWriteLockReconfirmationHook(
	ctx context.Context,
	hook WriteLockReconfirmationHook,
) context.Context {
	return context.WithValue(ctx, writeLockReconfirmationHookKey{}, hook)
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

	return acquireSourceLock(ctx, source, false, false, nil)
}

func acquireLock(ctx context.Context, root string, exclusive bool) (*Lock, error) {
	return acquireLockWithRootEnsurer(ctx, root, exclusive, ensureWriteLockRoot)
}

func acquireLockWithRootEnsurer(
	ctx context.Context,
	root string,
	exclusive bool,
	ensureRoot func(context.Context, string) (*durableWriteRoot, error),
) (*Lock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var (
		source      *RootedSource
		durableRoot *durableWriteRoot
	)

	if exclusive {
		var err error

		durableRoot, err = ensureRoot(ctx, root)
		if err != nil {
			return nil, fmt.Errorf("create archive root %s: %w", root, err)
		}

		if durableRoot == nil || durableRoot.source == nil {
			return nil, fmt.Errorf("create archive root %s: durable root identity is absent", root)
		}

		source = durableRoot.source
	} else {
		absolute, err := filepath.Abs(root)
		if err != nil {
			return nil, fmt.Errorf("resolve archive root %s: %w", root, err)
		}

		source, err = OpenRootedSource(absolute)
		if err != nil {
			return nil, err
		}
	}

	var verifyRootIdentity func() error

	if durableRoot != nil {
		runWriteLockBoundaryHook(ctx, WriteLockBoundaryBeforeRootLock)

		if err := durableRoot.reconfirm(ctx); err != nil {
			return nil, errors.Join(
				fmt.Errorf("verify durable archive root %s before locking: %w", root, err),
				wrapArchiveLockCloseError("archive root", source.Close()),
			)
		}

		verifyRootIdentity = func() error {
			return durableRoot.reconfirm(ctx)
		}
	}

	lock, err := acquireSourceLock(ctx, source, exclusive, false, verifyRootIdentity)
	if err != nil {
		return nil, errors.Join(err, wrapArchiveLockCloseError("archive root", source.Close()))
	}

	if durableRoot != nil {
		retainedContext := context.WithoutCancel(ctx)
		lock.verifyRootIdentity = func() error {
			return durableRoot.reconfirm(retainedContext)
		}
	}

	lock.ownsSource = true

	return lock, nil
}

func ensureWriteLockRoot(ctx context.Context, root string) (*durableWriteRoot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	durablePath, err := resolveWriteLockDurabilityPath(root)
	if err != nil {
		return nil, err
	}

	if err := EnsureDir(durablePath); err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve archive root %s: %w", root, err)
	}

	source, err := OpenRootedSource(absolute)
	if err != nil {
		return nil, err
	}

	durableIdentity, err := captureDurableDirectoryPath(ctx, durablePath)
	if err != nil {
		return nil, errors.Join(err, wrapArchiveLockCloseError("archive root", source.Close()))
	}

	requestedIdentity, err := captureDurablePath(ctx, absolute)
	if err != nil {
		return nil, errors.Join(err, wrapArchiveLockCloseError("archive root", source.Close()))
	}

	confirmed := &durableWriteRoot{
		source:            source,
		durableRootPath:   durablePath,
		requestedRootPath: absolute,
		durablePath:       durableIdentity,
		requestedPath:     requestedIdentity,
	}

	// The first pass creates and confirms the visible chain. The descriptor and every
	// component identity are then pinned before this second pass, so only that exact
	// rooted identity can inherit the final durability confirmation.
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, wrapArchiveLockCloseError("archive root", source.Close()))
	}

	if err := EnsureDir(durablePath); err != nil {
		return nil, errors.Join(err, wrapArchiveLockCloseError("archive root", source.Close()))
	}

	runWriteLockBoundaryHook(ctx, WriteLockBoundaryAfterDurability)

	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, wrapArchiveLockCloseError("archive root", source.Close()))
	}

	if err := confirmed.reconfirm(ctx); err != nil {
		return nil, errors.Join(err, wrapArchiveLockCloseError("archive root", source.Close()))
	}

	return confirmed, nil
}

func captureDurableDirectoryPath(ctx context.Context, path string) ([]durablePathEntry, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve durable archive directory %s: %w", path, err)
	}

	volume := filepath.VolumeName(absolute)
	rootPath := volume + string(filepath.Separator)

	relative, err := filepath.Rel(rootPath, absolute)
	if err != nil {
		return nil, fmt.Errorf("resolve durable archive directory components for %s: %w", path, err)
	}

	root, err := openArchiveRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf("open durable archive root %s: %w", rootPath, err)
	}

	rootInfo, err := root.Stat()
	if err != nil {
		_ = root.Close()

		return nil, fmt.Errorf("inspect durable archive root %s: %w", rootPath, err)
	}

	identity := []durablePathEntry{{path: rootPath, info: rootInfo}}
	parent := root
	parentPath := rootPath

	if relative != "." {
		for _, component := range strings.Split(relative, string(filepath.Separator)) {
			if err := ctx.Err(); err != nil {
				_ = parent.Close()

				return nil, err
			}

			childPath := filepath.Join(parentPath, component)

			parentInfo, statErr := parent.Stat()
			if statErr != nil {
				_ = parent.Close()

				return nil, fmt.Errorf("inspect durable archive parent %s: %w", parentPath, statErr)
			}

			child, openErr := openDurableDirectoryAt(parent, component, childPath)
			if openErr != nil {
				_ = parent.Close()

				return nil, fmt.Errorf("open durable archive directory %s: %w", childPath, openErr)
			}

			currentParentInfo, parentStatErr := parent.Stat()
			if parentStatErr != nil {
				_ = child.Close()
				_ = parent.Close()

				return nil, fmt.Errorf("reinspect durable archive parent %s: %w", parentPath, parentStatErr)
			}

			if !sameDurableNamespaceState(parentInfo, currentParentInfo) {
				_ = child.Close()
				_ = parent.Close()

				return nil, fmt.Errorf("durable archive parent %s changed while capturing %s: %w",
					parentPath, childPath, ErrArchiveLockChanged)
			}

			childInfo, childStatErr := child.Stat()
			if childStatErr != nil {
				_ = child.Close()
				_ = parent.Close()

				return nil, fmt.Errorf("inspect durable archive directory %s: %w", childPath, childStatErr)
			}

			identity = append(identity, durablePathEntry{
				path: childPath,
				info: childInfo,
			})

			if closeErr := parent.Close(); closeErr != nil {
				_ = child.Close()

				return nil, fmt.Errorf("close durable archive parent %s: %w", parentPath, closeErr)
			}

			parent = child
			parentPath = childPath
		}
	}

	if err := parent.Close(); err != nil {
		return nil, fmt.Errorf("close durable archive directory %s: %w", parentPath, err)
	}

	return identity, nil
}

func openDurableDirectoryPath(ctx context.Context, path string) (*os.File, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve durable archive directory %s: %w", path, err)
	}

	volume := filepath.VolumeName(absolute)
	rootPath := volume + string(filepath.Separator)

	relative, err := filepath.Rel(rootPath, absolute)
	if err != nil {
		return nil, fmt.Errorf("resolve durable archive directory components for %s: %w", path, err)
	}

	parent, err := openArchiveRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf("open durable archive root %s: %w", rootPath, err)
	}

	parentPath := rootPath

	if relative == "." {
		return parent, nil
	}

	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if err := ctx.Err(); err != nil {
			_ = parent.Close()

			return nil, err
		}

		childPath := filepath.Join(parentPath, component)

		child, openErr := openDurableDirectoryAt(parent, component, childPath)
		if openErr != nil {
			_ = parent.Close()

			return nil, fmt.Errorf("open durable archive directory %s: %w", childPath, openErr)
		}

		if closeErr := parent.Close(); closeErr != nil {
			_ = child.Close()

			return nil, fmt.Errorf("close durable archive parent %s: %w", parentPath, closeErr)
		}

		parent = child
		parentPath = childPath
	}

	return parent, nil
}

func captureDurablePath(ctx context.Context, path string) ([]durablePathEntry, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve durable archive path %s: %w", path, err)
	}

	volume := filepath.VolumeName(absolute)
	rootPath := volume + string(filepath.Separator)

	relative, err := filepath.Rel(rootPath, absolute)
	if err != nil {
		return nil, fmt.Errorf("resolve durable archive path components for %s: %w", path, err)
	}

	paths := []string{rootPath}
	if relative != "." {
		current := rootPath
		for _, component := range strings.Split(relative, string(filepath.Separator)) {
			current = filepath.Join(current, component)
			paths = append(paths, current)
		}
	}

	identity := make([]durablePathEntry, 0, len(paths))
	for _, current := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		info, statErr := os.Lstat(current)
		if statErr != nil {
			return nil, fmt.Errorf("inspect durable archive path component %s: %w", current, statErr)
		}

		entry := durablePathEntry{path: current, info: info}
		if info.Mode()&os.ModeSymlink != 0 {
			parentPath, evalErr := filepath.EvalSymlinks(filepath.Dir(current))
			if evalErr != nil {
				return nil, fmt.Errorf("resolve durable archive symlink parent %s: %w", current, evalErr)
			}

			parent, openErr := openDurableDirectoryPath(ctx, parentPath)
			if openErr != nil {
				return nil, openErr
			}

			parentInfo, parentStatErr := parent.Stat()
			if parentStatErr != nil {
				_ = parent.Close()

				return nil, fmt.Errorf("inspect durable archive symlink parent %s: %w",
					parentPath, parentStatErr)
			}

			target, readErr := readDurableSymlinkAt(parent, filepath.Base(current), current)

			closeErr := parent.Close()
			if readErr != nil || closeErr != nil {
				return nil, errors.Join(
					readErr,
					wrapArchiveLockCloseError(parentPath, closeErr),
				)
			}

			entry.parentPath = parentPath
			entry.parentInfo = parentInfo
			entry.symlinkTarget = target
		}

		identity = append(identity, entry)
	}

	return identity, nil
}

func (r *durableWriteRoot) verify() error {
	if r == nil || r.source == nil {
		return fmt.Errorf("durable archive root identity is absent: %w", ErrArchiveLockChanged)
	}

	if err := verifyDurablePath(r.durablePath); err != nil {
		return err
	}

	if err := verifyDurablePath(r.requestedPath); err != nil {
		return err
	}

	if err := r.source.verifyNamespaceCurrent(); err != nil {
		return fmt.Errorf("verify descriptor-confirmed archive root: %w",
			errors.Join(ErrArchiveLockChanged, err))
	}

	return nil
}

func (r *durableWriteRoot) reconfirm(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("durable archive root identity is absent: %w", ErrArchiveLockChanged)
	}

	runWriteLockBoundaryHook(ctx, WriteLockBoundaryBeforeDurabilityReconfirmation)

	if err := ctx.Err(); err != nil {
		return err
	}

	// A restored inode can satisfy every later SameFile check even when its latest
	// insertion was never persisted. Confirm the current chain first, so equality
	// proves that the captured identity received this invocation's fsync provenance.
	if err := reconfirmDurableDirectories(ctx, r.durablePath); err != nil {
		return fmt.Errorf("reconfirm durable archive root %s: %w", r.durableRootPath, err)
	}

	if filepath.Clean(r.requestedRootPath) != filepath.Clean(r.durableRootPath) {
		if err := reconfirmRequestedSymlinks(ctx, r.requestedPath); err != nil {
			return err
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return r.verify()
}

func reconfirmDurableDirectories(ctx context.Context, identity []durablePathEntry) error {
	if len(identity) == 0 {
		return fmt.Errorf("durable archive directory identity is empty: %w", ErrArchiveLockChanged)
	}

	parent, err := openArchiveRoot(identity[0].path)
	if err != nil {
		return fmt.Errorf("open durable archive root %s for reconfirmation: %w", identity[0].path, err)
	}

	parentPath := identity[0].path
	if err := verifyDurableDirectoryIdentity(parent, identity[0]); err != nil {
		return errors.Join(err, closeReconfirmationDirectory(ctx, parent, parentPath))
	}

	for index := 1; index < len(identity); index++ {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, closeReconfirmationDirectory(ctx, parent, parentPath))
		}

		entry := identity[index]
		if err := runWriteLockReconfirmationHook(
			ctx,
			WriteLockReconfirmationBeforeOpen,
			entry.path,
		); err != nil {
			return errors.Join(err, closeReconfirmationDirectory(ctx, parent, parentPath))
		}

		child, err := openDurableDirectoryAt(parent, filepath.Base(entry.path), entry.path)
		if err != nil {
			return errors.Join(
				fmt.Errorf("open durable archive entry %s in %s: %w", entry.path, parentPath, err),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		if err := verifyDurableDirectoryIdentity(child, entry); err != nil {
			return errors.Join(
				err,
				closeReconfirmationDirectory(ctx, child, entry.path),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		if err := runWriteLockReconfirmationHook(
			ctx,
			WriteLockReconfirmationBeforeSync,
			entry.path,
		); err != nil {
			return errors.Join(
				err,
				closeReconfirmationDirectory(ctx, child, entry.path),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		if err := syncDurableDirectory(parent); err != nil {
			return errors.Join(
				fmt.Errorf("reconfirm durable archive entry %s in %s: %w",
					entry.path, parentPath, err),
				closeReconfirmationDirectory(ctx, child, entry.path),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		confirmedNamespace, err := parent.Stat()
		if err != nil {
			return errors.Join(
				fmt.Errorf("capture confirmed durable archive namespace %s: %w", parentPath, err),
				closeReconfirmationDirectory(ctx, child, entry.path),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		if err := runWriteLockReconfirmationHook(
			ctx,
			WriteLockReconfirmationAfterSync,
			entry.path,
		); err != nil {
			return errors.Join(
				err,
				closeReconfirmationDirectory(ctx, child, entry.path),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		currentNamespace, err := parent.Stat()
		if err != nil {
			return errors.Join(
				fmt.Errorf("reinspect confirmed durable archive namespace %s: %w", parentPath, err),
				closeReconfirmationDirectory(ctx, child, entry.path),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		if !sameDurableNamespaceState(confirmedNamespace, currentNamespace) {
			return errors.Join(
				fmt.Errorf("durable archive namespace %s changed after confirming %s: %w",
					parentPath, entry.path, ErrArchiveLockChanged),
				closeReconfirmationDirectory(ctx, child, entry.path),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		probe, err := openDurableDirectoryAt(parent, filepath.Base(entry.path), entry.path)
		if err != nil {
			return errors.Join(
				fmt.Errorf("reopen confirmed durable archive entry %s in %s: %w",
					entry.path, parentPath, err),
				closeReconfirmationDirectory(ctx, child, entry.path),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		if err := verifyDurableDirectoryIdentity(probe, entry); err != nil {
			return errors.Join(
				err,
				closeReconfirmationDirectory(ctx, probe, entry.path),
				closeReconfirmationDirectory(ctx, child, entry.path),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		if err := closeReconfirmationDirectory(ctx, probe, entry.path); err != nil {
			return errors.Join(
				err,
				closeReconfirmationDirectory(ctx, child, entry.path),
				closeReconfirmationDirectory(ctx, parent, parentPath),
			)
		}

		if err := closeReconfirmationDirectory(ctx, parent, parentPath); err != nil {
			return errors.Join(err, closeReconfirmationDirectory(ctx, child, entry.path))
		}

		parent = child
		parentPath = entry.path
	}

	if err := runWriteLockReconfirmationHook(
		ctx,
		WriteLockReconfirmationBeforeSync,
		parentPath,
	); err != nil {
		return errors.Join(err, closeReconfirmationDirectory(ctx, parent, parentPath))
	}

	if err := syncDurableDirectory(parent); err != nil {
		return errors.Join(
			fmt.Errorf("reconfirm durable archive leaf %s: %w", parentPath, err),
			closeReconfirmationDirectory(ctx, parent, parentPath),
		)
	}

	if err := closeReconfirmationDirectory(ctx, parent, parentPath); err != nil {
		return err
	}

	return verifyDurablePath(identity)
}

func reconfirmRequestedSymlinks(ctx context.Context, identity []durablePathEntry) error {
	for _, entry := range identity {
		if entry.info.Mode()&os.ModeSymlink == 0 {
			continue
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		parent, err := openDurableDirectoryPath(ctx, entry.parentPath)
		if err != nil {
			return fmt.Errorf("open requested archive symlink parent %s: %w", entry.parentPath, err)
		}

		if err := verifyDurableParentIdentity(parent, entry); err != nil {
			return errors.Join(err, closeReconfirmationDirectory(ctx, parent, entry.parentPath))
		}

		if err := runWriteLockReconfirmationHook(
			ctx,
			WriteLockReconfirmationBeforeOpen,
			entry.path,
		); err != nil {
			return errors.Join(err, closeReconfirmationDirectory(ctx, parent, entry.parentPath))
		}

		target, err := readDurableSymlinkAt(parent, filepath.Base(entry.path), entry.path)
		if err != nil {
			return errors.Join(err, closeReconfirmationDirectory(ctx, parent, entry.parentPath))
		}

		if target != entry.symlinkTarget {
			return errors.Join(
				fmt.Errorf("requested archive symlink %s changed target: %w",
					entry.path, ErrArchiveLockChanged),
				closeReconfirmationDirectory(ctx, parent, entry.parentPath),
			)
		}

		if err := runWriteLockReconfirmationHook(
			ctx,
			WriteLockReconfirmationBeforeSync,
			entry.path,
		); err != nil {
			return errors.Join(err, closeReconfirmationDirectory(ctx, parent, entry.parentPath))
		}

		if err := syncDurableDirectory(parent); err != nil {
			return fmt.Errorf("reconfirm durable archive entry %s in %s: %w",
				entry.path, entry.parentPath, errors.Join(
					err,
					closeReconfirmationDirectory(ctx, parent, entry.parentPath),
				))
		}

		confirmedNamespace, err := parent.Stat()
		if err != nil {
			return errors.Join(
				fmt.Errorf("capture confirmed requested archive namespace %s: %w",
					entry.parentPath, err),
				closeReconfirmationDirectory(ctx, parent, entry.parentPath),
			)
		}

		if err := runWriteLockReconfirmationHook(
			ctx,
			WriteLockReconfirmationAfterSync,
			entry.path,
		); err != nil {
			return errors.Join(err, closeReconfirmationDirectory(ctx, parent, entry.parentPath))
		}

		currentNamespace, err := parent.Stat()
		if err != nil {
			return errors.Join(
				fmt.Errorf("reinspect confirmed requested archive namespace %s: %w",
					entry.parentPath, err),
				closeReconfirmationDirectory(ctx, parent, entry.parentPath),
			)
		}

		if !sameDurableNamespaceState(confirmedNamespace, currentNamespace) {
			return errors.Join(
				fmt.Errorf("requested archive namespace %s changed after confirming %s: %w",
					entry.parentPath, entry.path, ErrArchiveLockChanged),
				closeReconfirmationDirectory(ctx, parent, entry.parentPath),
			)
		}

		target, err = readDurableSymlinkAt(parent, filepath.Base(entry.path), entry.path)
		if err != nil {
			return errors.Join(err, closeReconfirmationDirectory(ctx, parent, entry.parentPath))
		}

		if target != entry.symlinkTarget {
			return errors.Join(
				fmt.Errorf("requested archive symlink %s changed target: %w",
					entry.path, ErrArchiveLockChanged),
				closeReconfirmationDirectory(ctx, parent, entry.parentPath),
			)
		}

		if err := closeReconfirmationDirectory(ctx, parent, entry.parentPath); err != nil {
			return err
		}
	}

	return verifyDurablePath(identity)
}

func verifyDurableDirectoryIdentity(directory *os.File, expected durablePathEntry) error {
	current, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("inspect exact durable archive directory %s: %w", expected.path, err)
	}

	if current.Mode().Type() != expected.info.Mode().Type() || !os.SameFile(current, expected.info) {
		return fmt.Errorf("exact durable archive directory %s changed: %w",
			expected.path, ErrArchiveLockChanged)
	}

	return nil
}

func verifyDurableParentIdentity(directory *os.File, entry durablePathEntry) error {
	current, err := directory.Stat()
	if err != nil {
		return fmt.Errorf("inspect durable archive namespace %s for %s: %w",
			entry.parentPath, entry.path, err)
	}

	if current.Mode().Type() != entry.parentInfo.Mode().Type() ||
		!os.SameFile(entry.parentInfo, current) {
		return fmt.Errorf("durable archive namespace %s changed before confirming %s: %w",
			entry.parentPath, entry.path, ErrArchiveLockChanged)
	}

	return nil
}

func sameDurableNamespaceState(expected, current os.FileInfo) bool {
	if expected == nil || current == nil {
		return false
	}

	return expected.Mode() == current.Mode() &&
		expected.Size() == current.Size() &&
		expected.ModTime().Equal(current.ModTime()) &&
		os.SameFile(expected, current) &&
		reflect.DeepEqual(expected.Sys(), current.Sys())
}

func closeReconfirmationDirectory(ctx context.Context, directory *os.File, path string) error {
	hookErr := runWriteLockReconfirmationHook(ctx, WriteLockReconfirmationBeforeClose, path)
	closeErr := directory.Close()

	return errors.Join(hookErr, wrapArchiveLockCloseError(path, closeErr))
}

func verifyDurablePath(identity []durablePathEntry) error {
	for _, expected := range identity {
		current, err := os.Lstat(expected.path)
		if err != nil {
			return fmt.Errorf("inspect current durable archive path component %s: %w",
				expected.path, errors.Join(ErrArchiveLockChanged, err))
		}

		if current.Mode().Type() != expected.info.Mode().Type() || !os.SameFile(current, expected.info) {
			return fmt.Errorf("durable archive path component %s changed after confirmation: %w",
				expected.path, ErrArchiveLockChanged)
		}
	}

	return nil
}

func runWriteLockBoundaryHook(ctx context.Context, boundary WriteLockBoundary) {
	hook, _ := ctx.Value(writeLockBoundaryHookKey{}).(WriteLockBoundaryHook)
	if hook != nil {
		hook(boundary)
	}
}

func runWriteLockReconfirmationHook(
	ctx context.Context,
	phase WriteLockReconfirmationPhase,
	path string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	hook, _ := ctx.Value(writeLockReconfirmationHookKey{}).(WriteLockReconfirmationHook)
	if hook == nil {
		return nil
	}

	hookErr := hook(phase, path)

	if err := ctx.Err(); err != nil {
		return err
	}

	return hookErr
}

func resolveWriteLockDurabilityPath(root string) (string, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve archive root %s: %w", root, err)
	}

	current := absolute
	missing := make([]string, 0, 4)

	for {
		_, err := os.Lstat(current)
		if err == nil {
			canonical, evalErr := filepath.EvalSymlinks(current)
			if evalErr != nil {
				return "", fmt.Errorf("resolve archive root ancestor %s: %w", current, evalErr)
			}

			for i := len(missing) - 1; i >= 0; i-- {
				canonical = filepath.Join(canonical, missing[i])
			}

			return canonical, nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("inspect archive root ancestor %s: %w", current, err)
		}

		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("find existing archive root ancestor for %s: %w", root, os.ErrNotExist)
		}

		missing = append(missing, filepath.Base(current))
		current = parent
	}
}

func acquireSourceLock(
	ctx context.Context,
	source *RootedSource,
	exclusive bool,
	ownsSource bool,
	verifyRootIdentity func() error,
) (*Lock, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if verifyRootIdentity != nil {
		if err := verifyRootIdentity(); err != nil {
			return nil, err
		}
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

	if verifyRootIdentity != nil {
		if err := verifyRootIdentity(); err != nil {
			return nil, errors.Join(
				err,
				wrapArchiveLockCloseError("archive root lock", unlockArchiveRootLock(source.dir)),
				wrapArchiveLockCloseError("archive namespace lock", unlockArchiveAnchorLock(anchor)),
				wrapArchiveLockCloseError("archive namespace anchor", closeArchiveLockAnchor(anchor)),
			)
		}
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
		source:             source,
		anchor:             anchor,
		file:               file,
		path:               lockPath,
		verifyRootIdentity: verifyRootIdentity,
		ownsSource:         ownsSource,
		held:               true,
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

	if l.verifyRootIdentity != nil {
		if err := l.verifyRootIdentity(); err != nil {
			return fmt.Errorf("verify durable archive root identity: %w", err)
		}
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
	l.verifyRootIdentity = nil

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
