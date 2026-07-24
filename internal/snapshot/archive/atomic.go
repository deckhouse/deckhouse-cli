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

// Package archive: deterministic naming + crash-safe file I/O for the snapshot output tree.
package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ErrAtomicReplaceUnsupported reports that the platform cannot provide the
// AtomicWriter replacement contract without weakening its guarantees.
// Windows returns this error before publication when the final path exists.
var ErrAtomicReplaceUnsupported = errors.New("atomic replacement is unsupported on this platform")

// AtomicWriter writes data to "<finalPath>.tmp" and syncs it before publication.
// Unix atomically replaces the final path, then syncs its parent directory.
// Windows durably creates a previously absent final with a write-through move,
// but fails with ErrAtomicReplaceUnsupported before replacing an existing final:
// documented Windows APIs do not combine atomic replacement with write-through
// namespace durability on every supported filesystem.
// Call Abort to remove the temporary file when an error occurs.
type AtomicWriter struct {
	finalPath string
	tmpPath   string
	f         *os.File
	ops       atomicCommitOps
	rooted    *RootedDestination
	finalRel  string
	tmpRel    string
}

type atomicCommitOps struct {
	syncTemp  func(*os.File) error
	closeTemp func(*os.File) error
	rename    func(string, string) error
	syncDir   func(string) error
}

// MutationPhase identifies a rooted destination operation boundary. Hooks run
// before namespace verification and before the operation can mutate the pinned
// tree.
type MutationPhase string

const (
	MutationCreate  MutationPhase = "create"
	MutationMkdir   MutationPhase = "mkdir"
	MutationRemove  MutationPhase = "remove"
	MutationRename  MutationPhase = "rename"
	MutationSync    MutationPhase = "sync"
	MutationStat    MutationPhase = "stat"
	MutationOpen    MutationPhase = "open"
	MutationReadDir MutationPhase = "read_dir"
)

// MutationBoundaryHook makes rooted mutation races deterministic in tests.
type MutationBoundaryHook func(phase MutationPhase, path string)

// RootedDestination confines every operation to the exact archive root held by
// a write lock. The os.Root and RootedSource handles are independently opened
// and identity-matched once. Descendant walks retain at most two rolling
// source/mutation pairs plus short-lived identity probes, independent of path
// depth; rename may hold two completed parent pairs. Rooted paths are limited
// to 16 KiB and 4096 components, leaving room for node and staging prefixes
// around the exporter's 4096-byte relative-path contract.
// Each operation remains descriptor-relative and revalidates the lock binding
// before it can mutate.
type RootedDestination struct {
	source             *RootedSource
	root               *os.Root
	path               string
	hook               MutationBoundaryHook
	directorySyncHook  DirectorySyncHook
	descriptorObserver func(int)
	ownsSource         bool

	mu               sync.RWMutex
	traversalContext context.Context
	lost             error
	onLoss           func(error)
	closed           bool
}

type rootedDestinationDir struct {
	destination *RootedDestination
	source      *PinnedDirectory
	root        *os.Root
	path        string
}

const (
	maxRootedPathBytes      = 16 << 10
	maxRootedPathComponents = 4096
	rootedCleanupBatchSize  = 128
)

// NewLockedRootedDestination binds a mutation view to lock's exact pinned root.
func NewLockedRootedDestination(lock *Lock, hook MutationBoundaryHook) (*RootedDestination, error) {
	if lock == nil {
		return nil, errors.New("open rooted destination: lock is nil")
	}

	lock.mu.Lock()
	source := lock.source
	held := lock.held
	lock.mu.Unlock()

	if !held || source == nil {
		return nil, errors.New("open rooted destination: lock is not held")
	}

	if err := lock.Verify(); err != nil {
		return nil, fmt.Errorf("verify rooted destination lock: %w", err)
	}

	return openRootedDestination(source, false, hook)
}

// OpenRootedDestination opens an unbound rooted mutation view. Production
// downloads use NewLockedRootedDestination; this entry point keeps archive and
// volume unit tests independent of command lock setup.
func OpenRootedDestination(path string, hook MutationBoundaryHook) (*RootedDestination, error) {
	source, err := OpenRootedSource(path)
	if err != nil {
		return nil, err
	}

	destination, err := openRootedDestination(source, true, hook)
	if err != nil {
		_ = source.Close()

		return nil, err
	}

	return destination, nil
}

func openRootedDestination(
	source *RootedSource,
	ownsSource bool,
	hook MutationBoundaryHook,
) (*RootedDestination, error) {
	root, err := os.OpenRoot(source.path)
	if err != nil {
		return nil, fmt.Errorf("open rooted mutation view %s: %w", source.path, err)
	}

	destination := &RootedDestination{
		source:           source,
		root:             root,
		path:             source.path,
		hook:             hook,
		ownsSource:       ownsSource,
		traversalContext: context.Background(),
	}

	if err := destination.verifyIdentity(); err != nil {
		_ = root.Close()

		return nil, err
	}

	if err := source.verifyCurrent(); err != nil {
		_ = root.Close()

		return nil, fmt.Errorf("verify rooted mutation view %s: %w", source.path, err)
	}

	return destination, nil
}

// Path returns the diagnostic absolute path represented by destination.
func (d *RootedDestination) Path() string {
	if d == nil {
		return ""
	}

	return d.path
}

// SetBindingLossHandler installs the cancellation callback used by the
// pipeline. The first namespace-binding failure wins.
func (d *RootedDestination) SetBindingLossHandler(handler func(error)) {
	d.mu.Lock()
	d.onLoss = handler
	lost := d.lost
	d.mu.Unlock()

	if lost != nil && handler != nil {
		handler(lost)
	}
}

// SetTraversalContext installs the cancellation context checked by bounded
// rooted path walks and recursive cleanup. A destination is bound to one
// pipeline run at a time; callers reset the context after that run finishes.
func (d *RootedDestination) SetTraversalContext(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	d.mu.Lock()
	d.traversalContext = ctx
	d.mu.Unlock()
}

// SetDirectorySyncHook installs a destination-scoped wrapper around rooted
// directory confirmations. Tests use it to inject operation failures without
// changing concurrent destinations.
func (d *RootedDestination) SetDirectorySyncHook(hook DirectorySyncHook) {
	d.mu.Lock()
	d.directorySyncHook = hook
	d.mu.Unlock()
}

func (d *RootedDestination) setDescriptorObserver(observer func(int)) {
	d.mu.Lock()
	d.descriptorObserver = observer
	d.mu.Unlock()
}

func (d *RootedDestination) observeDescriptors(delta int) {
	d.mu.RLock()
	observer := d.descriptorObserver
	d.mu.RUnlock()

	if observer != nil {
		observer(delta)
	}
}

// BindingError returns the first observed destination binding failure.
func (d *RootedDestination) BindingError() error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.lost
}

// Verify checks both the lock namespace and the independently pinned mutation root.
func (d *RootedDestination) Verify() error {
	if d == nil {
		return errors.New("verify rooted destination: destination is nil")
	}

	d.mu.RLock()
	closed := d.closed
	lost := d.lost
	d.mu.RUnlock()

	if closed {
		return errors.New("verify rooted destination: destination is closed")
	}

	if lost != nil {
		return lost
	}

	if err := d.source.verifyCurrent(); err != nil {
		return d.recordBindingLoss(fmt.Errorf("verify rooted destination %s: %w", d.path, err))
	}

	if err := d.verifyIdentity(); err != nil {
		return d.recordBindingLoss(err)
	}

	return nil
}

func (d *RootedDestination) verifyIdentity() error {
	rootFile, err := d.root.Open(".")
	if err != nil {
		return fmt.Errorf("open rooted destination identity %s: %w", d.path, err)
	}

	expected, expectedErr := d.source.dir.Stat()
	actual, actualErr := rootFile.Stat()

	closeErr := rootFile.Close()
	if expectedErr != nil || actualErr != nil || closeErr != nil {
		return errors.Join(
			wrapRootedDestinationError("inspect locked root", d.path, expectedErr),
			wrapRootedDestinationError("inspect mutation root", d.path, actualErr),
			wrapRootedDestinationError("close mutation root", d.path, closeErr),
		)
	}

	if !os.SameFile(expected, actual) {
		return fmt.Errorf("%s no longer identifies the locked destination: %w",
			d.path, ErrNonRegularArchiveArtifact)
	}

	return nil
}

func wrapRootedDestinationError(operation, path string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%s %s: %w", operation, path, err)
}

func (d *RootedDestination) recordBindingLoss(err error) error {
	if !errors.Is(err, ErrNonRegularArchiveArtifact) {
		err = errors.Join(err, ErrNonRegularArchiveArtifact)
	}

	d.mu.Lock()
	if d.lost == nil {
		d.lost = err
	}

	lost := d.lost
	handler := d.onLoss
	d.mu.Unlock()

	if handler != nil {
		handler(lost)
	}

	return lost
}

func (d *RootedDestination) before(phase MutationPhase, path string) error {
	if err := d.traversalErr(); err != nil {
		return err
	}

	if d.hook != nil {
		d.hook(phase, path)
	}

	return d.Verify()
}

func (d *RootedDestination) traversalErr() error {
	d.mu.RLock()
	ctx := d.traversalContext
	d.mu.RUnlock()

	if ctx == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Relative converts an absolute destination path to a safe root-relative path.
func (d *RootedDestination) Relative(path string) (string, error) {
	relative, err := filepath.Rel(d.path, path)
	if err != nil {
		return "", fmt.Errorf("resolve %s beneath rooted destination %s: %w", path, d.path, err)
	}

	relative = filepath.Clean(relative)
	if !filepath.IsLocal(relative) {
		return "", fmt.Errorf("%s escapes rooted destination %s: %w",
			path, d.path, ErrNonRegularArchiveArtifact)
	}

	return relative, nil
}

func (d *RootedDestination) cleanRelative(path string) (string, error) {
	var (
		clean string
		err   error
	)

	if filepath.IsAbs(path) {
		clean, err = d.Relative(path)
		if err != nil {
			return "", err
		}
	} else {
		clean = filepath.Clean(filepath.FromSlash(path))
	}

	if !filepath.IsLocal(clean) {
		return "", fmt.Errorf("invalid rooted destination path %q: %w",
			path, ErrNonRegularArchiveArtifact)
	}

	if len(clean) > maxRootedPathBytes {
		return "", fmt.Errorf(
			"rooted destination path %q exceeds %d bytes: %w",
			path,
			maxRootedPathBytes,
			ErrNonRegularArchiveArtifact,
		)
	}

	if clean != "." &&
		len(strings.Split(clean, string(filepath.Separator))) > maxRootedPathComponents {
		return "", fmt.Errorf(
			"rooted destination path %q exceeds %d components: %w",
			path,
			maxRootedPathComponents,
			ErrNonRegularArchiveArtifact,
		)
	}

	return clean, nil
}

func (d *RootedDestination) openDirectory(
	path string,
	create bool,
	confirmContaining bool,
) (*rootedDestinationDir, error) {
	relative, err := d.cleanRelative(path)
	if err != nil {
		return nil, err
	}

	components := []string{"."}
	if relative != "." {
		components = strings.Split(relative, string(filepath.Separator))
	}

	var (
		sourceParent = d.source.dir
		rootParent   = d.root
		currentPath  = d.path
		sourceOwned  *os.File
		rootOwned    *os.Root
	)

	closeOwned := func() {
		if rootOwned != nil {
			_ = rootOwned.Close()

			d.observeDescriptors(-1)
		}

		if sourceOwned != nil {
			_ = sourceOwned.Close()

			d.observeDescriptors(-1)
		}
	}

	for _, component := range components {
		if err := d.traversalErr(); err != nil {
			closeOwned()

			return nil, err
		}

		if component != "." {
			if err := validateArchiveComponent(component); err != nil {
				closeOwned()

				return nil, err
			}

			currentPath = filepath.Join(currentPath, component)
		}

		if err := d.before(MutationOpen, currentPath); err != nil {
			closeOwned()

			return nil, err
		}

		d.source.runHook(currentPath)

		childSource, sourceErr := openArchiveDirectoryAt(sourceParent, component, currentPath)
		if sourceErr == nil {
			d.observeDescriptors(1)
		}

		if sourceErr != nil && create && errors.Is(sourceErr, os.ErrNotExist) {
			if err := d.before(MutationMkdir, currentPath); err != nil {
				closeOwned()

				return nil, err
			}

			if mkdirErr := rootParent.Mkdir(component, 0o755); mkdirErr != nil &&
				!errors.Is(mkdirErr, os.ErrExist) {
				closeOwned()

				return nil, fmt.Errorf("create rooted directory %s: %w", currentPath, mkdirErr)
			}

			childSource, sourceErr = openArchiveDirectoryAt(sourceParent, component, currentPath)
			if sourceErr == nil {
				d.observeDescriptors(1)
			}
		}

		if sourceErr != nil {
			closeOwned()

			return nil, sourceErr
		}

		childRoot, rootErr := rootParent.OpenRoot(component)
		if rootErr != nil {
			_ = childSource.Close()

			d.observeDescriptors(-1)

			closeOwned()

			return nil, fmt.Errorf("open rooted mutation directory %s: %w", currentPath, rootErr)
		}

		d.observeDescriptors(1)

		child := &rootedDestinationDir{
			destination: d,
			source: &PinnedDirectory{
				dir:           childSource,
				path:          currentPath,
				hook:          d.source.hook,
				verifyBinding: d.source.verifyCurrent,
			},
			root: childRoot,
			path: currentPath,
		}

		if err := child.verifyIdentity(); err != nil {
			child.close()
			closeOwned()

			return nil, d.recordBindingLoss(err)
		}

		if confirmContaining && component != "." {
			containingPath := filepath.Dir(currentPath)
			if err := d.confirmDirectory(containingPath, rootParent); err != nil {
				child.close()
				closeOwned()

				return nil, err
			}
		}

		closeOwned()

		sourceOwned = childSource
		rootOwned = childRoot
		sourceParent = childSource
		rootParent = childRoot
	}

	return &rootedDestinationDir{
		destination: d,
		source: &PinnedDirectory{
			dir:           sourceOwned,
			path:          currentPath,
			hook:          d.source.hook,
			verifyBinding: d.source.verifyCurrent,
		},
		root: rootOwned,
		path: currentPath,
	}, nil
}

func (d *rootedDestinationDir) verifyIdentity() error {
	file, err := d.root.Open(".")
	if err != nil {
		return fmt.Errorf("open rooted directory identity %s: %w", d.path, err)
	}

	d.destination.observeDescriptors(1)

	expected, expectedErr := d.source.dir.Stat()
	actual, actualErr := file.Stat()

	closeErr := file.Close()

	d.destination.observeDescriptors(-1)

	if expectedErr != nil || actualErr != nil || closeErr != nil {
		return errors.Join(
			wrapRootedDestinationError("inspect source directory", d.path, expectedErr),
			wrapRootedDestinationError("inspect mutation directory", d.path, actualErr),
			wrapRootedDestinationError("close mutation directory", d.path, closeErr),
		)
	}

	if !os.SameFile(expected, actual) {
		return fmt.Errorf("%s changed while opening rooted mutation directory: %w",
			d.path, ErrNonRegularArchiveArtifact)
	}

	return nil
}

func (d *rootedDestinationDir) close() {
	_ = d.root.Close()
	d.destination.observeDescriptors(-1)
	_ = d.source.Close()
	d.destination.observeDescriptors(-1)
}

func (d *RootedDestination) confirmDirectory(path string, root *os.Root) error {
	if err := d.before(MutationSync, path); err != nil {
		return err
	}

	d.mu.RLock()
	hook := d.directorySyncHook
	d.mu.RUnlock()

	confirm := func() error {
		return syncRootedDirectory(root)
	}

	var err error
	if hook == nil {
		err = confirm()
	} else {
		err = hook(path, confirm)
	}

	if err != nil {
		return fmt.Errorf("sync rooted directory %s: %w", path, err)
	}

	return nil
}

// EnsureDir creates path descriptor-relatively and confirms the leaf plus every
// containing directory back to the pinned root. The rolling traversal retains
// one source/mutation pair regardless of depth. Each containing directory is
// leaf is confirmed first, then each containing directory is confirmed in one
// rolling root-to-leaf pass. Every call repeats the complete chain because a
// pre-existing entry may be residue from an earlier failed confirmation.
func (d *RootedDestination) EnsureDir(path string) error {
	directory, err := d.openDirectory(path, true, false)
	if err != nil {
		return err
	}

	if err := d.confirmDirectory(directory.path, directory.root); err != nil {
		directory.close()

		return err
	}

	directory.close()

	confirmed, err := d.openDirectory(path, false, true)
	if err != nil {
		return err
	}

	confirmed.close()

	return nil
}

// OpenRegular opens a no-follow regular file beneath the locked root.
func (d *RootedDestination) OpenRegular(path string) (*os.File, error) {
	relative, err := d.cleanRelative(path)
	if err != nil {
		return nil, err
	}

	if relative == "." {
		return nil, fmt.Errorf("open rooted regular file %s: %w", path, ErrNonRegularArchiveArtifact)
	}

	if err := d.before(MutationOpen, filepath.Join(d.path, relative)); err != nil {
		return nil, err
	}

	parent, err := d.openDirectory(filepath.Dir(relative), false, false)
	if err != nil {
		return nil, err
	}
	defer parent.close()

	return parent.source.OpenRegularFile(filepath.Base(relative))
}

// ReadFile reads a no-follow regular file beneath the locked root.
func (d *RootedDestination) ReadFile(path string) ([]byte, error) {
	file, err := d.OpenRegular(path)
	if err != nil {
		return nil, err
	}

	data, readErr := io.ReadAll(file)

	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}

	return data, nil
}

// Stat inspects a no-follow entry beneath the locked root.
func (d *RootedDestination) Stat(path string) (os.FileInfo, error) {
	relative, err := d.cleanRelative(path)
	if err != nil {
		return nil, err
	}

	if err := d.before(MutationStat, filepath.Join(d.path, relative)); err != nil {
		return nil, err
	}

	if relative == "." {
		return d.source.dir.Stat()
	}

	parent, err := d.openDirectory(filepath.Dir(relative), false, false)
	if err != nil {
		return nil, err
	}
	defer parent.close()

	name := filepath.Base(relative)

	info, err := parent.root.Lstat(name)
	if err != nil {
		return nil, err
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s resolves through a link: %w",
			filepath.Join(d.path, relative), ErrNonRegularArchiveArtifact)
	}

	if info.IsDir() {
		child, openErr := parent.source.OpenDirectory(name)
		if openErr != nil {
			return nil, openErr
		}

		defer func() { _ = child.Close() }()

		sourceInfo, statErr := child.dir.Stat()
		if statErr != nil {
			return nil, statErr
		}

		if !os.SameFile(info, sourceInfo) {
			return nil, d.recordBindingLoss(fmt.Errorf(
				"%s changed while inspecting rooted directory: %w",
				filepath.Join(d.path, relative),
				ErrNonRegularArchiveArtifact,
			))
		}

		return sourceInfo, nil
	}

	file, openErr := parent.source.OpenRegularFile(name)
	if openErr != nil {
		return nil, openErr
	}

	defer func() { _ = file.Close() }()

	sourceInfo, statErr := file.Stat()
	if statErr != nil {
		return nil, statErr
	}

	if !os.SameFile(info, sourceInfo) {
		return nil, d.recordBindingLoss(fmt.Errorf(
			"%s changed while inspecting rooted regular file: %w",
			filepath.Join(d.path, relative),
			ErrNonRegularArchiveArtifact,
		))
	}

	return sourceInfo, nil
}

// ReadDir reads one directory beneath the locked root.
func (d *RootedDestination) ReadDir(path string) ([]os.DirEntry, error) {
	if err := d.before(MutationReadDir, path); err != nil {
		return nil, err
	}

	directory, err := d.openDirectory(path, false, false)
	if err != nil {
		return nil, err
	}
	defer directory.close()

	return directory.source.ReadDirectory(-1)
}

// OpenPinnedDirectory opens one read-only directory through the locked source.
func (d *RootedDestination) OpenPinnedDirectory(path string) (*PinnedDirectory, error) {
	relative, err := d.cleanRelative(path)
	if err != nil {
		return nil, err
	}

	if err := d.before(MutationOpen, filepath.Join(d.path, relative)); err != nil {
		return nil, err
	}

	return d.source.OpenDirectoryPath(relative)
}

// CreateExclusive creates a new regular file without following any component.
func (d *RootedDestination) CreateExclusive(path string, perm os.FileMode) (*os.File, error) {
	relative, err := d.cleanRelative(path)
	if err != nil {
		return nil, err
	}

	parent, err := d.openDirectory(filepath.Dir(relative), false, false)
	if err != nil {
		return nil, err
	}
	defer parent.close()

	absolute := filepath.Join(d.path, relative)
	if err := d.before(MutationCreate, absolute); err != nil {
		return nil, err
	}

	file, err := parent.root.OpenFile(
		filepath.Base(relative),
		os.O_CREATE|os.O_EXCL|os.O_RDWR,
		perm,
	)
	if err != nil {
		return nil, fmt.Errorf("create rooted file %s: %w", absolute, err)
	}

	return file, nil
}

// OpenRegularFile opens an existing regular file for descriptor-bound reads or
// writes. Truncation at open time is rejected because identity must be compared
// before the first mutation; callers may call Truncate on the returned handle.
func (d *RootedDestination) OpenRegularFile(
	path string,
	flag int,
	perm os.FileMode,
) (*os.File, error) {
	if flag&os.O_TRUNC != 0 {
		return nil, fmt.Errorf("rooted open with O_TRUNC is unsafe before identity comparison: %w",
			ErrNonRegularArchiveArtifact)
	}

	relative, err := d.cleanRelative(path)
	if err != nil {
		return nil, err
	}

	parent, err := d.openDirectory(filepath.Dir(relative), false, false)
	if err != nil {
		return nil, err
	}
	defer parent.close()

	expected, err := parent.source.OpenRegularFile(filepath.Base(relative))
	if errors.Is(err, os.ErrNotExist) && flag&os.O_CREATE != 0 {
		return d.CreateExclusive(relative, perm)
	}

	if err != nil {
		return nil, err
	}

	defer func() { _ = expected.Close() }()

	absolute := filepath.Join(d.path, relative)
	if err := d.before(MutationOpen, absolute); err != nil {
		return nil, err
	}

	actual, err := parent.root.OpenFile(filepath.Base(relative), flag&^os.O_CREATE, perm)
	if err != nil {
		return nil, fmt.Errorf("open rooted regular file %s: %w", absolute, err)
	}

	expectedInfo, expectedErr := expected.Stat()

	actualInfo, actualErr := actual.Stat()
	if expectedErr != nil || actualErr != nil {
		_ = actual.Close()

		return nil, errors.Join(expectedErr, actualErr)
	}

	if !os.SameFile(expectedInfo, actualInfo) {
		_ = actual.Close()

		return nil, d.recordBindingLoss(fmt.Errorf(
			"%s changed while opening rooted regular file: %w",
			absolute,
			ErrNonRegularArchiveArtifact,
		))
	}

	return actual, nil
}

// Remove removes one entry without following links.
func (d *RootedDestination) Remove(path string) error {
	relative, err := d.cleanRelative(path)
	if err != nil {
		return err
	}

	if relative == "." {
		return fmt.Errorf("refuse to remove rooted destination itself: %w", ErrNonRegularArchiveArtifact)
	}

	parent, err := d.openDirectory(filepath.Dir(relative), false, false)
	if err != nil {
		return err
	}

	defer parent.close()

	absolute := filepath.Join(d.path, relative)
	if err := d.before(MutationRemove, absolute); err != nil {
		return err
	}

	if err := parent.root.Remove(filepath.Base(relative)); err != nil {
		return fmt.Errorf("remove rooted entry %s: %w", absolute, err)
	}

	return nil
}

// RemoveAll recursively removes one rooted subtree without crossing a mount,
// following a link, or reopening any absolute pathname.
func (d *RootedDestination) RemoveAll(path string) error {
	relative, err := d.cleanRelative(path)
	if err != nil {
		return err
	}

	if relative == "." {
		return fmt.Errorf("refuse to remove rooted destination itself: %w", ErrNonRegularArchiveArtifact)
	}

	return d.removeAllEntry(filepath.Dir(relative), filepath.Base(relative))
}

func (d *RootedDestination) removeAllEntry(parentPath, name string) error {
	if err := d.traversalErr(); err != nil {
		return err
	}

	parent, err := d.openDirectory(parentPath, false, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		return err
	}

	absolute := filepath.Join(parent.path, name)

	info, statErr := parent.root.Lstat(name)
	parent.close()

	if errors.Is(statErr, os.ErrNotExist) {
		return nil
	}

	if statErr != nil {
		return fmt.Errorf("inspect rooted cleanup entry %s: %w", absolute, statErr)
	}

	childPath := filepath.Join(parentPath, name)

	if info.IsDir() {
		for {
			child, openErr := d.openDirectory(childPath, false, false)
			if errors.Is(openErr, os.ErrNotExist) {
				return nil
			}

			if openErr != nil {
				return openErr
			}

			entries, readErr := child.source.ReadDirectory(rootedCleanupBatchSize)
			child.close()

			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return readErr
			}

			if len(entries) == 0 {
				break
			}

			for _, entry := range entries {
				if err := d.removeAllEntry(childPath, entry.Name()); err != nil {
					return err
				}
			}
		}
	}

	parent, err = d.openDirectory(parentPath, false, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		return err
	}

	defer parent.close()

	if err := d.before(MutationRemove, absolute); err != nil {
		return err
	}

	if err := parent.root.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove rooted cleanup entry %s: %w", absolute, err)
	}

	return nil
}

// Rename atomically renames two entries beneath the same locked root.
func (d *RootedDestination) Rename(oldPath, newPath string) error {
	oldRelative, err := d.cleanRelative(oldPath)
	if err != nil {
		return err
	}

	newRelative, err := d.cleanRelative(newPath)
	if err != nil {
		return err
	}

	oldParent, err := d.openDirectory(filepath.Dir(oldRelative), false, false)
	if err != nil {
		return err
	}
	defer oldParent.close()

	newParent, err := d.openDirectory(filepath.Dir(newRelative), false, false)
	if err != nil {
		return err
	}
	defer newParent.close()

	absolute := filepath.Join(d.path, newRelative)
	if err := d.before(MutationRename, absolute); err != nil {
		return err
	}

	if err := renameRootedDurably(
		oldParent.root,
		filepath.Base(oldRelative),
		newParent.root,
		filepath.Base(newRelative),
	); err != nil {
		return err
	}

	return nil
}

// SyncParent confirms the containing directory of path through its pinned handle.
func (d *RootedDestination) SyncParent(path string) error {
	relative, err := d.cleanRelative(path)
	if err != nil {
		return err
	}

	parent, err := d.openDirectory(filepath.Dir(relative), false, false)
	if err != nil {
		return err
	}
	defer parent.close()

	if err := d.before(MutationSync, parent.path); err != nil {
		return err
	}

	return syncRootedDirectory(parent.root)
}

// ComputeNodeChecksum hashes one node through the locked rooted view.
func (d *RootedDestination) ComputeNodeChecksum(nodeDir string) (NodeChecksum, error) {
	directory, err := d.openDirectory(nodeDir, false, false)
	if err != nil {
		return NodeChecksum{}, err
	}
	defer directory.close()

	return computeNodeChecksum(directory.source)
}

// ReadSnapshotYAML reads snapshot.yaml for one node through the locked view.
func (d *RootedDestination) ReadSnapshotYAML(nodeDir string) (SnapshotYAML, error) {
	directory, err := d.openDirectory(nodeDir, false, false)
	if err != nil {
		return SnapshotYAML{}, err
	}
	defer directory.close()

	return readSnapshotYAML(directory.source)
}

// FindBlockData classifies one node's block payload through the locked view.
func (d *RootedDestination) FindBlockData(nodeDir string) (BlockPayload, bool, error) {
	directory, err := d.openDirectory(nodeDir, false, false)
	if err != nil {
		return BlockPayload{}, false, err
	}
	defer directory.close()

	return ClassifyBlockPayloadIn(directory.source)
}

// ReadChunkMeta reads bounded chunk geometry through the locked view.
func (d *RootedDestination) ReadChunkMeta(
	ctx context.Context,
	dir string,
) (ChunkMeta, bool, error) {
	path := filepath.Join(dir, ChunkMetaFileName)

	file, err := d.OpenRegular(path)
	if errors.Is(err, os.ErrNotExist) {
		return ChunkMeta{}, false, nil
	}

	if err != nil {
		return ChunkMeta{}, false, err
	}

	meta, found, readErr := ReadChunkMetaFrom(ctx, file, path)
	closeErr := file.Close()

	return meta, found, errors.Join(readErr, closeErr)
}

// WriteChunkMeta writes chunk geometry atomically through the locked view.
func (d *RootedDestination) WriteChunkMeta(
	ctx context.Context,
	dir string,
	meta ChunkMeta,
) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal chunk metadata: %w", err)
	}

	return WriteFileAtomicRooted(
		ctx,
		d,
		filepath.Join(dir, ChunkMetaFileName),
		strings.NewReader(string(data)),
	)
}

// VerifyNode verifies one node through the locked rooted view.
func (d *RootedDestination) VerifyNode(nodeDir string) error {
	directory, err := d.openDirectory(nodeDir, false, false)
	if err != nil {
		return err
	}
	defer directory.close()

	snapshot, err := readSnapshotYAML(directory.source)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%s: %w", nodeDir, ErrSnapshotYAMLMissing)
		}

		return err
	}

	checksum, err := computeNodeChecksum(directory.source)
	if err != nil {
		return err
	}

	if checksum.Hex != snapshot.Checksum.Hex {
		return fmt.Errorf("node %s: stored %q computed %q: %w",
			nodeDir, snapshot.Checksum.Hex, checksum.Hex, ErrChecksumMismatch)
	}

	return nil
}

// Close releases the mutation root. A lock-owned RootedSource remains owned by
// its Lock and is closed only after this destination has been closed.
func (d *RootedDestination) Close() error {
	if d == nil {
		return nil
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()

		return nil
	}

	d.closed = true
	d.mu.Unlock()

	rootErr := d.root.Close()

	var sourceErr error
	if d.ownsSource {
		sourceErr = d.source.Close()
	}

	return errors.Join(rootErr, sourceErr)
}

// PublicationState describes whether an AtomicWriter commit error happened
// before or after the final path became visible.
type PublicationState uint8

const (
	// PublicationUnpublished means rename did not complete.
	PublicationUnpublished PublicationState = iota
	// PublicationPublished means rename completed but parent-directory
	// durability is not yet confirmed.
	PublicationPublished
)

// CommitError preserves the commit operation's original cause and records
// whether the final path was published before that operation failed.
type CommitError struct {
	state PublicationState
	err   error
}

// Error implements error.
func (e *CommitError) Error() string {
	return e.err.Error()
}

// Unwrap exposes the original commit failure for errors.Is/errors.As.
func (e *CommitError) Unwrap() error {
	return e.err
}

// PublicationState returns the final-path state at the failure boundary.
func (e *CommitError) PublicationState() PublicationState {
	return e.state
}

// CommitPublicationState returns the publication state carried by err. Errors
// that did not originate from an AtomicWriter commit are treated as
// PublicationUnpublished.
func CommitPublicationState(err error) PublicationState {
	var commitErr *CommitError
	if errors.As(err, &commitErr) {
		return commitErr.PublicationState()
	}

	return PublicationUnpublished
}

type directorySyncHookKey struct{}

// DirectorySyncHook wraps a platform durability confirmation. Calling next
// performs the real confirmation: a parent-directory sync on Unix and the
// post-write-through no-op on Windows. The hook is scoped to a context so
// deterministic operation injection does not affect concurrent writers.
type DirectorySyncHook func(path string, next func() error) error

// WithDirectorySyncHook returns a context that applies hook to
// AtomicWriter.CommitContext and ConfirmFileDurability confirmations.
func WithDirectorySyncHook(ctx context.Context, hook DirectorySyncHook) context.Context {
	if hook == nil {
		return ctx
	}

	return context.WithValue(ctx, directorySyncHookKey{}, hook)
}

// NewAtomicWriter creates (or truncates) "<path>.tmp" and returns a writer
// ready to receive data. The caller must call either Commit or Abort.
func NewAtomicWriter(path string) (*AtomicWriter, error) {
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", tmpPath, err)
	}

	return &AtomicWriter{
		finalPath: path,
		tmpPath:   tmpPath,
		f:         f,
		ops: atomicCommitOps{
			syncTemp:  (*os.File).Sync,
			closeTemp: (*os.File).Close,
			rename:    renameDurably,
			syncDir:   syncDir,
		},
	}, nil
}

// NewRootedAtomicWriter creates an AtomicWriter beneath destination.
func NewRootedAtomicWriter(destination *RootedDestination, path string) (*AtomicWriter, error) {
	if destination == nil {
		return nil, errors.New("open rooted atomic writer: destination is nil")
	}

	finalRel, err := destination.cleanRelative(path)
	if err != nil {
		return nil, err
	}

	tmpRel := finalRel + ".tmp"
	if err := destination.Remove(tmpRel); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale rooted atomic temporary %s: %w",
			filepath.Join(destination.path, tmpRel), err)
	}

	file, err := destination.CreateExclusive(tmpRel, 0o666)
	if err != nil {
		return nil, err
	}

	return &AtomicWriter{
		finalPath: filepath.Join(destination.path, finalRel),
		tmpPath:   filepath.Join(destination.path, tmpRel),
		f:         file,
		rooted:    destination,
		finalRel:  finalRel,
		tmpRel:    tmpRel,
		ops: atomicCommitOps{
			syncTemp:  (*os.File).Sync,
			closeTemp: (*os.File).Close,
		},
	}, nil
}

// Write implements io.Writer.
func (w *AtomicWriter) Write(p []byte) (int, error) {
	return w.f.Write(p)
}

// OpenTempReader opens the unpublished temporary file for validation. The
// caller must close the returned reader before calling Commit or Abort.
func (w *AtomicWriter) OpenTempReader() (io.ReadCloser, error) {
	if w.rooted != nil {
		f, err := w.rooted.OpenRegular(w.tmpRel)
		if err != nil {
			return nil, fmt.Errorf("opening %s for validation: %w", w.tmpPath, err)
		}

		return f, nil
	}

	f, err := os.Open(w.tmpPath)
	if err != nil {
		return nil, fmt.Errorf("opening %s for validation: %w", w.tmpPath, err)
	}

	return f, nil
}

// Commit is CommitContext with a non-cancellable context.
// After Commit the AtomicWriter must not be used again.
func (w *AtomicWriter) Commit() error {
	return w.CommitContext(context.Background())
}

// CommitContext syncs and closes the temporary file, checks cancellation, and
// publishes it according to the platform contract documented on AtomicWriter.
//
// Publication begins at the cancellation checkpoint immediately before Rename.
// Cancellation observed before that point removes the temporary file and leaves
// the final path unchanged. Once the checkpoint succeeds, cancellation no
// longer changes the result: publication and its platform-specific durability
// confirmation determine the return value, so CommitContext never reports
// pre-publication cancellation after publishing.
func (w *AtomicWriter) CommitContext(ctx context.Context) error {
	if err := w.ops.syncTemp(w.f); err != nil {
		w.Abort()

		return newCommitError(PublicationUnpublished, fmt.Errorf("syncing %s: %w", w.tmpPath, err))
	}

	if err := w.ops.closeTemp(w.f); err != nil {
		w.Abort()

		return newCommitError(PublicationUnpublished, fmt.Errorf("closing %s: %w", w.tmpPath, err))
	}

	if err := ctx.Err(); err != nil {
		w.Abort()

		return newCommitError(
			PublicationUnpublished,
			fmt.Errorf("committing %s cancelled before publication: %w", w.finalPath, err),
		)
	}

	if err := w.rename(); err != nil {
		w.Abort()

		return newCommitError(
			PublicationUnpublished,
			fmt.Errorf("renaming %s to %s: %w", w.tmpPath, w.finalPath, err),
		)
	}

	if err := w.syncParent(ctx); err != nil {
		return newCommitError(
			PublicationPublished,
			fmt.Errorf("syncing parent directory for %s: %w", w.finalPath, err),
		)
	}

	return nil
}

func (w *AtomicWriter) rename() error {
	if w.rooted != nil {
		return w.rooted.Rename(w.tmpRel, w.finalRel)
	}

	return w.ops.rename(w.tmpPath, w.finalPath)
}

func (w *AtomicWriter) syncParent(ctx context.Context) error {
	if w.rooted != nil {
		return runDirectorySync(ctx, filepath.Dir(w.finalPath), func(string) error {
			return w.rooted.SyncParent(w.finalRel)
		})
	}

	return runDirectorySync(ctx, filepath.Dir(w.finalPath), w.ops.syncDir)
}

func newCommitError(state PublicationState, err error) error {
	return &CommitError{state: state, err: err}
}

func runDirectorySync(ctx context.Context, path string, syncFn func(string) error) error {
	hook, _ := ctx.Value(directorySyncHookKey{}).(DirectorySyncHook)
	if hook == nil {
		return syncFn(path)
	}

	return hook(path, func() error {
		return syncFn(path)
	})
}

// ConfirmFileDurability applies the platform durability confirmation before an
// already published final file is trusted. Unix syncs the parent directory.
// A successful Windows AtomicWriter create publication is write-through, so no
// separate supported directory operation exists or is required. Cancellation
// observed before confirmation prevents it from starting; once it starts, its
// result wins.
func ConfirmFileDurability(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return newCommitError(
			PublicationPublished,
			fmt.Errorf("confirming durability for %s cancelled before sync: %w", path, err),
		)
	}

	if err := runDirectorySync(ctx, filepath.Dir(path), syncDir); err != nil {
		return newCommitError(
			PublicationPublished,
			fmt.Errorf("syncing parent directory for published file %s: %w", path, err),
		)
	}

	return nil
}

// ConfirmRootedFileDurability confirms a published file through destination.
func ConfirmRootedFileDurability(ctx context.Context, destination *RootedDestination, path string) error {
	if err := ctx.Err(); err != nil {
		return newCommitError(
			PublicationPublished,
			fmt.Errorf("confirming durability for %s cancelled before sync: %w", path, err),
		)
	}

	if err := runDirectorySync(ctx, filepath.Dir(path), func(string) error {
		return destination.SyncParent(path)
	}); err != nil {
		return newCommitError(
			PublicationPublished,
			fmt.Errorf("syncing rooted parent directory for published file %s: %w", path, err),
		)
	}

	return nil
}

// Abort closes and removes the temporary file. Safe to call even if Write
// returned an error. Errors from close/remove are intentionally suppressed
// because the caller's original error takes precedence.
func (w *AtomicWriter) Abort() {
	_ = w.f.Close()
	if w.rooted != nil {
		_ = w.rooted.Remove(w.tmpRel)

		return
	}

	_ = os.Remove(w.tmpPath)
}

// WriteFileAtomic is WriteFileAtomicContext with a non-cancellable context.
func WriteFileAtomic(path string, r io.Reader) error {
	return WriteFileAtomicContext(context.Background(), path, r)
}

// WriteFileAtomicContext copies r into path using an AtomicWriter.
// Pre-publication errors remove the temporary file and leave the old final
// unchanged. A PublicationPublished error means the complete final file is
// visible but its parent-directory durability remains unconfirmed.
func WriteFileAtomicContext(ctx context.Context, path string, r io.Reader) error {
	aw, err := NewAtomicWriter(path)
	if err != nil {
		return err
	}

	if _, err := io.Copy(aw, r); err != nil {
		aw.Abort()
		return fmt.Errorf("writing %s: %w", path, err)
	}

	return aw.CommitContext(ctx)
}

// WriteFileAtomicRooted copies r into path beneath destination.
func WriteFileAtomicRooted(
	ctx context.Context,
	destination *RootedDestination,
	path string,
	reader io.Reader,
) error {
	writer, err := NewRootedAtomicWriter(destination, path)
	if err != nil {
		return err
	}

	if _, err := io.Copy(writer, reader); err != nil {
		writer.Abort()

		return fmt.Errorf("writing %s: %w", path, err)
	}

	return writer.CommitContext(ctx)
}

// EnsureDir creates path and all parents with the platform durability contract.
// Unix persists every containing-directory entry back to the filesystem root.
// Windows has no documented unprivileged directory-flush API, so directory
// creation cannot be given the same explicit POSIX durability guarantee.
func EnsureDir(path string) error {
	return ensureDirDurably(path)
}
