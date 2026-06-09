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

package selfupdate

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"
)

const (
	// storeBinaryName is the file every stored version keeps its binary under:
	// <root>/versions/<version>/d8.
	storeBinaryName = "d8"

	// storeVersionsDirName holds one directory per installed version.
	storeVersionsDirName = "versions"

	// storeCurrentLinkName is the stable symlink the PATH entry points at; it is
	// repointed atomically to switch versions - the same `current` idea the
	// plugin layout uses.
	storeCurrentLinkName = "current"

	// storeStagedSuffix marks an entry that is still being written, so a
	// half-written binary is never visible under its final name.
	storeStagedSuffix = ".staged"

	// storeLockName serializes store mutations (installs, switches, migration).
	storeLockName = "install.lock"
)

// Store is the local store of installed d8 versions plus the `current` symlink
// that selects the active one - the same versions-directory-plus-symlink layout
// the plugin installer uses:
//
//	<root>/current            -> versions/<version>/d8 (atomic repoint on switch)
//	<root>/versions/<version>/d8
//
// The PATH entry (e.g. /opt/deckhouse/bin/d8) is a one-time-created symlink to
// <root>/current, so switching never touches root-owned directories. The store
// is deliberately addressed by its own well-known paths and never through
// os.Executable(): on Linux /proc/self/exe resolves to the symlink TARGET, so
// "replace whatever the executable resolves to" would overwrite a stored
// version in place instead of repointing the link.
//
// A nil *Store is a valid no-op store (all read methods are nil-safe), so
// callers degrade gracefully when the home directory cannot be resolved.
type Store struct {
	root string
}

// NewStore returns the per-user version store at ~/.deckhouse-cli/cli (next to
// the plugins home-fallback layout).
func NewStore() (*Store, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("locate home directory for the version store: %w", err)
	}

	return &Store{root: filepath.Join(home, ".deckhouse-cli", "cli")}, nil
}

// NewStoreAt returns a store rooted at an explicit directory (tests, tooling).
func NewStoreAt(root string) *Store {
	return &Store{root: root}
}

// BinaryPath returns the path the binary for tag is stored under (whether or
// not it exists).
func (s *Store) BinaryPath(tag string) string {
	return filepath.Join(s.root, storeVersionsDirName, tag, storeBinaryName)
}

// CurrentLinkPath returns the stable `current` symlink the PATH entry points at.
func (s *Store) CurrentLinkPath() string {
	return filepath.Join(s.root, storeCurrentLinkName)
}

// LockPath returns the lock file serializing store mutations.
func (s *Store) LockPath() string {
	return filepath.Join(s.root, storeLockName)
}

// Has reports whether tag's binary is present in the store.
func (s *Store) Has(tag string) bool {
	if s == nil {
		return false
	}

	info, err := os.Stat(s.BinaryPath(tag))

	return err == nil && info.Mode().IsRegular()
}

// Resolve returns the stored tag matching the requested version (semver
// comparison, so "0.13.1" finds an entry stored as "v0.13.1"), or "" when the
// version is not stored.
func (s *Store) Resolve(requested *semver.Version) string {
	for _, v := range s.List() {
		if v.Equal(requested) {
			return v.Original()
		}
	}

	return ""
}

// List returns the stored versions newest-first. Foreign entries (non-semver
// directory names, entries without a binary) are skipped rather than reported:
// the store is best-effort by design.
func (s *Store) List() []*semver.Version {
	if s == nil {
		return nil
	}

	entries, err := os.ReadDir(filepath.Join(s.root, storeVersionsDirName))
	if err != nil {
		return nil
	}

	versions := make([]*semver.Version, 0, len(entries))

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		v, err := semver.NewVersion(entry.Name())
		if err != nil || !s.Has(entry.Name()) {
			continue
		}

		versions = append(versions, v)
	}

	sort.Sort(sort.Reverse(semver.Collection(versions)))

	return versions
}

// CurrentTag returns the version the `current` symlink points at, or "" when
// the link is absent or points outside the expected versions layout.
func (s *Store) CurrentTag() string {
	if s == nil {
		return ""
	}

	target, err := os.Readlink(s.CurrentLinkPath())
	if err != nil {
		return ""
	}

	// The link target is versions/<tag>/d8 (relative or absolute) - the tag is
	// the parent directory's name.
	tag := filepath.Base(filepath.Dir(target))
	if _, err := semver.NewVersion(tag); err != nil {
		return ""
	}

	return tag
}

// SwitchCurrent atomically repoints the `current` symlink at tag's binary. The
// target is relative (versions/<tag>/d8), so the layout survives a moved home.
func (s *Store) SwitchCurrent(tag string) error {
	if !s.Has(tag) {
		return fmt.Errorf("version %s is not in the store", tag)
	}

	staged := s.CurrentLinkPath() + storeStagedSuffix
	_ = os.Remove(staged)

	target := filepath.Join(storeVersionsDirName, tag, storeBinaryName)
	if err := os.Symlink(target, staged); err != nil {
		return fmt.Errorf("stage current symlink: %w", err)
	}

	if err := os.Rename(staged, s.CurrentLinkPath()); err != nil {
		_ = os.Remove(staged)

		return fmt.Errorf("switch current symlink: %w", err)
	}

	return nil
}

// Contains reports whether path (already symlink-resolved) lies inside the
// store - i.e. the running binary is store-managed.
func (s *Store) Contains(path string) bool {
	if s == nil {
		return false
	}

	root, err := filepath.EvalSymlinks(s.root)
	if err != nil {
		return false
	}

	return strings.HasPrefix(path, root+string(filepath.Separator))
}

// Install materializes tag in the store via fetch (which writes the binary to
// the path it is given), smoke-testing the staged file BEFORE the entry becomes
// visible - a corrupt artifact never lands under its final name (where `list`
// would mark it installed and completion would suggest it). An existing entry
// is kept as is: a published version is immutable.
func (s *Store) Install(ctx context.Context, tag string, fetch func(dst string) error) error {
	if s == nil {
		return fmt.Errorf("version store is unavailable")
	}

	if _, err := semver.NewVersion(tag); err != nil {
		return fmt.Errorf("tag %q is not a semver version: %w", tag, err)
	}

	if s.Has(tag) {
		return nil
	}

	binPath := s.BinaryPath(tag)
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return fmt.Errorf("create version store entry: %w", err)
	}

	staged := binPath + storeStagedSuffix

	defer func() { _ = os.Remove(staged) }()

	if err := fetch(staged); err != nil {
		return err
	}

	if err := os.Chmod(staged, 0o755); err != nil {
		return fmt.Errorf("mark staged binary executable: %w", err)
	}

	if err := smokeTest(ctx, staged); err != nil {
		return err
	}

	if err := os.Rename(staged, binPath); err != nil {
		return fmt.Errorf("finalize version store entry: %w", err)
	}

	return nil
}

// Archive copies the binary at srcPath into the store under tag (used to seed
// the store with the running binary during migration). Same immutability and
// semver rules as Install.
func (s *Store) Archive(ctx context.Context, srcPath, tag string) error {
	if s == nil {
		return nil
	}

	return s.Install(ctx, tag, func(dst string) error {
		return copyFile(srcPath, dst)
	})
}

// copyFile copies src to dst as an executable file (0755).
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()

		return err
	}

	return out.Close()
}
