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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/lockfile"
	"github.com/deckhouse/deckhouse-cli/internal/version"
)

const (
	// OldSuffix marks the backup of a plain-file install made when it is migrated
	// to the store-managed (symlink) layout.
	OldSuffix = ".old"

	// smokeTestTimeout bounds the new binary's --version probe so a hung or
	// malformed binary fails the update instead of stalling it.
	smokeTestTimeout = 30 * time.Second

	// lockStaleAfter is how old the update lock may get before it is treated as
	// orphaned (a prior update was hard-killed before its deferred release ran).
	// An update takes seconds, so an hour-old lock is certainly stale. Mirrors the
	// plugin installer's reclaim so the two locks behave identically.
	lockStaleAfter = 1 * time.Hour
)

// Updater checks for and installs newer deckhouse-cli releases from a Source.
type Updater struct {
	source Source
	store  *Store
	logger *dkplog.Logger
}

// NewUpdater builds an Updater over the given Source. The store is where
// versions are installed and switched; without it (nil) updates cannot proceed.
func NewUpdater(source Source, store *Store, logger *dkplog.Logger) *Updater {
	return &Updater{source: source, store: store, logger: logger}
}

// Versions returns the published release versions sorted newest-first. Tags that
// are not valid semver are skipped (foreign platforms' suffixed tags survive the
// source normalization as raw strings, but they parse fine and are kept - they
// carry their suffix as a pre-release marker).
func (u *Updater) Versions(ctx context.Context) ([]*semver.Version, error) {
	tags, err := u.source.ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("list deckhouse-cli versions: %w", err)
	}

	versions := make([]*semver.Version, 0, len(tags))

	for _, tag := range tags {
		version, err := semver.NewVersion(tag)
		if err != nil {
			continue
		}

		versions = append(versions, version)
	}

	sort.Sort(sort.Reverse(semver.Collection(versions)))

	return versions, nil
}

// LatestVersion returns the highest available STABLE semver tag and whether it is
// newer than current (pre-releases are ignored - install them explicitly via
// --version). A non-semver current (e.g. a "dev" build) is treated as older than
// any real release, so an update is always offered.
func (u *Updater) LatestVersion(ctx context.Context, current string) (string, bool, error) {
	tags, err := u.source.ListTags(ctx)
	if err != nil {
		return "", false, fmt.Errorf("list deckhouse-cli versions: %w", err)
	}

	latest := maxSemver(tags)
	if latest == nil {
		return "", false, errors.New("no released deckhouse-cli versions found")
	}

	currentVersion, err := semver.NewVersion(current)
	if err != nil {
		return latest.Original(), true, nil
	}

	return latest.Original(), currentVersion.LessThan(latest), nil
}

// SwitchResult describes what a switch did, so commands can tell the user what
// was left behind and how to undo it.
type SwitchResult struct {
	// PrevTag is the version `current` pointed at before the switch ("" when the
	// install was not store-managed yet).
	PrevTag string
	// Migrated reports that the PATH entry was converted from a plain file to a
	// symlink into the store (the original is backed up with OldSuffix).
	Migrated bool
}

// Apply downloads tag into the version store (unless already present) and makes
// it the active version by repointing the store's `current` symlink. A plain-file
// install is migrated to the symlink layout on the way (backup kept as <exe>.old).
func (u *Updater) Apply(ctx context.Context, tag string) (SwitchResult, error) {
	exePath, err := CurrentExecutable()
	if err != nil {
		return SwitchResult{}, err
	}

	return u.applyTo(ctx, exePath, tag)
}

// applyTo performs the switch against an explicit executable path. It is
// separated from Apply so the logic can be tested without touching the running
// test binary.
func (u *Updater) applyTo(ctx context.Context, exePath, tag string) (SwitchResult, error) {
	return SwitchTo(ctx, exePath, tag, u.store, u.logger, func(dst string) error {
		if err := u.source.ExtractBinary(ctx, tag, dst); err != nil {
			return fmt.Errorf("download new binary: %w", err)
		}

		return nil
	})
}

// CurrentExecutable resolves the running binary to a real file path (symlinks
// evaluated). For a store-managed install this lands inside the store (the
// `current` chain resolved), which is exactly how SwitchTo detects the mode.
func CurrentExecutable() (string, error) {
	exePath, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate current executable: %w", err)
	}

	exePath, err = filepath.EvalSymlinks(exePath)
	if err != nil {
		return "", fmt.Errorf("resolve executable path: %w", err)
	}

	return exePath, nil
}

// SwitchTo makes tag the active d8 version: ensures it is in the store (fetching
// via stage when missing; stage == nil demands a store hit), smoke-tests it and
// atomically repoints the store's `current` symlink. The PATH binary is never
// rewritten - it is a symlink into the store, so the switch happens entirely in
// the user's home, with no elevated privileges and no file copies.
//
// exePath is the resolved path of the running binary. When it lies outside the
// store (a plain-file install), the switch MIGRATES it: the running binary is
// seeded into the store under its own version, and the PATH file is backed up
// as <exe>.old and replaced with a symlink to the store's `current`. The same
// path heals an install whose symlink was overwritten by external tooling.
func SwitchTo(ctx context.Context, exePath, tag string, store *Store, logger *dkplog.Logger, stage func(dst string) error) (SwitchResult, error) {
	if runtime.GOOS == "windows" {
		// Windows cannot replace a running .exe, and the image entry is d8, not
		// d8.exe. RPP is in-cluster (Linux/macOS), so this stays unsupported.
		return SwitchResult{}, errors.New("self-update is not supported on Windows; download the new d8 binary manually")
	}

	if store == nil {
		return SwitchResult{}, errors.New("version store is unavailable (home directory cannot be resolved)")
	}

	if err := os.MkdirAll(store.root, 0o755); err != nil {
		return SwitchResult{}, fmt.Errorf("create version store: %w", err)
	}

	release, err := acquireLock(store.lockPath(), logger)
	if err != nil {
		return SwitchResult{}, err
	}

	defer release()

	managed := store.Contains(exePath)

	// A plain-file install is about to become store-managed: seed the store with
	// the running binary first, so the displaced version stays switchable (and a
	// `use` of the running version needs no download at all). Best-effort - a dev
	// build (non-semver) cannot be addressed by `use` and is covered by the
	// <exe>.old backup below anyway.
	if !managed {
		retain(ctx, store, exePath, version.Version, logger)
	}

	if !store.has(tag) {
		if stage == nil {
			return SwitchResult{}, fmt.Errorf("version %s is not in the local store", tag)
		}

		if err := store.install(ctx, tag, stage); err != nil {
			return SwitchResult{}, err
		}
	}

	// Fresh installs were smoke-tested while staged; pre-existing entries are
	// re-checked before they become the active binary.
	if err := smokeTest(ctx, store.binaryPath(tag)); err != nil {
		return SwitchResult{}, err
	}

	result := SwitchResult{PrevTag: store.CurrentTag()}

	if err := store.switchCurrent(tag); err != nil {
		return SwitchResult{}, err
	}

	if !managed {
		if err := migratePathEntry(exePath, store); err != nil {
			restorePreviousCurrent(store, result.PrevTag, logger)

			return SwitchResult{}, err
		}

		result.Migrated = true
	}

	logger.Debug("switched deckhouse-cli version",
		slog.String("tag", tag), slog.String("previous", result.PrevTag), slog.Bool("migrated", result.Migrated))

	return result, nil
}

// migratePathEntry converts a plain-file install into a symlink onto the store's
// `current`: the original binary is kept as <exe>.old, and a failed link
// creation rolls the backup straight back.
func migratePathEntry(exePath string, store *Store) error {
	oldPath := exePath + OldSuffix

	if err := os.Rename(exePath, oldPath); err != nil {
		return withPrivilegeHint(exePath, fmt.Errorf("back up current binary: %w", err))
	}

	if err := os.Symlink(store.currentLinkPath(), exePath); err != nil {
		if restoreErr := os.Rename(oldPath, exePath); restoreErr != nil {
			return fmt.Errorf("replacing the binary with a symlink failed (%w); restoring the previous binary also failed (%v) - restore it manually from %s",
				err, restoreErr, oldPath)
		}

		return withPrivilegeHint(exePath, fmt.Errorf("replace binary with a symlink: %w", err))
	}

	return nil
}

// restorePreviousCurrent undoes switchCurrent after the PATH migration failed, so a
// failed switch does not leave `current` pointing at a version the PATH binary was
// never repointed to. Best-effort: the store lives in the user's home.
func restorePreviousCurrent(store *Store, prevTag string, logger *dkplog.Logger) {
	var err error
	if prevTag == "" {
		err = os.Remove(store.currentLinkPath())
	} else {
		err = store.switchCurrent(prevTag)
	}

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Debug("could not restore previous current after failed migration",
			slog.String("previous", prevTag), dkplog.Err(err))
	}
}

// retain seeds the store with binPath under tag, best-effort: a failure (notably
// a non-semver tag - a dev build cannot be addressed by `use` anyway) must never
// fail the switch itself.
func retain(ctx context.Context, store *Store, binPath, tag string, logger *dkplog.Logger) {
	if err := store.Archive(ctx, binPath, tag); err != nil {
		logger.Debug("binary not retained in the version store",
			slog.String("tag", tag), dkplog.Err(err))
	}
}

// withPrivilegeHint augments a permission error with a hint, since the install
// directory (e.g. /opt/deckhouse/bin) is usually root-owned.
func withPrivilegeHint(exePath string, err error) error {
	if errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("%w (updating %s requires elevated privileges; try running with sudo)", err, filepath.Dir(exePath))
	}

	return err
}

// smokeTest runs the freshly downloaded binary with --version so a corrupt or
// incompatible artifact is rejected before it replaces the working CLI. The
// binary's output is included in the error, turning a bare "exit status 1" into
// something diagnosable (missing shared lib, wrong arch, panic).
func smokeTest(ctx context.Context, binaryPath string) error {
	ctx, cancel := context.WithTimeout(ctx, smokeTestTimeout)
	defer cancel()

	if out, err := exec.CommandContext(ctx, binaryPath, "--version").CombinedOutput(); err != nil {
		return fmt.Errorf("new binary failed its --version smoke test: %w%s", err, outputTail(out))
	}

	return nil
}

// outputTail returns a short parenthesized tail of a failed probe's output, or "".
func outputTail(out []byte) string {
	msg := strings.TrimSpace(string(out))
	if msg == "" {
		return ""
	}

	const maxLen = 200
	if len(msg) > maxLen {
		msg = msg[:maxLen] + "..."
	}

	return fmt.Sprintf(" (output: %s)", msg)
}

// acquireLock serializes self-updates. A lock orphaned by a hard-killed update
// (older than lockStaleAfter) is reclaimed, so a SIGKILLed update does not block
// every future one.
func acquireLock(lockPath string, logger *dkplog.Logger) (func(), error) {
	release, err := lockfile.Acquire(lockPath, lockStaleAfter, func(age time.Duration) {
		logger.Warn("reclaiming a stale self-update lock",
			slog.String("lock", lockPath), slog.Duration("age", age))
	})

	if errors.Is(err, lockfile.ErrLocked) {
		return nil, fmt.Errorf("an update is already in progress (lock file %s exists)", lockPath)
	}

	if err != nil {
		return nil, err
	}

	return release, nil
}

func maxSemver(tags []string) *semver.Version {
	var latest *semver.Version

	for _, tag := range tags {
		version, err := semver.NewVersion(tag)
		if err != nil {
			continue
		}

		// The default update tracks stable releases only; pre-releases (rc/alpha/beta)
		// are installable explicitly via --version.
		if version.Prerelease() != "" {
			continue
		}

		if latest == nil || latest.LessThan(version) {
			latest = version
		}
	}

	return latest
}
