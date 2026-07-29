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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg/diagnostic"
)

// fakeSource serves fixed tags and writes binaryContent as the extracted binary.
type fakeSource struct {
	tags          []string
	binaryContent string
	err           error
}

func (s fakeSource) ListTags(context.Context) ([]string, error) {
	return s.tags, s.err
}

func (s fakeSource) ExtractBinary(_ context.Context, _, destination string) error {
	return os.WriteFile(destination, []byte(s.binaryContent), 0o755)
}

func TestLatestVersion(t *testing.T) {
	src := fakeSource{tags: []string{"v0.13.0", "v0.13.1", "latest"}}
	updater := NewUpdater(src, nil, dkplog.NewNop())

	tests := []struct {
		name      string
		current   string
		wantTag   string
		wantNewer bool
	}{
		{name: "older current", current: "v0.13.0", wantTag: "v0.13.1", wantNewer: true},
		{name: "current is latest", current: "v0.13.1", wantTag: "v0.13.1", wantNewer: false},
		{name: "non-semver dev build", current: "dev", wantTag: "v0.13.1", wantNewer: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			latest, newer, err := updater.LatestVersion(context.Background(), tt.current)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTag, latest)
			assert.Equal(t, tt.wantNewer, newer)
		})
	}
}

func TestLatestVersionIgnoresPrereleases(t *testing.T) {
	updater := NewUpdater(fakeSource{tags: []string{"v0.13.1", "v0.14.0-rc1"}}, nil, dkplog.NewNop())

	latest, newer, err := updater.LatestVersion(context.Background(), "v0.13.1")
	require.NoError(t, err)
	assert.Equal(t, "v0.13.1", latest, "a pre-release must not be offered as the latest stable")
	assert.False(t, newer)
}

func TestLatestVersionNoReleases(t *testing.T) {
	updater := NewUpdater(fakeSource{tags: []string{"latest", "main"}}, nil, dkplog.NewNop())

	_, _, err := updater.LatestVersion(context.Background(), "v1.0.0")
	require.Error(t, err)
}

func TestApplyMigratesPlainInstallAndKeepsBackup(t *testing.T) {
	dir := t.TempDir()
	exePath := filepath.Join(dir, "d8")
	require.NoError(t, os.WriteFile(exePath, []byte("OLD"), 0o755))

	// The "new binary" must run for the --version smoke test, so it is a tiny script.
	newBinary := "#!/bin/sh\nexit 0\n"
	store := &Store{root: filepath.Join(dir, "store")}
	updater := NewUpdater(fakeSource{tags: []string{"v1.0.0"}, binaryContent: newBinary}, store, dkplog.NewNop())

	res, err := updater.applyTo(context.Background(), exePath, "v1.0.0")
	require.NoError(t, err)
	assert.True(t, res.Migrated, "a plain-file install must be migrated to the symlink layout")

	info, err := os.Lstat(exePath)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink, "PATH entry must become a symlink")

	got, err := os.ReadFile(exePath) // follows the symlink chain into the store
	require.NoError(t, err)
	assert.Equal(t, newBinary, string(got))

	assert.Equal(t, "v1.0.0", store.CurrentTag())
	assert.True(t, store.has("v1.0.0"))

	backup, err := os.ReadFile(exePath + OldSuffix)
	require.NoError(t, err)
	assert.Equal(t, "OLD", string(backup))

	_, err = os.Stat(store.binaryPath("v1.0.0") + storeStagedSuffix)
	assert.True(t, os.IsNotExist(err), "staged store entry must be cleaned up")

	_, err = os.Stat(store.lockPath())
	assert.True(t, os.IsNotExist(err), "lock file must be released")
}

func TestAcquireLockReclaimsStale(t *testing.T) {
	lock := filepath.Join(t.TempDir(), "d8.lock")

	// A fresh lock blocks a second acquirer.
	require.NoError(t, os.WriteFile(lock, nil, 0o644))
	_, err := acquireLock(lock, dkplog.NewNop())
	assert.Error(t, err, "a fresh lock blocks")

	// A lock older than lockStaleAfter is reclaimed (orphaned by a hard kill),
	// otherwise a SIGKILLed update would block every future update forever.
	old := time.Now().Add(-2 * lockStaleAfter)
	require.NoError(t, os.Chtimes(lock, old, old))

	release, err := acquireLock(lock, dkplog.NewNop())
	require.NoError(t, err, "a stale lock is reclaimed")
	require.NotNil(t, release)

	release()

	_, statErr := os.Stat(lock)
	assert.True(t, os.IsNotExist(statErr), "release removes the lock")
}

func TestApplyMigrationPermissionErrorGetsPrivilegeHint(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}

	dir := t.TempDir()
	exeDir := filepath.Join(dir, "bin")
	require.NoError(t, os.MkdirAll(exeDir, 0o755))
	exePath := filepath.Join(exeDir, "d8")
	require.NoError(t, os.WriteFile(exePath, []byte("OLD"), 0o755))

	// The store lives in the (writable) home, but migrating the PATH entry needs
	// write access to its directory - a read-only one must produce a sudo hint.
	require.NoError(t, os.Chmod(exeDir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(exeDir, 0o755) })

	store := &Store{root: filepath.Join(dir, "store")}
	updater := NewUpdater(fakeSource{tags: []string{"v1.0.0"}, binaryContent: "#!/bin/sh\nexit 0\n"}, store, dkplog.NewNop())

	_, err := updater.applyTo(context.Background(), exePath, "v1.0.0")
	require.Error(t, err)

	var he *diagnostic.HelpfulError
	require.ErrorAs(t, err, &he, "a permission failure is a HelpfulError so the CLI colors it")
	require.Len(t, he.Suggestions, 1)
	assert.Contains(t, strings.Join(he.Suggestions[0].Solutions, " "), "sudo",
		"the diagnostic points the user at sudo")

	got, err := os.ReadFile(exePath)
	require.NoError(t, err)
	assert.Equal(t, "OLD", string(got), "original binary must be untouched on failure")
}

func TestApplyMigrationFailureRollsBackCurrent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}

	dir := t.TempDir()
	exeDir := filepath.Join(dir, "bin")
	require.NoError(t, os.MkdirAll(exeDir, 0o755))
	exePath := filepath.Join(exeDir, "d8")
	require.NoError(t, os.WriteFile(exePath, []byte("OLD"), 0o755))

	// Read-only PATH dir: migratePathEntry's rename fails after current was switched.
	require.NoError(t, os.Chmod(exeDir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(exeDir, 0o755) })

	store := &Store{root: filepath.Join(dir, "store")}
	updater := NewUpdater(fakeSource{tags: []string{"v1.0.0"}, binaryContent: "#!/bin/sh\nexit 0\n"}, store, dkplog.NewNop())

	_, err := updater.applyTo(context.Background(), exePath, "v1.0.0")
	require.Error(t, err)

	assert.Empty(t, store.CurrentTag(),
		"a failed migration must roll current back (fresh install: cleared), not leave it on the new tag")
}

func TestApplyRejectsBinaryThatFailsSmokeTest(t *testing.T) {
	dir := t.TempDir()
	exePath := filepath.Join(dir, "d8")
	require.NoError(t, os.WriteFile(exePath, []byte("OLD"), 0o755))

	// A non-executable payload fails the --version smoke test; the original must
	// stay, and the corrupt artifact must not become a visible store entry.
	store := &Store{root: filepath.Join(dir, "store")}
	updater := NewUpdater(fakeSource{tags: []string{"v1.0.0"}, binaryContent: "not a program"}, store, dkplog.NewNop())

	_, err := updater.applyTo(context.Background(), exePath, "v1.0.0")
	require.Error(t, err)

	got, err := os.ReadFile(exePath)
	require.NoError(t, err)
	assert.Equal(t, "OLD", string(got), "original binary must be untouched on failure")

	assert.False(t, store.has("v1.0.0"), "a corrupt artifact must not land in the store")
	assert.Empty(t, store.CurrentTag(), "current must not be switched on failure")
}
