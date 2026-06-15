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
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeTestBinary creates a runnable script (store entries are smoke-tested
// with --version, so plain text payloads would be rejected).
func writeTestBinary(t *testing.T, dir, name, marker string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(testScript(marker)), 0o755))

	return path
}

func testScript(marker string) string {
	return "#!/bin/sh\n# " + marker + "\nexit 0\n"
}

func TestStoreArchiveAndResolve(t *testing.T) {
	dir := t.TempDir()
	store := &Store{root: filepath.Join(dir, "cli")}

	src := writeTestBinary(t, dir, "binary", "PAYLOAD")
	require.NoError(t, store.Archive(context.Background(), src, "v1.2.3"))
	require.True(t, store.has("v1.2.3"))

	info, err := os.Stat(store.binaryPath("v1.2.3"))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o755), info.Mode().Perm(), "stored binary must be executable")

	// Resolve matches by semver value, not by string: "1.2.3" finds "v1.2.3".
	assert.Equal(t, "v1.2.3", store.Resolve(semver.MustParse("1.2.3")))
	assert.Empty(t, store.Resolve(semver.MustParse("9.9.9")))
}

func TestStoreArchiveKeepsExistingEntry(t *testing.T) {
	dir := t.TempDir()
	store := &Store{root: filepath.Join(dir, "cli")}

	first := writeTestBinary(t, dir, "first", "FIRST")
	require.NoError(t, store.Archive(context.Background(), first, "v1.0.0"))

	second := writeTestBinary(t, dir, "second", "SECOND")
	require.NoError(t, store.Archive(context.Background(), second, "v1.0.0"))

	got, err := os.ReadFile(store.binaryPath("v1.0.0"))
	require.NoError(t, err)
	assert.Equal(t, testScript("FIRST"), string(got), "a published version is immutable - the existing entry wins")
}

func TestStoreArchiveRejectsNonSemver(t *testing.T) {
	dir := t.TempDir()
	store := &Store{root: filepath.Join(dir, "cli")}

	src := writeTestBinary(t, dir, "binary", "PAYLOAD")
	require.Error(t, store.Archive(context.Background(), src, "local-dev"))
}

func TestStoreInstallRejectsCorruptBinary(t *testing.T) {
	dir := t.TempDir()
	store := &Store{root: filepath.Join(dir, "cli")}

	err := store.install(context.Background(), "v1.0.0", func(dst string) error {
		return os.WriteFile(dst, []byte("not a program"), 0o755)
	})
	require.Error(t, err, "a binary failing the smoke test must not be installed")
	assert.False(t, store.has("v1.0.0"))
}

func TestStoreListSkipsGarbageAndSortsNewestFirst(t *testing.T) {
	dir := t.TempDir()
	store := &Store{root: filepath.Join(dir, "cli")}

	src := writeTestBinary(t, dir, "binary", "PAYLOAD")
	for _, tag := range []string{"v0.1.0", "v0.2.0"} {
		require.NoError(t, store.Archive(context.Background(), src, tag))
	}

	// Junk the store must tolerate: a non-semver dir, a version dir without a
	// binary, a stray file.
	versionsDir := filepath.Join(store.root, storeVersionsDirName)
	require.NoError(t, os.MkdirAll(filepath.Join(versionsDir, "not-a-version"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(versionsDir, "v9.9.9"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(versionsDir, "stray"), nil, 0o644))

	got := make([]string, 0, 2)
	for _, v := range store.List() {
		got = append(got, v.Original())
	}

	assert.Equal(t, []string{"v0.2.0", "v0.1.0"}, got)
}

func TestStoreSwitchCurrentAndCurrentTag(t *testing.T) {
	dir := t.TempDir()
	store := &Store{root: filepath.Join(dir, "cli")}

	assert.Empty(t, store.CurrentTag(), "no link yet - no current tag")
	require.Error(t, store.switchCurrent("v1.0.0"), "cannot switch to a version that is not installed")

	src := writeTestBinary(t, dir, "binary", "PAYLOAD")
	for _, tag := range []string{"v1.0.0", "v2.0.0"} {
		require.NoError(t, store.Archive(context.Background(), src, tag))
	}

	require.NoError(t, store.switchCurrent("v1.0.0"))
	assert.Equal(t, "v1.0.0", store.CurrentTag())

	// Repointing is an atomic replace; reading through the link follows the chain.
	require.NoError(t, store.switchCurrent("v2.0.0"))
	assert.Equal(t, "v2.0.0", store.CurrentTag())

	got, err := os.ReadFile(store.currentLinkPath())
	require.NoError(t, err)
	assert.Equal(t, testScript("PAYLOAD"), string(got))
}

func TestStoreContains(t *testing.T) {
	dir := t.TempDir()
	store := &Store{root: filepath.Join(dir, "cli")}

	src := writeTestBinary(t, dir, "binary", "PAYLOAD")
	require.NoError(t, store.Archive(context.Background(), src, "v1.0.0"))

	inStore, err := filepath.EvalSymlinks(store.binaryPath("v1.0.0"))
	require.NoError(t, err)
	assert.True(t, store.Contains(inStore))

	outside, err := filepath.EvalSymlinks(src)
	require.NoError(t, err)
	assert.False(t, store.Contains(outside))
}

func TestNilStoreIsNoop(t *testing.T) {
	var store *Store

	assert.False(t, store.has("v1.0.0"))
	assert.Nil(t, store.List())
	assert.Empty(t, store.Resolve(semver.MustParse("1.0.0")))
	assert.Empty(t, store.CurrentTag())
	assert.False(t, store.Contains("/usr/local/bin/d8"))
	assert.NoError(t, store.Archive(context.Background(), "/dev/null", "v1.0.0"))
	assert.Error(t, store.install(context.Background(), "v1.0.0", nil))
}
