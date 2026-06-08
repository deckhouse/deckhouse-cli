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

package plugins

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/requirements"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

// fakeInstallSource is a PluginSource whose ExtractPlugin writes a caller-supplied
// binary, so the install pipeline (smoke test, rollback) can be exercised on disk.
type fakeInstallSource struct {
	contract *internal.Plugin
	extract  func(dest string) error

	// tags is what ListPluginTags returns; listedTags records the calls so tests
	// can assert which plugins an operation actually touched.
	tags       []string
	listedTags []string

	// contractByTag, when set, overrides contract per tag; a missing tag yields an
	// error (simulating a transient registry failure for that version).
	contractByTag map[string]*internal.Plugin
}

func (f *fakeInstallSource) ListPlugins(context.Context) ([]string, error) { return nil, nil }

func (f *fakeInstallSource) ListPluginTags(_ context.Context, pluginName string) ([]string, error) {
	f.listedTags = append(f.listedTags, pluginName)

	return f.tags, nil
}

func (f *fakeInstallSource) GetPluginContract(_ context.Context, _, tag string) (*internal.Plugin, error) {
	if f.contractByTag != nil {
		contract, ok := f.contractByTag[tag]
		if !ok {
			return nil, errors.New("transient contract fetch failure")
		}

		return contract, nil
	}

	return f.contract, nil
}

func (f *fakeInstallSource) ExtractPlugin(_ context.Context, _, _, dest string) error {
	return f.extract(dest)
}

// writeScriptBinary writes an executable shell script at dir/name. The script
// echoes versionOutput and exits with exitCode, standing in for a plugin binary.
func writeScriptBinary(t *testing.T, dir, name, versionOutput string, exitCode int) string {
	t.Helper()

	path := filepath.Join(dir, name)
	script := "#!/bin/sh\necho '" + versionOutput + "'\nexit " + strconv.Itoa(exitCode) + "\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))

	return path
}

func TestPluginBinaryVersion(t *testing.T) {
	dir := t.TempDir()

	ok := writeScriptBinary(t, dir, "ok", "v2.3.4", 0)
	version, err := pluginBinaryVersion(context.Background(), ok)
	require.NoError(t, err)
	assert.Equal(t, "2.3.4", version.String())

	bad := writeScriptBinary(t, dir, "bad", "not-a-version", 0)
	_, err = pluginBinaryVersion(context.Background(), bad)
	assert.Error(t, err, "non-semver output is a parse error")

	failing := writeScriptBinary(t, dir, "fail", "", 1)
	_, err = pluginBinaryVersion(context.Background(), failing)
	assert.Error(t, err, "a non-zero exit is a run error")
}

func TestPluginBinaryVersionFallsBackToVersionSubcommand(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fallback")
	// Fails on `--version`, succeeds on the `version` subcommand.
	script := "#!/bin/sh\nif [ \"$1\" = \"version\" ]; then echo 'v3.0.0'; exit 0; fi\nexit 1\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))

	version, err := pluginBinaryVersion(context.Background(), path)
	require.NoError(t, err)
	assert.Equal(t, "3.0.0", version.String())
}

func TestPluginAlreadyAtVersion(t *testing.T) {
	dir := t.TempDir()
	m := testManager()

	bin := writeScriptBinary(t, dir, "p", "v1.2.3", 0)

	assert.True(t, m.pluginAlreadyAtVersion(context.Background(), bin, semver.MustParse("v1.2.3")),
		"same version is already installed")
	assert.False(t, m.pluginAlreadyAtVersion(context.Background(), bin, semver.MustParse("v1.2.4")),
		"a newer version is not yet installed")
	assert.False(t, m.pluginAlreadyAtVersion(context.Background(), filepath.Join(dir, "missing"), semver.MustParse("v1.2.3")),
		"no binary means not installed")

	failing := writeScriptBinary(t, dir, "broken", "", 1)
	assert.False(t, m.pluginAlreadyAtVersion(context.Background(), failing, semver.MustParse("v1.2.3")),
		"an unrunnable binary is treated as not-at-version (do not skip the reinstall)")
}

func TestSmokeTestPlugin(t *testing.T) {
	dir := t.TempDir()
	m := testManager()

	good := writeScriptBinary(t, dir, "good", "v1.0.0", 0)
	require.NoError(t, m.smokeTestPlugin(context.Background(), "p", good))

	bad := writeScriptBinary(t, dir, "bad", "", 1)
	assert.Error(t, m.smokeTestPlugin(context.Background(), "p", bad),
		"a binary that cannot report its version fails the smoke test")
}

func TestSmokeTestPluginAcceptsNonSemverOutput(t *testing.T) {
	dir := t.TempDir()
	m := testManager()

	// Exits 0 but prints a human-readable banner, not a bare semver.
	banner := writeScriptBinary(t, dir, "banner", "myplugin version 1.2 (build abc)", 0)

	require.NoError(t, m.smokeTestPlugin(context.Background(), "p", banner),
		"smoke passes on a clean exit even when output is not a parseable version")

	_, err := pluginBinaryVersion(context.Background(), banner)
	assert.Error(t, err, "strict version parsing still rejects non-semver output")
}

func TestInstalledMajorFromDisk(t *testing.T) {
	root := t.TempDir()
	m := testManager()
	m.pluginDirectory = root

	_, ok := m.installedMajorFromDisk("p")
	assert.False(t, ok, "no install yet")

	installPluginFixture(t, root, "p", 2)

	major, ok := m.installedMajorFromDisk("p")
	require.True(t, ok)
	assert.Equal(t, 2, major, "major read from the current symlink target, not the binary")
}

func TestAcquireInstallLockIsExclusive(t *testing.T) {
	m := testManager()
	lock := filepath.Join(t.TempDir(), "p.lock")

	release, err := m.acquireInstallLock(lock)
	require.NoError(t, err)
	require.NotNil(t, release)

	_, err = m.acquireInstallLock(lock)
	assert.Error(t, err, "a held lock blocks a second acquirer")

	release()

	release2, err := m.acquireInstallLock(lock)
	require.NoError(t, err, "a released lock can be re-acquired")
	release2()
}

func TestAcquireInstallLockReclaimsStale(t *testing.T) {
	m := testManager()
	lock := filepath.Join(t.TempDir(), "p.lock")

	// A fresh lock blocks a second acquirer.
	require.NoError(t, os.WriteFile(lock, nil, 0o644))
	_, err := m.acquireInstallLock(lock)
	assert.Error(t, err, "a fresh lock blocks")

	// A lock older than installLockStaleAfter is reclaimed (orphaned by a hard kill).
	old := time.Now().Add(-2 * installLockStaleAfter)
	require.NoError(t, os.Chtimes(lock, old, old))

	release, err := m.acquireInstallLock(lock)
	require.NoError(t, err, "a stale lock is reclaimed")
	require.NotNil(t, release)

	release()

	_, statErr := os.Stat(lock)
	assert.True(t, os.IsNotExist(statErr), "release removes the lock")
}

func TestInstallPluginSwitchesToInstalledVersionWithoutDownload(t *testing.T) {
	root := t.TempDir()
	m := testManager()
	m.pluginDirectory = root

	extractCalled := false
	m.service = &fakeInstallSource{
		contract: &internal.Plugin{Name: "p", Version: "v2.0.0"},
		extract: func(dest string) error {
			extractCalled = true

			return os.WriteFile(dest, []byte("x"), 0o755)
		},
	}

	// Two majors already installed; current points at v1.
	v1 := layout.BinaryPath(root, "p", 1)
	v2 := layout.BinaryPath(root, "p", 2)
	require.NoError(t, os.MkdirAll(filepath.Dir(v1), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(v2), 0o755))
	require.NoError(t, os.WriteFile(v1, []byte("#!/bin/sh\necho v1.0.0\n"), 0o755))
	require.NoError(t, os.WriteFile(v2, []byte("#!/bin/sh\necho v2.0.0\n"), 0o755))
	require.NoError(t, os.Symlink(v1, layout.CurrentLinkPath(root, "p")))

	// Installing the already-present v2 must repoint current WITHOUT downloading.
	err := m.installPlugin(context.Background(), "p", semver.MustParse("v2.0.0"), false, false)
	require.NoError(t, err)
	assert.False(t, extractCalled, "switching to an installed version must not re-download")

	target, err := os.Readlink(layout.CurrentLinkPath(root, "p"))
	require.NoError(t, err)
	absV2, _ := filepath.Abs(v2)
	assert.Equal(t, absV2, target, "current was repointed to v2")
}

func TestInstallPluginSwitchBlockedByRequirements(t *testing.T) {
	root := t.TempDir()
	m := testManager()
	m.pluginDirectory = root
	m.clusterStateCache = &requirements.ClusterState{Kubernetes: semver.MustParse("v1.28.3")}
	m.service = &fakeInstallSource{
		contract: &internal.Plugin{
			Name:         "p",
			Version:      "v2.0.0",
			Requirements: internal.Requirements{Kubernetes: internal.KubernetesRequirement{Constraint: ">= 99.0"}},
		},
		extract: func(dest string) error { return os.WriteFile(dest, []byte("x"), 0o755) },
	}

	v1 := layout.BinaryPath(root, "p", 1)
	v2 := layout.BinaryPath(root, "p", 2)
	require.NoError(t, os.MkdirAll(filepath.Dir(v1), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Dir(v2), 0o755))
	require.NoError(t, os.WriteFile(v1, []byte("#!/bin/sh\necho v1.0.0\n"), 0o755))
	require.NoError(t, os.WriteFile(v2, []byte("#!/bin/sh\necho v2.0.0\n"), 0o755))
	require.NoError(t, os.Symlink(v1, layout.CurrentLinkPath(root, "p")))

	// Switching to the already-installed v2 must be BLOCKED: its requirements are not
	// met, and requirements are checked before the symlink switch.
	err := m.installPlugin(context.Background(), "p", semver.MustParse("v2.0.0"), false, false)
	require.Error(t, err, "switch is blocked when the target version's requirements are unmet")

	target, err := os.Readlink(layout.CurrentLinkPath(root, "p"))
	require.NoError(t, err)
	absV1, _ := filepath.Abs(v1)
	assert.Equal(t, absV1, target, "current stays at v1 - the unmet switch did not happen")
}

func TestInstallPluginSmokeFailureRollsBack(t *testing.T) {
	root := t.TempDir()
	m := testManager()
	m.pluginDirectory = root
	m.service = &fakeInstallSource{
		contract: &internal.Plugin{Name: "p", Version: "v1.0.0"},
		extract: func(dest string) error {
			// A binary that fails the smoke test (exits non-zero).
			return os.WriteFile(dest, []byte("#!/bin/sh\nexit 1\n"), 0o755)
		},
	}

	err := m.installPlugin(context.Background(), "p", semver.MustParse("v1.0.0"), false, false)
	require.Error(t, err, "a smoke-test failure aborts the install")

	_, statErr := os.Lstat(layout.CurrentLinkPath(root, "p"))
	assert.True(t, os.IsNotExist(statErr), "current is never repointed at a broken binary")

	_, binErr := os.Stat(layout.BinaryPath(root, "p", 1))
	assert.True(t, os.IsNotExist(binErr), "the broken binary is removed on rollback (fresh install, no .old)")
}

func TestInstallPluginDoesNotDowngradeOnContractError(t *testing.T) {
	root := t.TempDir()
	m := testManager()
	m.pluginDirectory = root

	extractCalled := false
	src := &fakeInstallSource{
		tags: []string{"v1.2.0", "v1.3.0"},
		contractByTag: map[string]*internal.Plugin{
			// v1.3.0 is deliberately absent: its contract fetch fails transiently,
			// so selection falls back to v1.2.0.
			"v1.2.0": {Name: "p", Version: "v1.2.0"},
		},
		extract: func(dest string) error {
			extractCalled = true

			return os.WriteFile(dest, []byte("x"), 0o755)
		},
	}
	m.service = src

	// v1.3.0 installed and current.
	v1 := layout.BinaryPath(root, "p", 1)
	require.NoError(t, os.MkdirAll(filepath.Dir(v1), 0o755))
	writeScriptBinary(t, filepath.Dir(v1), "p", "v1.3.0", 0)

	absV1, err := filepath.Abs(v1)
	require.NoError(t, err)
	require.NoError(t, os.Symlink(absV1, layout.CurrentLinkPath(root, "p")))

	require.NoError(t, m.InstallPlugin(context.Background(), "p"),
		"a transient contract error on the newest tag is not an install failure")

	assert.False(t, extractCalled, "the older selection must not be installed over a newer installed version")

	version, err := pluginBinaryVersion(context.Background(), v1)
	require.NoError(t, err)
	assert.Equal(t, "1.3.0", version.String(), "the installed newer version is kept, not silently downgraded")
}

func TestInstallPluginRejectsInvalidName(t *testing.T) {
	m := testManager()
	m.pluginDirectory = t.TempDir()

	for _, name := range []string{"../escape", "a/b", "..", "UPPER", "name?x"} {
		require.Error(t, m.InstallPlugin(context.Background(), name), "name %q must be rejected", name)
	}

	entries, err := os.ReadDir(m.pluginDirectory)
	require.NoError(t, err)
	assert.Empty(t, entries, "no directories are created for an invalid name")
}

func TestInstallPluginDownloadFailureRestoresPreviousBinary(t *testing.T) {
	root := t.TempDir()
	m := testManager()
	m.pluginDirectory = root
	m.service = &fakeInstallSource{
		contract: &internal.Plugin{Name: "p", Version: "v1.1.0"},
		extract: func(dest string) error {
			// A mid-stream failure leaves a truncated file at dest, exactly like the
			// real extractor's direct write does.
			_ = os.WriteFile(dest, []byte("truncated"), 0o755)

			return errors.New("connection reset")
		},
	}

	// v1.0.0 installed and current.
	v1 := layout.BinaryPath(root, "p", 1)
	require.NoError(t, os.MkdirAll(filepath.Dir(v1), 0o755))
	writeScriptBinary(t, filepath.Dir(v1), "p", "v1.0.0", 0)

	absV1, err := filepath.Abs(v1)
	require.NoError(t, err)
	require.NoError(t, os.Symlink(absV1, layout.CurrentLinkPath(root, "p")))

	err = m.installPlugin(context.Background(), "p", semver.MustParse("v1.1.0"), false, false)
	require.Error(t, err, "a failed download aborts the install")

	restored, err := os.ReadFile(v1)
	require.NoError(t, err, "the previous binary is back in place after a failed download")
	assert.Contains(t, string(restored), "v1.0.0", "the restored binary is the previous one, not the truncated download")

	_, oldErr := os.Stat(v1 + ".old")
	assert.True(t, os.IsNotExist(oldErr), ".old was consumed by the restore")
}

func TestRestoreOldBinary(t *testing.T) {
	dir := t.TempDir()
	m := testManager()

	// With a .old backup: the new binary is removed and the old one is restored.
	bin := filepath.Join(dir, "p")
	require.NoError(t, os.WriteFile(bin, []byte("new"), 0o755))
	require.NoError(t, os.WriteFile(bin+".old", []byte("old"), 0o755))

	m.restoreOldBinary(bin)

	restored, err := os.ReadFile(bin)
	require.NoError(t, err)
	assert.Equal(t, "old", string(restored), "previous binary restored")
	_, err = os.Stat(bin + ".old")
	assert.True(t, os.IsNotExist(err), ".old consumed by the rename")

	// Without a .old backup (fresh install): the bad binary is just removed.
	fresh := filepath.Join(dir, "fresh")
	require.NoError(t, os.WriteFile(fresh, []byte("new"), 0o755))

	m.restoreOldBinary(fresh)

	_, err = os.Stat(fresh)
	assert.True(t, os.IsNotExist(err), "bad binary removed when there is nothing to restore")
}
