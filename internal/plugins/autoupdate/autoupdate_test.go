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

package autoupdate

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

// installPluginFixture creates plugins/<name>/v<major>/<name> under root and the
// `current` symlink pointing at it, i.e. a minimally-installed plugin.
func installPluginFixture(t *testing.T, root, name string, major int) {
	t.Helper()

	binary := layout.BinaryPath(root, name, major)
	require.NoError(t, os.MkdirAll(filepath.Dir(binary), 0o755))
	require.NoError(t, os.WriteFile(binary, []byte("#!/bin/sh\n"), 0o755))
	require.NoError(t, os.Symlink(binary, layout.CurrentLinkPath(root, name)))
}

func TestHasInstalledPlugins(t *testing.T) {
	// Isolate the home fallback so the real ~/.deckhouse-cli does not leak in.
	t.Setenv("HOME", t.TempDir())

	// Missing plugins root.
	assert.False(t, hasInstalledPlugins(t.TempDir()))

	// Empty plugins root.
	emptyRoot := t.TempDir()
	require.NoError(t, os.MkdirAll(layout.PluginsRoot(emptyRoot), 0o755))
	assert.False(t, hasInstalledPlugins(emptyRoot))

	// A file (not a directory) under plugins is not a plugin.
	fileRoot := t.TempDir()
	require.NoError(t, os.MkdirAll(layout.PluginsRoot(fileRoot), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(layout.PluginsRoot(fileRoot), "stray"), []byte("x"), 0o644))
	assert.False(t, hasInstalledPlugins(fileRoot))

	// A leftover version dir from a failed install (no `current` symlink) is not
	// counted, so the auto-update does not fire forever for a broken install.
	brokenRoot := t.TempDir()
	require.NoError(t, os.MkdirAll(layout.VersionDir(brokenRoot, "stronghold", 1), 0o755))
	assert.False(t, hasInstalledPlugins(brokenRoot))

	// A real install (version dir + current symlink).
	withPlugin := t.TempDir()
	installPluginFixture(t, withPlugin, "stronghold", 1)
	assert.True(t, hasInstalledPlugins(withPlugin))
}

func TestHasInstalledPluginsHonorsHomeFallback(t *testing.T) {
	// A non-root install lands in ~/.deckhouse-cli; the foreground process passes
	// the (empty) configured dir, so the fallback must still be detected.
	t.Setenv("HOME", t.TempDir())

	fallback, err := layout.HomeFallbackPath()
	require.NoError(t, err)
	installPluginFixture(t, fallback, "stronghold", 1)

	assert.True(t, hasInstalledPlugins(t.TempDir()), "install in the home fallback is detected")
}

func TestMarkerRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "plugin-update-check.json")

	stamp := time.Now().Truncate(time.Second)
	require.NoError(t, saveMarker(path, marker{CheckedAt: stamp}))

	loaded, err := loadMarker(path)
	require.NoError(t, err)
	assert.True(t, loaded.CheckedAt.Equal(stamp), "checked_at survives a save/load round trip")
}

func TestMarkerIsStale(t *testing.T) {
	assert.False(t, marker{CheckedAt: time.Now()}.isStale(), "a fresh check is not stale")
	assert.True(t, marker{CheckedAt: time.Now().Add(-2 * pluginUpdateCheckTTL)}.isStale(),
		"a check older than the TTL is stale")
	assert.True(t, marker{}.isStale(), "a never-checked (zero time) marker is stale")
}

func TestLoadMarkerMissing(t *testing.T) {
	_, err := loadMarker(filepath.Join(t.TempDir(), "absent.json"))
	assert.Error(t, err, "a missing marker file is an error the caller treats as stale")
}
