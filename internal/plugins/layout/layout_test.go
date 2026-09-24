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

package layout

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// installPlugin gives root the on-disk shape of an installed plugin: a version
// directory holding the binary, plus the `current` symlink pointing at it.
func installPlugin(t *testing.T, root, name string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(VersionDir(root, name, 1), 0o755))
	require.NoError(t, os.WriteFile(BinaryPath(root, name, 1), []byte("#!/bin/sh\n"), 0o755))
	require.NoError(t, os.Symlink(BinaryPath(root, name, 1), CurrentLinkPath(root, name)))
}

// TestValidatePluginName pins the name grammar: one lowercase OCI path
// component. Anything that could leave the plugins root or change a registry
// route is rejected.
func TestValidatePluginName(t *testing.T) {
	valid := []string{"stronghold", "postgresql-mgr", "db_connector", "tool.v2", "a1"}
	for _, name := range valid {
		assert.NoError(t, ValidatePluginName(name), name)
	}

	invalid := []string{
		"",
		"..",
		"../../outside",
		"a/b",
		"Stronghold",
		"-leading",
		"trailing-",
		"double--dash",
		"with space",
		"tag:v1",
		"name@v1",
	}
	for _, name := range invalid {
		assert.Error(t, ValidatePluginName(name), name)
	}
}

// TestInstalledNames pins what counts as installed: a directory under
// <root>/plugins carrying a `current` symlink. A leftover version directory from a
// failed install has no symlink, so it must not be reported - the root command
// would otherwise hand the command to a plugin that has no binary to run.
func TestInstalledNames(t *testing.T) {
	root := t.TempDir()

	installPlugin(t, root, "stronghold")
	installPlugin(t, root, "system")

	// Leftover from a failed install: a version directory, but no `current` symlink.
	require.NoError(t, os.MkdirAll(VersionDir(root, "halfway", 1), 0o755))

	// A stray file in the plugins root is not a plugin either.
	require.NoError(t, os.WriteFile(path.Join(PluginsRoot(root), "README"), nil, 0o644))

	names, err := InstalledNames(root)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"stronghold", "system"}, names)
	assert.True(t, RootHasInstall(root))
}

func TestInstalledNamesMissingRoot(t *testing.T) {
	root := t.TempDir() // no plugins/ subdirectory at all

	_, err := InstalledNames(root)
	assert.Error(t, err)
	assert.False(t, RootHasInstall(root))
}

// TestResolveInstalledPrefersConfiguredRoot: the home fallback is consulted only
// when the configured root holds nothing, never in preference to it.
func TestResolveInstalledPrefersConfiguredRoot(t *testing.T) {
	configured, home := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)

	installPlugin(t, configured, "system")
	installPlugin(t, path.Join(home, ".deckhouse-cli"), "stronghold")

	root, names, ok := ResolveInstalled(configured)

	require.True(t, ok)
	assert.Equal(t, configured, root)
	assert.Equal(t, []string{"system"}, names)
}

// TestResolveInstalledFallsBackToHome covers the unwritable-default case: installs
// land in ~/.deckhouse-cli, and looking only at the configured root would miss them.
func TestResolveInstalledFallsBackToHome(t *testing.T) {
	configured, home := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)

	fallback := path.Join(home, ".deckhouse-cli")
	installPlugin(t, fallback, "stronghold")

	root, names, ok := ResolveInstalled(configured)

	require.True(t, ok)
	assert.Equal(t, fallback, root)
	assert.Equal(t, []string{"stronghold"}, names)
}

func TestResolveInstalledNothingInstalled(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	root, names, ok := ResolveInstalled(t.TempDir())

	assert.False(t, ok)
	assert.Empty(t, root)
	assert.Nil(t, names)
}
