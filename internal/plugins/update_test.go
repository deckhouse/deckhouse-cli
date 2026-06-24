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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/layout"
)

func TestUpdateAllSkipsGhostDirsWithoutInstall(t *testing.T) {
	// Isolate the home fallback so a real ~/.deckhouse-cli does not leak in.
	t.Setenv("HOME", t.TempDir())

	root := t.TempDir()
	m := testManager()
	m.pluginDirectory = root

	src := &fakeInstallSource{
		contract: &internal.Plugin{Name: "ghost", Version: "v1.0.0"},
		extract:  func(dest string) error { return os.WriteFile(dest, []byte("x"), 0o755) },
	}
	m.service = src

	// A ghost dir left by a failed install: a version dir exists but there is no
	// `current` symlink. It is NOT an installed plugin and must not become a fresh
	// install of something the user never had.
	require.NoError(t, os.MkdirAll(layout.VersionDir(root, "ghost", 1), 0o755))

	require.NoError(t, m.UpdateAll(context.Background()), "a ghost dir is skipped, not treated as an update failure")
	assert.Empty(t, src.listedTags, "no install was attempted for a plugin that is not installed")
}

func TestInstalledPluginNames(t *testing.T) {
	root := t.TempDir()
	m := testManager()
	m.pluginDirectory = root

	installPluginFixture(t, root, "real", 1)
	require.NoError(t, os.MkdirAll(layout.VersionDir(root, "ghost", 1), 0o755))

	names, err := m.InstalledPluginNames()
	require.NoError(t, err)
	assert.Equal(t, []string{"real"}, names, "only plugins with a current symlink count as installed")
}

func TestUpdateAllFallsBackToHomeInstallRoot(t *testing.T) {
	// A non-root install lives in ~/.deckhouse-cli while the configured root is
	// empty; `d8 plugins update all` runs against the configured root and must
	// still find (and update) the fallback install.
	t.Setenv("HOME", t.TempDir())

	fallback, err := layout.HomeFallbackPath()
	require.NoError(t, err)
	installPluginFixture(t, fallback, "homeplugin", 1)

	m := testManager()
	m.pluginDirectory = t.TempDir()

	src := &fakeInstallSource{
		contract: &internal.Plugin{Name: "homeplugin", Version: "v1.0.0"},
		tags:     []string{"v1.0.0"},
		contractByTag: map[string]*internal.Plugin{
			"v1.0.0": {Name: "homeplugin", Version: "v1.0.0"},
		},
		extract: func(dest string) error { return os.WriteFile(dest, []byte("#!/bin/sh\necho v1.0.0\n"), 0o755) },
	}
	m.service = src

	require.NoError(t, m.UpdateAll(context.Background()))
	assert.Equal(t, []string{"homeplugin"}, src.listedTags, "the fallback install is an update target")
	assert.Equal(t, fallback, m.pluginDirectory, "the run switched to the fallback root")
}
