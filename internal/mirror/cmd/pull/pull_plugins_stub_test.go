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

package pull

// Command-level plugin tests: the real `d8 mirror pull` path through
// Puller.Execute against the in-memory registry stub, which carries the
// cert-manager module and the cert-manager-tool plugin whose contract
// requires it.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
)

// TestPullerExecute_PluginsTarInBundle runs a real (non-dry-run) pull of
// modules and plugins: the module-driven plugin selection must produce a
// plugin tar next to the module tar in the bundle dir.
func TestPullerExecute_PluginsTarInBundle(t *testing.T) {
	t.Setenv("STUB_REGISTRY_CLIENT", "true")

	bundleDir := t.TempDir()
	tmpDir := t.TempDir()

	// NewCommand calls AddFlags which resets all flag vars to defaults; set flags after.
	cmd := NewCommand()
	defer saveFlagsAndRestore(t)()

	pullflags.ImagesBundlePath = bundleDir
	pullflags.TempDir = tmpDir
	pullflags.SourceRegistryRepo = "registry.deckhouse.ru/deckhouse/ee"
	pullflags.DeckhouseTag = "v1.69.0"
	pullflags.NoPlatform = true
	pullflags.NoSecurityDB = true
	pullflags.NoInstaller = true
	pullflags.NoModules = false
	pullflags.DryRun = false
	pullflags.DoGOSTDigest = false
	pullflags.NoPullResume = true
	pullflags.SkipVexImages = true
	pullflags.ModulesWhitelist = nil
	pullflags.ModulesBlacklist = nil

	ctx := context.Background()
	cmd.SetContext(ctx)

	puller := NewPuller(cmd)
	err := puller.Execute(ctx)
	require.NoError(t, err)

	assert.FileExists(t, filepath.Join(bundleDir, "module-cert-manager.tar"))
	assert.FileExists(t, filepath.Join(bundleDir, "plugin-cert-manager-tool.tar"),
		"the plugin selected for the mirrored module must land in the bundle")
}

// TestPullerExecute_DryRun_PluginsNoFiles: the same pull in dry-run mode
// resolves the plugin but writes nothing.
func TestPullerExecute_DryRun_PluginsNoFiles(t *testing.T) {
	t.Setenv("STUB_REGISTRY_CLIENT", "true")

	bundleDir := t.TempDir()
	tmpDir := t.TempDir()

	cmd := NewCommand()
	defer saveFlagsAndRestore(t)()

	pullflags.ImagesBundlePath = bundleDir
	pullflags.TempDir = tmpDir
	pullflags.SourceRegistryRepo = "registry.deckhouse.ru/deckhouse/ee"
	pullflags.DeckhouseTag = "v1.69.0"
	pullflags.NoPlatform = true
	pullflags.NoSecurityDB = true
	pullflags.NoInstaller = true
	pullflags.NoModules = false
	pullflags.DryRun = true
	pullflags.DoGOSTDigest = false
	pullflags.NoPullResume = true
	pullflags.SkipVexImages = true
	pullflags.ModulesWhitelist = nil
	pullflags.ModulesBlacklist = nil

	ctx := context.Background()
	cmd.SetContext(ctx)

	puller := NewPuller(cmd)
	err := puller.Execute(ctx)
	require.NoError(t, err)

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)

	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		assert.NotEqual(t, ".tar", ext, "dry-run must not write .tar files, found: %s", e.Name())
		assert.NotEqual(t, ".chunk", ext, "dry-run must not write .chunk files, found: %s", e.Name())
	}
}
