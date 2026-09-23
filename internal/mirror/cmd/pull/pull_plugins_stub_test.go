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

// Command-level tests for the d8 CLI distribution: the real `d8 mirror pull`
// path through Puller.Execute against the in-memory registry stub, which
// carries the cert-manager module, the cert-manager-tool plugin whose
// contract requires it, and the d8 binary itself.

import (
	"archive/tar"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	assert.FileExists(t, filepath.Join(bundleDir, "deckhouse-cli.tar"),
		"the d8 binary travels with the plugins that extend it")
}

// TestPullerExecute_CLITagPinned: --deckhouse-cli-tag reaches the phase from
// the flag set and mirrors the named version rather than the newest one.
func TestPullerExecute_CLITagPinned(t *testing.T) {
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
	pullflags.NoModules = true
	pullflags.DryRun = false
	pullflags.DoGOSTDigest = false
	pullflags.NoPullResume = true
	pullflags.SkipVexImages = true
	pullflags.DeckhouseCLITag = "v0.13.0"

	ctx := context.Background()
	cmd.SetContext(ctx)

	require.NoError(t, NewPuller(cmd).Execute(ctx))

	tarPath := filepath.Join(bundleDir, "deckhouse-cli.tar")
	require.FileExists(t, tarPath)
	assert.Equal(t, []string{"v0.13.0"}, bundleTarShortTags(t, tarPath),
		"the pinned version is the one that must land in the bundle")
}

// bundleTarShortTags reads a bundle tar and returns the
// io.deckhouse.image.short_tag annotations of its OCI index - the version tags
// that actually made it in.
func bundleTarShortTags(t *testing.T, tarPath string) []string {
	t.Helper()

	f, err := os.Open(tarPath)
	require.NoError(t, err)

	defer func() { require.NoError(t, f.Close()) }()

	var indexJSON []byte

	tr := tar.NewReader(f)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		if strings.HasSuffix(header.Name, "/index.json") {
			indexJSON, err = io.ReadAll(tr)
			require.NoError(t, err)
		}
	}

	require.NotEmpty(t, indexJSON, "the bundle tar must carry an OCI index")

	var index struct {
		Manifests []struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"manifests"`
	}
	require.NoError(t, json.Unmarshal(indexJSON, &index))

	tags := make([]string, 0, len(index.Manifests))
	for _, m := range index.Manifests {
		tags = append(tags, m.Annotations["io.deckhouse.image.short_tag"])
	}

	return tags
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
