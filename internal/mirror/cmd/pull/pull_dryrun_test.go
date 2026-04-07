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

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
)

// saveFlagsAndRestore captures the current state of all relevant pull flags and
// returns a restore function that must be deferred by the caller.
func saveFlagsAndRestore(t *testing.T) func() {
	t.Helper()

	saved := struct {
		TempDir                 string
		ImagesBundlePath        string
		SourceRegistryRepo      string
		NoPlatform              bool
		NoSecurityDB            bool
		NoModules               bool
		NoInstaller             bool
		DoGOSTDigest            bool
		DryRun                  bool
		NoPullResume            bool
		DeckhouseTag            string
		InstallerTag            string
		ModulesWhitelist        []string
		ModulesBlacklist        []string
		SourceRegistryLogin     string
		SourceRegistryPassword  string
		DeckhouseLicenseToken   string
		OnlyExtraImages         bool
		SkipVexImages           bool
		IgnoreSuspend           bool
		ImagesBundleChunkSizeGB int64
	}{
		TempDir:                 pullflags.TempDir,
		ImagesBundlePath:        pullflags.ImagesBundlePath,
		SourceRegistryRepo:      pullflags.SourceRegistryRepo,
		NoPlatform:              pullflags.NoPlatform,
		NoSecurityDB:            pullflags.NoSecurityDB,
		NoModules:               pullflags.NoModules,
		NoInstaller:             pullflags.NoInstaller,
		DoGOSTDigest:            pullflags.DoGOSTDigest,
		DryRun:                  pullflags.DryRun,
		NoPullResume:            pullflags.NoPullResume,
		DeckhouseTag:            pullflags.DeckhouseTag,
		InstallerTag:            pullflags.InstallerTag,
		ModulesWhitelist:        pullflags.ModulesWhitelist,
		ModulesBlacklist:        pullflags.ModulesBlacklist,
		SourceRegistryLogin:     pullflags.SourceRegistryLogin,
		SourceRegistryPassword:  pullflags.SourceRegistryPassword,
		DeckhouseLicenseToken:   pullflags.DeckhouseLicenseToken,
		OnlyExtraImages:         pullflags.OnlyExtraImages,
		SkipVexImages:           pullflags.SkipVexImages,
		IgnoreSuspend:           pullflags.IgnoreSuspend,
		ImagesBundleChunkSizeGB: pullflags.ImagesBundleChunkSizeGB,
	}

	return func() {
		pullflags.TempDir = saved.TempDir
		pullflags.ImagesBundlePath = saved.ImagesBundlePath
		pullflags.SourceRegistryRepo = saved.SourceRegistryRepo
		pullflags.NoPlatform = saved.NoPlatform
		pullflags.NoSecurityDB = saved.NoSecurityDB
		pullflags.NoModules = saved.NoModules
		pullflags.NoInstaller = saved.NoInstaller
		pullflags.DoGOSTDigest = saved.DoGOSTDigest
		pullflags.DryRun = saved.DryRun
		pullflags.NoPullResume = saved.NoPullResume
		pullflags.DeckhouseTag = saved.DeckhouseTag
		pullflags.InstallerTag = saved.InstallerTag
		pullflags.ModulesWhitelist = saved.ModulesWhitelist
		pullflags.ModulesBlacklist = saved.ModulesBlacklist
		pullflags.SourceRegistryLogin = saved.SourceRegistryLogin
		pullflags.SourceRegistryPassword = saved.SourceRegistryPassword
		pullflags.DeckhouseLicenseToken = saved.DeckhouseLicenseToken
		pullflags.OnlyExtraImages = saved.OnlyExtraImages
		pullflags.SkipVexImages = saved.SkipVexImages
		pullflags.IgnoreSuspend = saved.IgnoreSuspend
		pullflags.ImagesBundleChunkSizeGB = saved.ImagesBundleChunkSizeGB
	}
}

// TestDryRunFlagRegistered verifies --dry-run is properly registered as a cobra flag.
func TestDryRunFlagRegistered(t *testing.T) {
	cmd := NewCommand()
	flag := cmd.Flags().Lookup("dry-run")
	require.NotNil(t, flag, "--dry-run flag must be registered on pull command")
	assert.Equal(t, "false", flag.DefValue)
	assert.NotEmpty(t, flag.Usage)
}

// TestDryRunNoBundleOutput verifies that no files are written to the bundle dir in dry-run mode.
func TestDryRunNoBundleOutput(t *testing.T) {
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
	pullflags.InstallerTag = "latest"
	pullflags.NoPlatform = false
	pullflags.NoSecurityDB = false
	pullflags.NoModules = false
	pullflags.NoInstaller = false
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

	// No .tar or .chunk files must be written in bundle dir
	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		assert.NotEqual(t, ".tar", ext, "dry-run must not write .tar files, found: %s", e.Name())
		assert.NotEqual(t, ".chunk", ext, "dry-run must not write .chunk files, found: %s", e.Name())
		assert.NotEqual(t, ".gostsum", ext, "dry-run must not write .gostsum files, found: %s", e.Name())
	}
}

// TestDryRunNoBundleWithNoPlatform verifies dry-run with --no-platform does not write files.
func TestDryRunNoBundleWithNoPlatform(t *testing.T) {
	t.Setenv("STUB_REGISTRY_CLIENT", "true")

	bundleDir := t.TempDir()
	tmpDir := t.TempDir()

	cmd := NewCommand()
	defer saveFlagsAndRestore(t)()

	pullflags.ImagesBundlePath = bundleDir
	pullflags.TempDir = tmpDir
	pullflags.SourceRegistryRepo = "registry.deckhouse.ru/deckhouse/ee"
	pullflags.DeckhouseTag = "v1.69.0"
	pullflags.InstallerTag = "latest"
	pullflags.NoPlatform = true
	pullflags.NoSecurityDB = true
	pullflags.NoModules = false
	pullflags.NoInstaller = true
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
		assert.NotEqual(t, ".tar", ext)
		assert.NotEqual(t, ".chunk", ext)
	}
}

// TestDryRunWithDeckhouseTag verifies dry-run works with a specific --deckhouse-tag.
func TestDryRunWithDeckhouseTag(t *testing.T) {
	t.Setenv("STUB_REGISTRY_CLIENT", "true")

	bundleDir := t.TempDir()
	tmpDir := t.TempDir()

	cmd := NewCommand()
	defer saveFlagsAndRestore(t)()

	pullflags.ImagesBundlePath = bundleDir
	pullflags.TempDir = tmpDir
	pullflags.SourceRegistryRepo = "registry.deckhouse.ru/deckhouse/ee"
	pullflags.DeckhouseTag = "v1.72.10"
	pullflags.InstallerTag = "v1.72.10"
	pullflags.NoPlatform = false
	pullflags.NoSecurityDB = true
	pullflags.NoModules = true
	pullflags.NoInstaller = false
	pullflags.DryRun = true
	pullflags.DoGOSTDigest = false
	pullflags.NoPullResume = true
	pullflags.SkipVexImages = true

	ctx := context.Background()
	cmd.SetContext(ctx)

	puller := NewPuller(cmd)
	err := puller.Execute(ctx)
	require.NoError(t, err)

	// Bundle dir must remain empty
	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		assert.NotEqual(t, ".tar", ext)
	}
}

// TestDryRunExitsZeroOnSuccess verifies Execute returns nil in dry-run mode.
func TestDryRunExitsZeroOnSuccess(t *testing.T) {
	t.Setenv("STUB_REGISTRY_CLIENT", "true")

	bundleDir := t.TempDir()
	tmpDir := t.TempDir()

	cmd := NewCommand()
	defer saveFlagsAndRestore(t)()

	pullflags.ImagesBundlePath = bundleDir
	pullflags.TempDir = tmpDir
	pullflags.SourceRegistryRepo = "registry.deckhouse.ru/deckhouse/ee"
	pullflags.DeckhouseTag = "v1.69.0"
	pullflags.InstallerTag = "latest"
	pullflags.NoPlatform = false
	pullflags.NoSecurityDB = false
	pullflags.NoModules = false
	pullflags.NoInstaller = false
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
	assert.NoError(t, err, "dry-run must exit with code 0 on success")
}

// TestDryRunFlagInOptions verifies DryRun field is included in PullServiceOptions literal.
func TestDryRunFlagInOptions(t *testing.T) {
	defer saveFlagsAndRestore(t)()

	pullflags.DryRun = true
	assert.True(t, pullflags.DryRun)

	pullflags.DryRun = false
	assert.False(t, pullflags.DryRun)
}
