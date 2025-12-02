/*
Copyright 2025 Flant JSC

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
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
)

func TestNewCommand(t *testing.T) {
	cmd := NewCommand()

	assert.Equal(t, "pull <images-bundle-path>", cmd.Use)
	assert.Equal(t, "Copy Deckhouse Kubernetes Platform distribution to the local filesystem", cmd.Short)
	assert.Contains(t, cmd.Long, "Download Deckhouse Kubernetes Platform distribution")
	assert.Equal(t, []string{"images-bundle-path"}, cmd.ValidArgs)
	assert.True(t, cmd.SilenceErrors)
	assert.True(t, cmd.SilenceUsage)
	assert.NotNil(t, cmd.PreRunE)
	assert.NotNil(t, cmd.RunE)
	assert.NotNil(t, cmd.Flags())
}

func TestEnterpriseEditionRepo(t *testing.T) {
	assert.Equal(t, "registry.deckhouse.ru/deckhouse/ee", pullflags.EnterpriseEditionRepo)
	assert.Equal(t, pullflags.EnterpriseEditionRepo, pullflags.SourceRegistryRepo)
}

func TestGlobalVariableDefaults(t *testing.T) {
	// Reset to defaults for testing
	originalSourceRegistryRepo := pullflags.SourceRegistryRepo
	defer func() { pullflags.SourceRegistryRepo = originalSourceRegistryRepo }()

	pullflags.SourceRegistryRepo = pullflags.EnterpriseEditionRepo

	assert.Equal(t, pullflags.EnterpriseEditionRepo, pullflags.SourceRegistryRepo)
	assert.Equal(t, "/modules", pullflags.ModulesPathSuffix)
}

func TestErrorMessages(t *testing.T) {
	err := ErrPullFailed
	assert.Equal(t, "pull failed, see the log for details", err.Error())
}

func TestConfigFromFlags(t *testing.T) {
	// Save original flags
	originalTempDir := pullflags.TempDir
	originalImagesBundlePath := pullflags.ImagesBundlePath
	originalSourceRegistryRepo := pullflags.SourceRegistryRepo
	originalInsecure := pullflags.Insecure
	originalTLSSkipVerify := pullflags.TLSSkipVerify
	originalDoGOSTDigest := pullflags.DoGOSTDigest
	originalNoPlatform := pullflags.NoPlatform
	originalNoSecurityDB := pullflags.NoSecurityDB
	originalNoModules := pullflags.NoModules
	originalOnlyExtraImages := pullflags.OnlyExtraImages
	originalDeckhouseTag := pullflags.DeckhouseTag
	originalSinceVersion := pullflags.SinceVersion
	originalImagesBundleChunkSizeGB := pullflags.ImagesBundleChunkSizeGB

	defer func() {
		pullflags.TempDir = originalTempDir
		pullflags.ImagesBundlePath = originalImagesBundlePath
		pullflags.SourceRegistryRepo = originalSourceRegistryRepo
		pullflags.Insecure = originalInsecure
		pullflags.TLSSkipVerify = originalTLSSkipVerify
		pullflags.DoGOSTDigest = originalDoGOSTDigest
		pullflags.NoPlatform = originalNoPlatform
		pullflags.NoSecurityDB = originalNoSecurityDB
		pullflags.NoModules = originalNoModules
		pullflags.OnlyExtraImages = originalOnlyExtraImages
		pullflags.DeckhouseTag = originalDeckhouseTag
		pullflags.SinceVersion = originalSinceVersion
		pullflags.ImagesBundleChunkSizeGB = originalImagesBundleChunkSizeGB
	}()

	// Set test values
	pullflags.TempDir = "/tmp/test"
	pullflags.ImagesBundlePath = "/tmp/bundle"
	pullflags.SourceRegistryRepo = "registry.example.com/repo"
	pullflags.Insecure = true
	pullflags.TLSSkipVerify = true
	pullflags.DoGOSTDigest = true
	pullflags.NoPlatform = true
	pullflags.NoSecurityDB = true
	pullflags.NoModules = true
	pullflags.OnlyExtraImages = true
	pullflags.DeckhouseTag = "v1.57.3"
	pullflags.SinceVersion = semver.MustParse("1.56.0")
	pullflags.ImagesBundleChunkSizeGB = 5

	config, err := NewConfigFromFlags()
	require.NoError(t, err)
	require.NotNil(t, config)

	// Verify registry config
	assert.Equal(t, "registry.example.com/repo", config.Registry.URL)
	assert.True(t, config.Registry.Insecure)
	assert.True(t, config.Registry.SkipTLSVerify)

	// Verify bundle config
	assert.Equal(t, "/tmp/bundle", config.BundleDir)
	assert.Equal(t, int64(5*1000*1000*1000), config.BundleChunkSize)

	// Verify skip options
	assert.True(t, config.SkipPlatform)
	assert.True(t, config.SkipSecurity)
	assert.True(t, config.SkipModules)
	assert.True(t, config.OnlyExtraImages)
	assert.True(t, config.DoGOSTDigests)
	assert.Equal(t, "v1.57.3", config.TargetTag)
	assert.Equal(t, "1.56.0", config.SinceVersion.String())

	// Verify working dir
	assert.Equal(t, "/tmp/test", config.WorkingDir)
}

func TestConfigToPullOpts(t *testing.T) {
	sinceVersion := semver.MustParse("1.56.0")

	config := &Config{
		Registry: RegistryConfig{
			URL:           "registry.example.com/repo",
			Insecure:      true,
			SkipTLSVerify: true,
		},
		BundleDir:       "/tmp/bundle",
		BundleChunkSize: 5 * 1024 * 1024 * 1024,
		WorkingDir:      "/tmp/work",
		SkipPlatform:    true,
		SkipSecurity:    true,
		SkipModules:     true,
		OnlyExtraImages: true,
		DoGOSTDigests:   true,
		TargetTag:       "v1.57.3",
		SinceVersion:    sinceVersion,
	}

	opts := config.ToPullOpts()
	require.NotNil(t, opts)

	assert.Equal(t, "/tmp/bundle", opts.BundleDir)
	assert.Equal(t, "/tmp/work", opts.WorkingDir)
	assert.Equal(t, int64(5*1024*1024*1024), opts.BundleChunkSize)
	assert.True(t, opts.SkipPlatform)
	assert.True(t, opts.SkipSecurity)
	assert.True(t, opts.SkipModules)
	assert.True(t, opts.OnlyExtraImages)
	assert.True(t, opts.DoGOSTDigests)
	assert.Equal(t, "v1.57.3", opts.TargetTag)
	assert.Equal(t, "1.56.0", opts.SinceVersion.String())
}

func TestNewRunner(t *testing.T) {
	// Save original flags
	originalTempDir := pullflags.TempDir
	originalImagesBundlePath := pullflags.ImagesBundlePath
	originalSourceRegistryRepo := pullflags.SourceRegistryRepo

	defer func() {
		pullflags.TempDir = originalTempDir
		pullflags.ImagesBundlePath = originalImagesBundlePath
		pullflags.SourceRegistryRepo = originalSourceRegistryRepo
	}()

	// Set test values
	pullflags.TempDir = t.TempDir()
	pullflags.ImagesBundlePath = t.TempDir()
	pullflags.SourceRegistryRepo = "registry.example.com/repo"

	runner, err := NewRunner()
	require.NoError(t, err)
	require.NotNil(t, runner)
	require.NotNil(t, runner.config)
	require.NotNil(t, runner.opts)
	require.NotNil(t, runner.logger)
}

func TestCreateLogger(t *testing.T) {
	logger := createLogger()
	assert.NotNil(t, logger)
}

func TestCommandFlags(t *testing.T) {
	cmd := NewCommand()
	flags := cmd.Flags()

	// Verify flags are registered
	assert.NotNil(t, flags.Lookup("source"))
	assert.NotNil(t, flags.Lookup("license"))
	assert.NotNil(t, flags.Lookup("images-bundle-chunk-size"))
	assert.NotNil(t, flags.Lookup("no-pull-resume"))
	assert.NotNil(t, flags.Lookup("gost-digest"))
	assert.NotNil(t, flags.Lookup("since-version"))
	assert.NotNil(t, flags.Lookup("deckhouse-tag"))
	assert.NotNil(t, flags.Lookup("no-modules"))
	assert.NotNil(t, flags.Lookup("no-platform"))
	assert.NotNil(t, flags.Lookup("no-security-db"))
}

func TestRegistryConfigDefaults(t *testing.T) {
	// Save original flags
	originalSourceRegistryRepo := pullflags.SourceRegistryRepo
	originalSourceRegistryLogin := pullflags.SourceRegistryLogin
	originalSourceRegistryPassword := pullflags.SourceRegistryPassword
	originalDeckhouseLicenseToken := pullflags.DeckhouseLicenseToken

	defer func() {
		pullflags.SourceRegistryRepo = originalSourceRegistryRepo
		pullflags.SourceRegistryLogin = originalSourceRegistryLogin
		pullflags.SourceRegistryPassword = originalSourceRegistryPassword
		pullflags.DeckhouseLicenseToken = originalDeckhouseLicenseToken
	}()

	// Test with no auth
	pullflags.SourceRegistryRepo = "test-registry"
	pullflags.SourceRegistryLogin = ""
	pullflags.SourceRegistryPassword = ""
	pullflags.DeckhouseLicenseToken = ""

	config, err := NewConfigFromFlags()
	require.NoError(t, err)
	assert.Equal(t, "test-registry", config.Registry.URL)
	assert.NotNil(t, config.Registry.Auth) // Should be Anonymous

	// Test with login/password
	pullflags.SourceRegistryLogin = "user"
	pullflags.SourceRegistryPassword = "pass"

	config, err = NewConfigFromFlags()
	require.NoError(t, err)
	assert.NotNil(t, config.Registry.Auth)

	// Test with license token
	pullflags.SourceRegistryLogin = ""
	pullflags.SourceRegistryPassword = ""
	pullflags.DeckhouseLicenseToken = "token123"

	config, err = NewConfigFromFlags()
	require.NoError(t, err)
	assert.NotNil(t, config.Registry.Auth)
}
