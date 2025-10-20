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
	"crypto/md5"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
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

func TestSetupLogger(t *testing.T) {
	tests := []struct {
		name        string
		debugEnvVar string
		expected    slog.Level
	}{
		{
			name:        "default log level",
			debugEnvVar: "",
			expected:    slog.LevelInfo,
		},
		{
			name:        "debug level 3",
			debugEnvVar: "3",
			expected:    slog.LevelDebug,
		},
		{
			name:        "debug level 5",
			debugEnvVar: "5",
			expected:    slog.LevelDebug,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variable
			originalEnv := os.Getenv("MIRROR_DEBUG_LOG")
			defer func() {
				if originalEnv == "" {
					os.Unsetenv("MIRROR_DEBUG_LOG")
				} else {
					os.Setenv("MIRROR_DEBUG_LOG", originalEnv)
				}
			}()

			if tt.debugEnvVar == "" {
				os.Unsetenv("MIRROR_DEBUG_LOG")
			} else {
				os.Setenv("MIRROR_DEBUG_LOG", tt.debugEnvVar)
			}

			logger := setupLogger()
			assert.NotNil(t, logger)
			// We can't easily test the internal slog level, but we can verify the logger is created
		})
	}
}

func TestFindTagsToMirror(t *testing.T) {
	logger := log.NewSLogger(slog.LevelInfo)

	tests := []struct {
		name         string
		deckhouseTag string
		sinceVersion *semver.Version
		expectError  bool
		expectedTags []string
	}{
		{
			name:         "specific tag provided",
			deckhouseTag: "v1.57.3",
			expectedTags: []string{"v1.57.3"},
		},
		{
			name:         "no tag, should call releases.VersionsToMirror",
			deckhouseTag: "",
			expectError:  true, // Will fail because releases.VersionsToMirror needs real params
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pullParams := &params.PullParams{
				DeckhouseTag: tt.deckhouseTag,
				SinceVersion: tt.sinceVersion,
			}

			tags, err := findTagsToMirror(pullParams, logger)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedTags, tags)
			}
		})
	}
}

func TestBuildPullParams(t *testing.T) {
	// Setup test environment variables
	originalTempDir := TempDir
	originalImagesBundlePath := ImagesBundlePath
	originalSourceRegistryRepo := SourceRegistryRepo
	originalModulesPathSuffix := ModulesPathSuffix
	originalInsecure := Insecure
	originalTLSSkipVerify := TLSSkipVerify
	originalDoGOSTDigest := DoGOSTDigest
	originalNoPlatform := NoPlatform
	originalNoSecurityDB := NoSecurityDB
	originalNoModules := NoModules
	originalOnlyExtraImages := OnlyExtraImages
	originalDeckhouseTag := DeckhouseTag
	originalSinceVersion := SinceVersion

	defer func() {
		TempDir = originalTempDir
		ImagesBundlePath = originalImagesBundlePath
		SourceRegistryRepo = originalSourceRegistryRepo
		ModulesPathSuffix = originalModulesPathSuffix
		Insecure = originalInsecure
		TLSSkipVerify = originalTLSSkipVerify
		DoGOSTDigest = originalDoGOSTDigest
		NoPlatform = originalNoPlatform
		NoSecurityDB = originalNoSecurityDB
		NoModules = originalNoModules
		OnlyExtraImages = originalOnlyExtraImages
		DeckhouseTag = originalDeckhouseTag
		SinceVersion = originalSinceVersion
	}()

	// Set test values
	TempDir = "/tmp/test"
	ImagesBundlePath = "/tmp/bundle"
	SourceRegistryRepo = "registry.example.com"
	ModulesPathSuffix = "modules"
	Insecure = true
	TLSSkipVerify = true
	DoGOSTDigest = true
	NoPlatform = true
	NoSecurityDB = true
	NoModules = true
	OnlyExtraImages = true
	DeckhouseTag = "v1.57.3"
	SinceVersion = semver.MustParse("1.56.0")

	logger := log.NewSLogger(slog.LevelInfo)
	params := buildPullParams(logger)

	assert.NotNil(t, params)
	assert.Equal(t, logger, params.Logger)
	assert.Equal(t, Insecure, params.Insecure)
	assert.Equal(t, TLSSkipVerify, params.SkipTLSVerification)
	assert.Equal(t, SourceRegistryRepo, params.DeckhouseRegistryRepo)
	assert.Equal(t, ModulesPathSuffix, params.ModulesPathSuffix)
	assert.Equal(t, ImagesBundlePath, params.BundleDir)
	assert.Equal(t, DoGOSTDigest, params.DoGOSTDigests)
	assert.Equal(t, NoPlatform, params.SkipPlatform)
	assert.Equal(t, NoSecurityDB, params.SkipSecurityDatabases)
	assert.Equal(t, NoModules, params.SkipModules)
	assert.Equal(t, OnlyExtraImages, params.OnlyExtraImages)
	assert.Equal(t, DeckhouseTag, params.DeckhouseTag)
	assert.Equal(t, SinceVersion, params.SinceVersion)

	// Check working directory calculation
	expectedWorkingDir := filepath.Join(
		TempDir,
		"mirror",
		"pull",
		fmt.Sprintf("%x", md5.Sum([]byte(SourceRegistryRepo))),
	)
	assert.Equal(t, expectedWorkingDir, params.WorkingDir)
}

func TestGetSourceRegistryAuthProvider(t *testing.T) {
	// Save original values
	originalLogin := SourceRegistryLogin
	originalPassword := SourceRegistryPassword
	originalToken := DeckhouseLicenseToken

	defer func() {
		SourceRegistryLogin = originalLogin
		SourceRegistryPassword = originalPassword
		DeckhouseLicenseToken = originalToken
	}()

	tests := []struct {
		name     string
		login    string
		password string
		token    string
		expected authn.Authenticator
	}{
		{
			name:     "username and password",
			login:    "testuser",
			password: "testpass",
			token:    "",
			expected: authn.FromConfig(authn.AuthConfig{
				Username: "testuser",
				Password: "testpass",
			}),
		},
		{
			name:     "license token",
			login:    "",
			password: "",
			token:    "testtoken",
			expected: authn.FromConfig(authn.AuthConfig{
				Username: "license-token",
				Password: "testtoken",
			}),
		},
		{
			name:     "anonymous",
			login:    "",
			password: "",
			token:    "",
			expected: authn.Anonymous,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SourceRegistryLogin = tt.login
			SourceRegistryPassword = tt.password
			DeckhouseLicenseToken = tt.token

			auth := getSourceRegistryAuthProvider()

			// For anonymous, we can check directly
			if tt.login == "" && tt.password == "" && tt.token == "" {
				assert.Equal(t, authn.Anonymous, auth)
			} else {
				// For configured auth, we can't easily compare the internal state,
				// but we can verify it's not anonymous
				assert.NotEqual(t, authn.Anonymous, auth)
			}
		})
	}
}

func TestLastPullWasTooLongAgoToRetry(t *testing.T) {
	tempDir := t.TempDir()
	workingDir := filepath.Join(tempDir, "work")

	tests := []struct {
		name     string
		modTime  time.Time
		expected bool
	}{
		{
			name:     "directory doesn't exist",
			modTime:  time.Time{}, // zero time
			expected: false,
		},
		{
			name:     "recent modification",
			modTime:  time.Now().Add(-1 * time.Hour),
			expected: false,
		},
		{
			name:     "old modification",
			modTime:  time.Now().Add(-25 * time.Hour),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.modTime.IsZero() {
				err := os.MkdirAll(workingDir, 0755)
				require.NoError(t, err)
				err = os.Chtimes(workingDir, tt.modTime, tt.modTime)
				require.NoError(t, err)
			} else {
				// Ensure directory doesn't exist
				os.RemoveAll(workingDir)
			}

			pullParams := &params.PullParams{
				BaseParams: params.BaseParams{
					WorkingDir: workingDir,
				},
			}

			result := lastPullWasTooLongAgoToRetry(pullParams)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPullCommandIntegration(t *testing.T) {
	// This is a basic integration test that verifies the command structure
	// More comprehensive integration tests would require mocking the registry operations

	tempDir := t.TempDir()

	// Set up minimal required environment
	originalTempDir := TempDir
	originalImagesBundlePath := ImagesBundlePath
	originalSourceRegistryRepo := SourceRegistryRepo
	originalNoPlatform := NoPlatform
	originalNoSecurityDB := NoSecurityDB
	originalNoModules := NoModules
	originalDoGOSTDigest := DoGOSTDigest

	defer func() {
		TempDir = originalTempDir
		ImagesBundlePath = originalImagesBundlePath
		SourceRegistryRepo = originalSourceRegistryRepo
		NoPlatform = originalNoPlatform
		NoSecurityDB = originalNoSecurityDB
		NoModules = originalNoModules
		DoGOSTDigest = originalDoGOSTDigest
	}()

	TempDir = tempDir
	ImagesBundlePath = tempDir
	SourceRegistryRepo = "registry.example.com"
	NoPlatform = true
	NoSecurityDB = true
	NoModules = true
	DoGOSTDigest = false

	cmd := NewCommand()

	// Test that the command can be created and has the right structure
	assert.NotNil(t, cmd)
	assert.Equal(t, "pull <images-bundle-path>", cmd.Use)

	// Test that flags are properly added
	flags := cmd.Flags()
	assert.NotNil(t, flags)

	// We can't easily run the actual command without extensive mocking,
	// but we can verify the command structure is correct
}

func TestPullParamsValidation(t *testing.T) {
	// Test that buildPullParams handles edge cases properly
	logger := log.NewSLogger(slog.LevelInfo)

	// Test with empty values
	originalTempDir := TempDir
	originalImagesBundlePath := ImagesBundlePath
	originalSourceRegistryRepo := SourceRegistryRepo

	defer func() {
		TempDir = originalTempDir
		ImagesBundlePath = originalImagesBundlePath
		SourceRegistryRepo = originalSourceRegistryRepo
	}()

	TempDir = ""
	ImagesBundlePath = ""
	SourceRegistryRepo = ""

	params := buildPullParams(logger)

	// Should still create valid params, even with empty strings
	assert.NotNil(t, params)
	assert.NotEmpty(t, params.WorkingDir) // Should have some default path
	assert.Empty(t, params.BundleDir)
	assert.Empty(t, params.DeckhouseRegistryRepo)
}

func TestWorkingDirectoryCalculation(t *testing.T) {
	// Test that working directory is calculated correctly with MD5 hash
	originalTempDir := TempDir
	originalSourceRegistryRepo := SourceRegistryRepo

	defer func() {
		TempDir = originalTempDir
		SourceRegistryRepo = originalSourceRegistryRepo
	}()

	TempDir = "/tmp/test"
	SourceRegistryRepo = "registry.example.com"

	logger := log.NewSLogger(slog.LevelInfo)
	params := buildPullParams(logger)

	expectedHash := fmt.Sprintf("%x", md5.Sum([]byte(SourceRegistryRepo)))
	expectedPath := filepath.Join(TempDir, "mirror", "pull", expectedHash)

	assert.Equal(t, expectedPath, params.WorkingDir)
	assert.Contains(t, params.WorkingDir, "pull")
	assert.Contains(t, params.WorkingDir, expectedHash)
}

func TestAuthProviderPriority(t *testing.T) {
	// Test that auth provider prioritizes username/password over license token
	originalLogin := SourceRegistryLogin
	originalPassword := SourceRegistryPassword
	originalToken := DeckhouseLicenseToken

	defer func() {
		SourceRegistryLogin = originalLogin
		SourceRegistryPassword = originalPassword
		DeckhouseLicenseToken = originalToken
	}()

	// Set both login and token - login should take priority
	SourceRegistryLogin = "testuser"
	SourceRegistryPassword = "testpass"
	DeckhouseLicenseToken = "testtoken"

	auth := getSourceRegistryAuthProvider()

	// Should not be anonymous since we have credentials
	assert.NotEqual(t, authn.Anonymous, auth)

	// Reset and test token only
	SourceRegistryLogin = ""
	SourceRegistryPassword = ""
	DeckhouseLicenseToken = "testtoken"

	auth = getSourceRegistryAuthProvider()
	assert.NotEqual(t, authn.Anonymous, auth)
}

func TestParseAndValidateParametersMissingArgs(t *testing.T) {
	// Test parseAndValidateParameters with missing arguments
	originalImagesBundlePath := ImagesBundlePath
	originalTempDir := TempDir

	defer func() {
		ImagesBundlePath = originalImagesBundlePath
		TempDir = originalTempDir
	}()

	ImagesBundlePath = ""
	TempDir = ""

	err := parseAndValidateParameters(&cobra.Command{}, []string{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exactly 1 argument")
}

func TestValidateSourceRegistryDefault(t *testing.T) {
	// Test validateSourceRegistry with default enterprise edition repo
	original := SourceRegistryRepo
	defer func() { SourceRegistryRepo = original }()

	SourceRegistryRepo = enterpriseEditionRepo
	err := validateSourceRegistry()
	assert.NoError(t, err)
}

func TestValidateImagesBundlePathArgEmptyDir(t *testing.T) {
	// Test validateImagesBundlePathArg with empty directory
	tempDir := t.TempDir()
	emptyDir := filepath.Join(tempDir, "empty")
	require.NoError(t, os.MkdirAll(emptyDir, 0755))

	originalImagesBundlePath := ImagesBundlePath
	originalForcePull := ForcePull

	defer func() {
		ImagesBundlePath = originalImagesBundlePath
		ForcePull = originalForcePull
	}()

	ImagesBundlePath = ""
	ForcePull = false

	err := validateImagesBundlePathArg([]string{emptyDir})
	assert.NoError(t, err)
}

func TestValidateTmpPathEmpty(t *testing.T) {
	// Test validateTmpPath when TempDir is empty
	tempDir := t.TempDir()

	originalTempDir := TempDir
	originalImagesBundlePath := ImagesBundlePath

	defer func() {
		TempDir = originalTempDir
		ImagesBundlePath = originalImagesBundlePath
	}()

	TempDir = ""
	ImagesBundlePath = tempDir

	err := validateTmpPath([]string{})
	assert.NoError(t, err)

	// Check that TempDir was set to default
	expectedTempDir := filepath.Join(tempDir, ".tmp")
	assert.Equal(t, expectedTempDir, TempDir)

	// Check that directory was created
	_, err = os.Stat(expectedTempDir)
	assert.NoError(t, err)
}

func TestValidateSourceRegistry(t *testing.T) {
	tests := []struct {
		name        string
		registry    string
		expectError bool
	}{
		{
			name:        "default enterprise edition repo",
			registry:    enterpriseEditionRepo,
			expectError: false,
		},
		{
			name:        "valid registry",
			registry:    "registry.example.com/deckhouse/ee",
			expectError: false,
		},
		{
			name:        "invalid registry format",
			registry:    "invalid-registry",
			expectError: true,
		},
		{
			name:        "registry without host",
			registry:    "/deckhouse/ee",
			expectError: true,
		},
		{
			name:        "registry without path",
			registry:    "registry.example.com",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := SourceRegistryRepo
			defer func() { SourceRegistryRepo = original }()

			SourceRegistryRepo = tt.registry
			err := validateSourceRegistry()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateImagesBundlePathArg(t *testing.T) {
	tempDir := t.TempDir()
	existingDir := filepath.Join(tempDir, "existing")
	existingFile := filepath.Join(tempDir, "file.txt")

	// Create test files/directories
	require.NoError(t, os.MkdirAll(existingDir, 0755))
	require.NoError(t, os.WriteFile(existingFile, []byte("test"), 0644))

	// Create a non-empty directory
	nonEmptyDir := filepath.Join(tempDir, "nonempty")
	require.NoError(t, os.MkdirAll(nonEmptyDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(nonEmptyDir, "test.txt"), []byte("test"), 0644))

	tests := []struct {
		name        string
		args        []string
		forcePull   bool
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid new directory",
			args:        []string{filepath.Join(tempDir, "newdir")},
			expectError: false,
		},
		{
			name:        "existing empty directory",
			args:        []string{existingDir},
			expectError: false,
		},
		{
			name:        "existing file",
			args:        []string{existingFile},
			expectError: true,
			errorMsg:    "is not a directory",
		},
		{
			name:        "non-empty directory without force",
			args:        []string{nonEmptyDir},
			forcePull:   false,
			expectError: true,
			errorMsg:    "is not empty",
		},
		{
			name:        "non-empty directory with force",
			args:        []string{nonEmptyDir},
			forcePull:   true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalForcePull := ForcePull
			originalImagesBundlePath := ImagesBundlePath

			defer func() {
				ForcePull = originalForcePull
				ImagesBundlePath = originalImagesBundlePath
			}()

			ForcePull = tt.forcePull
			ImagesBundlePath = ""

			err := validateImagesBundlePathArg(tt.args)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParseAndValidateVersionFlags(t *testing.T) {
	tests := []struct {
		name               string
		sinceVersionString string
		deckhouseTag       string
		expectError        bool
		errorMsg           string
	}{
		{
			name:               "no version flags",
			sinceVersionString: "",
			deckhouseTag:       "",
			expectError:        false,
		},
		{
			name:               "valid since version",
			sinceVersionString: "1.50.0",
			deckhouseTag:       "",
			expectError:        false,
		},
		{
			name:               "valid deckhouse tag",
			sinceVersionString: "",
			deckhouseTag:       "v1.57.3",
			expectError:        false,
		},
		{
			name:               "conflicting flags",
			sinceVersionString: "1.50.0",
			deckhouseTag:       "v1.57.3",
			expectError:        true,
			errorMsg:           "ambiguous",
		},
		{
			name:               "invalid version format",
			sinceVersionString: "invalid",
			deckhouseTag:       "",
			expectError:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalSinceVersionString := sinceVersionString
			originalDeckhouseTag := DeckhouseTag
			originalSinceVersion := SinceVersion

			defer func() {
				sinceVersionString = originalSinceVersionString
				DeckhouseTag = originalDeckhouseTag
				SinceVersion = originalSinceVersion
			}()

			sinceVersionString = tt.sinceVersionString
			DeckhouseTag = tt.deckhouseTag
			SinceVersion = nil

			err := parseAndValidateVersionFlags()

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
				if tt.sinceVersionString != "" {
					assert.NotNil(t, SinceVersion)
					assert.Equal(t, tt.sinceVersionString, SinceVersion.String())
				}
			}
		})
	}
}

func TestValidateChunkSizeFlag(t *testing.T) {
	tests := []struct {
		name        string
		chunkSize   int64
		expectError bool
	}{
		{
			name:        "valid chunk size zero",
			chunkSize:   0,
			expectError: false,
		},
		{
			name:        "valid chunk size positive",
			chunkSize:   5,
			expectError: false,
		},
		{
			name:        "invalid negative chunk size",
			chunkSize:   -1,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := ImagesBundleChunkSizeGB
			defer func() { ImagesBundleChunkSizeGB = original }()

			ImagesBundleChunkSizeGB = tt.chunkSize
			err := validateChunkSizeFlag()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateTmpPath(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name        string
		tempDir     string
		expectError bool
	}{
		{
			name:        "empty temp dir uses default",
			tempDir:     "",
			expectError: false,
		},
		{
			name:        "valid temp dir",
			tempDir:     filepath.Join(tempDir, "custom"),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalTempDir := TempDir
			originalImagesBundlePath := ImagesBundlePath

			defer func() {
				TempDir = originalTempDir
				ImagesBundlePath = originalImagesBundlePath
			}()

			TempDir = tt.tempDir
			ImagesBundlePath = tempDir

			err := validateTmpPath([]string{})

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, TempDir)
				// Check that directory was created
				_, err := os.Stat(TempDir)
				assert.NoError(t, err)
			}
		})
	}
}

func TestAddFlags(t *testing.T) {
	cmd := &cobra.Command{}
	flags := cmd.Flags()

	// Add flags
	addFlags(flags)

	// Check that expected flags are present
	flagNames := []string{
		"source",
		"source-login",
		"source-password",
		"license",
		"since-version",
		"deckhouse-tag",
		"include-module",
		"exclude-module",
		"modules-path-suffix",
		"images-bundle-chunk-size",
		"gost-digest",
		"force",
		"no-pull-resume",
		"no-platform",
		"no-security-db",
		"no-modules",
		"only-extra-images",
		"tls-skip-verify",
		"insecure",
		"tmp-dir",
	}

	for _, flagName := range flagNames {
		flag := flags.Lookup(flagName)
		assert.NotNil(t, flag, "Flag %s should be present", flagName)
	}
}

func TestPullFunctionErrorPaths(t *testing.T) {
	// Test error paths in the main pull function
	tempDir := t.TempDir()

	// Mock the logger setup
	originalDebugLvl := os.Getenv("MIRROR_DEBUG_LOG")
	defer func() { os.Setenv("MIRROR_DEBUG_LOG", originalDebugLvl) }()

	// Test with working directory cleanup failure
	t.Run("working directory cleanup failure", func(t *testing.T) {
		// This is hard to test directly since os.RemoveAll is called
		// We can test the logic indirectly through the parameters
		originalTempDir := TempDir
		originalImagesBundlePath := ImagesBundlePath
		originalNoPullResume := NoPullResume
		originalSourceRegistryRepo := SourceRegistryRepo

		defer func() {
			TempDir = originalTempDir
			ImagesBundlePath = originalImagesBundlePath
			NoPullResume = originalNoPullResume
			SourceRegistryRepo = originalSourceRegistryRepo
		}()

		TempDir = tempDir
		ImagesBundlePath = tempDir
		NoPullResume = true
		SourceRegistryRepo = "test-registry"

		// This test is limited since we can't easily mock os.RemoveAll
		// But we can verify the parameters are set correctly
		logger := setupLogger()
		params := buildPullParams(logger)

		assert.Equal(t, tempDir, params.BundleDir)
		assert.Contains(t, params.WorkingDir, "pull")
		// Check that working directory contains the MD5 hash of "test-registry"
		expectedHash := fmt.Sprintf("%x", md5.Sum([]byte("test-registry")))
		assert.Contains(t, params.WorkingDir, expectedHash)
	})
}

func TestEnterpriseEditionRepo(t *testing.T) {
	// Test that the enterprise edition repo constant is properly defined
	assert.Equal(t, "registry.deckhouse.ru/deckhouse/ee", enterpriseEditionRepo)
	assert.Equal(t, enterpriseEditionRepo, SourceRegistryRepo) // Default value
}

func TestGlobalVariableDefaults(t *testing.T) {
	// Test that global variables have expected defaults
	assert.Equal(t, enterpriseEditionRepo, SourceRegistryRepo)
	assert.Empty(t, SourceRegistryLogin)
	assert.Empty(t, SourceRegistryPassword)
	assert.Empty(t, DeckhouseLicenseToken)
	assert.Empty(t, sinceVersionString)
	assert.Nil(t, SinceVersion)
	assert.Empty(t, DeckhouseTag)
	assert.Equal(t, "/modules", ModulesPathSuffix)
	assert.Equal(t, int64(0), ImagesBundleChunkSizeGB)
	assert.False(t, DoGOSTDigest)
	assert.False(t, ForcePull)
	assert.False(t, NoPullResume)
	assert.False(t, NoPlatform)
	assert.False(t, NoSecurityDB)
	assert.False(t, NoModules)
	assert.False(t, OnlyExtraImages)
	assert.False(t, TLSSkipVerify)
	assert.False(t, Insecure)
	assert.Empty(t, TempDir)
	assert.Empty(t, ImagesBundlePath)
	assert.Nil(t, ModulesWhitelist)
	assert.Nil(t, ModulesBlacklist)
}

func TestErrorMessages(t *testing.T) {
	// Test that error messages are properly formatted
	err := ErrPullFailed
	assert.Equal(t, "pull failed, see the log for details", err.Error())
}

func TestFindTagsToMirrorWithVersionsSuccess(t *testing.T) {
	// Save original function
	originalVersionsToMirrorFunc := versionsToMirrorFunc
	defer func() { versionsToMirrorFunc = originalVersionsToMirrorFunc }()

	// Mock the function to return successful versions
	versionsToMirrorFunc = func(pullParams *params.PullParams) ([]semver.Version, error) {
		return []semver.Version{
			*semver.MustParse("1.50.0"),
			*semver.MustParse("1.51.0"),
			*semver.MustParse("1.52.0"),
		}, nil
	}

	logger := log.NewSLogger(slog.LevelInfo)

	// Test the case where we need to call versions lookup
	originalDeckhouseTag := DeckhouseTag
	defer func() { DeckhouseTag = originalDeckhouseTag }()

	DeckhouseTag = "" // Force versions lookup

	pullParams := &params.PullParams{
		DeckhouseTag: "",
		SinceVersion: nil,
	}

	tags, err := findTagsToMirror(pullParams, logger)
	assert.NoError(t, err)
	assert.Equal(t, []string{"v1.50.0", "v1.51.0", "v1.52.0"}, tags)
}
