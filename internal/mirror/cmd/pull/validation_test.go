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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidationValidateSourceRegistry(t *testing.T) {
	tests := []struct {
		name        string
		registry    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "default enterprise edition repo",
			registry:    enterpriseEditionRepo,
			expectError: false,
		},
		{
			name:        "valid registry with host and path",
			registry:    "registry.example.com/deckhouse/ee",
			expectError: false,
		},
		{
			name:        "valid registry with port",
			registry:    "registry.example.com:8080/deckhouse/ee",
			expectError: false,
		},
		{
			name:        "invalid registry format - no host",
			registry:    "/deckhouse/ee",
			expectError: true,
			errorMsg:    "no registry host",
		},
		{
			name:        "invalid registry format - no path",
			registry:    "registry.example.com",
			expectError: true,
			errorMsg:    "no registry path",
		},
		{
			name:        "invalid registry - malformed",
			registry:    "invalid-registry-format",
			expectError: true,
		},
		{
			name:        "registry with invalid characters",
			registry:    "registry.example.com/path with spaces",
			expectError: true,
		},
		{
			name:        "valid registry with nested path",
			registry:    "registry.example.com/path/subpath/deckhouse",
			expectError: false,
		},
		{
			name:        "registry with IP address",
			registry:    "192.168.1.1:5000/deckhouse/ee",
			expectError: false,
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
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidationParseAndValidateParameters(t *testing.T) {
	// Since parseAndValidateParameters calls multiple validation functions,
	// we test it by setting up valid/invalid states and checking the overall result.
	// This is more of an integration test.

	tempDir := t.TempDir()
	validBundlePath := filepath.Join(tempDir, "bundle")

	tests := []struct {
		name        string
		args        []string
		setup       func()
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid parameters",
			args: []string{validBundlePath},
			setup: func() {
				SourceRegistryRepo = enterpriseEditionRepo
				sinceVersionString = ""
				DeckhouseTag = ""
				ImagesBundleChunkSizeGB = 0
				TempDir = ""
				ImagesBundlePath = ""
			},
			expectError: false,
		},
		{
			name: "invalid source registry",
			args: []string{validBundlePath},
			setup: func() {
				SourceRegistryRepo = "invalid"
				sinceVersionString = ""
				DeckhouseTag = ""
				ImagesBundleChunkSizeGB = 0
				TempDir = ""
				ImagesBundlePath = ""
			},
			expectError: true,
		},
		{
			name: "invalid version flags",
			args: []string{validBundlePath},
			setup: func() {
				SourceRegistryRepo = enterpriseEditionRepo
				sinceVersionString = "1.50.0"
				DeckhouseTag = "v1.57.3"
				ImagesBundleChunkSizeGB = 0
				TempDir = ""
				ImagesBundlePath = ""
			},
			expectError: true,
			errorMsg:    "ambiguous",
		},
		{
			name: "invalid bundle path - no args",
			args: []string{},
			setup: func() {
				SourceRegistryRepo = enterpriseEditionRepo
				sinceVersionString = ""
				DeckhouseTag = ""
				ImagesBundleChunkSizeGB = 0
				TempDir = ""
				ImagesBundlePath = ""
			},
			expectError: true,
			errorMsg:    "exactly 1 argument",
		},
		{
			name: "invalid chunk size",
			args: []string{validBundlePath},
			setup: func() {
				SourceRegistryRepo = enterpriseEditionRepo
				sinceVersionString = ""
				DeckhouseTag = ""
				ImagesBundleChunkSizeGB = -1
				TempDir = ""
				ImagesBundlePath = ""
			},
			expectError: true,
			errorMsg:    "less than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save originals
			originalSourceRegistryRepo := SourceRegistryRepo
			originalSinceVersionString := sinceVersionString
			originalDeckhouseTag := DeckhouseTag
			originalImagesBundleChunkSizeGB := ImagesBundleChunkSizeGB
			originalTempDir := TempDir
			originalImagesBundlePath := ImagesBundlePath

			defer func() {
				SourceRegistryRepo = originalSourceRegistryRepo
				sinceVersionString = originalSinceVersionString
				DeckhouseTag = originalDeckhouseTag
				ImagesBundleChunkSizeGB = originalImagesBundleChunkSizeGB
				TempDir = originalTempDir
				ImagesBundlePath = originalImagesBundlePath
			}()

			tt.setup()
			err := parseAndValidateParameters(nil, tt.args)

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

func TestValidationValidateImagesBundlePathArg(t *testing.T) {
	tempDir := t.TempDir()
	existingDir := filepath.Join(tempDir, "existing")
	existingFile := filepath.Join(tempDir, "file.txt")
	nonEmptyDir := filepath.Join(tempDir, "nonempty")
	onlyTmpDir := filepath.Join(tempDir, "onlytmp")

	// Setup test directories/files
	require.NoError(t, os.MkdirAll(existingDir, 0755))
	require.NoError(t, os.WriteFile(existingFile, []byte("test"), 0644))
	require.NoError(t, os.MkdirAll(nonEmptyDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(nonEmptyDir, "test.txt"), []byte("test"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(onlyTmpDir, ".tmp"), 0755))

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
			forcePull:   false,
			expectError: false,
		},
		{
			name:        "existing empty directory",
			args:        []string{existingDir},
			forcePull:   false,
			expectError: false,
		},
		{
			name:        "existing file",
			args:        []string{existingFile},
			forcePull:   false,
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
		{
			name:        "directory with only .tmp subdirectory",
			args:        []string{onlyTmpDir},
			forcePull:   false,
			expectError: false,
		},
		{
			name:        "no arguments",
			args:        []string{},
			forcePull:   false,
			expectError: true,
			errorMsg:    "exactly 1 argument",
		},
		{
			name:        "multiple arguments",
			args:        []string{"arg1", "arg2"},
			forcePull:   false,
			expectError: true,
			errorMsg:    "exactly 1 argument",
		},
		{
			name:        "relative path",
			args:        []string{"./relative/path"},
			forcePull:   false,
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

func TestValidationParseAndValidateVersionFlags(t *testing.T) {
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
		{
			name:               "empty since version string",
			sinceVersionString: "",
			deckhouseTag:       "",
			expectError:        false,
		},
		{
			name:               "since version with pre-release",
			sinceVersionString: "1.50.0-beta.1",
			deckhouseTag:       "",
			expectError:        false,
		},
		{
			name:               "since version with build metadata",
			sinceVersionString: "1.50.0+build.1",
			deckhouseTag:       "",
			expectError:        false,
		},
		{
			name:               "invalid version with letters",
			sinceVersionString: "1.50.0abc",
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

func TestValidationValidateChunkSizeFlag(t *testing.T) {
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
		{
			name:        "large positive chunk size",
			chunkSize:   1000,
			expectError: false,
		},
		{
			name:        "very large chunk size",
			chunkSize:   1000000,
			expectError: false,
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

func TestValidationValidateTmpPath(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name        string
		tempDir     string
		bundlePath  string
		expectError bool
	}{
		{
			name:        "empty temp dir uses default",
			tempDir:     "",
			bundlePath:  tempDir,
			expectError: false,
		},
		{
			name:        "valid temp dir",
			tempDir:     filepath.Join(tempDir, "custom"),
			bundlePath:  tempDir,
			expectError: false,
		},
		{
			name:        "temp dir with relative path",
			tempDir:     "relative/temp",
			bundlePath:  tempDir,
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
			ImagesBundlePath = tt.bundlePath

			err := validateTmpPath([]string{tt.tempDir})

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
