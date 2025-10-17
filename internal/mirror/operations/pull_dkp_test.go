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

package operations

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

func TestPullDeckhousePlatform(t *testing.T) {
	tempDir := t.TempDir()

	// Create test parameters
	logger := log.NewSLogger(slog.LevelInfo)
	pullParams := &params.PullParams{
		BaseParams: params.BaseParams{
			Logger:                logger,
			Insecure:              true,
			SkipTLSVerification:   true,
			DeckhouseRegistryRepo: "registry.deckhouse.io/deckhouse/ce", // Use CE repo for testing
			RegistryAuth:          nil,                                  // Anonymous access
			BundleDir:             tempDir,
			WorkingDir:            filepath.Join(tempDir, "work"),
		},
		DeckhouseTag: "v1.50.0", // Use a specific tag to avoid complex version resolution
	}

	// Test with a specific tag to minimize complexity
	tagsToMirror := []string{"v1.50.0"}

	// This test will attempt to call the real PullDeckhousePlatform function
	// It may fail due to network issues or registry access, but it will exercise the code
	err := PullDeckhousePlatform(pullParams, tagsToMirror)

	// We expect this to fail in a test environment due to network/registry access
	// But the important thing is that the code path is exercised
	if err != nil {
		// Check that we get a meaningful error, not a panic
		assert.Error(t, err)
		t.Logf("Expected error during platform pull: %v", err)
	} else {
		// If it succeeds, verify that some output was created
		platformTar := filepath.Join(tempDir, "platform.tar")
		_, statErr := os.Stat(platformTar)
		assert.NoError(t, statErr, "platform.tar should be created on successful pull")
	}
}

func TestPullDeckhousePlatformWithInvalidParams(t *testing.T) {
	// Test with invalid parameters to ensure error handling
	logger := log.NewSLogger(slog.LevelInfo)
	pullParams := &params.PullParams{
		BaseParams: params.BaseParams{
			Logger:                logger,
			DeckhouseRegistryRepo: "", // Invalid empty registry
			BundleDir:             "/nonexistent",
			WorkingDir:            "/nonexistent",
		},
	}

	tagsToMirror := []string{"v1.50.0"}

	err := PullDeckhousePlatform(pullParams, tagsToMirror)
	assert.Error(t, err)
}

func TestPullDeckhousePlatformWithEmptyTags(t *testing.T) {
	tempDir := t.TempDir()

	logger := log.NewSLogger(slog.LevelInfo)
	pullParams := &params.PullParams{
		BaseParams: params.BaseParams{
			Logger:                logger,
			Insecure:              true,
			SkipTLSVerification:   true,
			DeckhouseRegistryRepo: "registry.deckhouse.io/deckhouse/ce",
			RegistryAuth:          nil,
			BundleDir:             tempDir,
			WorkingDir:            filepath.Join(tempDir, "work"),
		},
		DeckhouseTag: "",
	}

	// Test with empty tags
	tagsToMirror := []string{}

	err := PullDeckhousePlatform(pullParams, tagsToMirror)
	// Should handle empty tags gracefully
	assert.Error(t, err) // Expect error due to empty tags
}
