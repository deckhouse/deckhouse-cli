/*
Copyright 2024 Flant JSC

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

package mirror

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	mirrorutil "github.com/deckhouse/deckhouse-cli/testing/util/mirror"
)

// TestMirrorE2E_FullCycle performs a complete mirror cycle:
// 1. Collects reference digests from source registry
// 2. Pulls images to local bundle
// 3. Pushes bundle to target registry
// 4. Validates target registry structure
// 5. Compares all digests between source and target
//
// Run with:
//
//	go test -v ./testing/e2e/mirror/... \
//	  -license-token=YOUR_TOKEN \
//	  -source-registry=registry.deckhouse.ru/deckhouse/fe
//
// Or using environment variables:
//
//	E2E_LICENSE_TOKEN=YOUR_TOKEN \
//	E2E_SOURCE_REGISTRY=registry.deckhouse.ru/deckhouse/fe \
//	go test -v ./testing/e2e/mirror/...
func TestMirrorE2E_FullCycle(t *testing.T) {
	cfg := GetConfig()

	// Debug: show config
	t.Logf("Config: SourceRegistry=%s, SourceUser=%s, HasAuth=%v",
		cfg.SourceRegistry, cfg.SourceUser, cfg.HasSourceAuth())

	if !cfg.HasSourceAuth() {
		t.Skip("Source authentication not provided (use -license-token or -source-user/-source-password)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	// Setup target registry
	targetHost, targetPath, cleanup := setupTargetRegistry(t, cfg)
	defer cleanup()

	targetRegistry := targetHost + targetPath
	t.Logf("Target registry: %s", targetRegistry)

	// Create bundle directory
	bundleDir := t.TempDir()
	if cfg.KeepBundle {
		bundleDir = filepath.Join(os.TempDir(), fmt.Sprintf("d8-mirror-e2e-%d", time.Now().Unix()))
		require.NoError(t, os.MkdirAll(bundleDir, 0755))
		t.Logf("Bundle directory (will be kept): %s", bundleDir)
	} else {
		t.Logf("Bundle directory: %s", bundleDir)
	}

	// Step 1: Collect reference digests from source
	t.Log("Step 1: Collecting reference digests from source registry...")
	t.Logf("Source: %s, tlsSkipVerify: %v", cfg.SourceRegistry, cfg.TLSSkipVerify)
	sourceCollector := NewDigestCollector(cfg.SourceRegistry, cfg.GetSourceAuth(), cfg.TLSSkipVerify)
	referenceDigests, err := sourceCollector.CollectAll(ctx)
	require.NoError(t, err, "Failed to collect reference digests")
	t.Logf("Collected %d reference digests", len(referenceDigests))

	// Step 2: Execute pull
	t.Log("Step 2: Pulling images to bundle...")
	pullCmd := buildPullCommand(cfg, bundleDir)
	t.Logf("Running: %s", pullCmd.String())

	pullOutput, err := pullCmd.CombinedOutput()
	if err != nil {
		t.Logf("Pull output:\n%s", string(pullOutput))
	}
	require.NoError(t, err, "Pull failed")
	t.Log("Pull completed successfully")

	// Verify bundle was created
	bundleFiles, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	t.Logf("Bundle contains %d files/dirs", len(bundleFiles))
	for _, f := range bundleFiles {
		t.Logf("  - %s", f.Name())
	}

	// Step 3: Execute push
	t.Log("Step 3: Pushing bundle to target registry...")
	pushCmd := buildPushCommand(cfg, bundleDir, targetRegistry)
	t.Logf("Running: %s", pushCmd.String())

	pushOutput, err := pushCmd.CombinedOutput()
	if err != nil {
		t.Logf("Push output:\n%s", string(pushOutput))
	}
	require.NoError(t, err, "Push failed")
	t.Log("Push completed successfully")

	// Step 4: Validate structure
	t.Log("Step 4: Validating target registry structure...")
	validator := NewStructureValidator(targetRegistry, cfg.GetTargetAuth(), cfg.TLSSkipVerify)
	validator.SetExpectedFromDigests(NormalizeDigests(referenceDigests))

	validationResult, err := validator.ValidateMinimal(ctx)
	require.NoError(t, err, "Validation error")

	if !validationResult.IsValid() {
		t.Logf("Validation issues:\n%s", validationResult.String())
	}
	require.True(t, validationResult.IsValid(), "Structure validation failed")
	t.Log("Structure validation passed")

	// Step 5: Compare digests
	t.Log("Step 5: Comparing digests...")
	targetCollector := NewDigestCollector(targetRegistry, cfg.GetTargetAuth(), cfg.TLSSkipVerify)
	targetDigests, err := targetCollector.CollectAll(ctx)
	require.NoError(t, err, "Failed to collect target digests")
	t.Logf("Collected %d target digests", len(targetDigests))

	// Normalize both maps for comparison
	normalizedReference := NormalizeDigests(referenceDigests)
	normalizedTarget := NormalizeDigests(targetDigests)

	compareResult := Compare(normalizedReference, normalizedTarget)
	if !compareResult.IsMatch() {
		t.Logf("Comparison failed:\n%s", compareResult.String())
	}
	require.True(t, compareResult.IsMatch(),
		"Digest comparison failed: %d mismatches found\n%s",
		len(compareResult.Mismatches),
		FormatMismatches(compareResult.Mismatches))

	t.Logf("SUCCESS: All %d digests match!", compareResult.MatchedCount)
}

// TestMirrorE2E_PullOnly tests only the pull operation
func TestMirrorE2E_PullOnly(t *testing.T) {
	cfg := GetConfig()

	if !cfg.HasSourceAuth() {
		t.Skip("Source authentication not provided")
	}

	bundleDir := t.TempDir()
	if cfg.KeepBundle {
		bundleDir = filepath.Join(os.TempDir(), fmt.Sprintf("d8-mirror-pull-%d", time.Now().Unix()))
		require.NoError(t, os.MkdirAll(bundleDir, 0755))
		t.Logf("Bundle directory (will be kept): %s", bundleDir)
	}

	pullCmd := buildPullCommand(cfg, bundleDir)
	t.Logf("Running: %s", pullCmd.String())

	output, err := pullCmd.CombinedOutput()
	t.Logf("Output:\n%s", string(output))
	require.NoError(t, err, "Pull failed")

	// Verify bundle was created
	bundleFiles, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	require.NotEmpty(t, bundleFiles, "Bundle directory is empty")

	t.Logf("Bundle created with %d files:", len(bundleFiles))
	for _, f := range bundleFiles {
		info, _ := f.Info()
		if info != nil {
			t.Logf("  - %s (%d bytes)", f.Name(), info.Size())
		} else {
			t.Logf("  - %s", f.Name())
		}
	}
}

// setupTargetRegistry sets up the target registry for testing
// Returns host, path, and cleanup function
func setupTargetRegistry(t *testing.T, cfg *Config) (string, string, func()) {
	t.Helper()

	if cfg.UseInMemoryRegistry() {
		// Use in-memory registry
		host, path, _ := mirrorutil.SetupEmptyRegistryRepo(false)
		t.Logf("Started in-memory registry at %s%s", host, path)
		return host, path, func() {
			// In-memory registry is cleaned up when test ends
		}
	}

	// Use external registry
	return cfg.TargetRegistry, "", func() {
		// External registry cleanup is user's responsibility
	}
}

// buildPullCommand builds the d8 mirror pull command
func buildPullCommand(cfg *Config, bundleDir string) *exec.Cmd {
	args := []string{
		"mirror", "pull",
		"--source", cfg.SourceRegistry,
		"--force", // overwrite if exists
	}

	// Add authentication flags
	if cfg.SourceUser != "" {
		args = append(args, "--source-login", cfg.SourceUser)
		args = append(args, "--source-password", cfg.SourcePassword)
	} else if cfg.LicenseToken != "" {
		args = append(args, "--license", cfg.LicenseToken)
	}

	// Add TLS skip verify flag (for self-signed certs)
	if cfg.TLSSkipVerify {
		args = append(args, "--tls-skip-verify")
	}

	args = append(args, bundleDir)

	cmd := exec.Command(cfg.D8Binary, args...)
	cmd.Env = append(os.Environ(),
		"HOME="+os.Getenv("HOME"),
	)

	return cmd
}

// buildPushCommand builds the d8 mirror push command
func buildPushCommand(cfg *Config, bundleDir, targetRegistry string) *exec.Cmd {
	args := []string{
		"mirror", "push",
		bundleDir,
		targetRegistry,
	}

	if cfg.TLSSkipVerify {
		args = append(args, "--tls-skip-verify")
	}

	if cfg.TargetUser != "" {
		args = append(args, "--registry-login", cfg.TargetUser)
		args = append(args, "--registry-password", cfg.TargetPassword)
	}

	cmd := exec.Command(cfg.D8Binary, args...)
	cmd.Env = append(os.Environ(),
		"HOME="+os.Getenv("HOME"),
	)

	return cmd
}
