//go:build e2e

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

package mirror

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/testing/e2e/mirror/internal"
)

func TestPlatformE2E(t *testing.T) {
	cfg := internal.GetConfig()

	if !cfg.HasSourceAuth() {
		t.Skip("Skipping: no source authentication configured (set E2E_LICENSE_TOKEN)")
	}

	cfg.NoModules = true
	cfg.NoSecurity = true

	env := setupTestEnvironment(t, cfg)
	defer env.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), internal.PlatformTestTimeout)
	defer cancel()

	runPlatformTest(t, ctx, cfg, env)
}

func runPlatformTest(t *testing.T, ctx context.Context, cfg *internal.Config, env *testEnv) {
	internal.PrintHeader("PLATFORM E2E TEST")

	internal.PrintStep(1, "Reading expected platform images from source")
	expected := readExpectedPlatformImages(t, ctx, cfg)

	if cfg.HasExistingBundle() {
		t.Logf("Using existing bundle: %s (skipping pull)", env.BundleDir)
		env.Report.AddStep("Pull (existing bundle)", "SKIP", 0, nil)
	} else {
		internal.PrintStep(2, "Pulling platform images")
		runPullStep(t, cfg, env)
	}

	internal.PrintStep(3, "Pushing to target registry")
	runPushStep(t, cfg, env)

	internal.PrintStep(4, "Verifying expected images in target")
	verifyExpectedInTarget(t, ctx, cfg, env, expected)

	internal.PrintSuccessBox(env.Report.MatchedImages, env.Report.FoundAttTags)
}

type ExpectedPlatformImages struct {
	Channels []internal.ReleaseChannelInfo
	Versions []string
	Digests  []string
}

func readExpectedPlatformImages(t *testing.T, ctx context.Context, cfg *internal.Config) *ExpectedPlatformImages {
	t.Helper()

	reader := createSourceReader(t, cfg)
	result := &ExpectedPlatformImages{}

	channels, err := reader.ReadReleaseChannels(ctx)
	require.NoError(t, err, "Failed to read release channels")
	result.Channels = channels

	t.Logf("Found %d release channels:", len(channels))
	for _, ch := range channels {
		t.Logf("  %s -> %s", ch.Channel, ch.Version)
	}

	if cfg.DeckhouseTag != "" {
		for _, ch := range channels {
			if ch.Channel == cfg.DeckhouseTag {
				channels = []internal.ReleaseChannelInfo{ch}
				break
			}
		}
	}

	platform, err := reader.ReadPlatformDigests(ctx, channels)
	require.NoError(t, err, "Failed to read platform digests")

	result.Versions = platform.Versions
	result.Digests = platform.ImageDigests

	t.Logf("Expected: %d versions, %d digests", len(result.Versions), len(result.Digests))

	return result
}

func verifyExpectedInTarget(t *testing.T, ctx context.Context, cfg *internal.Config, env *testEnv, expected *ExpectedPlatformImages) {
	t.Helper()
	verifyPlatformImages(t, ctx, cfg, env, cfg.DeckhouseTag)
	t.Logf("Platform verification passed: %d digests, %d .att tags", env.Report.MatchedImages, env.Report.FoundAttTags)
}

