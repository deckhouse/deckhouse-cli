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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/testing/e2e/mirror/internal"
)

func TestFullCycleE2E(t *testing.T) {
	cfg := internal.GetConfig()

	if !cfg.HasSourceAuth() {
		t.Skip("Skipping: no source authentication configured (set E2E_LICENSE_TOKEN)")
	}

	env := setupTestEnvironment(t, cfg)
	defer env.Cleanup()

	runFullCycleTest(t, cfg, env)
	printFinalReport(t, env)
}

func runFullCycleTest(t *testing.T, cfg *internal.Config, env *testEnv) {
	ctx, cancel := context.WithTimeout(context.Background(), internal.FullCycleTestTimeout)
	defer cancel()

	internal.PrintHeader("FULL CYCLE E2E TEST")

	internal.PrintStep(1, "Reading expected images from source registry")
	expected := readAllExpectedImages(t, ctx, cfg)
	env.Report.ExpectedModules = getModuleNames(expected)

	if cfg.HasExistingBundle() {
		t.Logf("Using existing bundle: %s (skipping pull)", env.BundleDir)
		env.Report.AddStep("Pull (existing bundle)", "SKIP", 0, nil)
	} else {
		internal.PrintStep(2, "Pulling images to bundle")
		runPullStep(t, cfg, env)
	}

	internal.PrintStep(3, "Pushing bundle to target registry")
	runPushStep(t, cfg, env)

	internal.PrintStep(4, "Verifying expected images in target")
	runVerificationStep(t, ctx, cfg, env, expected)

	internal.PrintSuccessBox(env.Report.MatchedImages, env.Report.FoundAttTags)
}

func readAllExpectedImages(t *testing.T, ctx context.Context, cfg *internal.Config) *internal.ExpectedImages {
	t.Helper()

	reader := createSourceReader(t, cfg)
	result := &internal.ExpectedImages{}

	if !cfg.NoPlatform {
		t.Log("Reading platform images...")
		channels, err := reader.ReadReleaseChannels(ctx)
		if err != nil {
			t.Logf("Warning: failed to read release channels: %v", err)
		} else {
			platform, err := reader.ReadPlatformDigests(ctx, channels)
			if err != nil {
				t.Logf("Warning: failed to read platform digests: %v", err)
			} else {
				result.Platform = platform
				t.Logf("Platform: %d versions, %d digests", len(platform.Versions), len(platform.ImageDigests))
			}
		}
	}

	if !cfg.NoModules {
		t.Log("Reading modules...")
		modules, err := reader.ReadModulesList(ctx)
		if err != nil {
			t.Logf("Warning: failed to read modules: %v", err)
		} else {
			modules = filterModules(modules, cfg.IncludeModules)

			for _, moduleName := range modules {
				info, err := reader.ReadModuleDigests(ctx, moduleName)
				if err != nil {
					t.Logf("Warning: failed to read module %s: %v", moduleName, err)
					continue
				}
				result.Modules = append(result.Modules, info)
			}
			t.Logf("Modules: %d", len(result.Modules))
		}
	}

	if !cfg.NoSecurity {
		t.Log("Reading security databases...")
		security, err := reader.ReadSecurityDigests(ctx)
		if err != nil {
			t.Logf("Warning: failed to read security: %v", err)
		} else {
			result.Security = security
			t.Logf("Security: %d databases", len(security.Databases))
		}
	}

	return result
}

func getModuleNames(expected *internal.ExpectedImages) []string {
	if expected == nil || len(expected.Modules) == 0 {
		return nil
	}
	names := make([]string, len(expected.Modules))
	for i, m := range expected.Modules {
		names[i] = m.Name
	}
	return names
}

func runVerificationStep(t *testing.T, ctx context.Context, cfg *internal.Config, env *testEnv, expected *internal.ExpectedImages) {
	t.Helper()
	stepStart := time.Now()

	verifier := createVerifier(t, cfg, env)
	result, err := verifier.VerifyFull(ctx, cfg.DeckhouseTag, cfg.IncludeModules)
	if err != nil {
		env.Report.AddStep("Verification", "FAIL", time.Since(stepStart), err)
		require.NoError(t, err, "Verification failed")
	}

	saveVerificationReport(t, env.ComparisonFile, result)

	env.Report.TotalImages = len(result.ExpectedDigests)
	env.Report.MatchedImages = len(result.FoundDigests)
	env.Report.MissingImages = len(result.MissingDigests)
	env.Report.ExpectedAttTags = len(result.ExpectedAttTags)
	env.Report.FoundAttTags = len(result.FoundAttTags)
	env.Report.MissingAttTags = len(result.MissingAttTags)
	env.Report.ModulesExpected = result.ModulesExpected
	env.Report.ModulesFound = result.ModulesFound
	env.Report.ModulesMissing = len(result.ModulesMissing)
	env.Report.ModuleVersionsTotal = result.ModuleVersionsTotal
	env.Report.ModuleVersionsFound = result.ModuleVersionsFound
	env.Report.ModuleVersionsMissing = len(result.ModuleVersionsMissing)
	env.Report.ModuleDigestsTotal = result.ModuleDigestsTotal
	env.Report.ModuleDigestsFound = result.ModuleDigestsFound
	env.Report.ModuleDigestsMissing = len(result.ModuleDigestsMissing)
	env.Report.SecurityExpected = result.SecurityExpected
	env.Report.SecurityFound = result.SecurityFound
	env.Report.SecurityMissing = len(result.SecurityMissing)
	env.Report.SourceImageCount = len(result.ExpectedDigests) + len(result.ExpectedAttTags)
	env.Report.TargetImageCount = len(result.FoundDigests) + len(result.FoundAttTags)

	t.Log("")
	t.Log(result.Summary())

	var failures []string
	if len(result.MissingDigests) > 0 {
		failures = append(failures, fmt.Sprintf("missing %d digests in target", len(result.MissingDigests)))
	}
	if len(result.MissingAttTags) > 0 {
		failures = append(failures, fmt.Sprintf("missing %d .att tags in target", len(result.MissingAttTags)))
	}
	if len(result.ModuleVersionsMissing) > 0 {
		failures = append(failures, fmt.Sprintf("missing %d module versions in target", len(result.ModuleVersionsMissing)))
	}
	if len(result.ModuleDigestsMissing) > 0 {
		failures = append(failures, fmt.Sprintf("missing %d module digests in target", len(result.ModuleDigestsMissing)))
	}

	if len(failures) > 0 {
		env.Report.AddStep(
			fmt.Sprintf("Verification (%d/%d digests, %d/%d .att)",
				len(result.FoundDigests), len(result.ExpectedDigests),
				len(result.FoundAttTags), len(result.ExpectedAttTags)),
			"FAIL", time.Since(stepStart),
			fmt.Errorf("%v", failures),
		)

		require.Empty(t, failures,
			"Mirror verification FAILED!\n\n%s\n\nSee %s for details",
			result.Summary(), env.ComparisonFile)
	}

	env.Report.AddStep(
		fmt.Sprintf("Verification (%d digests, %d .att tags)",
			len(result.FoundDigests), len(result.FoundAttTags)),
		"PASS", time.Since(stepStart), nil,
	)
}

func printFinalReport(t *testing.T, env *testEnv) {
	env.Report.EndTime = time.Now()
	report := env.Report.String()

	t.Log("")
	t.Log(report)

	saveReport(t, env.ReportFile, env.Report)
	t.Logf("Report written to: %s", env.ReportFile)
}

