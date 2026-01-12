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

func createVerifier(t *testing.T, cfg *internal.Config, env *testEnv) *internal.DigestVerifier {
	t.Helper()

	sourceReader := internal.NewSourceReader(cfg.SourceRegistry, cfg.GetSourceAuth(), cfg.TLSSkipVerify)
	verifier := internal.NewDigestVerifier(
		sourceReader,
		env.TargetRegistry,
		cfg.GetTargetAuth(),
		cfg.TLSSkipVerify,
	)
	verifier.SetProgressCallback(func(msg string) {
		t.Logf("  %s", msg)
	})
	return verifier
}

func createSourceReader(t *testing.T, cfg *internal.Config) *internal.SourceReader {
	t.Helper()

	reader := internal.NewSourceReader(cfg.SourceRegistry, cfg.GetSourceAuth(), cfg.TLSSkipVerify)
	reader.SetProgressCallback(func(msg string) {
		t.Logf("  %s", msg)
	})
	return reader
}

func filterModules(modules []string, include []string) []string {
	if len(include) == 0 {
		return modules
	}
	includeSet := make(map[string]bool, len(include))
	for _, m := range include {
		includeSet[m] = true
	}
	var filtered []string
	for _, m := range modules {
		if includeSet[m] {
			filtered = append(filtered, m)
		}
	}
	return filtered
}

func verifyPlatformImages(t *testing.T, ctx context.Context, cfg *internal.Config, env *testEnv, deckhouseTag string) {
	t.Helper()
	stepStart := time.Now()

	verifier := createVerifier(t, cfg, env)
	result, err := verifier.VerifyPlatform(ctx, deckhouseTag)
	require.NoError(t, err, "Verification failed")

	saveVerificationReport(t, env.ComparisonFile, result)

	env.Report.TotalImages = len(result.ExpectedDigests)
	env.Report.MatchedImages = len(result.FoundDigests)
	env.Report.MissingImages = len(result.MissingDigests)
	env.Report.ExpectedAttTags = len(result.ExpectedAttTags)
	env.Report.FoundAttTags = len(result.FoundAttTags)
	env.Report.MissingAttTags = len(result.MissingAttTags)
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

	if len(failures) > 0 {
		env.Report.AddStep(
			fmt.Sprintf("Verification (%d/%d digests, %d/%d .att)",
				len(result.FoundDigests), len(result.ExpectedDigests),
				len(result.FoundAttTags), len(result.ExpectedAttTags)),
			"FAIL", time.Since(stepStart),
			fmt.Errorf("%v", failures),
		)

		require.Empty(t, failures,
			"Platform verification FAILED!\n\n%s\n\nSee %s for details",
			result.Summary(), env.ComparisonFile)
		return
	}

	env.Report.AddStep(
		fmt.Sprintf("Verification (%d digests, %d .att tags)",
			len(result.FoundDigests), len(result.FoundAttTags)),
		"PASS", time.Since(stepStart), nil,
	)
}

func verifyModulesImages(t *testing.T, ctx context.Context, cfg *internal.Config, env *testEnv, expectedModules []string) {
	t.Helper()
	stepStart := time.Now()

	verifier := createVerifier(t, cfg, env)
	result, err := verifier.VerifyModules(ctx, expectedModules)
	require.NoError(t, err, "Modules verification failed")

	t.Logf("Found %d/%d modules in target", result.ModulesFound, result.ModulesExpected)
	t.Logf("Found %d/%d module versions in target", result.ModuleVersionsFound, result.ModuleVersionsTotal)
	t.Logf("Found %d/%d module digests in target", result.ModuleDigestsFound, result.ModuleDigestsTotal)

	for _, missing := range result.ModulesMissing {
		t.Logf("  ✗ module: %s", missing)
	}
	for _, missing := range result.ModuleVersionsMissing {
		t.Logf("  ✗ version: %s", missing)
	}
	for _, missing := range result.ModuleDigestsMissing {
		t.Logf("  ✗ digest: %s", missing)
	}

	var failures []string
	if len(result.ModulesMissing) > 0 {
		failures = append(failures, fmt.Sprintf("%d modules missing", len(result.ModulesMissing)))
	}
	if len(result.ModuleVersionsMissing) > 0 {
		failures = append(failures, fmt.Sprintf("%d versions missing", len(result.ModuleVersionsMissing)))
	}
	if len(result.ModuleDigestsMissing) > 0 {
		failures = append(failures, fmt.Sprintf("%d digests missing", len(result.ModuleDigestsMissing)))
	}

	if len(failures) > 0 {
		env.Report.AddStep(
			fmt.Sprintf("Modules Verification (%d modules, %d versions)",
				result.ModulesFound, result.ModuleVersionsFound),
			"FAIL", time.Since(stepStart),
			fmt.Errorf("%v", failures),
		)
		require.Empty(t, failures, "Modules verification failed: %v", failures)
		return
	}

	env.Report.ModulesExpected = result.ModulesExpected
	env.Report.ModulesFound = result.ModulesFound
	env.Report.ModulesMissing = len(result.ModulesMissing)
	env.Report.ModuleVersionsTotal = result.ModuleVersionsTotal
	env.Report.ModuleVersionsFound = result.ModuleVersionsFound
	env.Report.ModuleVersionsMissing = len(result.ModuleVersionsMissing)
	env.Report.ModuleDigestsTotal = result.ModuleDigestsTotal
	env.Report.ModuleDigestsFound = result.ModuleDigestsFound
	env.Report.ModuleDigestsMissing = len(result.ModuleDigestsMissing)

	env.Report.AddStep(
		fmt.Sprintf("Modules Verification (%d modules, %d versions, %d digests)",
			result.ModulesFound, result.ModuleVersionsFound, result.ModuleDigestsFound),
		"PASS", time.Since(stepStart), nil,
	)
	t.Log("Modules verification passed")
}

func verifySecurityImages(t *testing.T, ctx context.Context, cfg *internal.Config, env *testEnv) {
	t.Helper()
	stepStart := time.Now()

	verifier := createVerifier(t, cfg, env)
	result, err := verifier.VerifySecurity(ctx)
	require.NoError(t, err, "Security verification failed")

	t.Logf("Found %d/%d security databases in target", result.SecurityFound, result.SecurityExpected)

	for _, missing := range result.SecurityMissing {
		t.Logf("  ✗ %s", missing)
	}

	if len(result.SecurityMissing) > 0 {
		env.Report.AddStep(
			fmt.Sprintf("Security Verification (%d/%d found)",
				result.SecurityFound, result.SecurityExpected),
			"FAIL", time.Since(stepStart),
			fmt.Errorf("missing %d security databases: %v", len(result.SecurityMissing), result.SecurityMissing),
		)
		require.Empty(t, result.SecurityMissing, "Some security databases are missing in target")
		return
	}

	env.Report.SecurityExpected = result.SecurityExpected
	env.Report.SecurityFound = result.SecurityFound
	env.Report.SecurityMissing = len(result.SecurityMissing)

	env.Report.AddStep(
		fmt.Sprintf("Security Verification (%d databases)", result.SecurityFound),
		"PASS", time.Since(stepStart), nil,
	)
	t.Log("Security verification passed")
}

func saveVerificationReport(t *testing.T, path string, result *internal.VerificationResult) {
	t.Helper()

	report := result.DetailedReport()
	err := internal.WriteFile(path, []byte(report))
	if err != nil {
		t.Logf("Warning: failed to write verification report: %v", err)
	}
}

func saveReport(t *testing.T, path string, report *internal.TestReport) {
	t.Helper()
	if err := report.WriteToFile(path); err != nil {
		t.Logf("Warning: failed to write report: %v", err)
	}
}
