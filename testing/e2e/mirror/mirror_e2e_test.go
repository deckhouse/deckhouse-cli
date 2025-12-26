//go:build e2e

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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	mirrorutil "github.com/deckhouse/deckhouse-cli/testing/util/mirror"
)

// =============================================================================
// E2E Test: Full Mirror Cycle
// =============================================================================

// TestMirrorE2E_FullCycle performs a complete mirror cycle and validates
// that source and target registries are identical.
//
// This is a heavy E2E test that:
//  1. Discovers all repositories in source registry
//  2. Pulls all images to local bundle using d8 mirror pull
//  3. Pushes bundle to target registry using d8 mirror push
//  4. Discovers all repositories in target registry
//  5. Compares every tag and digest between source and target
//  6. Generates detailed comparison report
//
// Run with:
//
//	go test -v ./testing/e2e/mirror/... \
//	  -source-registry=localhost:443/deckhouse \
//	  -source-user=admin -source-password=admin \
//	  -tls-skip-verify
func TestMirrorE2E_FullCycle(t *testing.T) {
	cfg := GetConfig()

	// Skip if no auth provided
	if !cfg.HasSourceAuth() {
		t.Skip("Source authentication not provided (use -license-token or -source-user/-source-password)")
	}

	// Setup test environment
	env := setupTestEnvironment(t, cfg)
	defer env.Cleanup()

	// Print header
	printTestHeader("Mirror Full Cycle", cfg.SourceRegistry, env.LogDir)

	// Run test steps
	runFullCycleTest(t, cfg, env)
}

// =============================================================================
// Test Environment
// =============================================================================

// testEnv holds all test environment state
type testEnv struct {
	LogDir         string
	LogFile        string
	ReportFile     string
	ComparisonFile string
	BundleDir      string
	TargetRegistry string
	Report         *TestReport
	Cleanup        func()
}

// setupTestEnvironment prepares everything needed for the test
func setupTestEnvironment(t *testing.T, cfg *Config) *testEnv {
	t.Helper()

	// Create log directory
	logDir := getLogDir("fullcycle")
	require.NoError(t, os.MkdirAll(logDir, 0755))

	// Setup target registry
	targetHost, targetPath, registryCleanup := setupTargetRegistry(t, cfg)
	targetRegistry := targetHost + targetPath
	t.Logf("Target registry: %s", targetRegistry)

	// Setup bundle directory
	bundleDir := setupBundleDir(t, cfg)

	// Initialize report
	report := &TestReport{
		TestName:       "TestMirrorE2E_FullCycle",
		StartTime:      time.Now(),
		SourceRegistry: cfg.SourceRegistry,
		TargetRegistry: targetRegistry,
		LogDir:         logDir,
	}

	env := &testEnv{
		LogDir:         logDir,
		LogFile:        filepath.Join(logDir, "test.log"),
		ReportFile:     filepath.Join(logDir, "report.txt"),
		ComparisonFile: filepath.Join(logDir, "comparison.txt"),
		BundleDir:      bundleDir,
		TargetRegistry: targetRegistry,
		Report:         report,
	}

	// Setup cleanup
	env.Cleanup = func() {
		registryCleanup()
		finalizeReport(t, env)
	}

	return env
}

// setupBundleDir creates the bundle directory
func setupBundleDir(t *testing.T, cfg *Config) string {
	t.Helper()

	if cfg.KeepBundle {
		bundleDir := filepath.Join(os.TempDir(), fmt.Sprintf("d8-mirror-e2e-%d", time.Now().Unix()))
		require.NoError(t, os.MkdirAll(bundleDir, 0755))
		t.Logf("Bundle directory (will be kept): %s", bundleDir)
		return bundleDir
	}

	bundleDir := t.TempDir()
	t.Logf("Bundle directory: %s", bundleDir)
	return bundleDir
}

// setupTargetRegistry sets up the target registry for testing
func setupTargetRegistry(t *testing.T, cfg *Config) (host, path string, cleanup func()) {
	t.Helper()

	if cfg.UseInMemoryRegistry() {
		reg := mirrorutil.SetupTestRegistry(false)
		repoPath := "/deckhouse/ee"
		t.Logf("Started test registry at %s%s", reg.Host, repoPath)
		return reg.Host, repoPath, reg.Close
	}

	return cfg.TargetRegistry, "", func() {}
}

// finalizeReport writes the final report
func finalizeReport(t *testing.T, env *testEnv) {
	t.Helper()

	env.Report.EndTime = time.Now()
	env.Report.Print()

	if err := env.Report.WriteToFile(env.ReportFile); err != nil {
		t.Logf("Warning: failed to write report: %v", err)
	} else {
		t.Logf("Report written to: %s", env.ReportFile)
	}
}

// =============================================================================
// Test Steps
// =============================================================================

// runFullCycleTest executes all test steps
func runFullCycleTest(t *testing.T, cfg *Config, env *testEnv) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Minute)
	defer cancel()

	// Step 1: Analyze source registry
	runAnalyzeStep(t, cfg, env)

	// Step 2: Pull images
	runPullStep(t, cfg, env)

	// Step 3: Push images
	runPushStep(t, cfg, env)

	// Step 4: Compare registries
	runCompareStep(t, ctx, cfg, env)

	// Success!
	printSuccessBox(env.Report.MatchedImages, env.Report.MatchedLayers)
}

// -----------------------------------------------------------------------------
// Step 1: Analyze Source Registry
// -----------------------------------------------------------------------------

func runAnalyzeStep(t *testing.T, cfg *Config, env *testEnv) {
	t.Helper()
	stepStart := time.Now()
	printStep(1, "Analyzing source registry")

	comparator := NewRegistryComparator(
		cfg.SourceRegistry, "",
		cfg.GetSourceAuth(), nil,
		cfg.TLSSkipVerify,
	)
	comparator.SetProgressCallback(func(msg string) {
		t.Logf("  %s", msg)
	})

	repos := comparator.discoverRepositories(cfg.SourceRegistry, comparator.sourceRemoteOpts)
	env.Report.SourceRepoCount = len(repos)
	env.Report.AddStep(
		fmt.Sprintf("Analyze source (%d repos)", len(repos)),
		"PASS", time.Since(stepStart), nil,
	)
	t.Logf("Source registry: %d repositories", len(repos))
}

// -----------------------------------------------------------------------------
// Step 2: Pull Images
// -----------------------------------------------------------------------------

func runPullStep(t *testing.T, cfg *Config, env *testEnv) {
	t.Helper()
	stepStart := time.Now()
	printStep(2, "Pulling images to bundle")

	cmd := buildPullCommand(cfg, env.BundleDir)
	t.Logf("Running: %s", cmd.String())

	err := runCommandWithLog(t, cmd, env.LogFile)
	if err != nil {
		env.Report.AddStep("Pull images", "FAIL", time.Since(stepStart), err)
		require.NoError(t, err, "Pull failed")
	}

	// Calculate bundle size
	bundleSize := calculateBundleSize(t, env.BundleDir)
	env.Report.BundleSize = bundleSize

	env.Report.AddStep(
		fmt.Sprintf("Pull images (%.2f GB bundle)", float64(bundleSize)/(1024*1024*1024)),
		"PASS", time.Since(stepStart), nil,
	)
	t.Logf("Pull completed: %.2f GB total", float64(bundleSize)/(1024*1024*1024))
}

// calculateBundleSize returns total size of bundle files
func calculateBundleSize(t *testing.T, bundleDir string) int64 {
	t.Helper()

	files, err := os.ReadDir(bundleDir)
	require.NoError(t, err)

	var totalSize int64
	for _, f := range files {
		if info, err := f.Info(); err == nil {
			totalSize += info.Size()
			t.Logf("  %s (%.2f MB)", f.Name(), float64(info.Size())/(1024*1024))
		}
	}
	return totalSize
}

// -----------------------------------------------------------------------------
// Step 3: Push Images
// -----------------------------------------------------------------------------

func runPushStep(t *testing.T, cfg *Config, env *testEnv) {
	t.Helper()
	stepStart := time.Now()
	printStep(3, "Pushing bundle to target registry")

	cmd := buildPushCommand(cfg, env.BundleDir, env.TargetRegistry)
	t.Logf("Running: %s", cmd.String())

	err := runCommandWithLog(t, cmd, env.LogFile)
	if err != nil {
		env.Report.AddStep("Push to registry", "FAIL", time.Since(stepStart), err)
		require.NoError(t, err, "Push failed")
	}

	env.Report.AddStep("Push to registry", "PASS", time.Since(stepStart), nil)
	t.Log("Push completed successfully")
}

// -----------------------------------------------------------------------------
// Step 4: Compare Registries
// -----------------------------------------------------------------------------

func runCompareStep(t *testing.T, ctx context.Context, cfg *Config, env *testEnv) {
	t.Helper()
	stepStart := time.Now()
	printStep(4, "Deep comparison of registries")

	comparator := NewRegistryComparator(
		cfg.SourceRegistry, env.TargetRegistry,
		cfg.GetSourceAuth(), cfg.GetTargetAuth(),
		cfg.TLSSkipVerify,
	)
	comparator.SetProgressCallback(func(msg string) {
		t.Logf("  %s", msg)
	})

	comparison, err := comparator.Compare(ctx)
	if err != nil {
		env.Report.AddStep("Deep comparison", "FAIL", time.Since(stepStart), err)
		require.NoError(t, err, "Comparison failed")
	}

	// Save detailed comparison
	saveComparisonReport(t, env.ComparisonFile, comparison)

	// Update report with comparison stats
	updateReportWithComparison(env.Report, comparison)

	// Print summary
	t.Log("")
	t.Log(comparison.Summary())

	// Check if identical
	if !comparison.IsIdentical() {
		env.Report.AddStep(
			fmt.Sprintf("Deep comparison (%d matched, %d missing, %d mismatched)",
				comparison.MatchedImages,
				len(comparison.MissingImages),
				len(comparison.MismatchedImages)),
			"FAIL", time.Since(stepStart),
			fmt.Errorf("registries differ: %d missing, %d mismatched",
				len(comparison.MissingImages),
				len(comparison.MismatchedImages)),
		)

		require.True(t, comparison.IsIdentical(),
			"Registries are NOT identical!\n\n%s\n\nSee %s for details",
			comparison.Summary(), env.ComparisonFile)
	}

	env.Report.AddStep(
		fmt.Sprintf("Deep comparison (%d images verified)", comparison.MatchedImages),
		"PASS", time.Since(stepStart), nil,
	)
}

// saveComparisonReport writes the detailed comparison to file
func saveComparisonReport(t *testing.T, path string, comparison *ComparisonReport) {
	t.Helper()

	if err := os.WriteFile(path, []byte(comparison.DetailedReport()), 0644); err != nil {
		t.Logf("Warning: failed to write comparison file: %v", err)
	} else {
		t.Logf("Detailed comparison written to: %s", path)
	}
}

// updateReportWithComparison updates test report with comparison results
func updateReportWithComparison(report *TestReport, comparison *ComparisonReport) {
	report.SourceImageCount = comparison.TotalSourceImages
	report.TargetRepoCount = len(comparison.TargetRepositories)
	report.TargetImageCount = comparison.TotalTargetImages
	report.MatchedImages = comparison.MatchedImages
	report.MissingImages = len(comparison.MissingImages)
	report.MismatchedImages = len(comparison.MismatchedImages)
	report.SkippedImages = comparison.SkippedImages
	report.MatchedLayers = comparison.MatchedLayers
	report.MissingLayers = comparison.MissingLayers
	report.ComparisonReport = comparison
}

// =============================================================================
// Helpers
// =============================================================================

// getLogDir returns the log directory path for e2e tests
// Logs are stored in testing/e2e/.logs/<testname>-<timestamp>/
func getLogDir(testName string) string {
	projectRoot := findProjectRoot()
	timestamp := time.Now().Format("20060102-150405")
	return filepath.Join(projectRoot, "testing", "e2e", ".logs", fmt.Sprintf("%s-%s", testName, timestamp))
}

// findProjectRoot finds the project root by looking for go.mod
func findProjectRoot() string {
	dir, _ := os.Getwd()

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			// Fallback to current dir
			dir, _ = os.Getwd()
			return dir
		}
		dir = parent
	}
}
