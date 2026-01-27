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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/testing/e2e/mirror/internal"
	mirrorutil "github.com/deckhouse/deckhouse-cli/testing/util/mirror"
)

type testEnv struct {
	LogDir         string
	LogFile        string
	ReportFile     string
	ComparisonFile string
	BundleDir      string
	TargetRegistry string
	Report         *internal.TestReport
	Cleanup        func()
}

func setupTestEnvironment(t *testing.T, cfg *internal.Config) *testEnv {
	t.Helper()

	logDir := getLogDir(t.Name())
	require.NoError(t, os.MkdirAll(logDir, 0755))

	targetHost, targetPath, registryCleanup := setupTargetRegistry(t, cfg)
	targetRegistry := targetHost + targetPath
	t.Logf("Target registry: %s", targetRegistry)

	bundleDir := setupBundleDir(t, cfg)

	report := &internal.TestReport{
		TestName:       t.Name(),
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

	env.Cleanup = func() {
		registryCleanup()
		finalizeReport(t, env)
	}

	return env
}

func setupBundleDir(t *testing.T, cfg *internal.Config) string {
	t.Helper()

	if cfg.ExistingBundle != "" {
		if _, err := os.Stat(cfg.ExistingBundle); os.IsNotExist(err) {
			t.Fatalf("Existing bundle directory not found: %s", cfg.ExistingBundle)
		}
		t.Logf("Using existing bundle: %s", cfg.ExistingBundle)
		return cfg.ExistingBundle
	}

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

func setupTargetRegistry(t *testing.T, cfg *internal.Config) (host, path string, cleanup func()) {
	t.Helper()

	if cfg.UseInMemoryRegistry() {
		reg := mirrorutil.SetupTestRegistry(false)
		repoPath := "/deckhouse/ee"
		t.Logf("Started test registry at %s%s", reg.Host, repoPath)
		return reg.Host, repoPath, reg.Close
	}

	return cfg.TargetRegistry, "", func() {}
}

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

func runPullStep(t *testing.T, cfg *internal.Config, env *testEnv) {
	t.Helper()
	stepStart := time.Now()

	cmd := internal.BuildPullCommand(cfg, env.BundleDir)
	t.Logf("Running: %s", cmd.String())

	err := internal.RunCommandWithLog(t, cmd, env.LogFile)
	if err != nil {
		env.Report.AddStep("Pull images", "FAIL", time.Since(stepStart), err)
		require.NoError(t, err, "Pull failed")
	}

	bundleInfo := validateBundle(t, env.BundleDir, cfg)
	env.Report.BundleSize = bundleInfo.TotalSize
	if len(bundleInfo.Modules) > 0 {
		env.Report.ExpectedModules = bundleInfo.Modules
	}

	env.Report.AddStep(
		fmt.Sprintf("Pull images (%.2f GB bundle)", float64(bundleInfo.TotalSize)/(1024*1024*1024)),
		"PASS", time.Since(stepStart), nil,
	)
	t.Logf("Pull completed: %.2f GB total", float64(bundleInfo.TotalSize)/(1024*1024*1024))
}

func runPushStep(t *testing.T, cfg *internal.Config, env *testEnv) {
	t.Helper()
	stepStart := time.Now()

	cmd := internal.BuildPushCommand(cfg, env.BundleDir, env.TargetRegistry)
	t.Logf("Running: %s", cmd.String())

	err := internal.RunCommandWithLog(t, cmd, env.LogFile)
	if err != nil {
		env.Report.AddStep("Push to registry", "FAIL", time.Since(stepStart), err)
		require.NoError(t, err, "Push failed")
	}

	env.Report.AddStep("Push to registry", "PASS", time.Since(stepStart), nil)
	t.Log("Push completed successfully")
}

type BundleInfo struct {
	TotalSize   int64
	Modules     []string
	HasPlatform bool
	HasSecurity bool
}

func validateBundle(t *testing.T, bundleDir string, cfg *internal.Config) *BundleInfo {
	t.Helper()

	files, err := os.ReadDir(bundleDir)
	require.NoError(t, err, "Failed to read bundle directory")

	info := &BundleInfo{}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		finfo, err := f.Info()
		require.NoError(t, err)
		info.TotalSize += finfo.Size()

		name := f.Name()
		t.Logf("  %s (%.2f MB)", name, float64(finfo.Size())/(1024*1024))

		switch {
		case name == "platform.tar" || strings.HasPrefix(name, "platform."):
			info.HasPlatform = true
		case name == "security.tar" || strings.HasPrefix(name, "security."):
			info.HasSecurity = true
		case strings.HasPrefix(name, "module-") && strings.Contains(name, ".tar"):
			moduleName := strings.TrimPrefix(name, "module-")
			moduleName = strings.Split(moduleName, ".")[0]
			if moduleName != "" && !slices.Contains(info.Modules, moduleName) {
				info.Modules = append(info.Modules, moduleName)
			}
		}
	}

	if !cfg.NoPlatform {
		require.True(t, info.HasPlatform, "Bundle missing platform.tar - pull may have failed!")
	}
	if !cfg.NoSecurity {
		require.True(t, info.HasSecurity, "Bundle missing security.tar - pull may have failed!")
	}
	if !cfg.NoModules && len(cfg.IncludeModules) == 0 {
		require.NotEmpty(t, info.Modules, "Bundle has no modules - pull may have failed!")
	}

	if len(info.Modules) > 0 {
		t.Logf("Bundle contains %d modules: %v", len(info.Modules), info.Modules)
	}

	return info
}

func getLogDir(testName string) string {
	projectRoot := internal.FindProjectRoot()
	timestamp := time.Now().Format("20060102-150405")
	safeName := strings.ReplaceAll(testName, "/", "-")
	return filepath.Join(projectRoot, "testing", "e2e", ".logs", fmt.Sprintf("%s-%s", safeName, timestamp))
}

