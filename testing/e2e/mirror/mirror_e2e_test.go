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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"github.com/stretchr/testify/require"
	"golang.org/x/term"

	mirrorutil "github.com/deckhouse/deckhouse-cli/testing/util/mirror"
)

func init() {
	// Force color output - go test buffers stdout which disables color detection
	// Check stderr instead (usually unbuffered) or honor FORCE_COLOR env
	if term.IsTerminal(int(os.Stderr.Fd())) || os.Getenv("FORCE_COLOR") != "" || os.Getenv("TERM") != "" {
		lipgloss.DefaultRenderer().SetColorProfile(termenv.TrueColor)
	}
}

// output is a helper to write to stderr (which preserves colors in go test)
var output = os.Stderr

func printLine(format string, args ...interface{}) {
	fmt.Fprintf(output, format+"\n", args...)
}

func print(format string, args ...interface{}) {
	fmt.Fprintf(output, format, args...)
}

// Lipgloss styles for beautiful terminal output
var (
	// Colors
	cyan   = lipgloss.Color("6")
	green  = lipgloss.Color("2")
	red    = lipgloss.Color("1")
	yellow = lipgloss.Color("3")
	blue   = lipgloss.Color("4")
	white  = lipgloss.Color("15")
	gray   = lipgloss.Color("8")

	// Text styles
	titleStyle   = lipgloss.NewStyle().Bold(true).Foreground(white)
	headerStyle  = lipgloss.NewStyle().Bold(true).Foreground(cyan)
	labelStyle   = lipgloss.NewStyle().Foreground(gray)
	valueStyle   = lipgloss.NewStyle().Foreground(white)
	dimStyle     = lipgloss.NewStyle().Foreground(gray)
	successStyle = lipgloss.NewStyle().Foreground(green)
	errorStyle   = lipgloss.NewStyle().Foreground(red)

	// Badge styles
	okBadge   = lipgloss.NewStyle().Bold(true).Foreground(green).Render("[OK]")
	failBadge = lipgloss.NewStyle().Bold(true).Foreground(red).Render("[FAIL]")
	skipBadge = lipgloss.NewStyle().Foreground(yellow).Render("[SKIP]")

	// Step styles
	stepNumStyle  = lipgloss.NewStyle().Bold(true).Foreground(blue)
	stepTextStyle = lipgloss.NewStyle().Bold(true).Foreground(white)

	// Separator
	separatorStyle = lipgloss.NewStyle().Foreground(cyan)
)

// printStep prints a formatted step header
func printStep(num int, description string) {
	badge := stepNumStyle.Render(fmt.Sprintf("[STEP %d]", num))
	text := stepTextStyle.Render(description)
	printLine("\n%s %s", badge, text)
}

// renderSeparator creates a separator line
func renderSeparator(char string, width int) string {
	return separatorStyle.Render(strings.Repeat(char, width))
}

// TestMirrorE2E_FullCycle performs a complete mirror cycle and validates
// that source and target registries are identical.
//
// This is a heavy E2E test that:
// 1. Discovers all repositories in source registry
// 2. Pulls all images to local bundle using d8 mirror pull
// 3. Pushes bundle to target registry using d8 mirror push
// 4. Discovers all repositories in target registry
// 5. Compares EVERY tag and digest between source and target
// 6. Generates detailed comparison report
//
// Run with:
//
//	go test -v ./testing/e2e/mirror/... \
//	  -source-registry=localhost:443/deckhouse \
//	  -source-user=admin -source-password=admin \
//	  -tls-skip-verify
func TestMirrorE2E_FullCycle(t *testing.T) {
	cfg := GetConfig()

	// Create log directory in project (gitignored)
	logDir := getLogDir("fullcycle")
	require.NoError(t, os.MkdirAll(logDir, 0755))
	logFile := filepath.Join(logDir, "test.log")
	reportFile := filepath.Join(logDir, "report.txt")
	comparisonFile := filepath.Join(logDir, "comparison.txt")

	printLine("")
	printLine(renderSeparator("═", 80))
	printLine("  %s", titleStyle.Render("E2E TEST: Mirror Full Cycle"))
	printLine(renderSeparator("═", 80))
	printLine("  %s %s", labelStyle.Render("Source:"), valueStyle.Render(cfg.SourceRegistry))
	printLine("  %s %s", labelStyle.Render("Logs:  "), dimStyle.Render(logDir))
	printLine(renderSeparator("═", 80))
	printLine("")

	if !cfg.HasSourceAuth() {
		t.Skip("Source authentication not provided (use -license-token or -source-user/-source-password)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Minute)
	defer cancel()

	// Initialize report
	report := &TestReport{
		TestName:       "TestMirrorE2E_FullCycle",
		StartTime:      time.Now(),
		SourceRegistry: cfg.SourceRegistry,
		LogDir:         logDir,
	}

	// Ensure report is written at the end
	defer func() {
		report.EndTime = time.Now()
		report.Print()
		if err := report.WriteToFile(reportFile); err != nil {
			t.Logf("Warning: failed to write report: %v", err)
		} else {
			t.Logf("Report written to: %s", reportFile)
		}
	}()

	// Setup target registry
	targetHost, targetPath, cleanup := setupTargetRegistry(t, cfg)
	defer cleanup()

	targetRegistry := targetHost + targetPath
	report.TargetRegistry = targetRegistry
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

	// ========================================================================
	// STEP 1: Analyze source registry
	// ========================================================================
	stepStart := time.Now()
	printStep(1, "Analyzing source registry")

	sourceComparator := NewRegistryComparator(
		cfg.SourceRegistry, "",
		cfg.GetSourceAuth(), nil,
		cfg.TLSSkipVerify,
	)
	sourceComparator.SetProgressCallback(func(msg string) {
		t.Logf("  %s", msg)
	})

	sourceRepos, err := sourceComparator.discoverRepositories(ctx, cfg.SourceRegistry, sourceComparator.sourceRemoteOpts)
	if err != nil {
		report.AddStep("Analyze source registry", "FAIL", time.Since(stepStart), err)
		require.NoError(t, err, "Failed to analyze source registry")
	}

	report.SourceRepoCount = len(sourceRepos)
	report.AddStep(fmt.Sprintf("Analyze source (%d repos)", len(sourceRepos)),
		"PASS", time.Since(stepStart), nil)
	t.Logf("Source registry: %d repositories", len(sourceRepos))

	// ========================================================================
	// STEP 2: Execute pull
	// ========================================================================
	stepStart = time.Now()
	printStep(2, "Pulling images to bundle")

	pullCmd := buildPullCommand(cfg, bundleDir)
	t.Logf("Running: %s", pullCmd.String())

	_, err = runCommandWithLog(t, pullCmd, logFile)
	if err != nil {
		report.AddStep("Pull images", "FAIL", time.Since(stepStart), err)
		require.NoError(t, err, "Pull failed")
	}

	// Log bundle contents
	bundleFiles, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	var totalSize int64
	for _, f := range bundleFiles {
		if info, err := f.Info(); err == nil {
			totalSize += info.Size()
			t.Logf("  %s (%.2f MB)", f.Name(), float64(info.Size())/(1024*1024))
		}
	}
	report.BundleSize = totalSize
	report.AddStep(fmt.Sprintf("Pull images (%.2f GB bundle)", float64(totalSize)/(1024*1024*1024)),
		"PASS", time.Since(stepStart), nil)
	t.Logf("Pull completed: %d files, %.2f GB total", len(bundleFiles), float64(totalSize)/(1024*1024*1024))

	// ========================================================================
	// STEP 3: Execute push
	// ========================================================================
	stepStart = time.Now()
	printStep(3, "Pushing bundle to target registry")

	pushCmd := buildPushCommand(cfg, bundleDir, targetRegistry)
	t.Logf("Running: %s", pushCmd.String())

	_, err = runCommandWithLog(t, pushCmd, logFile)
	if err != nil {
		report.AddStep("Push to registry", "FAIL", time.Since(stepStart), err)
		require.NoError(t, err, "Push failed")
	}
	report.AddStep("Push to registry", "PASS", time.Since(stepStart), nil)
	t.Log("Push completed successfully")

	// ========================================================================
	// STEP 4: Deep comparison of source and target
	// ========================================================================
	stepStart = time.Now()
	printStep(4, "Deep comparison of registries")

	comparator := NewRegistryComparator(
		cfg.SourceRegistry, targetRegistry,
		cfg.GetSourceAuth(), cfg.GetTargetAuth(),
		cfg.TLSSkipVerify,
	)
	comparator.SetProgressCallback(func(msg string) {
		t.Logf("  %s", msg)
	})

	comparisonReport, err := comparator.Compare(ctx)
	if err != nil {
		report.AddStep("Deep comparison", "FAIL", time.Since(stepStart), err)
		require.NoError(t, err, "Comparison failed")
	}

	// Save detailed comparison report
	if err := os.WriteFile(comparisonFile, []byte(comparisonReport.DetailedReport()), 0644); err != nil {
		t.Logf("Warning: failed to write comparison file: %v", err)
	} else {
		t.Logf("Detailed comparison written to: %s", comparisonFile)
	}

	// Update report with comparison stats
	report.SourceImageCount = comparisonReport.TotalSourceImages
	report.TargetRepoCount = len(comparisonReport.TargetRepositories)
	report.TargetImageCount = comparisonReport.TotalTargetImages
	report.MatchedImages = comparisonReport.MatchedImages
	report.MissingImages = len(comparisonReport.MissingImages)
	report.MismatchedImages = len(comparisonReport.MismatchedImages)
	report.SkippedImages = comparisonReport.SkippedImages
	report.MatchedLayers = comparisonReport.MatchedLayers
	report.MissingLayers = comparisonReport.MissingLayers
	report.ComparisonReport = comparisonReport

	// Print summary
	t.Log("")
	t.Log(comparisonReport.Summary())

	if !comparisonReport.IsIdentical() {
		report.AddStep(fmt.Sprintf("Deep comparison (%d matched, %d missing, %d mismatched)",
			comparisonReport.MatchedImages,
			len(comparisonReport.MissingImages),
			len(comparisonReport.MismatchedImages)),
			"FAIL", time.Since(stepStart),
			fmt.Errorf("registries differ: %d missing, %d mismatched",
				len(comparisonReport.MissingImages),
				len(comparisonReport.MismatchedImages)))

		require.True(t, comparisonReport.IsIdentical(),
			"Registries are NOT identical!\n\n%s\n\nSee %s for details",
			comparisonReport.Summary(), comparisonFile)
	}

	report.AddStep(fmt.Sprintf("Deep comparison (%d images verified)", comparisonReport.MatchedImages),
		"PASS", time.Since(stepStart), nil)

	// ========================================================================
	// SUCCESS
	// ========================================================================
	successBox := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		BorderForeground(green).
		Padding(0, 2).
		Foreground(green)

	printLine("")
	printLine(successBox.Render(fmt.Sprintf(
		"SUCCESS: REGISTRIES ARE IDENTICAL\n\nVerified: %d images, %d layers\nAll manifest, config, and layer digests match!",
		comparisonReport.MatchedImages,
		comparisonReport.MatchedLayers,
	)))
}

// getLogDir returns the log directory path for e2e tests.
// Logs are stored in testing/e2e/.logs/<testname>-<timestamp>/
func getLogDir(testName string) string {
	// Get project root by finding go.mod
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			// Fallback to current dir if go.mod not found
			dir, _ = os.Getwd()
			break
		}
		dir = parent
	}

	timestamp := time.Now().Format("20060102-150405")
	return filepath.Join(dir, "testing", "e2e", ".logs", fmt.Sprintf("%s-%s", testName, timestamp))
}

// setupTargetRegistry sets up the target registry for testing
// Returns host, path, and cleanup function
func setupTargetRegistry(t *testing.T, cfg *Config) (string, string, func()) {
	t.Helper()

	if cfg.UseInMemoryRegistry() {
		// Use disk-based test registry
		reg := mirrorutil.SetupTestRegistry(false)
		repoPath := "/deckhouse/ee"
		t.Logf("Started test registry at %s%s", reg.Host, repoPath)
		return reg.Host, repoPath, func() {
			reg.Close()
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

	// Skip modules (for testing failure scenarios)
	if cfg.NoModules {
		args = append(args, "--no-modules")
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

// runCommandWithLog runs command with streaming and saves full output to a log file
func runCommandWithLog(t *testing.T, cmd *exec.Cmd, logFile string) ([]byte, error) {
	t.Helper()

	var buf bytes.Buffer

	// Open log file for appending
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Logf("Warning: could not open log file %s: %v", logFile, err)
		// Fallback to just streaming
		cmd.Stdout = io.MultiWriter(os.Stdout, &buf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &buf)
		return buf.Bytes(), cmd.Run()
	}
	defer f.Close()

	// Write command to log
	fmt.Fprintf(f, "\n\n========== COMMAND: %s ==========\n", cmd.String())
	fmt.Fprintf(f, "Started: %s\n\n", time.Now().Format(time.RFC3339))

	// Create multi-writers to write to stdout, buffer, AND log file
	cmd.Stdout = io.MultiWriter(os.Stdout, &buf, f)
	cmd.Stderr = io.MultiWriter(os.Stderr, &buf, f)

	cmdErr := cmd.Run()

	// Write result to log
	if cmdErr != nil {
		fmt.Fprintf(f, "\n\n========== COMMAND FAILED: %v ==========\n", cmdErr)
	} else {
		fmt.Fprintf(f, "\n\n========== COMMAND SUCCEEDED ==========\n")
	}

	return buf.Bytes(), cmdErr
}

// TestReport collects test execution results for final summary
type TestReport struct {
	TestName       string
	StartTime      time.Time
	EndTime        time.Time
	SourceRegistry string
	TargetRegistry string
	LogDir         string

	// Source stats
	SourceRepoCount  int
	SourceImageCount int

	// Target stats
	TargetRepoCount  int
	TargetImageCount int

	// Comparison stats
	MatchedImages    int
	MissingImages    int
	MismatchedImages int
	SkippedImages    int // Digest-based, .att, .sig tags

	// Deep comparison stats
	MatchedLayers int
	MissingLayers int

	// Bundle stats
	BundleSize int64

	// Steps
	Steps []StepResult

	// Full comparison report
	ComparisonReport *ComparisonReport
}

// StepResult represents a single step in the test
type StepResult struct {
	Name     string
	Status   string // "PASS", "FAIL", "SKIP"
	Duration time.Duration
	Error    string
}

// AddStep adds a step result to the report
func (r *TestReport) AddStep(name, status string, duration time.Duration, err error) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	r.Steps = append(r.Steps, StepResult{
		Name:     name,
		Status:   status,
		Duration: duration,
		Error:    errStr,
	})
}

// WriteToFile writes the report to a file
func (r *TestReport) WriteToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "================================================================================\n")
	fmt.Fprintf(f, "E2E TEST REPORT: %s\n", r.TestName)
	fmt.Fprintf(f, "================================================================================\n\n")

	fmt.Fprintf(f, "EXECUTION:\n")
	fmt.Fprintf(f, "  Started:  %s\n", r.StartTime.Format(time.RFC3339))
	fmt.Fprintf(f, "  Finished: %s\n", r.EndTime.Format(time.RFC3339))
	fmt.Fprintf(f, "  Duration: %s\n", r.EndTime.Sub(r.StartTime).Round(time.Second))
	fmt.Fprintf(f, "  Log dir:  %s\n\n", r.LogDir)

	fmt.Fprintf(f, "REGISTRIES:\n")
	fmt.Fprintf(f, "  Source: %s\n", r.SourceRegistry)
	fmt.Fprintf(f, "  Target: %s\n\n", r.TargetRegistry)

	fmt.Fprintf(f, "IMAGES TO VERIFY:\n")
	fmt.Fprintf(f, "  Source: %d images (%d repos)\n", r.SourceImageCount, r.SourceRepoCount)
	fmt.Fprintf(f, "  Target: %d images (%d repos)\n", r.TargetImageCount, r.TargetRepoCount)
	if r.SkippedImages > 0 {
		fmt.Fprintf(f, "  (excluded %d internal tags from comparison)\n", r.SkippedImages)
	}
	fmt.Fprintf(f, "\n")

	fmt.Fprintf(f, "BUNDLE:\n")
	fmt.Fprintf(f, "  Size: %.2f GB\n\n", float64(r.BundleSize)/(1024*1024*1024))

	fmt.Fprintf(f, "VERIFICATION RESULTS:\n")
	fmt.Fprintf(f, "  Images matched:    %d (manifest + config + layers)\n", r.MatchedImages)
	fmt.Fprintf(f, "  Layers verified:   %d\n", r.MatchedLayers)
	fmt.Fprintf(f, "  Missing images:    %d\n", r.MissingImages)
	fmt.Fprintf(f, "  Digest mismatch:   %d\n", r.MismatchedImages)
	fmt.Fprintf(f, "  Missing layers:    %d\n\n", r.MissingLayers)

	fmt.Fprintf(f, "STEPS:\n")
	passCount, failCount := 0, 0
	for _, step := range r.Steps {
		if step.Status == "PASS" {
			passCount++
			fmt.Fprintf(f, "  [PASS] %s (%s)\n", step.Name, step.Duration.Round(time.Millisecond))
		} else if step.Status == "FAIL" {
			failCount++
			fmt.Fprintf(f, "  [FAIL] %s (%s)\n", step.Name, step.Duration.Round(time.Millisecond))
			if step.Error != "" {
				fmt.Fprintf(f, "         ERROR: %s\n", step.Error)
			}
		} else {
			fmt.Fprintf(f, "  [SKIP] %s\n", step.Name)
		}
	}

	fmt.Fprintf(f, "\n================================================================================\n")
	if failCount > 0 {
		fmt.Fprintf(f, "RESULT: FAILED (%d passed, %d failed)\n", passCount, failCount)
	} else {
		fmt.Fprintf(f, "RESULT: PASSED - REGISTRIES ARE IDENTICAL\n")
		fmt.Fprintf(f, "  %d repositories verified\n", r.SourceRepoCount)
		fmt.Fprintf(f, "  %d images verified\n", r.MatchedImages)
	}
	fmt.Fprintf(f, "================================================================================\n")

	return nil
}

// Print prints the report to stdout with beautiful lipgloss styling
func (r *TestReport) Print() {
	duration := r.EndTime.Sub(r.StartTime)
	if r.EndTime.IsZero() {
		duration = time.Since(r.StartTime)
	}

	var content strings.Builder

	// Header
	content.WriteString("\n")
	content.WriteString(renderSeparator("═", 80) + "\n")
	content.WriteString("  " + titleStyle.Render("E2E TEST REPORT") + "\n")
	content.WriteString(renderSeparator("═", 80) + "\n\n")

	// Duration
	content.WriteString("  " + labelStyle.Render("Duration: ") + dimStyle.Render(duration.Round(time.Second).String()) + "\n\n")

	// Registries section
	content.WriteString("  " + headerStyle.Render("REGISTRIES") + "\n")
	content.WriteString("    " + labelStyle.Render("Source:  ") + valueStyle.Render(r.SourceRegistry) + "\n")
	content.WriteString("    " + labelStyle.Render("Target:  ") + valueStyle.Render(r.TargetRegistry) + "\n\n")

	// Images to verify section
	content.WriteString("  " + headerStyle.Render("IMAGES TO VERIFY") + "\n")
	content.WriteString(fmt.Sprintf("    %s %s images (%d repos)\n",
		labelStyle.Render("Source:"),
		valueStyle.Render(fmt.Sprintf("%d", r.SourceImageCount)),
		r.SourceRepoCount))
	content.WriteString(fmt.Sprintf("    %s %s images (%d repos)\n",
		labelStyle.Render("Target:"),
		valueStyle.Render(fmt.Sprintf("%d", r.TargetImageCount)),
		r.TargetRepoCount))
	if r.SkippedImages > 0 {
		content.WriteString("    " + dimStyle.Render(fmt.Sprintf("(%d internal tags excluded)", r.SkippedImages)) + "\n")
	}
	content.WriteString("\n")

	// Verification results section
	content.WriteString("  " + headerStyle.Render("VERIFICATION") + "\n")
	content.WriteString(fmt.Sprintf("    %s %s %s\n",
		okBadge,
		labelStyle.Render("Images matched: "),
		successStyle.Render(fmt.Sprintf("%d", r.MatchedImages))+" "+dimStyle.Render("(manifest + config + layers)")))
	content.WriteString(fmt.Sprintf("    %s %s %s\n",
		okBadge,
		labelStyle.Render("Layers verified:"),
		successStyle.Render(fmt.Sprintf("%d", r.MatchedLayers))))

	if r.MissingImages > 0 {
		content.WriteString(fmt.Sprintf("    %s %s %s\n",
			failBadge,
			labelStyle.Render("Missing images: "),
			errorStyle.Render(fmt.Sprintf("%d", r.MissingImages))))
	}
	if r.MismatchedImages > 0 {
		content.WriteString(fmt.Sprintf("    %s %s %s\n",
			failBadge,
			labelStyle.Render("Digest mismatch:"),
			errorStyle.Render(fmt.Sprintf("%d", r.MismatchedImages))))
	}
	if r.MissingLayers > 0 {
		content.WriteString(fmt.Sprintf("    %s %s %s\n",
			failBadge,
			labelStyle.Render("Missing layers: "),
			errorStyle.Render(fmt.Sprintf("%d", r.MissingLayers))))
	}
	content.WriteString("\n")

	// Steps section
	content.WriteString("  " + headerStyle.Render("STEPS") + "\n")
	passCount, failCount := 0, 0
	for _, step := range r.Steps {
		dur := dimStyle.Render(fmt.Sprintf("(%s)", step.Duration.Round(time.Millisecond)))
		if step.Status == "PASS" {
			passCount++
			content.WriteString(fmt.Sprintf("    %s %s %s\n", okBadge, step.Name, dur))
		} else if step.Status == "FAIL" {
			failCount++
			content.WriteString(fmt.Sprintf("    %s %s %s\n", failBadge, step.Name, dur))
			if step.Error != "" {
				content.WriteString("           " + errorStyle.Render("ERROR: "+step.Error) + "\n")
			}
		} else {
			content.WriteString(fmt.Sprintf("    %s %s\n", skipBadge, step.Name))
		}
	}
	content.WriteString("\n")

	// Result box
	content.WriteString(renderSeparator("─", 80) + "\n")
	if failCount > 0 {
		resultStyle := lipgloss.NewStyle().Bold(true).Foreground(red)
		content.WriteString("  " + resultStyle.Render("RESULT: FAILED") + fmt.Sprintf(" (%d passed, %d failed)\n", passCount, failCount))
	} else {
		resultStyle := lipgloss.NewStyle().Bold(true).Foreground(green)
		content.WriteString("  " + resultStyle.Render("RESULT: PASSED") + " - REGISTRIES ARE IDENTICAL\n")
		content.WriteString("  " + successStyle.Render(fmt.Sprintf("%d images, %d layers", r.MatchedImages, r.MatchedLayers)) + " - all hashes verified\n")
	}
	content.WriteString(renderSeparator("═", 80) + "\n")

	print("%s", content.String())
}
