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
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// =============================================================================
// Test Report
// =============================================================================

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

// =============================================================================
// Report Methods
// =============================================================================

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

// WriteToFile writes the report to a file in plain text format
func (r *TestReport) WriteToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := func(format string, args ...interface{}) {
		fmt.Fprintf(f, format, args...)
	}

	// Header
	w("================================================================================\n")
	w("E2E TEST REPORT: %s\n", r.TestName)
	w("================================================================================\n\n")

	// Execution info
	w("EXECUTION:\n")
	w("  Started:  %s\n", r.StartTime.Format(time.RFC3339))
	w("  Finished: %s\n", r.EndTime.Format(time.RFC3339))
	w("  Duration: %s\n", r.EndTime.Sub(r.StartTime).Round(time.Second))
	w("  Log dir:  %s\n\n", r.LogDir)

	// Registries
	w("REGISTRIES:\n")
	w("  Source: %s\n", r.SourceRegistry)
	w("  Target: %s\n\n", r.TargetRegistry)

	// Images
	w("IMAGES TO VERIFY:\n")
	w("  Source: %d images (%d repos)\n", r.SourceImageCount, r.SourceRepoCount)
	w("  Target: %d images (%d repos)\n", r.TargetImageCount, r.TargetRepoCount)
	if r.SkippedImages > 0 {
		w("  (excluded %d internal tags from comparison)\n", r.SkippedImages)
	}
	w("\n")

	// Bundle
	w("BUNDLE:\n")
	w("  Size: %.2f GB\n\n", float64(r.BundleSize)/(1024*1024*1024))

	// Verification results
	w("VERIFICATION RESULTS:\n")
	w("  Images matched:    %d (manifest + config + layers)\n", r.MatchedImages)
	w("  Layers verified:   %d\n", r.MatchedLayers)
	w("  Missing images:    %d\n", r.MissingImages)
	w("  Digest mismatch:   %d\n", r.MismatchedImages)
	w("  Missing layers:    %d\n\n", r.MissingLayers)

	// Steps
	w("STEPS:\n")
	passCount, failCount := r.countSteps()
	for _, step := range r.Steps {
		switch step.Status {
		case "PASS":
			w("  [PASS] %s (%s)\n", step.Name, step.Duration.Round(time.Millisecond))
		case "FAIL":
			w("  [FAIL] %s (%s)\n", step.Name, step.Duration.Round(time.Millisecond))
			if step.Error != "" {
				w("         ERROR: %s\n", step.Error)
			}
		default:
			w("  [SKIP] %s\n", step.Name)
		}
	}

	// Result
	w("\n================================================================================\n")
	if failCount > 0 {
		w("RESULT: FAILED (%d passed, %d failed)\n", passCount, failCount)
	} else {
		w("RESULT: PASSED - REGISTRIES ARE IDENTICAL\n")
		w("  %d repositories verified\n", r.SourceRepoCount)
		w("  %d images verified\n", r.MatchedImages)
	}
	w("================================================================================\n")

	return nil
}

// Print prints the report to stderr with beautiful lipgloss styling
func (r *TestReport) Print() {
	duration := r.EndTime.Sub(r.StartTime)
	if r.EndTime.IsZero() {
		duration = time.Since(r.StartTime)
	}

	var b strings.Builder

	// Header
	b.WriteString("\n")
	b.WriteString(separator("═") + "\n")
	b.WriteString("  " + styleTitle.Render("E2E TEST REPORT") + "\n")
	b.WriteString(separator("═") + "\n\n")

	// Duration
	b.WriteString("  " + styleLabel.Render("Duration: ") + styleDim.Render(duration.Round(time.Second).String()) + "\n\n")

	// Registries
	b.WriteString("  " + styleHeader.Render("REGISTRIES") + "\n")
	b.WriteString("    " + styleLabel.Render("Source:  ") + styleValue.Render(r.SourceRegistry) + "\n")
	b.WriteString("    " + styleLabel.Render("Target:  ") + styleValue.Render(r.TargetRegistry) + "\n\n")

	// Images to verify
	b.WriteString("  " + styleHeader.Render("IMAGES TO VERIFY") + "\n")
	b.WriteString(fmt.Sprintf("    %s %s images (%d repos)\n",
		styleLabel.Render("Source:"),
		styleValue.Render(fmt.Sprintf("%d", r.SourceImageCount)),
		r.SourceRepoCount))
	b.WriteString(fmt.Sprintf("    %s %s images (%d repos)\n",
		styleLabel.Render("Target:"),
		styleValue.Render(fmt.Sprintf("%d", r.TargetImageCount)),
		r.TargetRepoCount))
	if r.SkippedImages > 0 {
		b.WriteString("    " + styleDim.Render(fmt.Sprintf("(%d internal tags excluded)", r.SkippedImages)) + "\n")
	}
	b.WriteString("\n")

	// Verification results
	b.WriteString("  " + styleHeader.Render("VERIFICATION") + "\n")
	b.WriteString(fmt.Sprintf("    %s %s %s\n",
		badgeOK,
		styleLabel.Render("Images matched: "),
		styleSuccess.Render(fmt.Sprintf("%d", r.MatchedImages))+" "+styleDim.Render("(manifest + config + layers)")))
	b.WriteString(fmt.Sprintf("    %s %s %s\n",
		badgeOK,
		styleLabel.Render("Layers verified:"),
		styleSuccess.Render(fmt.Sprintf("%d", r.MatchedLayers))))

	if r.MissingImages > 0 {
		b.WriteString(fmt.Sprintf("    %s %s %s\n",
			badgeFail,
			styleLabel.Render("Missing images: "),
			styleError.Render(fmt.Sprintf("%d", r.MissingImages))))
	}
	if r.MismatchedImages > 0 {
		b.WriteString(fmt.Sprintf("    %s %s %s\n",
			badgeFail,
			styleLabel.Render("Digest mismatch:"),
			styleError.Render(fmt.Sprintf("%d", r.MismatchedImages))))
	}
	if r.MissingLayers > 0 {
		b.WriteString(fmt.Sprintf("    %s %s %s\n",
			badgeFail,
			styleLabel.Render("Missing layers: "),
			styleError.Render(fmt.Sprintf("%d", r.MissingLayers))))
	}
	b.WriteString("\n")

	// Steps
	b.WriteString("  " + styleHeader.Render("STEPS") + "\n")
	passCount, failCount := r.countSteps()
	for _, step := range r.Steps {
		dur := styleDim.Render(fmt.Sprintf("(%s)", step.Duration.Round(time.Millisecond)))
		switch step.Status {
		case "PASS":
			b.WriteString(fmt.Sprintf("    %s %s %s\n", badgeOK, step.Name, dur))
		case "FAIL":
			b.WriteString(fmt.Sprintf("    %s %s %s\n", badgeFail, step.Name, dur))
			if step.Error != "" {
				b.WriteString("           " + styleError.Render("ERROR: "+step.Error) + "\n")
			}
		default:
			b.WriteString(fmt.Sprintf("    %s %s\n", badgeSkip, step.Name))
		}
	}
	b.WriteString("\n")

	// Result box
	b.WriteString(separator("─") + "\n")
	if failCount > 0 {
		resultStyle := lipgloss.NewStyle().Bold(true).Foreground(colorRed)
		b.WriteString("  " + resultStyle.Render("RESULT: FAILED") + fmt.Sprintf(" (%d passed, %d failed)\n", passCount, failCount))
	} else {
		resultStyle := lipgloss.NewStyle().Bold(true).Foreground(colorGreen)
		b.WriteString("  " + resultStyle.Render("RESULT: PASSED") + " - REGISTRIES ARE IDENTICAL\n")
		b.WriteString("  " + styleSuccess.Render(fmt.Sprintf("%d images, %d layers", r.MatchedImages, r.MatchedLayers)) + " - all hashes verified\n")
	}
	b.WriteString(separator("═") + "\n")

	writeRawf("%s", b.String())
}

// countSteps counts passed and failed steps
func (r *TestReport) countSteps() (int, int) {
	var passed, failed int
	for _, step := range r.Steps {
		switch step.Status {
		case "PASS":
			passed++
		case "FAIL":
			failed++
		}
	}
	return passed, failed
}
