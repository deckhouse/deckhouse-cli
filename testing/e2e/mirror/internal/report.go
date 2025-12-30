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

package internal

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

type TestReport struct {
	TestName       string
	StartTime      time.Time
	EndTime        time.Time
	SourceRegistry string
	TargetRegistry string
	LogDir         string

	SourceImageCount int
	TargetImageCount int

	TotalImages   int
	MatchedImages int
	MissingImages int

	ExpectedAttTags int
	FoundAttTags    int
	MissingAttTags  int

	ModulesExpected int
	ModulesFound     int
	ModulesMissing   int

	SecurityExpected int
	SecurityFound     int
	SecurityMissing   int

	BundleSize      int64
	ExpectedModules  []string

	Steps []StepResult
}

type StepResult struct {
	Name     string
	Status   string
	Duration time.Duration
	Error    string
}

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

func (r *TestReport) WriteToFile(path string) error {
	content := r.String()
	return os.WriteFile(path, []byte(content), 0644)
}

func (r *TestReport) formatSection(useColors bool, label string, found, expected, missing int) string {
	if expected == 0 && missing == 0 {
		return ""
	}

	var badge, value string
	if useColors {
		if missing > 0 {
			badge = BadgeFail
			value = StyleError.Render(fmt.Sprintf("%d / %d", found, expected))
		} else {
			badge = BadgeOK
			value = StyleSuccess.Render(fmt.Sprintf("%d / %d", found, expected))
		}
		label = StyleLabel.Render(label)
		return fmt.Sprintf("    %s %s %s\n", badge, label, value)
	}

	status := "[OK]"
	if missing > 0 {
		status = "[FAIL]"
	}
	labelText := strings.TrimSuffix(label, ":")
	result := fmt.Sprintf("  %s %s:   %d / %d\n", status, labelText, found, expected)
	if missing > 0 {
		result += fmt.Sprintf("  Missing %s:    %d\n", strings.ToLower(labelText), missing)
	}
	return result
}

func (r *TestReport) formatStep(useColors bool, step StepResult) string {
	dur := step.Duration.Round(time.Millisecond).String()
	if useColors {
		dur = StyleDim.Render(fmt.Sprintf("(%s)", dur))
		switch step.Status {
		case "PASS":
			return fmt.Sprintf("    %s %s %s\n", BadgeOK, step.Name, dur)
		case "FAIL":
			result := fmt.Sprintf("    %s %s %s\n", BadgeFail, step.Name, dur)
			if step.Error != "" {
				result += "           " + StyleError.Render("ERROR: "+step.Error) + "\n"
			}
			return result
		default:
			return fmt.Sprintf("    %s %s\n", BadgeSkip, step.Name)
		}
	}

	switch step.Status {
	case "PASS":
		return fmt.Sprintf("  [PASS] %s (%s)\n", step.Name, dur)
	case "FAIL":
		result := fmt.Sprintf("  [FAIL] %s (%s)\n", step.Name, dur)
		if step.Error != "" {
			result += fmt.Sprintf("         ERROR: %s\n", step.Error)
		}
		return result
	default:
		return fmt.Sprintf("  [SKIP] %s\n", step.Name)
	}
}

func (r *TestReport) formatSummary(useColors bool, passCount, failCount int) string {
	var parts []string
	if r.MatchedImages > 0 {
		parts = append(parts, fmt.Sprintf("%d platform digests", r.MatchedImages))
	}
	if r.FoundAttTags > 0 {
		parts = append(parts, fmt.Sprintf("%d attestations", r.FoundAttTags))
	}
	if r.ModulesFound > 0 {
		parts = append(parts, fmt.Sprintf("%d modules", r.ModulesFound))
	}
	if r.SecurityFound > 0 {
		parts = append(parts, fmt.Sprintf("%d security databases", r.SecurityFound))
	}

	if useColors {
		if failCount > 0 {
			resultStyle := lipgloss.NewStyle().Bold(true).Foreground(ColorRed)
			return "  " + resultStyle.Render("RESULT: FAILED") + fmt.Sprintf(" (%d passed, %d failed)\n", passCount, failCount)
		}
		resultStyle := lipgloss.NewStyle().Bold(true).Foreground(ColorGreen)
		result := "  " + resultStyle.Render("RESULT: PASSED") + "\n"
		if len(parts) > 0 {
			result += "  " + StyleSuccess.Render(strings.Join(parts, ", ")+" verified") + "\n"
		}
		return result
	}

	if failCount > 0 {
		return fmt.Sprintf("RESULT: FAILED (%d passed, %d failed)\n", passCount, failCount)
	}
	result := "RESULT: PASSED\n"
	if len(parts) > 0 {
		result += fmt.Sprintf("  %s verified\n", strings.Join(parts, ", "))
	}
	return result
}

func (r *TestReport) format(useColors bool) string {
	duration := r.EndTime.Sub(r.StartTime)
	if r.EndTime.IsZero() {
		duration = time.Since(r.StartTime)
	}

	var b strings.Builder

	if useColors {
		b.WriteString("\n")
		b.WriteString(Separator("═") + "\n")
		b.WriteString("  " + StyleTitle.Render("E2E TEST REPORT") + "\n")
		b.WriteString(Separator("═") + "\n\n")
		b.WriteString("  " + StyleLabel.Render("Duration: ") + StyleDim.Render(duration.Round(time.Second).String()) + "\n\n")
		b.WriteString("  " + StyleHeader.Render("REGISTRIES") + "\n")
		b.WriteString("    " + StyleLabel.Render("Source:  ") + StyleValue.Render(r.SourceRegistry) + "\n")
		b.WriteString("    " + StyleLabel.Render("Target:  ") + StyleValue.Render(r.TargetRegistry) + "\n\n")
		b.WriteString("  " + StyleHeader.Render("VERIFICATION") + "\n")
	} else {
		b.WriteString("================================================================================\n")
		b.WriteString(fmt.Sprintf("E2E TEST REPORT: %s\n", r.TestName))
		b.WriteString("================================================================================\n\n")
		b.WriteString("EXECUTION:\n")
		b.WriteString(fmt.Sprintf("  Started:  %s\n", r.StartTime.Format(time.RFC3339)))
		b.WriteString(fmt.Sprintf("  Finished: %s\n", r.EndTime.Format(time.RFC3339)))
		b.WriteString(fmt.Sprintf("  Duration: %s\n", duration.Round(time.Second)))
		b.WriteString(fmt.Sprintf("  Log dir:  %s\n\n", r.LogDir))
		b.WriteString("REGISTRIES:\n")
		b.WriteString(fmt.Sprintf("  Source: %s\n", r.SourceRegistry))
		b.WriteString(fmt.Sprintf("  Target: %s\n\n", r.TargetRegistry))
		b.WriteString("VERIFICATION:\n")
	}

	if section := r.formatSection(useColors, "Platform digests: ", r.MatchedImages, r.TotalImages, r.MissingImages); section != "" {
		b.WriteString(section)
	}
	if section := r.formatSection(useColors, "Attestation tags: ", r.FoundAttTags, r.ExpectedAttTags, r.MissingAttTags); section != "" {
		b.WriteString(section)
	}
	if r.ModulesExpected > 0 {
		b.WriteString(r.formatSection(useColors, "Modules verified: ", r.ModulesFound, r.ModulesExpected, r.ModulesMissing))
	}
	if r.SecurityExpected > 0 {
		b.WriteString(r.formatSection(useColors, "Security verified:", r.SecurityFound, r.SecurityExpected, r.SecurityMissing))
	}
	b.WriteString("\n")

	if useColors {
		b.WriteString("  " + StyleHeader.Render("STEPS") + "\n")
	} else {
		b.WriteString("STEPS:\n")
	}
	passCount, failCount := r.countSteps()
	for _, step := range r.Steps {
		b.WriteString(r.formatStep(useColors, step))
	}
	b.WriteString("\n")

	if useColors {
		b.WriteString(Separator("─") + "\n")
		b.WriteString(r.formatSummary(useColors, passCount, failCount))
		b.WriteString(Separator("═") + "\n")
	} else {
		b.WriteString("================================================================================\n")
		b.WriteString(r.formatSummary(useColors, passCount, failCount))
		b.WriteString("================================================================================\n")
	}

	return b.String()
}

func (r *TestReport) Print() {
	WriteRawf("%s", r.format(true))
}

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

func (r *TestReport) String() string {
	return r.format(false)
}

