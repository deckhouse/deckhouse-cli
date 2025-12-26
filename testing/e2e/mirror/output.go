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
	"sync"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"golang.org/x/term"
)

// =============================================================================
// Colors
// =============================================================================

var (
	colorCyan   = lipgloss.Color("6")
	colorGreen  = lipgloss.Color("2")
	colorRed    = lipgloss.Color("1")
	colorYellow = lipgloss.Color("3")
	colorBlue   = lipgloss.Color("4")
	colorWhite  = lipgloss.Color("15")
	colorGray   = lipgloss.Color("8")
)

// =============================================================================
// Text Styles
// =============================================================================

var (
	styleTitle   = lipgloss.NewStyle().Bold(true).Foreground(colorWhite)
	styleHeader  = lipgloss.NewStyle().Bold(true).Foreground(colorCyan)
	styleLabel   = lipgloss.NewStyle().Foreground(colorGray)
	styleValue   = lipgloss.NewStyle().Foreground(colorWhite)
	styleDim     = lipgloss.NewStyle().Foreground(colorGray)
	styleSuccess = lipgloss.NewStyle().Foreground(colorGreen)
	styleError   = lipgloss.NewStyle().Foreground(colorRed)
)

// =============================================================================
// Badges
// =============================================================================

var (
	badgeOK   = lipgloss.NewStyle().Bold(true).Foreground(colorGreen).Render("[OK]")
	badgeFail = lipgloss.NewStyle().Bold(true).Foreground(colorRed).Render("[FAIL]")
	badgeSkip = lipgloss.NewStyle().Foreground(colorYellow).Render("[SKIP]")
)

// =============================================================================
// Step Styles
// =============================================================================

var (
	styleStepNum  = lipgloss.NewStyle().Bold(true).Foreground(colorBlue)
	styleStepText = lipgloss.NewStyle().Bold(true).Foreground(colorWhite)
)

// =============================================================================
// Output Functions
// =============================================================================

// output is the destination for styled output (stderr preserves colors in go test)
var output = os.Stderr

var colorInitOnce sync.Once

// ensureColorInit initializes color profile for lipgloss (replaces init())
func ensureColorInit() {
	colorInitOnce.Do(func() {
		// Force color output - go test buffers stdout which disables color detection
		// Check stderr instead (usually unbuffered) or honor FORCE_COLOR env
		if term.IsTerminal(int(os.Stderr.Fd())) || os.Getenv("FORCE_COLOR") != "" || os.Getenv("TERM") != "" {
			lipgloss.DefaultRenderer().SetColorProfile(termenv.TrueColor)
		}
	})
}

// writeLinef writes a formatted line to output
func writeLinef(format string, args ...interface{}) {
	ensureColorInit()
	fmt.Fprintf(output, format+"\n", args...)
}

// writeRawf writes formatted text to output without newline
func writeRawf(format string, args ...interface{}) {
	ensureColorInit()
	fmt.Fprintf(output, format, args...)
}

// printStep prints a formatted step header
func printStep(num int, description string) {
	badge := styleStepNum.Render(fmt.Sprintf("[STEP %d]", num))
	text := styleStepText.Render(description)
	writeLinef("\n%s %s", badge, text)
}

// =============================================================================
// Separators
// =============================================================================

var styleSeparator = lipgloss.NewStyle().Foreground(colorCyan)

const separatorWidth = 80

// separator creates a separator line
func separator(char string) string {
	return styleSeparator.Render(strings.Repeat(char, separatorWidth))
}

// =============================================================================
// Test Header/Footer
// =============================================================================

// printTestHeader prints the test header with configuration info
func printTestHeader(testName, sourceRegistry, logDir string) {
	writeLinef("")
	writeLinef(separator("═"))
	writeLinef("  %s", styleTitle.Render("E2E TEST: "+testName))
	writeLinef(separator("═"))
	writeLinef("  %s %s", styleLabel.Render("Source:"), styleValue.Render(sourceRegistry))
	writeLinef("  %s %s", styleLabel.Render("Logs:  "), styleDim.Render(logDir))
	writeLinef(separator("═"))
	writeLinef("")
}

// printSuccessBox prints a success message in a styled box
func printSuccessBox(matchedImages, matchedLayers int) {
	box := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		BorderForeground(colorGreen).
		Padding(0, 2).
		Foreground(colorGreen)

	writeLinef("")
	writeLinef(box.Render(fmt.Sprintf(
		"SUCCESS: REGISTRIES ARE IDENTICAL\n\nVerified: %d images, %d layers\nAll manifest, config, and layer digests match!",
		matchedImages,
		matchedLayers,
	)))
}
