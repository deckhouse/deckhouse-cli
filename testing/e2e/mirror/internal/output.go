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
	"sync"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"golang.org/x/term"
)

var (
	ColorCyan   = lipgloss.Color("6")
	ColorGreen  = lipgloss.Color("2")
	ColorRed    = lipgloss.Color("1")
	ColorYellow = lipgloss.Color("3")
	ColorBlue   = lipgloss.Color("4")
	ColorWhite  = lipgloss.Color("15")
	ColorGray   = lipgloss.Color("8")
)

type styles struct {
	Title   lipgloss.Style
	Header  lipgloss.Style
	Label   lipgloss.Style
	Value   lipgloss.Style
	Dim     lipgloss.Style
	Success lipgloss.Style
	Error   lipgloss.Style
	StepNum lipgloss.Style
	StepText lipgloss.Style
	Separator lipgloss.Style
}

func initStyles() styles {
	return styles{
		Title:     lipgloss.NewStyle().Bold(true).Foreground(ColorWhite),
		Header:    lipgloss.NewStyle().Bold(true).Foreground(ColorCyan),
		Label:     lipgloss.NewStyle().Foreground(ColorGray),
		Value:     lipgloss.NewStyle().Foreground(ColorWhite),
		Dim:       lipgloss.NewStyle().Foreground(ColorGray),
		Success:   lipgloss.NewStyle().Foreground(ColorGreen),
		Error:     lipgloss.NewStyle().Foreground(ColorRed),
		StepNum:   lipgloss.NewStyle().Bold(true).Foreground(ColorBlue),
		StepText:  lipgloss.NewStyle().Bold(true).Foreground(ColorWhite),
		Separator: lipgloss.NewStyle().Foreground(ColorCyan),
	}
}

var defaultStyles = initStyles()

var (
	StyleTitle   = defaultStyles.Title
	StyleHeader  = defaultStyles.Header
	StyleLabel   = defaultStyles.Label
	StyleValue   = defaultStyles.Value
	StyleDim     = defaultStyles.Dim
	StyleSuccess = defaultStyles.Success
	StyleError   = defaultStyles.Error
)

var (
	BadgeOK   = defaultStyles.Success.Copy().Bold(true).Render("[OK]")
	BadgeFail = defaultStyles.Error.Copy().Bold(true).Render("[FAIL]")
	BadgeSkip = lipgloss.NewStyle().Foreground(ColorYellow).Render("[SKIP]")
)

var output = os.Stderr
var colorInitOnce sync.Once

func EnsureColorInit() {
	colorInitOnce.Do(func() {
		if term.IsTerminal(int(os.Stderr.Fd())) || os.Getenv("FORCE_COLOR") != "" || os.Getenv("TERM") != "" {
			lipgloss.DefaultRenderer().SetColorProfile(termenv.TrueColor)
		}
	})
}

func WriteLinef(format string, args ...interface{}) {
	EnsureColorInit()
	fmt.Fprintf(output, format+"\n", args...)
}

func WriteRawf(format string, args ...interface{}) {
	EnsureColorInit()
	fmt.Fprintf(output, format, args...)
}

func PrintStep(num int, description string) {
	badge := defaultStyles.StepNum.Render(fmt.Sprintf("[STEP %d]", num))
	text := defaultStyles.StepText.Render(description)
	WriteLinef("\n%s %s", badge, text)
}

const separatorWidth = 80

func Separator(char string) string {
	return defaultStyles.Separator.Render(strings.Repeat(char, separatorWidth))
}

func PrintHeader(title string) {
	WriteLinef("")
	WriteLinef(Separator("═"))
	WriteLinef("  %s", StyleTitle.Render(title))
	WriteLinef(Separator("═"))
}

func PrintSuccessBox(matchedDigests, matchedAttTags int) {
	box := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		BorderForeground(ColorGreen).
		Padding(0, 2).
		Foreground(ColorGreen)

	WriteLinef("")
	WriteLinef(box.Render(fmt.Sprintf(
		"SUCCESS: ALL EXPECTED IMAGES VERIFIED\n\nVerified: %d digests, %d .att tags\nAll expected images are present in target registry!",
		matchedDigests,
		matchedAttTags,
	)))
}

func WriteFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0644)
}

