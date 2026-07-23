/*
Copyright 2026 Flant JSC

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

// Package summaryui holds the shared presentation primitives for the mirror
// pull and push summaries: the framed box, the accent colours, and the registry
// layout section. Keeping them here lets both summaries look identical.
package summaryui

import (
	"fmt"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/fatih/color"

	"github.com/deckhouse/deckhouse-cli/internal/mirror"
)

const (
	// FrameWidth is the inner width of the summary box, in runes.
	FrameWidth = 56
	// LabelWidth aligns the category labels in the summary body.
	LabelWidth = 11
	// NameWidth left-aligns module names and bundle artifact names.
	NameWidth = 30
	// SizeWidth right-aligns bundle artifact sizes.
	SizeWidth = 10
	// layoutNameWidth left-aligns component names in the registry layout section.
	layoutNameWidth = 10
)

// Semantic accent colours for the summary. fatih/color disables them when
// stdout is not a TTY or NO_COLOR is set (the summary is logged to stdout), so
// escape codes never reach pipes or files.
//
// Apply every colour AFTER width padding (PadLabel, %-30s, %10s): the codes are
// zero-width on screen but count toward fmt's field widths and break columns.
var (
	Frame   = color.New(color.FgHiBlack).SprintFunc()            // box borders - recede
	Title   = color.New(color.FgCyan, color.Bold).SprintFunc()   // block title
	Label   = color.New(color.FgCyan).SprintFunc()               // category labels (scan anchors)
	Count   = color.New(color.Bold).SprintFunc()                 // primary numbers
	Dim     = color.New(color.FgHiBlack).SprintFunc()            // units and secondary text
	Good    = color.New(color.FgGreen).SprintFunc()              // complete (e.g. 4/4 databases)
	Warn    = color.New(color.FgYellow).SprintFunc()             // attention (partial, not-available, non-default)
	Bad     = color.New(color.FgRed).SprintFunc()                // failure (cancelled)
	VEX     = color.New(color.FgMagenta).SprintFunc()            // VEX attestations (a distinct class)
	Version = color.New(color.FgGreen).SprintFunc()              // resolved versions (the headline)
	Size    = color.New(color.FgCyan).SprintFunc()               // bundle artifact sizes
	TotalSz = color.New(color.FgYellow, color.Bold).SprintFunc() // the bundle TOTAL - the action number
)

// Bar returns the coloured left border of a body line.
func Bar() string { return Frame("║") }

// ConfigureColor re-enables colour when FORCE_COLOR / CLICOLOR_FORCE is set
// (e.g. piping to `less -R` or capturing a coloured log). NO_COLOR wins;
// otherwise fatih/color's stdout-TTY check decides.
func ConfigureColor() {
	if os.Getenv("NO_COLOR") == "" &&
		(os.Getenv("FORCE_COLOR") != "" || os.Getenv("CLICOLOR_FORCE") != "") {
		color.NoColor = false
	}
}

// WriteTopBorder writes the framed-block top border with the given title.
func WriteTopBorder(b *strings.Builder, title string) {
	prefix := "╔══ "
	suffix := " "
	used := utf8.RuneCountInString(prefix) + utf8.RuneCountInString(title) + utf8.RuneCountInString(suffix)

	pad := max(0, FrameWidth-used)

	b.WriteString(Frame(prefix) + Title(title) + suffix + Frame(strings.Repeat("═", pad)) + "\n")
}

// PadLabel formats a category label as a fixed-width "Name:" column.
func PadLabel(name string) string {
	return fmt.Sprintf("%-*s", LabelWidth, name+":")
}

// FormatDuration renders an elapsed duration compactly, keeping millisecond
// precision for sub-second runs (so a fast dry-run does not report "0s").
func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}

	return d.Round(time.Second).String()
}

// HumanSize returns a compact human-readable size using binary (IEC) units
// (e.g. "789 B", "12.3 KiB", "2.9 GiB"). The divisor is 1024, so the labels are
// KiB/MiB/GiB rather than the decimal KB/MB/GB - this keeps the printed unit
// honest with the math, which matters when an operator sizes transfer media off
// this number. Adapted from internal/cr/internal/output.HumanSize, which is not
// importable here because it lives behind the internal/cr/internal/ barrier.
func HumanSize(n int64) string {
	const unit = int64(1024)
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}

	div, exp := unit, 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}

	units := "KMGTPE"

	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), units[exp])
}

// WriteRegistryLayout writes the registry layout section: the root registry and
// where each component lives, with any non-default path (a moved modules path)
// in yellow plus a dimmed hint of the standard path. Writes nothing when show is
// false.
//
// Example output for --modules-path-suffix mymods (colour stripped; the Modules
// path is yellow):
//
//	║ Registry:   registry.deckhouse.io/deckhouse/ee
//	║   Platform   registry.deckhouse.io/deckhouse/ee
//	║   Modules    registry.deckhouse.io/deckhouse/ee/mymods
//	║              default: registry.deckhouse.io/deckhouse/ee/modules
//	║   Security   registry.deckhouse.io/deckhouse/ee/security
//	║   Packages   registry.deckhouse.io/deckhouse/ee/packages
//	║   Installer  registry.deckhouse.io/deckhouse/installer
//
// With a default modules path the Modules line is plain and the "default:" hint
// is omitted.
func WriteRegistryLayout(b *strings.Builder, layout mirror.RegistryLayout, show bool) {
	if !show {
		return
	}

	fmt.Fprintf(b, "%s %s %s\n", Bar(), Label(PadLabel("Registry")), layout.Root)

	for _, row := range layout.Rows {
		name := fmt.Sprintf("%-*s", layoutNameWidth, row.Label)

		if row.NonDefault {
			fmt.Fprintf(b, "%s   %s %s\n", Bar(), Label(name), Warn(row.Path))
			fmt.Fprintf(b, "%s   %s %s\n", Bar(), strings.Repeat(" ", layoutNameWidth), Dim("default: "+row.DefaultPath))
			continue
		}

		fmt.Fprintf(b, "%s   %s %s\n", Bar(), Label(name), row.Path)
	}
}
