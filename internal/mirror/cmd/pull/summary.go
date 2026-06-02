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

package pull

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Masterminds/semver/v3"
	"github.com/fatih/color"

	"github.com/deckhouse/deckhouse-cli/internal/mirror"
)

const (
	// frameWidth is the inner width of the summary box, in runes.
	frameWidth = 56
	// labelWidth aligns the category labels in the summary body.
	labelWidth = 11
	// nameWidth left-aligns module names and bundle artifact names.
	nameWidth = 30
	// sizeWidth right-aligns bundle artifact sizes.
	sizeWidth = 10
)

// Semantic accent colours for the summary. fatih/color disables them when
// stdout is not a TTY or NO_COLOR is set (the summary is logged to stdout), so
// escape codes never reach pipes or files.
//
// Apply every colour AFTER width padding (padLabel, %-30s, %10s): the codes are
// zero-width on screen but count toward fmt's field widths and break columns.
var (
	cFrame   = color.New(color.FgHiBlack).SprintFunc()            // box borders - recede
	cTitle   = color.New(color.FgCyan, color.Bold).SprintFunc()   // block title
	cLabel   = color.New(color.FgCyan).SprintFunc()               // category labels (scan anchors)
	cCount   = color.New(color.Bold).SprintFunc()                 // primary numbers
	cDim     = color.New(color.FgHiBlack).SprintFunc()            // units and secondary text
	cGood    = color.New(color.FgGreen).SprintFunc()              // complete (e.g. 4/4 databases)
	cWarn    = color.New(color.FgYellow).SprintFunc()             // attention (partial, not-available, dry-run)
	cBad     = color.New(color.FgRed).SprintFunc()                // failure (cancelled)
	cVEX     = color.New(color.FgMagenta).SprintFunc()            // VEX attestations (a distinct class)
	cVersion = color.New(color.FgGreen).SprintFunc()              // resolved versions (the headline)
	cSize    = color.New(color.FgCyan).SprintFunc()               // bundle artifact sizes
	cTotalSz = color.New(color.FgYellow, color.Bold).SprintFunc() // the bundle TOTAL - the action number
)

// bar returns the coloured left border of a body line.
func bar() string { return cFrame("║") }

// configureSummaryColor re-enables colour when FORCE_COLOR / CLICOLOR_FORCE is
// set (e.g. piping to `less -R` or capturing a coloured log). NO_COLOR wins;
// otherwise fatih/color's stdout-TTY check decides.
func configureSummaryColor() {
	if os.Getenv("NO_COLOR") == "" &&
		(os.Getenv("FORCE_COLOR") != "" || os.Getenv("CLICOLOR_FORCE") != "") {
		color.NoColor = false
	}
}

// renderPullSummary formats a PullSummary as a single multi-line, framed block.
// It is emitted through a single logger call so the structured-logging handler
// stamps a timestamp only once, at the top of the block.
//
// When verbose is true, the modules section lists every module with its
// resolved versions (and VEX count, when present); otherwise it prints only the
// aggregate "N modules" line.
func renderPullSummary(s *mirror.PullSummary, verbose bool) string {
	var b strings.Builder

	b.WriteByte('\n')
	writeTopBorder(&b, summaryTitle(s))

	// Edition (e.g. CE/EE), shown above the components. Omitted for a custom
	// registry where no edition could be parsed from the source path.
	if s.Edition != "" {
		fmt.Fprintf(&b, "%s %s %s\n", bar(), cLabel(padLabel("Edition")), cCount(strings.ToUpper(s.Edition)))
	}

	writeComponent(&b, "Platform", s.Platform)
	writeComponent(&b, "Installer", s.Installer)
	writeSecurity(&b, s.Security)
	writeModules(&b, s.Modules, verbose)

	if !s.DryRun && len(s.Bundle.Files) > 0 {
		b.WriteString(bar() + "\n")
		writeBundle(&b, s.Bundle)
	}

	b.WriteString(bar() + "\n")

	// Cancelled is checked before DryRun: a Ctrl+C during a dry-run sets both, and
	// the cancellation is the more important fact to surface.
	switch {
	case s.Failed:
		fmt.Fprintf(&b, "%s %s\n", bar(), cBad("Pull failed; the above reflects what completed before the error."))
	case s.Cancelled:
		fmt.Fprintf(&b, "%s %s\n", bar(), cBad("Pull was cancelled; the above reflects what completed."))
	case s.DryRun:
		fmt.Fprintf(&b, "%s %s\n", bar(), cWarn("No images were downloaded (dry-run)."))
	}

	fmt.Fprintf(&b, "%s %s\n", bar(), cDim("Elapsed: "+formatDuration(s.Elapsed)))
	b.WriteString(cFrame("╚" + strings.Repeat("═", frameWidth-1)))

	return b.String()
}

// summaryTitle is the framed-block title for the pull's terminal state.
func summaryTitle(s *mirror.PullSummary) string {
	switch {
	case s.Failed:
		return "Pull failed"
	case s.DryRun:
		return "Pull plan (dry-run)"
	default:
		return "Pull summary"
	}
}

func writeTopBorder(b *strings.Builder, title string) {
	prefix := "╔══ "
	suffix := " "
	used := utf8.RuneCountInString(prefix) + utf8.RuneCountInString(title) + utf8.RuneCountInString(suffix)

	pad := max(0, frameWidth-used)

	b.WriteString(cFrame(prefix) + cTitle(title) + suffix + cFrame(strings.Repeat("═", pad)) + "\n")
}

func writeComponent(b *strings.Builder, name string, c mirror.ComponentStats) {
	label := cLabel(padLabel(name))

	switch {
	case c.Skipped:
		fmt.Fprintf(b, "%s %s %s\n", bar(), label, cDim("skipped"))
	case !c.Attempted:
		// Phase never ran (cancelled/failed pull).
		fmt.Fprintf(b, "%s %s %s\n", bar(), label, cWarn("not pulled"))
	default:
		fmt.Fprintf(b, "%s %s %s\n", bar(), label, componentContent(c))
	}
}

// componentContent renders a component's resolved versions, plus the channel
// count for the platform (e.g. "v1.69.0 (5 channels)", or "latest" for the
// installer). Falls back to "included" when no version/ref is known. No image
// count by design.
func componentContent(c mirror.ComponentStats) string {
	if len(c.Versions) == 0 {
		return cDim("included")
	}

	content := cVersion(strings.Join(sortVersions(c.Versions), ", "))
	if len(c.Channels) > 0 {
		content += " " + cDim(fmt.Sprintf("(%d channels)", len(c.Channels)))
	}

	return content
}

// sortVersions returns a copy of versions ordered newest-first by semver (see
// compareSemverDesc); non-semver tags sort lexically after the semver ones.
func sortVersions(versions []string) []string {
	sorted := append([]string(nil), versions...)

	sort.SliceStable(sorted, func(i, j int) bool {
		return compareSemverDesc(sorted[i], sorted[j])
	})

	return sorted
}

// compareSemverDesc reports whether tag a should sort before tag b in the
// newest-first ordering: valid semver compares by version descending; a semver
// tag precedes a non-semver one; two non-semver tags compare lexically.
func compareSemverDesc(a, b string) bool {
	va, ea := semver.NewVersion(a)
	vb, eb := semver.NewVersion(b)

	switch {
	case ea == nil && eb == nil:
		return va.GreaterThan(vb)
	case ea == nil:
		return true
	case eb == nil:
		return false
	default:
		return a < b
	}
}

func writeSecurity(b *strings.Builder, s mirror.SecurityStats) {
	label := cLabel(padLabel("Security"))

	switch {
	case s.Skipped:
		fmt.Fprintf(b, "%s %s %s\n", bar(), label, cDim("skipped"))
	case !s.Attempted:
		fmt.Fprintf(b, "%s %s %s\n", bar(), label, cWarn("not pulled"))
	case !s.Available:
		fmt.Fprintf(b, "%s %s %s\n", bar(), label, cWarn("not available in this edition"))
	default:
		// Green when complete, yellow when partial (an incomplete vulnerability set).
		databases := fmt.Sprintf("%d/%d databases", s.Databases, s.AvailableDatabases)

		if s.Databases < s.AvailableDatabases {
			fmt.Fprintf(b, "%s %s %s\n", bar(), label, cWarn(databases))
			return
		}

		fmt.Fprintf(b, "%s %s %s\n", bar(), label, cGood(databases))
	}
}

func writeModules(b *strings.Builder, m mirror.ModulesStats, verbose bool) {
	label := cLabel(padLabel("Modules"))

	if m.Skipped {
		fmt.Fprintf(b, "%s %s %s\n", bar(), label, cDim("skipped"))
		return
	}

	if !m.Attempted {
		fmt.Fprintf(b, "%s %s %s\n", bar(), label, cWarn("not pulled"))
		return
	}

	// Aggregate: module count, "extra images only" when applicable, and total VEX.
	// No image count by design.
	parts := []string{cCount(fmt.Sprint(len(m.Modules))) + " " + cDim("modules")}
	if m.OnlyExtraImages {
		parts = append(parts, cDim("extra images only"))
	}

	if m.TotalVEX > 0 {
		parts = append(parts, cVEX(fmt.Sprintf("%d VEXes", m.TotalVEX)))
	}

	fmt.Fprintf(b, "%s %s %s\n", bar(), label, strings.Join(parts, "  "+cDim("·")+"  "))

	// Per-module breakdown only in verbose mode (--verbose-summary).
	if !verbose {
		return
	}

	for _, ms := range m.Modules {
		versions := ""
		if len(ms.Versions) > 0 {
			versions = "  " + cVersion("["+strings.Join(sortVersions(ms.Versions), ", ")+"]")
		}

		moduleVex := ""
		if ms.VEX > 0 {
			moduleVex = "  " + cVEX(fmt.Sprintf("(%d VEX)", ms.VEX))
		}

		// Name, VEX subset when present, then versions in []. Pad the name only
		// when something follows, to avoid trailing whitespace.
		line := ms.Name
		if moduleVex != "" || versions != "" {
			line = fmt.Sprintf("%-*s%s%s", nameWidth, ms.Name, moduleVex, versions)
		}

		fmt.Fprintf(b, "%s     %s\n", bar(), line)
	}
}

func writeBundle(b *strings.Builder, bundle mirror.BundleStats) {
	fmt.Fprintf(b, "%s %s\n", bar(), cLabel(fmt.Sprintf("Bundle artifacts (%d files)", physicalFileCount(bundle))))

	for _, f := range bundle.Files {
		chunks := ""
		if f.Chunks > 0 {
			chunks = "  " + cDim(fmt.Sprintf("(%d chunks)", f.Chunks))
		}

		fmt.Fprintf(b, "%s   %-*s %s%s\n", bar(), nameWidth, f.Name, cSize(fmt.Sprintf("%*s", sizeWidth, humanSize(f.Bytes))), chunks)
	}

	fmt.Fprintf(b, "%s   %s %s\n", bar(), cCount(fmt.Sprintf("%-*s", nameWidth, "TOTAL")), cTotalSz(fmt.Sprintf("%*s", sizeWidth, humanSize(bundle.TotalBytes))))
}

// physicalFileCount returns the number of files on disk: each chunked artifact
// contributes its chunk count, each single-file artifact contributes one.
func physicalFileCount(bundle mirror.BundleStats) int {
	count := 0

	for _, f := range bundle.Files {
		if f.Chunks > 0 {
			count += f.Chunks
			continue
		}

		count++
	}

	return count
}

func padLabel(name string) string {
	return fmt.Sprintf("%-*s", labelWidth, name+":")
}

// formatDuration renders an elapsed duration compactly, keeping millisecond
// precision for sub-second runs (so a fast dry-run does not report "0s").
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return d.Round(time.Millisecond).String()
	}

	return d.Round(time.Second).String()
}

// humanSize returns a compact human-readable size using binary (IEC) units
// (e.g. "789 B", "12.3 KiB", "2.9 GiB"). The divisor is 1024, so the labels are
// KiB/MiB/GiB rather than the decimal KB/MB/GB - this keeps the printed unit
// honest with the math, which matters when an operator sizes transfer media off
// this number. Adapted from internal/cr/internal/output.HumanSize, which is not
// importable here because it lives behind the internal/cr/internal/ barrier.
func humanSize(n int64) string {
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
