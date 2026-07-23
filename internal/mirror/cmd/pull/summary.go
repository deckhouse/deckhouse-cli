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
	"sort"
	"strings"

	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/internal/mirror"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/summaryui"
)

// Framed-summary primitives live in summaryui so the pull and push summaries
// share one look. Alias them here to keep the renderer and its callers unchanged.
const (
	frameWidth = summaryui.FrameWidth
	nameWidth  = summaryui.NameWidth
	sizeWidth  = summaryui.SizeWidth
)

var (
	cFrame   = summaryui.Frame
	cLabel   = summaryui.Label
	cCount   = summaryui.Count
	cDim     = summaryui.Dim
	cGood    = summaryui.Good
	cWarn    = summaryui.Warn
	cBad     = summaryui.Bad
	cVEX     = summaryui.VEX
	cVersion = summaryui.Version
	cSize    = summaryui.Size
	cTotalSz = summaryui.TotalSz
)

var (
	bar                   = summaryui.Bar
	configureSummaryColor = summaryui.ConfigureColor
	writeTopBorder        = summaryui.WriteTopBorder
	padLabel              = summaryui.PadLabel
	formatDuration        = summaryui.FormatDuration
	humanSize             = summaryui.HumanSize
)

// renderPullSummary formats a PullSummary as a single multi-line, framed block.
// It is emitted through a single logger call so the structured-logging handler
// stamps a timestamp only once, at the top of the block.
//
// When verbose is true, the modules and packages sections list every entry with
// its resolved versions (and VEX count, when present); otherwise they print only
// the aggregate count (the category label already names what is counted).
//
// Example output (verbose pull with --modules-path-suffix mymods, colour
// stripped; the warning header and the moved Modules path are yellow):
//
//	╔══ Pull summary ═══════════════════════════════════════
//	║ Edition:    EE
//	║
//	║ Warning: modules use a non-default path (--modules-path-suffix)
//	║   Modules    registry.deckhouse.io/deckhouse/ee/mymods
//	║              default: registry.deckhouse.io/deckhouse/ee/modules
//	║
//	║ Platform:   v1.69.1 (5 channels)
//	║ Installer:  v1.69.1
//	║ Security:   4/4 databases
//	║ Modules:    2  ·  3 VEXes
//	║     console                         [v1.40.0]
//	║     csi-nfs                         (3 VEX)  [v0.6.2, v0.6.1]
//	║ Packages:   1
//	║     deckhouse                       [v1.69.1]
//	║
//	║ Bundle artifacts (3 files)
//	║   platform.tar                      2.9 GiB
//	║   modules.tar                       1.1 GiB  (2 chunks)
//	║   TOTAL                             4.0 GiB
//	║
//	║ Elapsed: 3m12s
//	╚═══════════════════════════════════════════════════════
//
// The Warning block appears only when --modules-path-suffix moved modules off
// the default and modules were actually pulled. The Elapsed line is preceded by
// a state line on a non-success outcome: "Pull failed; ...", "Pull was
// cancelled; ...", or "No images were downloaded (dry-run).".
func renderPullSummary(s *mirror.PullSummary, verbose bool) string {
	var b strings.Builder

	b.WriteByte('\n')
	writeTopBorder(&b, summaryTitle(s))

	// Edition (e.g. CE/EE), shown above the components. Omitted for a custom
	// registry where no edition could be parsed from the source path.
	if s.Edition != "" {
		fmt.Fprintf(&b, "%s %s %s\n", bar(), cLabel(padLabel("Edition")), cCount(strings.ToUpper(s.Edition)))
	}

	// Warn only when the modules path was moved and modules were actually pulled.
	summaryui.WriteModulesPathWarning(&b, s.ModulesPath, modulesPulled(s.Modules))

	writeComponent(&b, "Platform", s.Platform)
	writeComponent(&b, "Installer", s.Installer)
	writeSecurity(&b, s.Security)
	writeModules(&b, s.Modules, verbose)
	writePackages(&b, s.Packages, verbose)

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

// writeComponent renders one platform-like category (Platform, Installer).
// e.g. `║ Platform:   v1.69.1 (5 channels)`; "included" when no version is
// known; "skipped" / "not pulled" when the phase did not run.
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

// writeSecurity renders the trivy databases line.
// e.g. `║ Security:   4/4 databases` (green; yellow on a partial "3/4
// databases"); also "not available in this edition" / "skipped" / "not pulled".
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

// modulesPulled reports whether at least one module was pulled (or planned, in
// dry-run). A moved modules path is only worth warning about when modules
// actually went through it.
func modulesPulled(m mirror.ModulesStats) bool {
	return len(m.Modules) > 0
}

// writeModules renders the modules line, and per-module detail when verbose.
// e.g. `║ Modules:    2  ·  3 VEXes`, then per module (verbose)
// `║     csi-nfs                         (3 VEX)  [v0.6.2, v0.6.1]`.
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

	// Aggregate: bare count (the "Modules:" label already names the category),
	// "extra images only" when applicable, and total VEX. No image count by design.
	parts := []string{cCount(fmt.Sprint(len(m.Modules)))}
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

// writePackages mirrors writeModules for packages.
// e.g. `║ Packages:   1`, then per package (verbose)
// `║     deckhouse                       [v1.69.1]`.
func writePackages(b *strings.Builder, p mirror.PackagesStats, verbose bool) {
	label := cLabel(padLabel("Packages"))

	if p.Skipped {
		fmt.Fprintf(b, "%s %s %s\n", bar(), label, cDim("skipped"))
		return
	}

	if !p.Attempted {
		fmt.Fprintf(b, "%s %s %s\n", bar(), label, cWarn("not pulled"))
		return
	}

	// Aggregate: bare count (the "Packages:" label already names the category),
	// "extra images only" when applicable, and total VEX. No image count by design.
	parts := []string{cCount(fmt.Sprint(len(p.Packages)))}
	if p.OnlyExtraImages {
		parts = append(parts, cDim("extra images only"))
	}

	if p.TotalVEX > 0 {
		parts = append(parts, cVEX(fmt.Sprintf("%d VEXes", p.TotalVEX)))
	}

	fmt.Fprintf(b, "%s %s %s\n", bar(), label, strings.Join(parts, "  "+cDim("·")+"  "))

	// Per-package breakdown only in verbose mode (--verbose-summary).
	if !verbose {
		return
	}

	for _, ps := range p.Packages {
		versions := ""
		if len(ps.Versions) > 0 {
			versions = "  " + cVersion("["+strings.Join(sortVersions(ps.Versions), ", ")+"]")
		}

		packageVex := ""
		if ps.VEX > 0 {
			packageVex = "  " + cVEX(fmt.Sprintf("(%d VEX)", ps.VEX))
		}

		// Name, VEX subset when present, then versions in []. Pad the name only
		// when something follows, to avoid trailing whitespace.
		line := ps.Name
		if packageVex != "" || versions != "" {
			line = fmt.Sprintf("%-*s%s%s", nameWidth, ps.Name, packageVex, versions)
		}

		fmt.Fprintf(b, "%s     %s\n", bar(), line)
	}
}

// writeBundle renders the on-disk bundle artifact block (real pull only).
// e.g.:
//
//	║ Bundle artifacts (3 files)
//	║   platform.tar                      2.9 GiB
//	║   modules.tar                       1.1 GiB  (2 chunks)
//	║   TOTAL                             4.0 GiB
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
