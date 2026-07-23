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

package push

import (
	"fmt"
	"strings"

	"github.com/deckhouse/deckhouse-cli/internal/mirror"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/summaryui"
)

// renderPushSummary formats a PushSummary as a single multi-line, framed block,
// matching the pull summary's look. The registry layout section is always shown:
// telling the operator where each component landed is the point of the push
// summary, and it highlights a moved modules path (--modules-path-suffix).
//
// Example output for --modules-path-suffix / (colour stripped; the warning
// header and the moved Modules path are yellow):
//
//	╔══ Push summary ═══════════════════════════════════════
//	║
//	║ Warning: modules use a non-default path (--modules-path-suffix)
//	║ Registry:   registry.example.com/deckhouse/ee
//	║   Platform   registry.example.com/deckhouse/ee
//	║   Modules    registry.example.com/deckhouse/ee
//	║              default: registry.example.com/deckhouse/ee/modules
//	║   Security   registry.example.com/deckhouse/ee/security
//	║   Packages   registry.example.com/deckhouse/ee/packages
//	║   Installer  registry.example.com/deckhouse/ee/installer
//	║
//	║ Platform:   pushed
//	║ Installer:  not present
//	║ Security:   4 databases
//	║ Modules:    12
//	║ Packages:   3
//	║
//	║ Elapsed: 2m4s
//	╚═══════════════════════════════════════════════════════
//
// The Elapsed line is preceded by a state line when the push did not succeed:
// "Push failed; ..." or "Push was cancelled; ...".
func renderPushSummary(s *mirror.PushSummary) string {
	var b strings.Builder

	b.WriteByte('\n')
	summaryui.WriteTopBorder(&b, pushSummaryTitle(s))

	summaryui.WriteRegistryLayout(&b, s.Registry, true)

	writePushPresence(&b, "Platform", s.PlatformPushed)
	writePushPresence(&b, "Installer", s.InstallerPushed)
	writePushSecurity(&b, s.SecurityDatabases)
	writePushCount(&b, "Modules", s.Modules)
	writePushCount(&b, "Packages", s.Packages)

	b.WriteString(summaryui.Bar() + "\n")

	switch {
	case s.Failed:
		fmt.Fprintf(&b, "%s %s\n", summaryui.Bar(), summaryui.Bad("Push failed; the above reflects what completed before the error."))
	case s.Cancelled:
		fmt.Fprintf(&b, "%s %s\n", summaryui.Bar(), summaryui.Bad("Push was cancelled; the above reflects what completed."))
	}

	fmt.Fprintf(&b, "%s %s\n", summaryui.Bar(), summaryui.Dim("Elapsed: "+summaryui.FormatDuration(s.Elapsed)))
	b.WriteString(summaryui.Frame("╚" + strings.Repeat("═", summaryui.FrameWidth-1)))

	return b.String()
}

// pushSummaryTitle is the framed-block title for the push's terminal state.
func pushSummaryTitle(s *mirror.PushSummary) string {
	if s.Failed {
		return "Push failed"
	}

	return "Push summary"
}

// writePushPresence renders a component that is either pushed or absent.
// e.g. `║ Platform:   pushed` / `║ Installer:  not present`.
func writePushPresence(b *strings.Builder, name string, pushed bool) {
	label := summaryui.Label(summaryui.PadLabel(name))

	if pushed {
		fmt.Fprintf(b, "%s %s %s\n", summaryui.Bar(), label, summaryui.Good("pushed"))
		return
	}

	fmt.Fprintf(b, "%s %s %s\n", summaryui.Bar(), label, summaryui.Dim("not present"))
}

// writePushSecurity renders the security databases count.
// e.g. `║ Security:   4 databases` / `║ Security:   not present`.
func writePushSecurity(b *strings.Builder, count int) {
	label := summaryui.Label(summaryui.PadLabel("Security"))

	if count == 0 {
		fmt.Fprintf(b, "%s %s %s\n", summaryui.Bar(), label, summaryui.Dim("not present"))
		return
	}

	fmt.Fprintf(b, "%s %s %s\n", summaryui.Bar(), label, summaryui.Good(fmt.Sprintf("%d databases", count)))
}

// writePushCount renders a repository count (modules, packages).
// e.g. `║ Modules:    12` / `║ Modules:    not present`.
func writePushCount(b *strings.Builder, name string, count int) {
	label := summaryui.Label(summaryui.PadLabel(name))

	if count == 0 {
		fmt.Fprintf(b, "%s %s %s\n", summaryui.Bar(), label, summaryui.Dim("not present"))
		return
	}

	fmt.Fprintf(b, "%s %s %s\n", summaryui.Bar(), label, summaryui.Count(fmt.Sprint(count)))
}
