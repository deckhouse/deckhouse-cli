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

package summaryui

import (
	"strings"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/mirror"
)

const layoutRoot = "registry.example.com/deckhouse/ee"

func renderWarning(t *testing.T, m mirror.ModulesPathReport, transferred bool) string {
	t.Helper()

	var b strings.Builder
	WriteModulesPathWarning(&b, m, transferred)

	return b.String()
}

func TestWriteModulesPathWarning_DefaultPath(t *testing.T) {
	orig := color.NoColor
	defer func() { color.NoColor = orig }()
	color.NoColor = true

	// A default modules path is never worth a warning, even when modules moved.
	out := renderWarning(t, mirror.BuildModulesPathReport(layoutRoot, "/modules"), true)
	require.Empty(t, out, "default path writes nothing")
}

func TestWriteModulesPathWarning_MovedButNoModules(t *testing.T) {
	orig := color.NoColor
	defer func() { color.NoColor = orig }()
	color.NoColor = true

	// Path moved, but nothing went through it: no warning.
	out := renderWarning(t, mirror.BuildModulesPathReport(layoutRoot, "/"), false)
	require.Empty(t, out, "a moved path with no transferred modules writes nothing")
}

func TestWriteModulesPathWarning_MovedWithModules(t *testing.T) {
	orig := color.NoColor
	defer func() { color.NoColor = orig }()
	color.NoColor = true

	// "/" places modules at the repo root; the hint points at the standard path.
	out := renderWarning(t, mirror.BuildModulesPathReport(layoutRoot, "/"), true)

	require.Contains(t, out, "Warning: modules use a non-default path (--modules-path-suffix)")
	require.Contains(t, out, "Modules")
	require.Contains(t, out, "default: "+layoutRoot+"/modules")

	// Only the moved modules path is reported: no full layout, no other rows.
	require.NotContains(t, out, "Registry:")
	for _, label := range []string{"Platform", "Security", "Packages", "Installer"} {
		require.NotContains(t, out, label)
	}
}

func TestWriteModulesPathWarning_Color(t *testing.T) {
	orig := color.NoColor
	defer func() { color.NoColor = orig }()

	m := mirror.BuildModulesPathReport(layoutRoot, "mymods")

	color.NoColor = false
	require.Contains(t, renderWarning(t, m, true), "\x1b[",
		"non-default modules path must be highlighted when colour is enabled")

	color.NoColor = true
	require.NotContains(t, renderWarning(t, m, true), "\x1b[",
		"no ANSI escape codes when colour is disabled")
}
