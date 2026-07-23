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

func renderLayout(t *testing.T, l mirror.RegistryLayout, show bool) string {
	t.Helper()

	var b strings.Builder
	WriteRegistryLayout(&b, l, show)

	return b.String()
}

func TestWriteRegistryLayout_Hidden(t *testing.T) {
	out := renderLayout(t, mirror.BuildRegistryLayout(layoutRoot, "/", ""), false)
	require.Empty(t, out, "show=false writes nothing")
}

func TestWriteRegistryLayout_Default(t *testing.T) {
	orig := color.NoColor
	defer func() { color.NoColor = orig }()
	color.NoColor = true

	out := renderLayout(t, mirror.BuildRegistryLayout(layoutRoot, "/modules", ""), true)

	require.Contains(t, out, "Registry:")
	require.Contains(t, out, layoutRoot)
	for _, label := range []string{"Platform", "Modules", "Security", "Packages"} {
		require.Contains(t, out, label)
	}
	require.Contains(t, out, layoutRoot+"/modules")
	require.NotContains(t, out, "default:", "no default hint without an override")
}

func TestWriteRegistryLayout_OverrideHint(t *testing.T) {
	orig := color.NoColor
	defer func() { color.NoColor = orig }()
	color.NoColor = true

	// "/" places modules at the repo root; the hint points at the standard path.
	out := renderLayout(t, mirror.BuildRegistryLayout(layoutRoot, "/", ""), true)

	require.Contains(t, out, "default: "+layoutRoot+"/modules")
}

func TestWriteRegistryLayout_OverrideColor(t *testing.T) {
	orig := color.NoColor
	defer func() { color.NoColor = orig }()

	l := mirror.BuildRegistryLayout(layoutRoot, "mymods", "")

	color.NoColor = false
	require.Contains(t, renderLayout(t, l, true), "\x1b[",
		"non-default modules path must be highlighted when colour is enabled")

	color.NoColor = true
	require.NotContains(t, renderLayout(t, l, true), "\x1b[",
		"no ANSI escape codes when colour is disabled")
}
