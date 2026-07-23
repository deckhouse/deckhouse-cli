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

package mirror

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rowByLabel(t *testing.T, layout RegistryLayout, label string) RegistryPathRow {
	t.Helper()

	for _, row := range layout.Rows {
		if row.Label == label {
			return row
		}
	}

	t.Fatalf("row %q not found in layout", label)

	return RegistryPathRow{}
}

func TestBuildRegistryLayout_Modules(t *testing.T) {
	const root = "registry.example.com/deckhouse/ee"

	tests := []struct {
		name          string
		suffix        string
		wantPath      string
		wantNonDefault bool
	}{
		{name: "flag default", suffix: "/modules", wantPath: root + "/modules", wantNonDefault: false},
		{name: "empty is default", suffix: "", wantPath: root + "/modules", wantNonDefault: false},
		{name: "bare modules is default", suffix: "modules", wantPath: root + "/modules", wantNonDefault: false},
		{name: "root suffix", suffix: "/", wantPath: root, wantNonDefault: true},
		{name: "custom single segment", suffix: "mymods", wantPath: root + "/mymods", wantNonDefault: true},
		{name: "custom multi segment", suffix: "my/mods", wantPath: root + "/my/mods", wantNonDefault: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layout := BuildRegistryLayout(root, tt.suffix, "")

			modules := rowByLabel(t, layout, "Modules")
			assert.Equal(t, tt.wantPath, modules.Path)
			assert.Equal(t, tt.wantNonDefault, modules.NonDefault)
			assert.Equal(t, root+"/modules", modules.DefaultPath)
			assert.Equal(t, tt.wantNonDefault, layout.HasOverride)
		})
	}
}

func TestBuildRegistryLayout_FixedComponents(t *testing.T) {
	const root = "registry.example.com/deckhouse/ee"

	layout := BuildRegistryLayout(root, "/modules", "")

	assert.Equal(t, root, layout.Root)
	assert.Equal(t, root, rowByLabel(t, layout, "Platform").Path)
	assert.Equal(t, root+"/security", rowByLabel(t, layout, "Security").Path)
	assert.Equal(t, root+"/packages", rowByLabel(t, layout, "Packages").Path)

	// Fixed components never carry an override flag.
	for _, label := range []string{"Platform", "Security", "Packages"} {
		assert.False(t, rowByLabel(t, layout, label).NonDefault, "%s must be default", label)
	}
}

func TestBuildRegistryLayout_InstallerRow(t *testing.T) {
	const root = "registry.example.com/deckhouse/ee"

	// Empty installer path omits the row.
	layout := BuildRegistryLayout(root, "/modules", "")
	for _, row := range layout.Rows {
		require.NotEqual(t, "Installer", row.Label)
	}

	// A supplied installer path adds the row verbatim (it lives outside root).
	const installer = "registry.example.com/deckhouse/installer"
	layout = BuildRegistryLayout(root, "/modules", installer)
	assert.Equal(t, installer, rowByLabel(t, layout, "Installer").Path)
}

func TestBuildRegistryLayout_TrimsRootSlash(t *testing.T) {
	layout := BuildRegistryLayout("registry.example.com/deckhouse/ee/", "/", "")

	assert.Equal(t, "registry.example.com/deckhouse/ee", layout.Root)
	assert.Equal(t, "registry.example.com/deckhouse/ee", rowByLabel(t, layout, "Modules").Path)
}
