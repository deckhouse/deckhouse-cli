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

package layouts

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateEmptyImageLayoutAtPath(t *testing.T) {
	p, err := os.MkdirTemp(os.TempDir(), "create_layout_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(p)
	})

	_, err = CreateEmptyImageLayout(p)
	require.NoError(t, err)
	require.DirExists(t, p)
	require.FileExists(t, filepath.Join(p, "oci-layout"))
	require.FileExists(t, filepath.Join(p, "index.json"))
}

func TestImagesLayoutsAllLayouts(t *testing.T) {
	il := &ImageLayouts{
		Deckhouse:         createEmptyOCILayout(t),
		ReleaseChannel:    createEmptyOCILayout(t),
		InstallStandalone: createEmptyOCILayout(t),
		Modules: map[string]ModuleImageLayout{
			"commander-agent": {ModuleLayout: createEmptyOCILayout(t), ReleasesLayout: createEmptyOCILayout(t)},
			"commander":       {ModuleLayout: createEmptyOCILayout(t), ReleasesLayout: createEmptyOCILayout(t)},
		},
	}

	layouts := il.Layouts()
	require.Len(t, layouts, 7, "Layouts should return exactly the number of non-empty layouts defined within it")
	require.Contains(t, layouts, il.Deckhouse, "All non-empty layouts should be returned")
	require.Contains(t, layouts, il.ReleaseChannel, "All non-empty layouts should be returned")
	require.Contains(t, layouts, il.InstallStandalone, "All non-empty layouts should be returned")
	require.Contains(t, layouts, il.Modules["commander"].ModuleLayout, "All non-empty layouts should be returned")
	require.Contains(t, layouts, il.Modules["commander"].ReleasesLayout, "All non-empty layouts should be returned")
	require.Contains(t, layouts, il.Modules["commander-agent"].ModuleLayout, "All non-empty layouts should be returned")
	require.Contains(t, layouts, il.Modules["commander-agent"].ReleasesLayout, "All non-empty layouts should be returned")
}
