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
	"reflect"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/stretchr/testify/require"
)

func TestCreateEmptyImageLayoutAtPath(t *testing.T) {
	p, err := os.MkdirTemp(os.TempDir(), "create_layout_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(p)
	})

	_, err = CreateEmptyImageLayoutAtPath(p)
	require.NoError(t, err)
	require.DirExists(t, p)
	require.FileExists(t, filepath.Join(p, "oci-layout"))
	require.FileExists(t, filepath.Join(p, "index.json"))
}

func TestImagesLayoutsAllLayouts(t *testing.T) {
	il := &ImageLayouts{
		Modules: map[string]ModuleImageLayout{
			"commander-agent": {ModuleLayout: createEmptyOCILayout(t), ReleasesLayout: createEmptyOCILayout(t)},
			"commander":       {ModuleLayout: createEmptyOCILayout(t), ReleasesLayout: createEmptyOCILayout(t)},
		},
	}

	v := reflect.ValueOf(il).Elem()
	layoutPathType := reflect.TypeOf(layout.Path(""))
	expectedLayouts := make([]layout.Path, 0)
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Type() != layoutPathType {
			continue
		}

		newLayout := string(createEmptyOCILayout(t))
		v.Field(i).SetString(newLayout)
		expectedLayouts = append(expectedLayouts, layout.Path(v.Field(i).String()))
	}
	for _, moduleImageLayout := range il.Modules {
		expectedLayouts = append(expectedLayouts, moduleImageLayout.ModuleLayout, moduleImageLayout.ReleasesLayout)
	}

	layouts := il.AllLayouts()
	require.Len(t, layouts, len(expectedLayouts), "AllLayouts should return exactly the number of layouts defined within it")
	require.ElementsMatch(t, expectedLayouts, layouts, "AllLayouts should return every layout.Path value within it")
}
