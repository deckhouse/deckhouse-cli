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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

func TestCollectBundleStats(t *testing.T) {
	dir := t.TempDir()

	write := func(name string, size int) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o644))
	}

	// Single-file artifacts.
	write("platform.tar", 100)
	write("installer.tar", 50)
	// One chunked artifact spread over two chunks.
	write("module-a.tar.0000.chunk", 30)
	write("module-a.tar.0001.chunk", 20)
	// Files that must be ignored.
	write("platform.tar.gostsum", 7)
	write("notes.txt", 5)
	require.NoError(t, os.Mkdir(filepath.Join(dir, "subdir"), 0o755))

	p := &Puller{
		params: &params.PullParams{
			BaseParams: params.BaseParams{BundleDir: dir},
		},
	}

	stats, err := p.collectBundleStats()
	require.NoError(t, err)

	// Three logical artifacts: platform.tar, installer.tar, module-a.tar.
	require.Len(t, stats.Files, 3)

	byName := map[string]int64{}
	chunksByName := map[string]int{}
	for _, f := range stats.Files {
		byName[f.Name] = f.Bytes
		chunksByName[f.Name] = f.Chunks
	}

	require.Equal(t, int64(100), byName["platform.tar"])
	require.Equal(t, int64(50), byName["installer.tar"])
	require.Equal(t, int64(50), byName["module-a.tar"]) // 30 + 20
	require.Equal(t, 0, chunksByName["platform.tar"])
	require.Equal(t, 2, chunksByName["module-a.tar"])

	// Total excludes .gostsum, .txt and the directory.
	require.Equal(t, int64(200), stats.TotalBytes)

	// Sorted by logical name.
	require.Equal(t, "installer.tar", stats.Files[0].Name)
	require.Equal(t, "module-a.tar", stats.Files[1].Name)
	require.Equal(t, "platform.tar", stats.Files[2].Name)
}

func TestCollectBundleStats_MissingDir(t *testing.T) {
	p := &Puller{
		params: &params.PullParams{
			BaseParams: params.BaseParams{BundleDir: filepath.Join(t.TempDir(), "does-not-exist")},
		},
	}

	_, err := p.collectBundleStats()
	require.Error(t, err)
}
