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

package bundle

import (
	"crypto/rand"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/contexts"
)

func TestBundlePackingAndUnpacking(t *testing.T) {
	tmpDir := os.TempDir()
	tarBundlePath := filepath.Join(tmpDir, "pack_test.tar")

	packFromDir, err := os.MkdirTemp(os.TempDir(), "pack_test")
	require.NoError(t, err)
	unpackToDir, err := os.MkdirTemp(os.TempDir(), "unpack_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(packFromDir)
		_ = os.RemoveAll(unpackToDir)
		_ = os.Remove(tarBundlePath)
	})

	fillTestFileTree(t, packFromDir)
	expectedFiles := findAllPaths(t, packFromDir)

	err = Pack(&contexts.PullContext{
		BaseContext: contexts.BaseContext{
			BundlePath:         tarBundlePath,
			UnpackedImagesPath: packFromDir,
		},
	})
	require.NoError(t, err, "Packing should finish without errors")
	require.FileExists(t, tarBundlePath)

	err = Unpack(&contexts.BaseContext{
		BundlePath:         tarBundlePath,
		UnpackedImagesPath: unpackToDir,
	})
	require.NoError(t, err, "Unpacking should finish without errors")

	resultingFiles := findAllPaths(t, unpackToDir)
	require.Equal(t, expectedFiles, resultingFiles, "Expected to find same file trees under source and target dirs")
}

func TestChunkedBundlePackingAndUnpacking(t *testing.T) {
	tmpDir := os.TempDir()
	bundlePath := filepath.Join(tmpDir, "pack_test.tar")

	packFromDir, err := os.MkdirTemp(os.TempDir(), "pack_test")
	require.NoError(t, err)
	unpackToDir, err := os.MkdirTemp(os.TempDir(), "unpack_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(packFromDir)
		_ = os.RemoveAll(unpackToDir)
		_ = os.Remove(bundlePath)
	})

	fillTestFileTree(t, packFromDir)
	expectedFiles := findAllPaths(t, packFromDir)

	err = Pack(&contexts.PullContext{
		BaseContext: contexts.BaseContext{
			BundlePath:         bundlePath,
			UnpackedImagesPath: packFromDir,
		},
		BundleChunkSize: 3 * 1024 * 1024,
	})
	require.NoError(t, err, "Packing should finish without errors")

	expectedChunks := []string{
		"pack_test.tar.0000.chunk",
		"pack_test.tar.0001.chunk",
		"pack_test.tar.0002.chunk",
		"pack_test.tar.0003.chunk",
	}
	for _, chunkName := range expectedChunks {
		require.FileExists(t, filepath.Join(filepath.Dir(bundlePath), chunkName))
	}

	err = Unpack(&contexts.BaseContext{
		BundlePath:         bundlePath,
		UnpackedImagesPath: unpackToDir,
	})
	require.NoError(t, err, "Unpacking should finish without errors")

	resultingFiles := findAllPaths(t, unpackToDir)
	require.Equal(t, expectedFiles, resultingFiles, "Expected to find same file trees under source and target dirs")
}

func fillTestFileTree(t *testing.T, packFromDir string) {
	t.Helper()

	files := []string{
		filepath.Join(packFromDir, "file"),
		filepath.Join(packFromDir, "dir", "file1"),
		filepath.Join(packFromDir, "dir", "dir2", "file3"),
	}

	require.NoError(t, os.MkdirAll(filepath.Join(packFromDir, "dir", "dir2"), 0o777))

	for _, filePath := range files {
		file, err := os.Create(filePath)
		require.NoError(t, err)

		_, err = io.CopyN(file, rand.Reader, 10*1024*1024)
		require.NoError(t, err)

		require.NoError(t, file.Sync())
		require.NoError(t, file.Close())
	}
}

func findAllPaths(t *testing.T, dir string) []string {
	t.Helper()

	files := make([]string, 0)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		files = append(files, strings.TrimPrefix(path, dir))
		return nil
	})
	require.NoError(t, err, "Expected no errors during directory traversal")

	return files
}
