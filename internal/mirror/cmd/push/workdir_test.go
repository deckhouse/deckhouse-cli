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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateWorkDir(t *testing.T) {
	t.Run("work dir lives in parent and cleanup keeps everything else", func(t *testing.T) {
		parent := t.TempDir()
		foreignFile := filepath.Join(parent, "data.db")
		require.NoError(t, os.WriteFile(foreignFile, []byte("keep"), 0o644))
		bundleTar := filepath.Join(parent, "upload", "platform.tar")
		require.NoError(t, os.MkdirAll(filepath.Dir(bundleTar), 0o755))
		require.NoError(t, os.WriteFile(bundleTar, []byte("keep"), 0o644))

		dir, cleanup, err := createWorkDir(parent, false)
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(parent, workDirName), dir)
		assert.DirExists(t, dir)

		require.NoError(t, os.MkdirAll(filepath.Join(dir, "unified", "blobs"), 0o755))
		require.NoError(t, cleanup())

		assert.NoDirExists(t, dir)
		assert.FileExists(t, foreignFile)
		assert.FileExists(t, bundleTar)
	})

	t.Run("missing parent is created", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "a", "b")

		dir, cleanup, err := createWorkDir(parent, false)
		require.NoError(t, err)
		assert.DirExists(t, dir)
		require.NoError(t, cleanup())
	})

	t.Run("empty parent is kept unless asked to remove it", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), ".tmp")

		_, cleanup, err := createWorkDir(parent, false)
		require.NoError(t, err)
		require.NoError(t, cleanup())

		assert.DirExists(t, parent)
	})

	t.Run("empty parent is removed when asked", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), ".tmp")

		_, cleanup, err := createWorkDir(parent, true)
		require.NoError(t, err)
		require.NoError(t, cleanup())

		assert.NoDirExists(t, parent)
	})

	t.Run("non-empty parent survives removal of an empty parent", func(t *testing.T) {
		parent := t.TempDir()
		foreignFile := filepath.Join(parent, "keep.txt")
		require.NoError(t, os.WriteFile(foreignFile, []byte("keep"), 0o644))

		_, cleanup, err := createWorkDir(parent, true)
		require.NoError(t, err)
		require.NoError(t, cleanup())

		assert.FileExists(t, foreignFile)
	})

	t.Run("relative parent resolves to an absolute work dir", func(t *testing.T) {
		t.Chdir(t.TempDir())

		dir, cleanup, err := createWorkDir("module-foo", false)
		require.NoError(t, err)
		t.Cleanup(func() { _ = cleanup() })

		assert.True(t, filepath.IsAbs(dir), "work dir %q must be absolute", dir)
		assert.Equal(t, workDirName, filepath.Base(dir))
	})

	t.Run("existing non-empty work dir is refused and left intact", func(t *testing.T) {
		parent := t.TempDir()
		existing := filepath.Join(parent, workDirName)
		inner := filepath.Join(existing, "precious")
		require.NoError(t, os.MkdirAll(existing, 0o755))
		require.NoError(t, os.WriteFile(inner, []byte("keep"), 0o644))

		_, cleanup, err := createWorkDir(parent, true)
		require.Error(t, err)
		assert.Nil(t, cleanup)
		assert.ErrorContains(t, err, existing)
		assert.ErrorContains(t, err, "already exists and is not empty")
		assert.FileExists(t, inner)
	})

	t.Run("existing empty work dir is refused and left intact", func(t *testing.T) {
		parent := t.TempDir()
		existing := filepath.Join(parent, workDirName)
		require.NoError(t, os.Mkdir(existing, 0o755))

		_, _, err := createWorkDir(parent, false)
		require.Error(t, err)
		assert.ErrorContains(t, err, existing+" already exists:")
		assert.DirExists(t, existing)
	})

	t.Run("symlink in place of the work dir is refused and not followed", func(t *testing.T) {
		parent := t.TempDir()
		target := t.TempDir()
		targetFile := filepath.Join(target, "keep.txt")
		require.NoError(t, os.WriteFile(targetFile, []byte("keep"), 0o644))
		require.NoError(t, os.Symlink(target, filepath.Join(parent, workDirName)))

		_, _, err := createWorkDir(parent, false)
		require.Error(t, err)
		assert.ErrorContains(t, err, "is not a directory")
		assert.FileExists(t, targetFile)
	})
}
