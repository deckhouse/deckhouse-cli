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

package rpp

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testMaxBytes = 1 << 20

func gzipTar(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	for name, content := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(content)),
			Typeflag: tar.TypeReg,
		}))
		_, err := tw.Write([]byte(content))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	return buf.Bytes()
}

func TestExtractFileToPathForcesMode(t *testing.T) {
	archive := gzipTar(t, map[string]string{"./d8": "BINARY", "README": "ignored"})
	dest := filepath.Join(t.TempDir(), "d8")

	// Source mode is 0o644; the forced mode must win so the binary is executable.
	require.NoError(t, ExtractFileToPath(bytes.NewReader(archive), "d8", dest, 0o755, testMaxBytes))

	got, err := os.ReadFile(dest)
	require.NoError(t, err)
	assert.Equal(t, "BINARY", string(got))

	info, err := os.Stat(dest)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o755), info.Mode().Perm())
}

func TestExtractFileToPathNotFound(t *testing.T) {
	archive := gzipTar(t, map[string]string{"README": "x"})

	err := ExtractFileToPath(bytes.NewReader(archive), "d8", filepath.Join(t.TempDir(), "d8"), 0o755, testMaxBytes)
	require.ErrorIs(t, err, ErrFileNotFound)
}

func TestExtractFileToPathTooBig(t *testing.T) {
	archive := gzipTar(t, map[string]string{"d8": "way-too-large"})

	err := ExtractFileToPath(bytes.NewReader(archive), "d8", filepath.Join(t.TempDir(), "d8"), 0o755, 4)
	require.Error(t, err)
}

func TestReadFilePresent(t *testing.T) {
	archive := gzipTar(t, map[string]string{"contract.json": `{"name":"x"}`})

	data, found, err := ReadFile(bytes.NewReader(archive), "contract.json", testMaxBytes)
	require.NoError(t, err)
	require.True(t, found)
	assert.JSONEq(t, `{"name":"x"}`, string(data))
}

func TestReadFileAbsent(t *testing.T) {
	archive := gzipTar(t, map[string]string{"d8": "BINARY"})

	data, found, err := ReadFile(bytes.NewReader(archive), "contract.json", testMaxBytes)
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, data)
}
