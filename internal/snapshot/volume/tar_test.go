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

package volume_test

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/volume"
)

// readTar reads all headers and file contents from a tar file at path.
// Returns headers in archive order.
func readTar(t *testing.T, path string) ([]*tar.Header, map[string][]byte) {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err, "open tar")
	defer func() { _ = f.Close() }()

	tr := tar.NewReader(f)

	var headers []*tar.Header

	contents := make(map[string][]byte)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "read tar header")

		headers = append(headers, hdr)

		if hdr.Typeflag == tar.TypeReg {
			data, err := io.ReadAll(tr)
			require.NoError(t, err, "read tar entry %s", hdr.Name)
			contents[hdr.Name] = data
		}
	}

	return headers, contents
}

// writeStagingFile creates a file at stagingDir/relPath with the given content.
func writeStagingFile(t *testing.T, stagingDir, relPath string, content []byte) {
	t.Helper()

	full := filepath.Join(stagingDir, filepath.FromSlash(relPath))

	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(t, os.WriteFile(full, content, 0o644))
}

func TestWriteTar_Basic(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	writeStagingFile(t, stagingDir, "hello.txt", []byte("hello world"))
	writeStagingFile(t, stagingDir, "sub/file.txt", []byte("sub content"))

	mtime := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	entries := []volume.TarEntry{
		{RelPath: "hello.txt", Type: "file", Mode: 0o644, Mtime: mtime},
		{RelPath: "sub/", Type: "dir", Mode: 0o755, Mtime: mtime},
		{RelPath: "sub/file.txt", Type: "file", Mode: 0o600, Mtime: mtime},
		{RelPath: "link.txt", Type: "link", Linkname: "hello.txt", Mode: 0o777, Mtime: mtime},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(outPath, stagingDir, entries)
	require.NoError(t, err)

	_, err = os.Stat(outPath)
	require.NoError(t, err, "output tar must exist")

	headers, contents := readTar(t, outPath)

	// After sorting: "hello.txt" < "link.txt" < "sub/" < "sub/file.txt"
	require.Len(t, headers, 4)

	assert.Equal(t, "hello.txt", headers[0].Name)
	assert.Equal(t, byte(tar.TypeReg), headers[0].Typeflag)
	assert.Equal(t, int64(0o644), headers[0].Mode)
	assert.True(t, mtime.Equal(headers[0].ModTime), "mtime mismatch: want %v got %v", mtime, headers[0].ModTime)
	assert.Equal(t, []byte("hello world"), contents["hello.txt"])

	assert.Equal(t, "link.txt", headers[1].Name)
	assert.Equal(t, byte(tar.TypeSymlink), headers[1].Typeflag)
	assert.Equal(t, "hello.txt", headers[1].Linkname)

	assert.Equal(t, "sub/", headers[2].Name)
	assert.Equal(t, byte(tar.TypeDir), headers[2].Typeflag)
	assert.Equal(t, int64(0o755), headers[2].Mode)

	assert.Equal(t, "sub/file.txt", headers[3].Name)
	assert.Equal(t, byte(tar.TypeReg), headers[3].Typeflag)
	assert.Equal(t, int64(0o600), headers[3].Mode)
	assert.Equal(t, []byte("sub content"), contents["sub/file.txt"])
}

func TestWriteTar_Sorted(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	writeStagingFile(t, stagingDir, "z_last.txt", []byte("z"))
	writeStagingFile(t, stagingDir, "a_first.txt", []byte("a"))
	writeStagingFile(t, stagingDir, "m_middle.txt", []byte("m"))

	// Provide entries in reverse alphabetical order; output must be sorted.
	entries := []volume.TarEntry{
		{RelPath: "z_last.txt", Type: "file"},
		{RelPath: "m_middle.txt", Type: "file"},
		{RelPath: "a_first.txt", Type: "file"},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(outPath, stagingDir, entries)
	require.NoError(t, err)

	headers, _ := readTar(t, outPath)

	require.Len(t, headers, 3)

	assert.Equal(t, "a_first.txt", headers[0].Name)
	assert.Equal(t, "m_middle.txt", headers[1].Name)
	assert.Equal(t, "z_last.txt", headers[2].Name)
}

func TestWriteTar_Defaults(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	writeStagingFile(t, stagingDir, "file.txt", []byte("data"))

	entries := []volume.TarEntry{
		{RelPath: "file.txt", Type: "file"},
		{RelPath: "mydir", Type: "dir"},
		{RelPath: "sym", Type: "link", Linkname: "file.txt"},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(outPath, stagingDir, entries)
	require.NoError(t, err)

	headers, _ := readTar(t, outPath)

	require.Len(t, headers, 3)

	assert.Equal(t, int64(0o644), headers[0].Mode, "default file mode")
	assert.Equal(t, 0, headers[0].Uid, "default uid")
	assert.Equal(t, 0, headers[0].Gid, "default gid")
	// PAX stores timestamps as Unix seconds; zero time.Time{} round-trips as epoch 0.
	assert.Equal(t, int64(0), headers[0].ModTime.Unix(), "default mtime is epoch 0")

	assert.Equal(t, int64(0o755), headers[1].Mode, "default dir mode")

	assert.Equal(t, int64(0o777), headers[2].Mode, "default link mode")
}

func TestWriteTar_Atomic(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	outPath := filepath.Join(outputDir, "data.tar")

	// Request a file entry whose staging file does not exist — WriteTar must fail.
	entries := []volume.TarEntry{
		{RelPath: "missing.txt", Type: "file"},
	}

	err := volume.WriteTar(outPath, stagingDir, entries)
	require.Error(t, err, "WriteTar must fail for missing staging file")

	// The final output file must NOT exist after a failed write.
	_, statErr := os.Stat(outPath)
	assert.True(t, os.IsNotExist(statErr), "partial output must not exist after failure")
}

func TestWriteTar_Empty(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(outPath, t.TempDir(), nil)
	require.NoError(t, err, "empty entry list must produce a valid (empty) tar")

	headers, _ := readTar(t, outPath)
	assert.Empty(t, headers)
}

func TestWriteTar_PAXFormat(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// A name > 100 chars (USTAR limit for the name field with no prefix) forces PAX.
	longName := strings.Repeat("a", 101) + ".txt"

	writeStagingFile(t, stagingDir, longName, []byte("x"))

	entries := []volume.TarEntry{
		{RelPath: longName, Type: "file"},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(outPath, stagingDir, entries)
	require.NoError(t, err)

	headers, _ := readTar(t, outPath)
	require.Len(t, headers, 1)

	assert.Equal(t, tar.FormatPAX, headers[0].Format, "tar format must be PAX")
}
