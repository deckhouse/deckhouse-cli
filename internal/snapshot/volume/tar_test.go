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
	"bytes"
	"cmp"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
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

func sortedTarEntries(entries []volume.TarEntry) volume.TarEntrySource {
	sorted := slices.Clone(entries)
	slices.SortFunc(sorted, func(a, b volume.TarEntry) int {
		return cmp.Compare(a.RelPath, b.RelPath)
	})

	return func(yield func(volume.TarEntry) error) error {
		for _, entry := range sorted {
			if err := yield(entry); err != nil {
				return err
			}
		}

		return nil
	}
}

func TestWriteTar_Basic(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	writeStagingFile(t, stagingDir, "hello.txt", []byte("hello world"))
	writeStagingFile(t, stagingDir, "sub/file.txt", []byte("sub content"))

	mtime := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	entries := []volume.TarEntry{
		{RelPath: "hello.txt", Type: "file", Codec: "none", OriginalPath: "hello.txt", RawSize: 11, Mode: 0o644, Mtime: mtime},
		{RelPath: "sub/", Type: "dir", Mode: 0o755, Mtime: mtime},
		{RelPath: "sub/file.txt", Type: "file", Codec: "none", OriginalPath: "sub/file.txt", RawSize: 11, Mode: 0o600, Mtime: mtime},
		{RelPath: "link.txt", Type: "link", Linkname: "hello.txt", Mode: 0o777, Mtime: mtime},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(context.Background(), outPath, stagingDir, sortedTarEntries(entries))
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
		{RelPath: "z_last.txt", Type: "file", Codec: "none", OriginalPath: "z_last.txt", RawSize: 1},
		{RelPath: "m_middle.txt", Type: "file", Codec: "none", OriginalPath: "m_middle.txt", RawSize: 1},
		{RelPath: "a_first.txt", Type: "file", Codec: "none", OriginalPath: "a_first.txt", RawSize: 1},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(context.Background(), outPath, stagingDir, sortedTarEntries(entries))
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
		{RelPath: "file.txt", Type: "file", Codec: "none", OriginalPath: "file.txt", RawSize: 4},
		{RelPath: "mydir", Type: "dir"},
		{RelPath: "sym", Type: "link", Linkname: "file.txt"},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(context.Background(), outPath, stagingDir, sortedTarEntries(entries))
	require.NoError(t, err)

	headers, _ := readTar(t, outPath)

	require.Len(t, headers, 3)

	assert.Equal(t, int64(0o644), headers[0].Mode, "default file mode")
	assert.Equal(t, 0, headers[0].Uid, "default uid")
	assert.Equal(t, 0, headers[0].Gid, "default gid")
	// Zero Mtime is normalized to Unix epoch 0 before writing; reads back as epoch 0.
	assert.Equal(t, int64(0), headers[0].ModTime.Unix(), "default mtime is epoch 0")

	assert.Equal(t, int64(0o755), headers[1].Mode, "default dir mode")

	assert.Equal(t, int64(0o777), headers[2].Mode, "default link mode")
}

// TestWriteTar_CancelledContext proves that WriteTar honors an already-
// cancelled context: it must return promptly with an error wrapping
// ctx.Err(), and must not leave a partial file at the final (non-.tmp) output
// path — the in-progress AtomicWriter is aborted rather than committed. The
// staging directory (untouched by WriteTar) survives so a subsequent run can
// resume assembly from the same staged files.
func TestWriteTar_CancelledContext(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	writeStagingFile(t, stagingDir, "hello.txt", []byte("hello world"))

	entries := []volume.TarEntry{
		{RelPath: "hello.txt", Type: "file", Codec: "none", OriginalPath: "hello.txt", RawSize: 11},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := volume.WriteTar(ctx, outPath, stagingDir, sortedTarEntries(entries))
	require.Error(t, err, "WriteTar must fail with an already-cancelled context")
	assert.True(t, errors.Is(err, context.Canceled), "error must wrap context.Canceled, got: %v", err)

	_, statErr := os.Stat(outPath)
	assert.True(t, os.IsNotExist(statErr), "partial output must not exist after cancellation")

	_, stagingErr := os.Stat(filepath.Join(stagingDir, "hello.txt"))
	assert.NoError(t, stagingErr, "staging file must survive a cancelled assembly for resume")
}

func TestWriteTar_CancelledDuringOnlyLargeEntryPreservesRetry(t *testing.T) {
	const (
		cancelAfter = int64(96 << 10)
		copyBound   = int64(128 << 10)
	)

	content := bytes.Repeat([]byte("sole-final-entry-"), 256<<10)
	stagingDir := t.TempDir()
	outputDir := t.TempDir()
	relPath := "large.bin"
	stagedPath := filepath.Join(stagingDir, relPath)
	writeStagingFile(t, stagingDir, relPath, content)

	entries := sortedTarEntries([]volume.TarEntry{{
		RelPath:      relPath,
		Type:         "file",
		Codec:        "none",
		OriginalPath: relPath,
		RawSize:      int64(len(content)),
	}})
	outPath := filepath.Join(outputDir, "data.tar")
	ctx := &cancelWhenTempGrowsContext{
		Context:   context.Background(),
		tempPath:  outPath + ".tmp",
		threshold: cancelAfter,
	}

	err := volume.WriteTar(ctx, outPath, stagingDir, entries)
	require.ErrorIs(t, err, context.Canceled)
	require.GreaterOrEqual(t, ctx.triggerSize, cancelAfter)
	require.LessOrEqual(t, ctx.triggerSize, cancelAfter+copyBound,
		"cancellation must stop within a fixed number of copy buffers")

	_, err = os.Stat(outPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(outPath + ".tmp")
	require.ErrorIs(t, err, os.ErrNotExist)

	movedPath := stagedPath + ".moved"
	require.NoError(t, os.Rename(stagedPath, movedPath), "cancelled copy must close its staging descriptor")
	require.NoError(t, os.Rename(movedPath, stagedPath))

	require.NoError(t, volume.WriteTar(context.Background(), outPath, stagingDir, entries))
	retried, err := os.ReadFile(outPath)
	require.NoError(t, err)

	uninterruptedPath := filepath.Join(t.TempDir(), "data.tar")
	require.NoError(t, volume.WriteTar(context.Background(), uninterruptedPath, stagingDir, entries))
	uninterrupted, err := os.ReadFile(uninterruptedPath)
	require.NoError(t, err)
	require.Equal(t, uninterrupted, retried, "retry must produce byte-identical deterministic tar output")
}

func TestWriteTar_CancelledAfterFinalEntryBeforeCommit(t *testing.T) {
	stagingDir := t.TempDir()
	outPath := filepath.Join(t.TempDir(), "data.tar")
	writeStagingFile(t, stagingDir, "final.bin", bytes.Repeat([]byte("F"), 128<<10))

	ctx, cancel := context.WithCancel(context.Background())
	entries := func(yield func(volume.TarEntry) error) error {
		err := yield(volume.TarEntry{
			RelPath:      "final.bin",
			Type:         "file",
			Codec:        "none",
			OriginalPath: "final.bin",
			RawSize:      128 << 10,
		})
		cancel()

		return err
	}

	err := volume.WriteTar(ctx, outPath, stagingDir, entries)
	require.ErrorIs(t, err, context.Canceled)
	_, err = os.Stat(outPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(outPath + ".tmp")
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestWriteTar_PreservesReadErrorConcurrentWithCancellation(t *testing.T) {
	stagingDir := t.TempDir()
	entryPath := filepath.Join(stagingDir, "unreadable")
	require.NoError(t, os.Mkdir(entryPath, 0o755))

	probe, err := os.Open(entryPath)
	require.NoError(t, err)
	_, readErr := probe.Read(make([]byte, 1))
	require.NoError(t, probe.Close())
	if readErr == nil || errors.Is(readErr, io.EOF) {
		t.Skip("platform does not return a non-EOF error when reading a directory")
	}

	underlyingReadErr := errors.Unwrap(readErr)
	require.Error(t, underlyingReadErr)

	ctx := &cancelDuringTarReadContext{
		Context:  context.Background(),
		cancelAt: 2,
		done:     make(chan struct{}),
	}
	outPath := filepath.Join(t.TempDir(), "data.tar")
	entries := sortedTarEntries([]volume.TarEntry{{
		RelPath:      "unreadable",
		Type:         "file",
		Codec:        "none",
		OriginalPath: "unreadable",
	}})

	err = volume.WriteTar(ctx, outPath, stagingDir, entries)
	require.ErrorIs(t, err, underlyingReadErr)
	require.NotErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	_, err = os.Stat(outPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(outPath + ".tmp")
	require.ErrorIs(t, err, os.ErrNotExist)
}

type cancelDuringTarReadContext struct {
	context.Context
	cancelAt  int
	checks    int
	cancelled bool
	done      chan struct{}
}

func (c *cancelDuringTarReadContext) Done() <-chan struct{} {
	return c.done
}

func (c *cancelDuringTarReadContext) Err() error {
	if c.cancelled {
		return context.Canceled
	}

	c.checks++
	if c.checks == c.cancelAt {
		c.cancelled = true
		close(c.done)

		return nil
	}

	return nil
}

type cancelWhenTempGrowsContext struct {
	context.Context
	tempPath    string
	threshold   int64
	triggerSize int64
	cancelled   bool
}

func (c *cancelWhenTempGrowsContext) Err() error {
	if c.cancelled {
		return context.Canceled
	}

	info, err := os.Stat(c.tempPath)
	if err == nil && info.Size() >= c.threshold {
		c.cancelled = true
		c.triggerSize = info.Size()

		return context.Canceled
	}

	return nil
}

func TestWriteTar_Atomic(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	outPath := filepath.Join(outputDir, "data.tar")

	// Request a file entry whose staging file does not exist — WriteTar must fail.
	entries := []volume.TarEntry{
		{RelPath: "missing.txt", Type: "file", Codec: "none", OriginalPath: "missing.txt"},
	}

	err := volume.WriteTar(context.Background(), outPath, stagingDir, sortedTarEntries(entries))
	require.Error(t, err, "WriteTar must fail for missing staging file")

	// The final output file must NOT exist after a failed write.
	_, statErr := os.Stat(outPath)
	assert.True(t, os.IsNotExist(statErr), "partial output must not exist after failure")
}

func TestWriteTar_Empty(t *testing.T) {
	t.Parallel()

	outputDir := t.TempDir()

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(context.Background(), outPath, t.TempDir(), sortedTarEntries(nil))
	require.NoError(t, err, "empty entry list must produce a valid (empty) tar")

	headers, _ := readTar(t, outPath)
	assert.Empty(t, headers)
}

// TestWriteTar_ZeroMtime verifies that a zero-value TarEntry.Mtime is normalized
// to Unix epoch 0 (not year 0001) in the assembled tar, for all three entry types.
func TestWriteTar_ZeroMtime(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	writeStagingFile(t, stagingDir, "file.txt", []byte("data"))

	entries := []volume.TarEntry{
		{RelPath: "file.txt", Type: "file", Codec: "none", OriginalPath: "file.txt", RawSize: 4},
		{RelPath: "mydir", Type: "dir"},
		{RelPath: "sym", Type: "link", Linkname: "file.txt"},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(context.Background(), outPath, stagingDir, sortedTarEntries(entries))
	require.NoError(t, err)

	headers, _ := readTar(t, outPath)
	require.Len(t, headers, 3)

	epoch := time.Unix(0, 0).UTC()

	for _, hdr := range headers {
		assert.True(t, epoch.Equal(hdr.ModTime),
			"entry %q: zero Mtime must produce epoch 0, got %v", hdr.Name, hdr.ModTime)
	}
}

func TestWriteTar_PAXFormat(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// A name > 100 chars (USTAR limit for the name field with no prefix) forces PAX.
	longName := strings.Repeat("a", 101) + ".txt"

	writeStagingFile(t, stagingDir, longName, []byte("x"))

	entries := []volume.TarEntry{
		{RelPath: longName, Type: "file", Codec: "none", OriginalPath: longName, RawSize: 1},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	err := volume.WriteTar(context.Background(), outPath, stagingDir, sortedTarEntries(entries))
	require.NoError(t, err)

	headers, _ := readTar(t, outPath)
	require.Len(t, headers, 1)

	assert.Equal(t, tar.FormatPAX, headers[0].Format, "tar format must be PAX")
	assert.Equal(t, "none", headers[0].PAXRecords[archive.PAXFSCodec])
	assert.Equal(t, longName, headers[0].PAXRecords[archive.PAXFSOriginalPath])
	assert.Equal(t, "1", headers[0].PAXRecords[archive.PAXFSRawSize])
}

func TestWriteTar_RejectsMissingFileMetadata(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "data.tar")
	writeStagingFile(t, stagingDir, "file.txt", []byte("data"))

	err := volume.WriteTar(context.Background(), outputPath, stagingDir, sortedTarEntries([]volume.TarEntry{{
		RelPath: "file.txt",
		Type:    "file",
	}}))

	require.ErrorIs(t, err, archive.ErrInvalidFSMetadata)

	_, statErr := os.Stat(outputPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

// TestWriteTar_CompressedFileEntries verifies that when file TarEntries carry
// a compressed name (<relPath><ext>) pointing to a pre-compressed staging file,
// WriteTar copies the compressed bytes verbatim into the tar under that name.
// Dir and link entries keep their plain relPath (no extension suffix).
// Extracting + per-file decoding must reproduce the original bytes.
func TestWriteTar_CompressedFileEntries(t *testing.T) {
	t.Parallel()

	// mustCodec is defined in fs_test.go (same package).
	codec := mustCodec(t, "zstd")
	ext := codec.Ext()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	alphaOrig := []byte("alpha-original-content")
	betaOrig := []byte("beta-original-content")

	var alphaBuf, betaBuf bytes.Buffer

	require.NoError(t, codec.EncodeStream(&alphaBuf, bytes.NewReader(alphaOrig)))
	require.NoError(t, codec.EncodeStream(&betaBuf, bytes.NewReader(betaOrig)))

	// Staging files are named <relPath><ext>, mirroring stageCompressedFile output.
	writeStagingFile(t, stagingDir, "alpha.txt"+ext, alphaBuf.Bytes())
	writeStagingFile(t, stagingDir, "sub/beta.txt"+ext, betaBuf.Bytes())

	entries := []volume.TarEntry{
		{RelPath: "alpha.txt" + ext, Type: "file", Codec: "zstd", OriginalPath: "alpha.txt", RawSize: int64(len(alphaOrig))},
		{RelPath: "sub/", Type: "dir"},
		{RelPath: "sub/beta.txt" + ext, Type: "file", Codec: "zstd", OriginalPath: "sub/beta.txt", RawSize: int64(len(betaOrig))},
		{RelPath: "link.txt", Type: "link", Linkname: "alpha.txt" + ext},
	}

	outPath := filepath.Join(outputDir, "data.tar")

	require.NoError(t, volume.WriteTar(context.Background(), outPath, stagingDir, sortedTarEntries(entries)))

	headers, contents := readTar(t, outPath)

	// Sorted order: "alpha.txt.zst" < "link.txt" < "sub/" < "sub/beta.txt.zst"
	require.Len(t, headers, 4)

	// File entries carry the compressed name (<relPath><ext>).
	assert.Equal(t, "alpha.txt"+ext, headers[0].Name)
	assert.Equal(t, byte(tar.TypeReg), headers[0].Typeflag)

	// Link entries keep the plain name — no codec extension suffix.
	assert.Equal(t, "link.txt", headers[1].Name)
	assert.Equal(t, byte(tar.TypeSymlink), headers[1].Typeflag)

	// Dir entries keep the plain name — no codec extension suffix.
	assert.Equal(t, "sub/", headers[2].Name)
	assert.Equal(t, byte(tar.TypeDir), headers[2].Typeflag)

	// Nested file entry also carries the compressed name.
	assert.Equal(t, "sub/beta.txt"+ext, headers[3].Name)
	assert.Equal(t, byte(tar.TypeReg), headers[3].Typeflag)

	// Extracting + per-file decoding must reproduce the original bytes.
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)

	defer dec.Close()

	alphaDecoded, err := dec.DecodeAll(contents["alpha.txt"+ext], nil)
	require.NoError(t, err, "decode alpha.txt entry")
	assert.Equal(t, alphaOrig, alphaDecoded)

	betaDecoded, err := dec.DecodeAll(contents["sub/beta.txt"+ext], nil)
	require.NoError(t, err, "decode sub/beta.txt entry")
	assert.Equal(t, betaOrig, betaDecoded)
}

// ── symlink target sanitization (sanitize-server-provided-paths) ───────────

// TestWriteTar_RejectsUnsafeSymlinkTargets is the primary regression test for the
// symlink half of sanitize-server-provided-paths: a target that is absolute or that
// climbs above the volume root once resolved relative to its own entry's directory
// must be rejected with a wrapped ErrUnsafePath BEFORE the tar header is written, and
// the output tar must not exist afterward. An in-root relative target ("sibling")
// must keep working unchanged.
func TestWriteTar_RejectsUnsafeSymlinkTargets(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		entryPath  string
		linkTarget string
	}{
		{name: "AbsoluteTarget", entryPath: "link", linkTarget: "/abs"},
		{name: "RootLevelParentEscape", entryPath: "link", linkTarget: "../../outside"},
		{name: "NestedParentEscape", entryPath: "a/b/link", linkTarget: "../../../etc"},
		{name: "EmptyTarget", entryPath: "link", linkTarget: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			outputDir := t.TempDir()
			outPath := filepath.Join(outputDir, "data.tar")

			entries := []volume.TarEntry{
				{RelPath: tc.entryPath, Type: "link", Linkname: tc.linkTarget},
			}

			err := volume.WriteTar(context.Background(), outPath, t.TempDir(), sortedTarEntries(entries))
			if err == nil {
				t.Fatal("expected an error for an unsafe symlink target, got nil")
			}

			assert.ErrorIs(t, err, volume.ErrUnsafePath)

			_, statErr := os.Stat(outPath)
			assert.True(t, os.IsNotExist(statErr), "output tar must not exist after a rejected symlink target")
		})
	}
}

// TestWriteTar_KeepsInRootRelativeSymlinkTargets proves the sanitization guard does
// not disturb legitimate relative symlinks: a sibling target, and a target that dips
// below and back above a subdirectory without net escaping the root, both keep
// producing a normal TypeSymlink entry with the target preserved verbatim.
func TestWriteTar_KeepsInRootRelativeSymlinkTargets(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		entryPath  string
		linkTarget string
	}{
		{name: "Sibling", entryPath: "a/link", linkTarget: "sibling"},
		{name: "UpAndBackDownWithinRoot", entryPath: "a/b/link", linkTarget: "../../c"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			outputDir := t.TempDir()
			outPath := filepath.Join(outputDir, "data.tar")

			entries := []volume.TarEntry{
				{RelPath: tc.entryPath, Type: "link", Linkname: tc.linkTarget},
			}

			require.NoError(t, volume.WriteTar(context.Background(), outPath, t.TempDir(), sortedTarEntries(entries)))

			headers, _ := readTar(t, outPath)
			require.Len(t, headers, 1)
			assert.Equal(t, byte(tar.TypeSymlink), headers[0].Typeflag)
			assert.Equal(t, tc.linkTarget, headers[0].Linkname)
		})
	}
}
