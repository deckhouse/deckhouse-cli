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
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/require"
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

	tarFile, err := os.Create(tarBundlePath)
	require.NoError(t, err)
	err = Pack(context.TODO(), packFromDir, tarFile)
	require.NoError(t, tarFile.Sync())
	require.NoError(t, tarFile.Close())

	require.NoError(t, err, "Packing should finish without errors")
	require.FileExists(t, tarBundlePath)

	tarBundle, err := os.Open(tarBundlePath)
	require.NoError(t, err)
	err = Unpack(context.TODO(), tarBundle, unpackToDir, "")
	require.NoError(t, err, "Unpacking should finish without errors")

	resultingFiles := findAllPaths(t, unpackToDir)
	require.Equal(t, expectedFiles, resultingFiles, "Expected to find same file trees under source and target dirs")
}

// TestUnpackMergesCollidingIndexes reproduces the bundle bug where two archives
// carry the same OCI layout path (packages/foo/version) but expose different tag
// subsets. The first archive holds the full set of tags; the second (built from
// channels only) holds a subset. Unpacking both into a shared directory must keep
// every tag instead of letting the second archive's index.json overwrite the first.
func TestUnpackMergesCollidingIndexes(t *testing.T) {
	unpackDir, err := os.MkdirTemp(os.TempDir(), "unpack_merge_test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(unpackDir) })

	const layoutPath = "packages/foo/version/index.json"

	fullTags := []string{"stable", "v1.0.0", "v1.1.0", "v1.2.0"}
	subsetTags := []string{"stable", "v1.2.0"}

	// package-foo.tar: full index. Unpacked first (alphabetical order).
	fooTar := makeIndexTar(t, layoutPath, fullTags)
	require.NoError(t, Unpack(context.TODO(), bytes.NewReader(fooTar), unpackDir, "package-foo"))

	// package-versions.tar: truncated index sharing the same path.
	versionsTar := makeIndexTar(t, layoutPath, subsetTags)
	require.NoError(t, Unpack(context.TODO(), bytes.NewReader(versionsTar), unpackDir, "package-versions"))

	gotTags := readIndexTags(t, filepath.Join(unpackDir, layoutPath))
	require.ElementsMatch(t, fullTags, gotTags, "all tags must survive the merge of colliding index.json files")
}

// TestUnpackMergesThreeArchives verifies the merge accumulates across more than
// two archives unpacked into the same layout path.
func TestUnpackMergesThreeArchives(t *testing.T) {
	unpackDir, err := os.MkdirTemp(os.TempDir(), "unpack_merge3_test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(unpackDir) })

	const layoutPath = "packages/foo/version/index.json"

	require.NoError(t, Unpack(context.TODO(), bytes.NewReader(makeIndexTar(t, layoutPath, []string{"v1.0.0"})), unpackDir, "package-a"))
	require.NoError(t, Unpack(context.TODO(), bytes.NewReader(makeIndexTar(t, layoutPath, []string{"v1.1.0"})), unpackDir, "package-b"))
	require.NoError(t, Unpack(context.TODO(), bytes.NewReader(makeIndexTar(t, layoutPath, []string{"v1.2.0"})), unpackDir, "package-c"))

	gotTags := readIndexTags(t, filepath.Join(unpackDir, layoutPath))
	require.ElementsMatch(t, []string{"v1.0.0", "v1.1.0", "v1.2.0"}, gotTags)
}

// TestUnpackSubsetFirstThenFull verifies that the truncated archive arriving
// first does not prevent the full archive's extra tags from being added.
func TestUnpackSubsetFirstThenFull(t *testing.T) {
	unpackDir, err := os.MkdirTemp(os.TempDir(), "unpack_subset_first_test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(unpackDir) })

	const layoutPath = "packages/foo/version/index.json"

	require.NoError(t, Unpack(context.TODO(), bytes.NewReader(makeIndexTar(t, layoutPath, []string{"stable", "v1.2.0"})), unpackDir, "package-versions"))
	require.NoError(t, Unpack(context.TODO(), bytes.NewReader(makeIndexTar(t, layoutPath, []string{"stable", "v1.0.0", "v1.1.0", "v1.2.0"})), unpackDir, "package-foo"))

	gotTags := readIndexTags(t, filepath.Join(unpackDir, layoutPath))
	require.ElementsMatch(t, []string{"stable", "v1.0.0", "v1.1.0", "v1.2.0"}, gotTags)
}

// TestUnpackNoCollisionKeepsSingleIndex verifies the common case where a layout
// path appears in only one archive: the index is moved as-is.
func TestUnpackNoCollisionKeepsSingleIndex(t *testing.T) {
	unpackDir, err := os.MkdirTemp(os.TempDir(), "unpack_no_collision_test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(unpackDir) })

	const layoutPath = "packages/foo/version/index.json"

	tags := []string{"stable", "v2.0.0"}
	require.NoError(t, Unpack(context.TODO(), bytes.NewReader(makeIndexTar(t, layoutPath, tags)), unpackDir, "package-foo"))

	gotTags := readIndexTags(t, filepath.Join(unpackDir, layoutPath))
	require.ElementsMatch(t, tags, gotTags)
}

func TestMergeIndexJSON(t *testing.T) {
	t.Run("union deduplicates identical descriptors", func(t *testing.T) {
		dir := t.TempDir()
		dst := filepath.Join(dir, "dst.json")
		src := filepath.Join(dir, "src.json")

		writeIndexFile(t, dst, []string{"stable", "v1.0.0", "v1.1.0", "v1.2.0"})
		writeIndexFile(t, src, []string{"stable", "v1.2.0"})

		require.NoError(t, mergeIndexJSON(src, dst))

		merged := readOCIIndexFile(t, dst)
		require.Len(t, merged.Manifests, 4, "shared tags must not be duplicated")
		require.ElementsMatch(t, []string{"stable", "v1.0.0", "v1.1.0", "v1.2.0"}, tagsOf(merged))
	})

	t.Run("same tag with distinct digests are both kept", func(t *testing.T) {
		dir := t.TempDir()
		dst := filepath.Join(dir, "dst.json")
		src := filepath.Join(dir, "src.json")

		writeIndexWithDescriptors(t, dst, []v1.Descriptor{descriptorFor("stable", "aaaa")})
		writeIndexWithDescriptors(t, src, []v1.Descriptor{descriptorFor("stable", "bbbb")})

		require.NoError(t, mergeIndexJSON(src, dst))

		merged := readOCIIndexFile(t, dst)
		require.Len(t, merged.Manifests, 2, "same tag pointing at different digests must both survive")
	})

	t.Run("result is sorted by ref name", func(t *testing.T) {
		dir := t.TempDir()
		dst := filepath.Join(dir, "dst.json")
		src := filepath.Join(dir, "src.json")

		writeIndexFile(t, dst, []string{"v1.2.0", "stable"})
		writeIndexFile(t, src, []string{"v1.0.0", "v1.1.0"})

		require.NoError(t, mergeIndexJSON(src, dst))

		refs := make([]string, 0)
		for _, m := range readOCIIndexFile(t, dst).Manifests {
			refs = append(refs, m.Annotations[refNameAnnotation])
		}

		require.IsIncreasing(t, refs, "manifests must be deterministically sorted by ref name")
	})

	t.Run("schema and media type fall back to source when destination lacks them", func(t *testing.T) {
		dir := t.TempDir()
		dst := filepath.Join(dir, "dst.json")
		src := filepath.Join(dir, "src.json")

		// Destination missing schemaVersion/mediaType.
		require.NoError(t, os.WriteFile(dst, []byte(`{"manifests":[]}`), 0o600))
		writeIndexFile(t, src, []string{"v1.0.0"})

		require.NoError(t, mergeIndexJSON(src, dst))

		merged := readOCIIndexFile(t, dst)
		require.Equal(t, int64(2), merged.SchemaVersion)
		require.Equal(t, "application/vnd.oci.image.index.v1+json", string(merged.MediaType))
		require.ElementsMatch(t, []string{"v1.0.0"}, tagsOf(merged))
	})

	t.Run("invalid source json returns error", func(t *testing.T) {
		dir := t.TempDir()
		dst := filepath.Join(dir, "dst.json")
		src := filepath.Join(dir, "src.json")

		writeIndexFile(t, dst, []string{"v1.0.0"})
		require.NoError(t, os.WriteFile(src, []byte("{not json"), 0o600))

		require.Error(t, mergeIndexJSON(src, dst))
	})

	t.Run("missing source file returns error", func(t *testing.T) {
		dir := t.TempDir()
		dst := filepath.Join(dir, "dst.json")
		writeIndexFile(t, dst, []string{"v1.0.0"})

		require.Error(t, mergeIndexJSON(filepath.Join(dir, "does-not-exist.json"), dst))
	})
}

func descriptorFor(tag, hexSeed string) v1.Descriptor {
	hex := hexSeed
	for len(hex) < 64 {
		hex += "0"
	}

	return v1.Descriptor{
		MediaType: "application/vnd.oci.image.manifest.v1+json",
		Size:      123,
		Digest:    v1.Hash{Algorithm: "sha256", Hex: hex[:64]},
		Annotations: map[string]string{
			refNameAnnotation: "example.com/packages/foo/version:" + tag,
		},
	}
}

func writeIndexFile(t *testing.T, path string, tags []string) {
	t.Helper()

	descriptors := make([]v1.Descriptor, 0, len(tags))
	for _, tag := range tags {
		descriptors = append(descriptors, descriptorFor(tag, hexDigestForTag(tag)))
	}

	writeIndexWithDescriptors(t, path, descriptors)
}

func writeIndexWithDescriptors(t *testing.T, path string, descriptors []v1.Descriptor) {
	t.Helper()

	index := ociIndex{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.index.v1+json",
		Manifests:     descriptors,
	}

	raw, err := json.MarshalIndent(index, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
}

func readOCIIndexFile(t *testing.T, path string) *ociIndex {
	t.Helper()

	index, err := readOCIIndex(path)
	require.NoError(t, err)

	return index
}

func tagsOf(index *ociIndex) []string {
	tags := make([]string, 0, len(index.Manifests))
	for _, m := range index.Manifests {
		_, tag, _ := strings.Cut(m.Annotations[refNameAnnotation], ":")
		tags = append(tags, tag)
	}

	return tags
}

// makeIndexTar builds an in-memory tar with a single OCI index.json at indexPath,
// containing one manifest per tag (distinct digests).
func makeIndexTar(t *testing.T, indexPath string, tags []string) []byte {
	t.Helper()

	manifests := make([]v1.Descriptor, 0, len(tags))
	for i, tag := range tags {
		manifests = append(manifests, v1.Descriptor{
			MediaType: "application/vnd.oci.image.manifest.v1+json",
			Size:      int64(100 + i),
			Digest:    v1.Hash{Algorithm: "sha256", Hex: hexDigestForTag(tag)},
			Annotations: map[string]string{
				refNameAnnotation: "example.com/packages/foo/version:" + tag,
			},
		})
	}

	index := ociIndex{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.index.v1+json",
		Manifests:     manifests,
	}

	raw, err := json.MarshalIndent(index, "", "  ")
	require.NoError(t, err)

	buf := &bytes.Buffer{}
	tw := tar.NewWriter(buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     indexPath,
		Size:     int64(len(raw)),
		Mode:     0o644,
	}))
	_, err = tw.Write(raw)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	return buf.Bytes()
}

func readIndexTags(t *testing.T, path string) []string {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	index := &ociIndex{}
	require.NoError(t, json.Unmarshal(raw, index))

	tags := make([]string, 0, len(index.Manifests))
	for _, m := range index.Manifests {
		ref := m.Annotations[refNameAnnotation]
		_, tag, found := strings.Cut(ref, ":")
		require.True(t, found, "manifest ref %q must contain a tag", ref)
		tags = append(tags, tag)
	}

	return tags
}

func hexDigestForTag(tag string) string {
	const hexLen = 64
	out := make([]byte, hexLen)
	for i := range out {
		out[i] = '0'
	}

	for i := 0; i < len(tag) && i < hexLen; i++ {
		out[hexLen-1-i] = "0123456789abcdef"[tag[len(tag)-1-i]%16]
	}

	return string(out)
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
