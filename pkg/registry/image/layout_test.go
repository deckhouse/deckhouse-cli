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

package image_test

import (
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// wrapImage wraps a v1.Image as a *regimage.Image with metadata populated
// from its digest, mirroring how the puller assembles images before calling
// ImageLayout.AddImage. The returned image carries TagReference=tagRef and
// Digest=img.Digest().
func wrapImage(t *testing.T, img v1.Image, tagRef string) *regimage.Image {
	t.Helper()
	wrapped, err := regimage.NewImage(img, regimage.WithFetchingMetadata(tagRef))
	require.NoError(t, err, "wrap v1.Image into regimage.Image")
	return wrapped
}

// indexDescriptorCount reads index.json of the OCI layout and returns the
// number of descriptors currently recorded. It is the source of truth for
// "what mirror push will see".
func indexDescriptorCount(t *testing.T, l *regimage.ImageLayout) int {
	t.Helper()
	index, err := l.Path().ImageIndex()
	require.NoError(t, err, "read image index")
	manifest, err := index.IndexManifest()
	require.NoError(t, err, "parse index manifest")
	return len(manifest.Manifests)
}

// TestAddImage_AppendsOnceForNewTag is a sanity check that AddImage does what
// the previous behaviour did when called with a fresh tag.
func TestAddImage_AppendsOnceForNewTag(t *testing.T) {
	l, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	v1img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.0.0"}`).
		MustBuild()

	require.NoError(t, l.AddImage(wrapImage(t, v1img, "example.io/repo:v1.0.0"), "v1.0.0"))

	assert.Equal(t, 1, indexDescriptorCount(t, l),
		"single AddImage call must produce exactly one descriptor")
}

// TestAddImage_IdempotentForSameTagAndDigest is the regression test for the
// duplicate-tag bug observed in `mirror push` (e.g. "[1 / 337] ... cse:v1.73.2"
// followed by "[125 / 337] ... cse:v1.73.2"). The duplicates originated in
// the puller, which iterates the same image set more than once during platform
// pulls; the second pass calls AddImage with the same image again. Before the
// idempotency guard each call appended a new descriptor to index.json.
//
// We assert here that calling AddImage twice with the same (tag, image)
// produces exactly one descriptor, so the layout itself can no longer host
// the duplicate.
func TestAddImage_IdempotentForSameTagAndDigest(t *testing.T) {
	l, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	v1img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.73.2"}`).
		MustBuild()

	const tag = "v1.73.2"

	require.NoError(t, l.AddImage(wrapImage(t, v1img, "example.io/repo:"+tag), tag),
		"first AddImage must succeed")
	require.NoError(t, l.AddImage(wrapImage(t, v1img, "example.io/repo:"+tag), tag),
		"second AddImage with same tag+digest must be a no-op, not an error")

	assert.Equal(t, 1, indexDescriptorCount(t, l),
		"AddImage must not append a second descriptor for the same (tag, digest)")
}

// TestAddImage_NewDescriptorForSameTagDifferentDigest pins the deliberate
// fall-through path: when the same tag points to a different image digest,
// AddImage still appends a new descriptor and updates the in-memory metadata
// to the latest one. This preserves "re-tag" semantics that callers may rely
// on; the pusher then deduplicates by short_tag with last-wins semantics.
func TestAddImage_NewDescriptorForSameTagDifferentDigest(t *testing.T) {
	l, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	const tag = "v1.73.2"

	imgA := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.73.2","build":"A"}`).
		MustBuild()
	imgB := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.73.2","build":"B"}`).
		MustBuild()

	digestA, err := imgA.Digest()
	require.NoError(t, err)
	digestB, err := imgB.Digest()
	require.NoError(t, err)
	require.NotEqual(t, digestA.String(), digestB.String(),
		"the two builder outputs must differ so we are actually exercising the conflict path")

	require.NoError(t, l.AddImage(wrapImage(t, imgA, "example.io/repo:"+tag), tag))
	require.NoError(t, l.AddImage(wrapImage(t, imgB, "example.io/repo:"+tag), tag))

	assert.Equal(t, 2, indexDescriptorCount(t, l),
		"different digests under the same tag must remain visible in the index")

	meta, err := l.GetMeta(tag)
	require.NoError(t, err)
	require.NotNil(t, meta.GetDigest())
	assert.Equal(t, digestB.String(), meta.GetDigest().String(),
		"in-memory metadata for the tag must reflect the latest AddImage call")
}

// buildMultiPlatformIndex builds a two-platform OCI index with a top-level
// annotation - the shape CLI plugin images are published in.
func buildMultiPlatformIndex(t *testing.T, contract string) v1.ImageIndex {
	t.Helper()

	linuxImg := upfake.NewImageBuilder().WithFile("plugin", "linux-bin").MustBuild()
	darwinImg := upfake.NewImageBuilder().WithFile("plugin", "darwin-bin").MustBuild()

	idx := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{
			Add:        linuxImg,
			Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}},
		},
		mutate.IndexAddendum{
			Add:        darwinImg,
			Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "darwin", Architecture: "arm64"}},
		},
	)

	withAnnotations := mutate.Annotations(idx, map[string]string{"contract": contract})

	annotated, ok := withAnnotations.(v1.ImageIndex)
	require.True(t, ok, "mutate.Annotations on an index must return an index")

	return annotated
}

// TestAddIndex_PreservesIndexStructure checks that the index lands as one
// descriptor, the children keep their platforms, the contract annotation
// survives byte-exact, and the ref/short_tag annotations for the pusher are
// set.
func TestAddIndex_PreservesIndexStructure(t *testing.T) {
	l, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	idx := buildMultiPlatformIndex(t, "ZmFrZS1jb250cmFjdA==")
	idxDigest, err := idx.Digest()
	require.NoError(t, err)

	const tagRef = "example.io/deckhouse-cli/plugins/foo:v1.0.0"
	require.NoError(t, l.AddIndex(idx, "v1.0.0", tagRef))

	require.Equal(t, 1, indexDescriptorCount(t, l),
		"a single AddIndex call must produce exactly one descriptor")

	topIndex, err := l.Path().ImageIndex()
	require.NoError(t, err)
	topManifest, err := topIndex.IndexManifest()
	require.NoError(t, err)

	desc := topManifest.Manifests[0]
	assert.True(t, desc.MediaType.IsIndex(), "the descriptor must keep the index media type")
	assert.Equal(t, idxDigest, desc.Digest, "the index digest must be preserved")
	assert.Equal(t, tagRef, desc.Annotations[regimage.AnnotationImageReferenceName])
	assert.Equal(t, "v1.0.0", desc.Annotations[regimage.AnnotationImageShortTag])

	nested, err := topIndex.ImageIndex(desc.Digest)
	require.NoError(t, err, "the nested index must be readable from the layout")
	nestedManifest, err := nested.IndexManifest()
	require.NoError(t, err)

	require.Len(t, nestedManifest.Manifests, 2, "both platform children must survive")
	platforms := []string{
		nestedManifest.Manifests[0].Platform.String(),
		nestedManifest.Manifests[1].Platform.String(),
	}
	assert.ElementsMatch(t, []string{"linux/amd64", "darwin/arm64"}, platforms)
	assert.Equal(t, "ZmFrZS1jb250cmFjdA==", nestedManifest.Annotations["contract"],
		"the top-level contract annotation must survive intact")

	meta, err := l.GetMeta("v1.0.0")
	require.NoError(t, err)
	require.NotNil(t, meta.GetDigest())
	assert.Equal(t, idxDigest.String(), meta.GetDigest().String())
	assert.Equal(t, "example.io/deckhouse-cli/plugins/foo@"+idxDigest.String(), meta.GetDigestReference())
}

// TestAddIndex_IdempotentForSameTagAndDigest: same guard as AddImage - a
// repeated call with the same (tag, digest) must not append a duplicate
// descriptor, or retried pulls would inflate the layout and the push.
func TestAddIndex_IdempotentForSameTagAndDigest(t *testing.T) {
	l, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	idx := buildMultiPlatformIndex(t, "ZmFrZS1jb250cmFjdA==")

	require.NoError(t, l.AddIndex(idx, "v1.0.0", "example.io/repo:v1.0.0"))
	require.NoError(t, l.AddIndex(idx, "v1.0.0", "example.io/repo:v1.0.0"),
		"second AddIndex with same tag+digest must be a no-op, not an error")

	assert.Equal(t, 1, indexDescriptorCount(t, l),
		"AddIndex must not append a second descriptor for the same (tag, digest)")
}

// TestCountManifestsMatching verifies that CountManifestsMatching counts only
// the descriptors whose short-tag annotation satisfies the predicate - the
// mechanism the summary uses to separate VEX attestations (".att" tags) from
// regular images that share the same layout.
func TestCountManifestsMatching(t *testing.T) {
	l, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	img := func(v string) v1.Image {
		return upfake.NewImageBuilder().WithFile("version.json", `{"v":"`+v+`"}`).MustBuild()
	}
	// Two regular images and one VEX attestation, all in the same layout.
	require.NoError(t, l.AddImage(wrapImage(t, img("a"), "example.io/repo:v1.0.0"), "v1.0.0"))
	require.NoError(t, l.AddImage(wrapImage(t, img("b"), "example.io/repo:v2.0.0"), "v2.0.0"))
	require.NoError(t, l.AddImage(wrapImage(t, img("c"), "example.io/repo:sha256-abc.att"), "sha256-abc.att"))

	paths := []layout.Path{l.Path()}

	assert.Equal(t, 3, regimage.CountManifests(paths),
		"CountManifests must count every manifest")

	vex := regimage.CountManifestsMatching(paths, func(a map[string]string) bool {
		return strings.HasSuffix(a[regimage.AnnotationImageShortTag], ".att")
	})
	assert.Equal(t, 1, vex, "only the .att manifest must match the VEX predicate")

	none := regimage.CountManifestsMatching(paths, func(map[string]string) bool { return false })
	assert.Equal(t, 0, none, "a never-true predicate matches nothing")
}
