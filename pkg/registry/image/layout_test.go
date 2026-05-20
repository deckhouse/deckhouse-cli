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
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
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
