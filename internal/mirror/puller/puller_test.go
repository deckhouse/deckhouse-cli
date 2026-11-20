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

package puller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/vex/vextest"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// TestPullImages_AllowMissingTagsDoesNotSwallowContextCancellation locks in
// the root-cause fix for the empty 5120-byte module tar bug.
//
// Pre-fix behavior: with AllowMissingTags=true, a context.Canceled returned
// by GetDigest was silently swallowed and the loop continued doing nothing
// useful for every remaining image. The caller then thought the pull
// succeeded, ran the pack phase, and produced empty stub tars for every
// module that had a layout pre-created but no images actually pulled.
//
// Post-fix behavior: context cancellation must propagate immediately so the
// caller knows to stop and never reaches the pack phase with empty state.
func TestPullImages_AllowMissingTagsDoesNotSwallowContextCancellation(t *testing.T) {
	gs := &fakeGetterService{
		getDigest: func(ctx context.Context, _ string) (*v1.Hash, error) {
			return nil, context.Canceled
		},
	}

	cfg := PullConfig{
		Name: "test",
		ImageSet: map[string]*ImageMeta{
			"reg.example/foo:v1": nil,
			"reg.example/bar:v1": nil,
		},
		AllowMissingTags: true, // the trapdoor that used to swallow cancellation
		GetterService:    gs,
	}

	ps := NewPullerService(dkplog.NewLogger(dkplog.WithLevel(slog.LevelError)), log.NewNop())
	err := ps.PullImages(context.Background(), cfg)
	require.ErrorIs(t, err, context.Canceled,
		"context.Canceled from GetDigest must propagate even with AllowMissingTags=true")
}

// TestPullImages_AlreadyCancelledContextBailsBeforeFirstCall ensures that we
// don't spend the user's retry budget on calls that are guaranteed to fail.
func TestPullImages_AlreadyCancelledContextBailsBeforeFirstCall(t *testing.T) {
	var calls atomic.Int32
	gs := &fakeGetterService{
		getDigest: func(ctx context.Context, _ string) (*v1.Hash, error) {
			calls.Add(1)
			return nil, errors.New("must not be called after ctx cancel")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := PullConfig{
		Name:             "test",
		ImageSet:         map[string]*ImageMeta{"reg.example/foo:v1": nil},
		AllowMissingTags: true,
		GetterService:    gs,
	}

	ps := NewPullerService(dkplog.NewLogger(dkplog.WithLevel(slog.LevelError)), log.NewNop())
	err := ps.PullImages(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(0), calls.Load(),
		"GetDigest must not be invoked once the context is already cancelled")
}

// TestPullImages_AllowMissingTagsStillSwallowsRealNotFound covers the
// happy-path use of AllowMissingTags: a genuine "tag not found" must still
// be silently skipped so callers can speculatively probe optional images.
func TestPullImages_AllowMissingTagsStillSwallowsRealNotFound(t *testing.T) {
	gs := &fakeGetterService{
		getDigest: func(ctx context.Context, _ string) (*v1.Hash, error) {
			return nil, errors.New("not found in registry")
		},
	}

	cfg := PullConfig{
		Name:             "test",
		ImageSet:         map[string]*ImageMeta{"reg.example/foo:v1": nil},
		AllowMissingTags: true,
		GetterService:    gs,
	}

	ps := NewPullerService(dkplog.NewLogger(dkplog.WithLevel(slog.LevelError)), log.NewNop())
	err := ps.PullImages(context.Background(), cfg)
	require.NoError(t, err, "AllowMissingTags must continue to swallow regular not-found errors")
}

// TestVerifyPlannedImagesLanded_ReportsMissingImages locks in the
// plan-vs-layout cross-check: a pull whose layout misses a planned image
// must fail loudly instead of producing an incomplete bundle.
func TestVerifyPlannedImagesLanded_ReportsMissingImages(t *testing.T) {
	imgLayout, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	written := upfake.NewImageBuilder().WithFile("payload.txt", "written").MustBuild()
	writtenDigest, err := written.Digest()
	require.NoError(t, err)

	writtenRef := "reg.example/repo@" + writtenDigest.String()
	regImg, err := regimage.NewImage(written)
	require.NoError(t, err)
	regImg.SetMetadata(&regimage.ImageMeta{
		TagReference:    writtenRef,
		DigestReference: writtenRef,
		Digest:          &writtenDigest,
	})
	require.NoError(t, imgLayout.AddImage(regImg, writtenDigest.Hex))

	lostDigest, err := v1.NewHash("sha256:ae6870cab389252be4cda8bfd752fc50ef85f6846da25d37faf638650a70a0b1")
	require.NoError(t, err)
	lostRef := "reg.example/repo@" + lostDigest.String()

	imageSet := map[string]*ImageMeta{
		writtenRef: NewImageMeta("v1", writtenRef, &writtenDigest),
		lostRef:    NewImageMeta("v1", lostRef, &lostDigest),
	}

	err = verifyPlannedImagesLanded(imageSet, imgLayout)
	require.Error(t, err, "layout misses a planned image, the check must fail")
	require.ErrorContains(t, err, lostRef)
	require.NotContains(t, err.Error(), writtenDigest.Hex)

	// Complete plan passes.
	delete(imageSet, lostRef)
	require.NoError(t, verifyPlannedImagesLanded(imageSet, imgLayout))
}

// TestPullImageSet_CompletePullPassesVerification guards the cross-check
// against false positives: a healthy pull of both digest- and tag-style
// references must not trip it.
func TestPullImageSet_CompletePullPassesVerification(t *testing.T) {
	imgLayout, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	fakeImg := upfake.NewImageBuilder().WithFile("payload.txt", "ok").MustBuild()
	digest, err := fakeImg.Digest()
	require.NoError(t, err)

	digestRef := "reg.example/repo@" + digest.String()
	tagRef := "reg.example/repo:v1.76.2"
	imageSet := map[string]*ImageMeta{
		digestRef: NewImageMeta("v1.76.2", digestRef, &digest),
		tagRef:    NewImageMeta("v1.76.2", tagRef, &digest),
	}

	getter := func(ctx context.Context, tag string, opts ...registry.ImageGetOption) (pkg.RegistryImage, error) {
		return regimage.NewImage(fakeImg)
	}

	ps := NewPullerService(dkplog.NewLogger(dkplog.WithLevel(slog.LevelError)), log.NewNop())
	require.NoError(t, ps.PullImageSet(context.Background(), imageSet, imgLayout, getter))
}

// TestPullIndexes_StoresReferrersIndexWhole covers the VEX referrers index: it
// must land in the layout byte for byte under its sha256-<hex> tag, with the
// artifactType and annotations of its descriptors, by which consumers find the
// attestation, and with the attestation artifact itself.
func TestPullIndexes_StoresReferrersIndexWhole(t *testing.T) {
	const payload = `{"payloadType":"application/vnd.in-toto+json"}`

	subject := upfake.NewImageBuilder().WithFile("payload.txt", "subject").MustBuild()
	subjectDigest, err := subject.Digest()
	require.NoError(t, err)

	idx, err := vextest.ReferrersIndex(subject, payload)
	require.NoError(t, err)
	idxDigest, err := idx.Digest()
	require.NoError(t, err)
	srcRaw, err := idx.RawManifest()
	require.NoError(t, err)

	tag := "sha256-" + subjectDigest.Hex
	reg := upfake.NewRegistry("reg.example")
	reg.MustAddIndex("repo", tag, idx)
	client := upfake.NewClient(reg).WithSegment("repo")

	imgLayout, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	ref := "reg.example/repo:" + tag
	cfg := PullConfig{
		Name:          "VEX indexes",
		ImageSet:      map[string]*ImageMeta{ref: nil},
		Layout:        imgLayout,
		GetterService: &fakeGetterService{getDigest: client.GetDigest, getIndex: client.GetIndex},
	}

	ps := NewPullerService(dkplog.NewLogger(dkplog.WithLevel(slog.LevelError)), log.NewNop())
	require.NoError(t, ps.PullIndexes(context.Background(), cfg))

	layoutIndex, err := imgLayout.Path().ImageIndex()
	require.NoError(t, err)
	layoutManifest, err := layoutIndex.IndexManifest()
	require.NoError(t, err)
	require.Len(t, layoutManifest.Manifests, 1)

	desc := layoutManifest.Manifests[0]
	assert.Equal(t, tag, desc.Annotations[regimage.AnnotationImageShortTag])
	assert.Equal(t, ref, desc.Annotations[regimage.AnnotationImageReferenceName])
	assert.Equal(t, types.OCIImageIndex, desc.MediaType, "stored as an index, not resolved to a platform image")
	assert.Equal(t, idxDigest, desc.Digest)

	stored, err := layoutIndex.ImageIndex(desc.Digest)
	require.NoError(t, err)
	storedRaw, err := stored.RawManifest()
	require.NoError(t, err)
	assert.Equal(t, string(srcRaw), string(storedRaw), "the index must be stored byte for byte")

	storedManifest, err := stored.IndexManifest()
	require.NoError(t, err)
	require.Len(t, storedManifest.Manifests, 1)

	artifactDesc := storedManifest.Manifests[0]
	assert.Equal(t, vextest.BundleMediaType, artifactDesc.ArtifactType)
	assert.Equal(t, vextest.OpenVEXPredicateType, artifactDesc.Annotations[vextest.PredicateTypeAnnotation])

	artifact, err := stored.Image(artifactDesc.Digest)
	require.NoError(t, err)
	artifactManifest, err := artifact.Manifest()
	require.NoError(t, err)
	require.NotNil(t, artifactManifest.Subject)
	assert.Equal(t, subjectDigest, artifactManifest.Subject.Digest)

	layers, err := artifact.Layers()
	require.NoError(t, err)
	require.Len(t, layers, 1)
	rc, err := layers[0].Compressed()
	require.NoError(t, err)
	defer rc.Close()
	storedPayload, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, payload, string(storedPayload), "the attestation blob must be in the layout")
}

// fakeGetterService is a tiny pkg.BasicService that lets each test rewire
// only the call it cares about.
type fakeGetterService struct {
	getDigest        func(ctx context.Context, tag string) (*v1.Hash, error)
	getImage         func(ctx context.Context, tag string, opts ...registry.ImageGetOption) (pkg.RegistryImage, error)
	getIndex         func(ctx context.Context, tag string) (v1.ImageIndex, error)
	checkImageExists func(ctx context.Context, tag string) error
	listTags         func(ctx context.Context) ([]string, error)
}

func (f *fakeGetterService) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	if f.getDigest != nil {
		return f.getDigest(ctx, tag)
	}
	return nil, errors.New("getDigest not implemented")
}

func (f *fakeGetterService) GetImage(ctx context.Context, tag string, opts ...registry.ImageGetOption) (pkg.RegistryImage, error) {
	if f.getImage != nil {
		return f.getImage(ctx, tag, opts...)
	}
	return nil, errors.New("getImage not implemented")
}

func (f *fakeGetterService) GetIndex(ctx context.Context, tag string) (v1.ImageIndex, error) {
	if f.getIndex != nil {
		return f.getIndex(ctx, tag)
	}
	return nil, errors.New("getIndex not implemented")
}

func (f *fakeGetterService) CheckImageExists(ctx context.Context, tag string) error {
	if f.checkImageExists != nil {
		return f.checkImageExists(ctx, tag)
	}
	return errors.New("checkImageExists not implemented")
}

func (f *fakeGetterService) ListTags(ctx context.Context) ([]string, error) {
	if f.listTags != nil {
		return f.listTags(ctx)
	}
	return nil, errors.New("listTags not implemented")
}
