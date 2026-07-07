// Copyright 2026 Flant JSC
// SPDX-License-Identifier: Apache-2.0

// Regression tests: a transient network failure during the blob download of
// one image, followed by a successful-looking retry, must not lose the image
// from the bundle (field incident "pulled 363 / pushed 362": the lost image
// surfaced only when the cluster hit "not found" on its digest; v1.76.2 EE,
// harbor.sdkp.ru report, 2026-07-02).
//
// The invariant under test (ImageLayout.AddImage in
// pkg/registry/image/layout.go): metaByTag, the (tag, digest) idempotency
// guard, is recorded only after AppendImage succeeds. Recording it before the
// write turns a failed first attempt into a lost image:
//
//  1. attempt 1: metaByTag[tag] is recorded, then AppendImage fails mid-write
//     (connection reset) -> the pull task errors and schedules a retry
//     ("failed, next retry in 10s" in the log);
//  2. attempt 2: AddImage sees metaByTag[tag] with the same digest and returns
//     nil WITHOUT writing anything -> the retry "succeeds" instantly;
//  3. index.json never receives the manifest -> the image is absent from
//     platform.tar -> push uploads N-1 images.

package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	localreg "github.com/deckhouse/deckhouse/pkg/registry"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// -----------------------------------------------------------------------------
// Flaky plumbing: injects a fixed number of transient blob-read failures.
// The failure surfaces inside layout.Path.WriteImage -> layer.Compressed(),
// which is exactly where a mid-download connection reset lands with a real
// (lazy) remote image.
// -----------------------------------------------------------------------------

type blobFailer struct {
	mu        sync.Mutex
	remaining int
	injected  int
}

func (b *blobFailer) failOnce() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.remaining > 0 {
		b.remaining--
		b.injected++

		return errors.New("simulated transient failure: read tcp 10.0.0.1:443: connection reset by peer")
	}

	return nil
}

type flakyLayer struct {
	v1.Layer
	failer *blobFailer
}

func (l *flakyLayer) Compressed() (io.ReadCloser, error) {
	if err := l.failer.failOnce(); err != nil {
		return nil, err
	}

	return l.Layer.Compressed()
}

type flakyV1Image struct {
	v1.Image
	failer *blobFailer
}

func (f *flakyV1Image) Layers() ([]v1.Layer, error) {
	layers, err := f.Image.Layers()
	if err != nil {
		return nil, err
	}

	wrapped := make([]v1.Layer, len(layers))
	for i, l := range layers {
		wrapped[i] = &flakyLayer{Layer: l, failer: f.failer}
	}

	return wrapped, nil
}

// flakyDkpImage satisfies localreg.Image (v1.Image + Extract).
type flakyDkpImage struct {
	v1.Image
	orig localreg.Image
}

func (f *flakyDkpImage) Extract() io.ReadCloser { return f.orig.Extract() }

// flakyClient decorates a registry client: images fetched by a reference that
// contains targetHex get flaky layers sharing one failure budget across
// retry attempts (the failer must survive re-fetching the image on retry).
type flakyClient struct {
	localreg.Client

	targetHex string
	failer    *blobFailer
}

func (c *flakyClient) WithSegment(segments ...string) localreg.Client {
	return &flakyClient{
		Client:    c.Client.WithSegment(segments...),
		targetHex: c.targetHex,
		failer:    c.failer,
	}
}

func (c *flakyClient) GetImage(ctx context.Context, ref string, opts ...localreg.ImageGetOption) (localreg.Image, error) {
	img, err := c.Client.GetImage(ctx, ref, opts...)
	if err != nil || !strings.Contains(ref, c.targetHex) {
		return img, err
	}

	return &flakyDkpImage{
		Image: &flakyV1Image{Image: img, failer: c.failer},
		orig:  img,
	}, nil
}

// -----------------------------------------------------------------------------
// Root-cause pin: ImageLayout.AddImage alone.
// -----------------------------------------------------------------------------

// TestRepro_AddImage_RetryAfterFailedWrite_ImageMustLand: attempt 1 fails
// mid-write, attempt 2 (same tag, same digest) succeeds. The image must be in
// the layout afterwards: the (tag, digest) guard counts a pair as done only
// after a successful AppendImage, so the retry re-runs the write.
func TestRepro_AddImage_RetryAfterFailedWrite_ImageMustLand(t *testing.T) {
	imgLayout, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	base := upfake.NewImageBuilder().
		WithFile("payload.txt", "flaky-image-payload").
		MustBuild()
	digest, err := base.Digest()
	require.NoError(t, err)

	failer := &blobFailer{remaining: 1}
	flaky := &flakyV1Image{Image: base, failer: failer}

	tag := digest.Hex // digest-pulled images are stored under their hex as short_tag
	regImg, err := regimage.NewImage(flaky)
	require.NoError(t, err)
	regImg.SetMetadata(&regimage.ImageMeta{
		TagReference:    "registry.example.com/deckhouse/ee@" + digest.String(),
		DigestReference: "registry.example.com/deckhouse/ee@" + digest.String(),
		Digest:          &digest,
	})

	// Attempt 1: blob write fails (transient network error). The pull task
	// treats this as retryable - "failed, next retry in 10s".
	err = imgLayout.AddImage(regImg, tag)
	require.Error(t, err, "attempt 1 must fail: the blob download was interrupted")
	t.Logf("attempt 1 failed as injected: %v", err)

	// Attempt 2: the retry. Same tag, same digest, network is healthy again.
	err = imgLayout.AddImage(regImg, tag)
	require.NoError(t, err, "attempt 2 (retry) reports success")

	got := regimage.CountManifests([]layout.Path{imgLayout.Path()})
	assert.Equal(t, 1, got,
		"BUG: AddImage reported success on retry but the image is not in the layout "+
			"(metaByTag was recorded before the failed write, so the retry became a silent no-op)")
}

// TestRepro_AddImage_NoFailure_Control: control run - without the injected
// failure the very same flow lands the image. Proves the harness itself does
// not lose images.
func TestRepro_AddImage_NoFailure_Control(t *testing.T) {
	imgLayout, err := regimage.NewImageLayout(t.TempDir())
	require.NoError(t, err)

	base := upfake.NewImageBuilder().
		WithFile("payload.txt", "healthy-image-payload").
		MustBuild()
	digest, err := base.Digest()
	require.NoError(t, err)

	regImg, err := regimage.NewImage(base)
	require.NoError(t, err)
	regImg.SetMetadata(&regimage.ImageMeta{
		TagReference:    "registry.example.com/deckhouse/ee@" + digest.String(),
		DigestReference: "registry.example.com/deckhouse/ee@" + digest.String(),
		Digest:          &digest,
	})

	require.NoError(t, imgLayout.AddImage(regImg, digest.Hex))
	assert.Equal(t, 1, regimage.CountManifests([]layout.Path{imgLayout.Path()}))
}

// -----------------------------------------------------------------------------
// End-to-end: PullService.Pull -> platform.tar -> PushService.Push,
// mirroring the exact field scenario (d8 mirror pull --deckhouse-tag v1.76.2
// --no-modules --no-security-db --no-installer, then d8 mirror push).
// -----------------------------------------------------------------------------

// TestRepro_MirrorPullThenPush_TransientBlobFailureLosesImage seeds a fake
// source registry with a v1.76.2-like platform whose images_digests.json
// references three digest images. The blob download of one of them ("beta")
// fails once and then recovers - the retry machinery reports success, the
// pull exits 0, but the pushed registry must still contain ALL three digests.
// A dropped "beta" reproduces the 363-pulled / 362-pushed mismatch from the
// field report.
func TestRepro_MirrorPullThenPush_TransientBlobFailureLosesImage(t *testing.T) {
	const (
		rootHost  = "registry.example.com/deckhouse/ee"
		targetTag = "v1.76.2"
	)

	payloads := map[string]v1.Image{
		"alpha": upfake.NewImageBuilder().WithFile("payload.txt", "payload-alpha").MustBuild(),
		"beta":  upfake.NewImageBuilder().WithFile("payload.txt", "payload-beta").MustBuild(),
		"gamma": upfake.NewImageBuilder().WithFile("payload.txt", "payload-gamma").MustBuild(),
	}

	digests := map[string]v1.Hash{}
	for name, img := range payloads {
		d, err := img.Digest()
		require.NoError(t, err)

		digests[name] = d
	}

	digestsJSON, err := json.Marshal(map[string]map[string]string{
		"common": {
			"alpha": digests["alpha"].String(),
			"beta":  digests["beta"].String(),
			"gamma": digests["gamma"].String(),
		},
	})
	require.NoError(t, err)

	reg := upfake.NewRegistry(rootHost)

	platformImg := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+targetTag+`"}`).
		WithFile("deckhouse/candi/images_digests.json", string(digestsJSON)).
		MustBuild()
	reg.MustAddImage("", targetTag, platformImg)
	reg.MustAddImage("install", targetTag, platformImg)
	reg.MustAddImage("install-standalone", targetTag, platformImg)
	reg.MustAddImage("release-channel", targetTag,
		upfake.NewImageBuilder().WithFile("version.json", `{"version":"`+targetTag+`"}`).MustBuild())

	// Payload images are stored under throwaway tags; the pull references them
	// by digest only (as images_digests.json entries).
	for name, img := range payloads {
		reg.MustAddImage("", "payload-"+name, img)
	}

	// "beta" loses its connection once mid-download, then recovers.
	failer := &blobFailer{remaining: 1}
	srcClient := &flakyClient{
		Client:    pkgclient.Adapt(upfake.NewClient(reg)),
		targetHex: digests["beta"].Hex,
		failer:    failer,
	}

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	// Info level: the raw test output carries the same "[N / M] Pulling ...",
	// "failed, next retry in 10s" and "[N / M] Pushing ..." lines as the field log.
	userLogger := log.NewSLogger(slog.LevelInfo)

	bundleDir := t.TempDir()
	regSvc := registryservice.NewService(srcClient, pkg.NoEdition, logger)
	pullSvc := NewPullService(regSvc, t.TempDir(), targetTag, &PullServiceOptions{
		BundleDir:     bundleDir,
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
		SkipVexImages: true,
	}, logger, userLogger)

	_, pullErr := pullSvc.Pull(context.Background())
	require.NoError(t, pullErr, "d8 mirror pull reports SUCCESS despite the transient failure")
	require.Equal(t, 1, failer.injected, "exactly one transient blob failure was injected")

	destReg := upfake.NewRegistry("harbor.example.com/deckhouse/ee")
	destClient := pkgclient.Adapt(upfake.NewClient(destReg))

	pushSvc := NewPushService(destClient, &PushServiceOptions{
		BundleDir:  bundleDir,
		WorkingDir: t.TempDir(),
	}, logger, userLogger)
	require.NoError(t, pushSvc.Push(context.Background()),
		"d8 mirror push must succeed")

	// Every digest the pull reported as successfully mirrored must exist in
	// the target registry. Digest images are pushed under their hex short_tag.
	for name, d := range digests {
		err := destClient.CheckImageExists(context.Background(), d.Hex)
		assert.NoErrorf(t, err,
			"BUG: payload %q (%s) was reported as pulled but is MISSING in the target registry "+
				"(the kubelet later fails with 'not found' on exactly this digest)", name, d)
	}
}
