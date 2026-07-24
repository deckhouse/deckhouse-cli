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

package modules

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

// =============================================================================
// Tests: extra images discovery (findExtraImages)
// =============================================================================

const extraImagesTag = "v1.0.0"

// A version either declares extra images or it doesn't - both are normal, and
// neither is an error. A missing version image is treated the same way.
func TestFindExtraImages_Discovery(t *testing.T) {
	cases := []struct {
		name     string
		image    v1.Image            // placed at modules/<name>:<tag>; nil = don't add (GetImage -> not found)
		wantRefs map[string][]string // extra-name -> tags
	}{
		{
			name:  "version with extra_images.json yields extra images",
			image: extraImagesImage(`{"scanner":"v1.2.3","enforcer":"v4.5.6"}`),
			wantRefs: map[string][]string{
				"scanner":  {"v1.2.3"},
				"enforcer": {"v4.5.6"},
			},
		},
		{
			name:     "version without extra_images.json is skipped",
			image:    versionImage(extraImagesTag), // carries only version.json
			wantRefs: map[string][]string{},
		},
		{
			name:     "missing module version image is skipped",
			image:    nil,
			wantRefs: map[string][]string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := upfake.NewRegistry(testHost)
			if tc.image != nil {
				reg.MustAddImage("modules/"+testModuleName, extraImagesTag, tc.image)
			}
			svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)

			got, err := svc.findExtraImages(context.Background(), testModuleName, []string{extraImagesTag})

			require.NoError(t, err)
			assertExtraImageRefs(t, tc.wantRefs, got)
		})
	}
}

// A transient registry error during discovery must be retried, not swallowed:
// the version's extra images still land once the registry recovers.
func TestFindExtraImages_RetriesTransientError(t *testing.T) {
	setShortRetryDelay(t)

	reg := upfake.NewRegistry(testHost)
	reg.MustAddImage("modules/"+testModuleName, extraImagesTag, extraImagesImage(`{"scanner":"v1.2.3"}`))

	// Fail the first two GetImage calls, then let the third pass through.
	client := newGetImageErrClient(upfake.NewClient(reg), errors.New("simulated registry 503"), 2)
	svc := newService(t, pkgclient.Adapt(client), nil)

	got, err := svc.findExtraImages(context.Background(), testModuleName, []string{extraImagesTag})

	require.NoError(t, err)
	assertExtraImageRefs(t, map[string][]string{"scanner": {"v1.2.3"}}, got)
	assert.Equal(t, int64(3), client.calls.Load(), "two failed attempts then one success")
}

// A persistent registry error must fail the pull loudly instead of silently
// dropping the version's extra images from the bundle.
func TestFindExtraImages_FailsOnPersistentError(t *testing.T) {
	setShortRetryDelay(t)

	transientErr := errors.New("simulated registry 503")

	reg := upfake.NewRegistry(testHost)
	reg.MustAddImage("modules/"+testModuleName, extraImagesTag, extraImagesImage(`{"scanner":"v1.2.3"}`))

	// Fail every attempt.
	client := newGetImageErrClient(upfake.NewClient(reg), transientErr, int(extraImagesFetchRetries))
	svc := newService(t, pkgclient.Adapt(client), nil)

	_, err := svc.findExtraImages(context.Background(), testModuleName, []string{extraImagesTag})

	require.Error(t, err)
	assert.ErrorIs(t, err, transientErr)
	assert.Equal(t, int64(extraImagesFetchRetries), client.calls.Load(), "all retry attempts must be spent")
}

// =============================================================================
// Helpers
// =============================================================================

// extraImagesImage builds a v1.Image whose flattened tar carries extra_images.json.
func extraImagesImage(extraImagesJSON string) v1.Image {
	return upfake.NewImageBuilder().
		WithFile("extra_images.json", extraImagesJSON).
		MustBuild()
}

// setShortRetryDelay shrinks the discovery retry delay for the duration of a
// test so the retry paths don't wait the production 10s between attempts.
func setShortRetryDelay(t *testing.T) {
	t.Helper()
	restore := extraImagesFetchRetryDelay
	extraImagesFetchRetryDelay = time.Millisecond
	t.Cleanup(func() { extraImagesFetchRetryDelay = restore })
}

// assertExtraImageRefs checks the discovered map against want (extra-name -> tags),
// and that each entry carries the expected Name and full registry ref.
func assertExtraImageRefs(t *testing.T, want map[string][]string, got map[string][]extraImageInfo) {
	t.Helper()

	require.Len(t, got, len(want), "extra image group count mismatch: got %v", got)

	for name, tags := range want {
		infos, ok := got[name]
		require.True(t, ok, "missing extra image group %q", name)

		gotTags := make([]string, 0, len(infos))
		for _, info := range infos {
			assert.Equal(t, name, info.Name)
			wantRef := testHost + "/modules/" + testModuleName + "/extra/" + name + ":" + info.Tag
			assert.Equal(t, wantRef, info.FullRef)
			gotTags = append(gotTags, info.Tag)
		}

		assert.ElementsMatch(t, tags, gotTags, "tags for extra image %q", name)
	}
}

// =============================================================================
// Test doubles
// =============================================================================

// getImageErrClient returns a configured error from the first failFirst GetImage
// calls, then delegates. The counter is shared across the WithSegment chain so
// it tallies every GetImage regardless of the client's path.
type getImageErrClient struct {
	dkpreg.Client
	err       error
	failFirst int
	calls     *atomic.Int64
}

func newGetImageErrClient(c dkpreg.Client, err error, failFirst int) *getImageErrClient {
	return &getImageErrClient{Client: c, err: err, failFirst: failFirst, calls: new(atomic.Int64)}
}

func (c *getImageErrClient) WithSegment(segments ...string) dkpreg.Client {
	return &getImageErrClient{
		Client:    c.Client.WithSegment(segments...),
		err:       c.err,
		failFirst: c.failFirst,
		calls:     c.calls,
	}
}

func (c *getImageErrClient) GetImage(ctx context.Context, tag string, opts ...dkpreg.ImageGetOption) (dkpreg.Image, error) {
	if c.calls.Add(1) <= int64(c.failFirst) {
		return nil, c.err
	}

	return c.Client.GetImage(ctx, tag, opts...)
}
