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

package platform

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/vex"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/vex/vextest"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// TestPullPlatform_PullsVEXInBothFormats: the installer lists two component
// images by digest, A with VEX in both formats, B with the referrers index
// only. platform.tar must hold A's .att as an image and both indexes whole and
// byte for byte, next to the components; --skip-vex-images must drop them all.
func TestPullPlatform_PullsVEXInBothFormats(t *testing.T) {
	const ver = "v1.73.5"

	componentA := upfake.NewImageBuilder().WithFile("bin", "a").MustBuild()
	componentB := upfake.NewImageBuilder().WithFile("bin", "b").MustBuild()
	digestA, digestB := mustDigest(t, componentA), mustDigest(t, componentB)

	legacyA, err := vextest.LegacyAttestation(`{"vex":"a-legacy"}`)
	require.NoError(t, err)
	indexA, err := vextest.ReferrersIndex(componentA, `{"vex":"a"}`)
	require.NoError(t, err)
	indexB, err := vextest.ReferrersIndex(componentB, `{"vex":"b"}`)
	require.NoError(t, err)

	reg := upfake.NewRegistry(stubRootURL)

	release := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+ver+`"}`).
		WithFile("deckhouse/candi/images_digests.json",
			fmt.Sprintf(`{"common":{"a":%q,"b":%q}}`, digestA, digestB)).
		MustBuild()
	for _, tag := range []string{"lts", ver} {
		reg.MustAddImage("release-channel", tag, release)
		reg.MustAddImage("", tag, release)
		reg.MustAddImage("install", tag, release)
		reg.MustAddImage("install-standalone", tag, release)
	}

	// The fake resolves a digest only for an image stored under some tag.
	reg.MustAddImage("", digestA.Hex, componentA)
	reg.MustAddImage("", digestB.Hex, componentB)
	reg.MustAddImage("", "sha256-"+digestA.Hex+".att", legacyA)
	reg.MustAddIndex("", "sha256-"+digestA.Hex, indexA)
	reg.MustAddIndex("", "sha256-"+digestB.Hex, indexB)

	for _, skipVex := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip VEX %t", skipVex), func(t *testing.T) {
			logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
			userLogger := log.NewSLogger(slog.LevelWarn)
			bundleDir := t.TempDir()

			svc := NewService(
				registryservice.NewService(pkgclient.Adapt(upfake.NewClient(reg)), pkg.NoEdition, logger),
				t.TempDir(),
				&Options{TargetTag: "lts", BundleDir: bundleDir, SkipVexImages: skipVex},
				logger,
				userLogger,
			)
			require.NoError(t, svc.PullPlatform(context.Background()))

			root := unpackPlatform(t, bundleDir)
			byTag := descriptorsByShortTag(t, root)

			assert.Contains(t, byTag, digestA.Hex, "component A must be pulled")
			assert.Contains(t, byTag, digestB.Hex, "component B must be pulled")

			if skipVex {
				for tag := range byTag {
					assert.Falsef(t, vex.IsTag(tag), "--skip-vex-images must not pull %s", tag)
				}

				return
			}

			legacyDesc, ok := byTag["sha256-"+digestA.Hex+".att"]
			require.True(t, ok, "the legacy .att of A must be pulled")
			assert.True(t, legacyDesc.MediaType.IsImage())
			assert.Equal(t, mustDigest(t, legacyA), legacyDesc.Digest)

			assert.NotContains(t, byTag, "sha256-"+digestB.Hex+".att", "B has no legacy attestation")

			for _, tc := range []struct {
				subject v1.Hash
				index   v1.ImageIndex
			}{{digestA, indexA}, {digestB, indexB}} {
				desc, ok := byTag["sha256-"+tc.subject.Hex]
				require.Truef(t, ok, "the referrers index of %s must be pulled", tc.subject)
				assert.True(t, desc.MediaType.IsIndex(), "a referrers index is stored as an index")
				assert.Equal(t, mustDigest(t, tc.index), desc.Digest)

				layoutIndex, err := root.ImageIndex()
				require.NoError(t, err)
				stored, err := layoutIndex.ImageIndex(desc.Digest)
				require.NoError(t, err)
				storedRaw, err := stored.RawManifest()
				require.NoError(t, err)
				srcRaw, err := tc.index.RawManifest()
				require.NoError(t, err)
				assert.Equal(t, string(srcRaw), string(storedRaw))

				storedManifest, err := stored.IndexManifest()
				require.NoError(t, err)
				require.Len(t, storedManifest.Manifests, 1)
				_, err = stored.Image(storedManifest.Manifests[0].Digest)
				assert.NoError(t, err, "the attestation artifact must be in the bundle")
			}
		})
	}
}

func unpackPlatform(t *testing.T, bundleDir string) layout.Path {
	t.Helper()

	tarFile, err := os.Open(filepath.Join(bundleDir, "platform.tar"))
	require.NoError(t, err)
	defer tarFile.Close()

	unpackDir := t.TempDir()
	require.NoError(t, bundle.Unpack(context.Background(), tarFile, unpackDir, "platform"))

	return layout.Path(unpackDir)
}

func descriptorsByShortTag(t *testing.T, l layout.Path) map[string]v1.Descriptor {
	t.Helper()

	index, err := l.ImageIndex()
	require.NoError(t, err)
	manifest, err := index.IndexManifest()
	require.NoError(t, err)

	byTag := make(map[string]v1.Descriptor, len(manifest.Manifests))
	for _, desc := range manifest.Manifests {
		byTag[desc.Annotations[regimage.AnnotationImageShortTag]] = desc
	}

	return byTag
}

type digester interface {
	Digest() (v1.Hash, error)
}

func mustDigest(t *testing.T, d digester) v1.Hash {
	t.Helper()

	h, err := d.Digest()
	require.NoError(t, err)

	return h
}
