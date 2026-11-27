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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
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

func TestPullPlatform_TaggedVEXInSubjectRepository(t *testing.T) {
	reg := upfake.NewRegistry(stubRootURL)
	release := upfake.NewImageBuilder().
		WithFile(imagesTagsFile, `{"common":{"controller":"component-tag"}}`).
		MustBuild()
	component := upfake.NewImageBuilder().WithFile("bin", "component").MustBuild()

	subjects := []struct {
		repo, tag, payload string
		image              v1.Image
	}{
		{"install", "main", `{"vex":"installer"}`, release},
		{"", "main", `{"vex":"deckhouse"}`, release},
		{"install-standalone", "main", `{"vex":"standalone"}`, release},
		{"", "component-tag", `{"vex":"component"}`, component},
	}

	for _, subject := range subjects {
		reg.MustAddImage(subject.repo, subject.tag, subject.image)
		idx, err := vextest.ReferrersIndex(subject.image, subject.payload)
		require.NoError(t, err)
		reg.MustAddIndex(subject.repo, "sha256-"+mustDigest(t, subject.image).Hex, idx)
	}

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	bundleDir := t.TempDir()
	svc := NewService(
		registryservice.NewService(pkgclient.Adapt(upfake.NewClient(reg)), pkg.NoEdition, logger),
		t.TempDir(), &Options{TargetTag: "main", BundleDir: bundleDir},
		logger, log.NewSLogger(slog.LevelWarn),
	)
	require.NoError(t, svc.PullPlatform(context.Background()))

	root := unpackPlatform(t, bundleDir)
	for _, subject := range subjects {
		t.Run(subject.repo+":"+subject.tag, func(t *testing.T) {
			repo := layout.Path(filepath.Join(string(root), subject.repo))
			tags := descriptorsByShortTag(t, repo)
			expected := mustDigest(t, subject.image)
			require.Equal(t, expected, tags[subject.tag].Digest)
			tag := "sha256-" + expected.Hex
			require.Contains(t, tags, tag)
			index, err := repo.ImageIndex()
			require.NoError(t, err)
			vexIndex, err := index.ImageIndex(tags[tag].Digest)
			require.NoError(t, err)
			manifest, err := vexIndex.IndexManifest()
			require.NoError(t, err)
			require.Len(t, manifest.Manifests, 1)
			child, err := vexIndex.Image(manifest.Manifests[0].Digest)
			require.NoError(t, err)
			attestation, err := child.Manifest()
			require.NoError(t, err)
			require.NotNil(t, attestation.Subject)
			require.Equal(t, expected, attestation.Subject.Digest)
			require.Len(t, attestation.Layers, 1)
			layer, err := child.LayerByDigest(attestation.Layers[0].Digest)
			require.NoError(t, err)
			reader, err := layer.Compressed()
			require.NoError(t, err)
			defer reader.Close()
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, subject.payload, string(got))
		})
	}
}

func TestPullPlatform_MultiPlatformInstallerWithVEX(t *testing.T) {
	for _, tc := range []struct {
		name          string
		parent, child bool
	}{
		{"parent only", true, false},
		{"selected child only", false, true},
		{"parent and selected child", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testMultiPlatformInstallerVEX(t, tc.parent, tc.child)
		})
	}
}

func testMultiPlatformInstallerVEX(t *testing.T, parentVEX, childVEX bool) {
	reg := upfake.NewRegistry(stubRootURL)
	component := upfake.NewImageBuilder().WithFile("bin", "component").MustBuild()
	reg.MustAddImage("", "component-tag", component)
	reg.MustAddImage("", "main", component)
	reg.MustAddImage("release-channel", "main", component)

	amd64 := upfake.NewImageBuilder().WithFile(imagesTagsFile, `{"common":{"controller":"component-tag"}}`).MustBuild()
	arm64 := upfake.NewImageBuilder().WithFile("bin", "arm64").MustBuild()
	installer := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: amd64, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}},
		mutate.IndexAddendum{Add: arm64, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "arm64"}}},
	)
	parentDigest := mustDigest(t, installer)
	reg.MustAddIndex("install", "main", installer)
	vexTag := "sha256-" + parentDigest.Hex
	referrers, err := vextest.ReferrersIndex(installer, `{"vex":"all platforms"}`)
	require.NoError(t, err)
	if parentVEX {
		reg.MustAddIndex("install", vexTag, referrers)
	}
	selected := mustDigest(t, amd64)
	childTag := "sha256-" + selected.Hex
	childIndex, err := vextest.ReferrersIndex(amd64, `{"vex":"linux/amd64"}`)
	require.NoError(t, err)
	childAtt := upfake.NewImageBuilder().WithFile("legacy", "linux/amd64").MustBuild()
	if childVEX {
		reg.MustAddIndex("install", childTag, childIndex)
		reg.MustAddImage("install", childTag+".att", childAtt)
	}

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	bundleDir := t.TempDir()
	svc := NewService(
		registryservice.NewService(pkgclient.Adapt(upfake.NewClient(reg)), pkg.NoEdition, logger),
		t.TempDir(), &Options{TargetTag: "main", BundleDir: bundleDir},
		logger, log.NewSLogger(slog.LevelWarn),
	)
	require.NoError(t, svc.PullPlatform(context.Background()))

	root := unpackPlatform(t, bundleDir)
	require.Equal(t, mustDigest(t, component), descriptorsByShortTag(t, root)["component-tag"].Digest)
	install := layout.Path(filepath.Join(string(root), "install"))
	tags := descriptorsByShortTag(t, install)
	index, err := install.ImageIndex()
	require.NoError(t, err)
	if childVEX {
		require.Equal(t, mustDigest(t, childIndex), tags[childTag].Digest)
		require.Equal(t, mustDigest(t, childAtt), tags[childTag+".att"].Digest)
		stored, err := index.ImageIndex(tags[childTag].Digest)
		require.NoError(t, err)
		refs, err := stored.IndexManifest()
		require.NoError(t, err)
		require.Len(t, refs.Manifests, 1)
		artifact, err := stored.Image(refs.Manifests[0].Digest)
		require.NoError(t, err)
		manifest, err := artifact.Manifest()
		require.NoError(t, err)
		require.NotNil(t, manifest.Subject)
		require.Equal(t, selected, manifest.Subject.Digest)
	}
	if !parentVEX {
		require.Equal(t, selected, tags["main"].Digest)
		require.Equal(t, selected, tags["stable"].Digest)
		require.NotContains(t, tags, vexTag)
		return
	}
	require.Equal(t, parentDigest, tags["main"].Digest)
	require.Equal(t, parentDigest, tags["stable"].Digest)
	require.Equal(t, mustDigest(t, referrers), tags[vexTag].Digest)
	parent, err := index.ImageIndex(parentDigest)
	require.NoError(t, err)
	for _, platformImage := range []v1.Image{amd64, arm64} {
		img, err := parent.Image(mustDigest(t, platformImage))
		require.NoError(t, err)
		_, err = img.RawConfigFile()
		require.NoError(t, err)
	}
}

func TestPullPlatform_MissingSelectedInstaller(t *testing.T) {
	reg := upfake.NewRegistry(stubRootURL)
	reg.MustAddImage("", "main", empty.Image)
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	svc := NewService(
		registryservice.NewService(pkgclient.Adapt(upfake.NewClient(reg)), pkg.NoEdition, logger),
		t.TempDir(), &Options{TargetTag: "main", BundleDir: t.TempDir()},
		logger, log.NewSLogger(slog.LevelWarn),
	)
	err := svc.PullPlatform(context.Background())
	require.ErrorContains(t, err, "install:main")
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
