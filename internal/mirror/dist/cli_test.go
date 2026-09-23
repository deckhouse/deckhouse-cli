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

package dist

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// cliRepo is where the d8 binary lives in the fake registry: the bare root,
// outside any edition segment.
const cliRepo = "deckhouse-cli"

// addCLIVersion publishes one deckhouse-cli release in the fake registry.
func addCLIVersion(t *testing.T, reg *upfake.Registry, tag string) {
	t.Helper()

	reg.MustAddImage(cliRepo, tag, upfake.NewImageBuilder().WithFile("d8", "binary-"+tag).MustBuild())
}

// shortTags returns the tags an OCI index.json records for its manifests.
func shortTags(t *testing.T, indexJSON []byte) []string {
	t.Helper()

	var index struct {
		Manifests []struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"manifests"`
	}
	require.NoError(t, json.Unmarshal(indexJSON, &index))

	tags := make([]string, 0, len(index.Manifests))
	for _, m := range index.Manifests {
		tags = append(tags, m.Annotations["io.deckhouse.image.short_tag"])
	}

	return tags
}

// TestPullCLI_PicksNewestStable is the phase's happy path: of everything
// published, exactly one version - the newest stable - lands in
// deckhouse-cli.tar under the deckhouse-cli/ prefix mirror push uploads
// verbatim.
func TestPullCLI_PicksNewestStable(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addCLIVersion(t, reg, "v0.13.0")
	addCLIVersion(t, reg, "v0.13.1")
	// Neither a release nor a version: both must be ignored by the selection.
	addCLIVersion(t, reg, "v0.14.0-rc.1")
	addCLIVersion(t, reg, "meta-123456")

	bundleDir := t.TempDir()
	svc := newPhaseService(t, reg, &Options{BundleDir: bundleDir})

	require.NoError(t, svc.PullCLI(context.Background()))

	tarPath := filepath.Join(bundleDir, "deckhouse-cli.tar")
	require.FileExists(t, tarPath)

	entries, indexJSON := readBundleTar(t, tarPath)
	require.NotEmpty(t, entries)

	for _, entry := range entries {
		assert.Truef(t, strings.HasPrefix(entry, "deckhouse-cli/"),
			"every tar entry must carry the registry prefix, got %q", entry)
		assert.Falsef(t, strings.HasPrefix(entry, "deckhouse-cli/plugins/"),
			"the binary must not land in the plugins catalog, got %q", entry)
	}

	assert.Equal(t, []string{"v0.13.1"}, shortTags(t, indexJSON),
		"only the newest stable version belongs in the bundle")

	stats := svc.CLIStats()
	assert.True(t, stats.Attempted)
	assert.Equal(t, "v0.13.1", stats.Version)
	assert.Equal(t, 1, stats.Images)
	assert.Empty(t, stats.SkipReason)
}

// TestPullCLI_PinnedTag: --deckhouse-cli-tag mirrors that exact version, newer
// published ones notwithstanding.
func TestPullCLI_PinnedTag(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addCLIVersion(t, reg, "v0.13.0")
	addCLIVersion(t, reg, "v0.13.1")

	bundleDir := t.TempDir()
	svc := newPhaseService(t, reg, &Options{BundleDir: bundleDir, CLITag: "v0.13.0"})

	require.NoError(t, svc.PullCLI(context.Background()))

	_, indexJSON := readBundleTar(t, filepath.Join(bundleDir, "deckhouse-cli.tar"))
	assert.Equal(t, []string{"v0.13.0"}, shortTags(t, indexJSON))
	assert.Equal(t, "v0.13.0", svc.CLIStats().Version)
}

// TestPullCLI_PinnedTagMissingFails: an explicit request that cannot be met is
// an error, not a warning - the user asked for a version by name.
func TestPullCLI_PinnedTagMissingFails(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addCLIVersion(t, reg, "v0.13.1")

	bundleDir := t.TempDir()
	svc := newPhaseService(t, reg, &Options{BundleDir: bundleDir, CLITag: "v9.9.9"})

	err := svc.PullCLI(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "v9.9.9")

	files, readErr := os.ReadDir(bundleDir)
	require.NoError(t, readErr)
	assert.Empty(t, files, "a failed pin must leave no bundle file behind")
}

// TestPullCLI_NoRepositoryIsWarnedNotFailed: registries that publish no
// deckhouse-cli repository at all (the public one today) must not break a
// pull - the rest of the bundle is still worth building.
func TestPullCLI_NoRepositoryIsWarnedNotFailed(t *testing.T) {
	reg := upfake.NewRegistry(testHost)

	bundleDir := t.TempDir()
	svc := newPhaseService(t, reg, &Options{BundleDir: bundleDir})

	require.NoError(t, svc.PullCLI(context.Background()))

	files, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, files)

	stats := svc.CLIStats()
	assert.True(t, stats.Attempted)
	assert.Empty(t, stats.Version)
	assert.NotEmpty(t, stats.SkipReason, "the summary must say why no CLI was mirrored")
}

// TestPullCLI_NoStableVersions: a repository holding only pre-releases and
// build junk mirrors nothing, with a reason.
func TestPullCLI_NoStableVersions(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addCLIVersion(t, reg, "v0.14.0-rc.1")
	addCLIVersion(t, reg, "meta-123456")

	svc := newPhaseService(t, reg, &Options{BundleDir: t.TempDir()})

	require.NoError(t, svc.PullCLI(context.Background()))

	stats := svc.CLIStats()
	assert.Empty(t, stats.Version)
	assert.Contains(t, stats.SkipReason, "no published deckhouse-cli versions")
}

// TestPullCLI_ProxyRegistry: a proxy registry serves no tag listing, so
// automatic selection is impossible - but an exact pin still resolves, because
// it addresses a manifest by tag.
func TestPullCLI_ProxyRegistry(t *testing.T) {
	t.Run("without a pin the phase is skipped with a reason", func(t *testing.T) {
		reg := upfake.NewRegistry(testHost)
		addCLIVersion(t, reg, "v0.13.1")

		svc := newPhaseService(t, reg, &Options{BundleDir: t.TempDir(), ProxyRegistry: true})

		require.NoError(t, svc.PullCLI(context.Background()))
		assert.Contains(t, svc.CLIStats().SkipReason, "--deckhouse-cli-tag")
	})

	t.Run("a pinned version is mirrored", func(t *testing.T) {
		reg := upfake.NewRegistry(testHost)
		addCLIVersion(t, reg, "v0.13.1")

		bundleDir := t.TempDir()
		svc := newPhaseService(t, reg, &Options{BundleDir: bundleDir, ProxyRegistry: true, CLITag: "v0.13.1"})

		require.NoError(t, svc.PullCLI(context.Background()))
		require.FileExists(t, filepath.Join(bundleDir, "deckhouse-cli.tar"))
		assert.Equal(t, "v0.13.1", svc.CLIStats().Version)
	})
}

// TestPullCLI_DryRun records the version that would be mirrored without
// writing anything.
func TestPullCLI_DryRun(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addCLIVersion(t, reg, "v0.13.1")

	bundleDir := t.TempDir()
	svc := newPhaseService(t, reg, &Options{BundleDir: bundleDir, DryRun: true})

	require.NoError(t, svc.PullCLI(context.Background()))

	files, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, files, "dry-run must not write bundle files")

	stats := svc.CLIStats()
	assert.Equal(t, "v0.13.1", stats.Version)
	assert.Zero(t, stats.Images, "dry-run pulls no images")
}

// TestPullCLI_MultiPlatformIndexPreserved is the shape the real registry
// publishes: a multi-platform index whose children include an attestation
// manifest. All of it must survive resolve -> pull -> pack, because the
// registry-packages-proxy picks the viewer's platform out of that index.
func TestPullCLI_MultiPlatformIndexPreserved(t *testing.T) {
	srv := httptest.NewServer(ggcrregistry.New())
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	linuxImg := upfake.NewImageBuilder().WithFile("d8", "linux-bin").MustBuild()
	darwinImg := upfake.NewImageBuilder().WithFile("d8", "darwin-bin").MustBuild()
	attestation := upfake.NewImageBuilder().WithFile("attestation", "{}").MustBuild()

	source := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: linuxImg, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}},
		mutate.IndexAddendum{Add: darwinImg, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "darwin", Architecture: "arm64"}}},
		mutate.IndexAddendum{Add: attestation, Descriptor: v1.Descriptor{
			Platform:    &v1.Platform{OS: "unknown", Architecture: "unknown"},
			Annotations: map[string]string{"vnd.docker.reference.type": "attestation-manifest"},
		}},
	)

	sourceDigest, err := source.Digest()
	require.NoError(t, err)

	ref, err := name.ParseReference(host+"/deckhouse-cli:v0.13.1", name.Insecure)
	require.NoError(t, err)
	require.NoError(t, remote.WriteIndex(ref, source))

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	regSvc := registryservice.NewService(pkgclient.NewFromOptions(host, regclient.WithInsecure(true)), pkg.NoEdition, logger)

	bundleDir := t.TempDir()
	svc := NewService(regSvc, t.TempDir(), &Options{BundleDir: bundleDir}, logger, log.NewSLogger(slog.LevelWarn))

	require.NoError(t, svc.PullCLI(context.Background()))

	extracted := t.TempDir()
	untarTo(t, filepath.Join(bundleDir, "deckhouse-cli.tar"), extracted)

	layoutPath := layout.Path(filepath.Join(extracted, "deckhouse-cli"))
	topIndex, err := layoutPath.ImageIndex()
	require.NoError(t, err)
	topManifest, err := topIndex.IndexManifest()
	require.NoError(t, err)

	require.Len(t, topManifest.Manifests, 1)
	desc := topManifest.Manifests[0]
	assert.True(t, desc.MediaType.IsIndex(), "the CLI release must stay an index in the bundle")
	assert.Equal(t, "v0.13.1", desc.Annotations["io.deckhouse.image.short_tag"])
	assert.Equal(t, sourceDigest, desc.Digest, "the index must reach the bundle byte-identical")

	nested, err := topIndex.ImageIndex(desc.Digest)
	require.NoError(t, err)
	nestedManifest, err := nested.IndexManifest()
	require.NoError(t, err)

	require.Len(t, nestedManifest.Manifests, 3, "both platforms and the attestation must survive")
	assert.Equal(t, "attestation-manifest",
		nestedManifest.Manifests[2].Annotations["vnd.docker.reference.type"],
		"the attestation child keeps its annotations")
}
