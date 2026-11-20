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

package mirror

import (
	"context"
	"fmt"
	"io"
	golog "log"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/vex/vextest"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

// TestPullPush_VEXRoundTrip mirrors a module whose images carry VEX in both
// formats delivery-kit has used: image A has a legacy sha256-<hex>.att and a
// referrers index sha256-<hex>, image B only the index. After pull and push,
// the target must hold the .att, the indexes byte for byte, and attestations
// that the referrers API (what cosign and trivy query) finds by artifactType,
// whether or not the target registry implements that API.
func TestPullPush_VEXRoundTrip(t *testing.T) {
	const module = "console"

	internalA := upfake.NewImageBuilder().WithFile("bin", "a").MustBuild()
	internalB := upfake.NewImageBuilder().WithFile("bin", "b").MustBuild()
	digestA, digestB := mustDigest(t, internalA), mustDigest(t, internalB)

	legacyA, err := vextest.LegacyAttestation(`{"vex":"a-legacy"}`)
	require.NoError(t, err)
	indexA, err := vextest.ReferrersIndex(internalA, `{"vex":"a"}`)
	require.NoError(t, err)
	indexB, err := vextest.ReferrersIndex(internalB, `{"vex":"b"}`)
	require.NoError(t, err)

	version := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.0.0"}`).
		WithFile("images_digests.json", fmt.Sprintf(`{"a":%q,"b":%q}`, digestA, digestB)).
		MustBuild()

	repo := "modules/" + module
	reg := upfake.NewRegistry(pullStubRootURL)
	reg.MustAddImage("modules", module, version)
	reg.MustAddImage(repo, "v1.0.0", version)
	reg.MustAddImage(repo+"/release", "stable", version)
	reg.MustAddImage(repo+"/release", "v1.0.0", version)
	// The fake resolves a digest only for an image stored under some tag.
	reg.MustAddImage(repo, digestA.Hex, internalA)
	reg.MustAddImage(repo, digestB.Hex, internalB)
	reg.MustAddImage(repo, "sha256-"+digestA.Hex+".att", legacyA)
	reg.MustAddIndex(repo, "sha256-"+digestA.Hex, indexA)
	reg.MustAddIndex(repo, "sha256-"+digestB.Hex, indexB)

	bundleDir := t.TempDir()
	pullSvc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		BundleDir:     bundleDir,
		SkipPlatform:  true,
		SkipSecurity:  true,
		SkipInstaller: true,
		SkipPackages:  true,
	})

	summary, err := pullSvc.Pull(context.Background())
	require.NoError(t, err)
	require.Len(t, summary.Modules.Modules, 1)
	assert.Equal(t, 3, summary.Modules.TotalVEX, "A's .att and index, and B's index")

	for _, referrersAPI := range []bool{false, true} {
		t.Run(fmt.Sprintf("referrers API %t", referrersAPI), func(t *testing.T) {
			srv := httptest.NewServer(ggcrregistry.New(
				ggcrregistry.WithReferrersSupport(referrersAPI),
				ggcrregistry.Logger(golog.New(io.Discard, "", 0)),
			))
			t.Cleanup(srv.Close)

			target := strings.TrimPrefix(srv.URL, "http://") + "/deckhouse/ee"
			pushSvc := NewPushService(
				pkgclient.NewFromOptions(target, regclient.WithInsecure(true)),
				&PushServiceOptions{
					Packages:   []string{filepath.Join(bundleDir, "module-"+module+".tar")},
					WorkingDir: t.TempDir(),
				},
				pullSvc.logger, pullSvc.userLogger,
			)

			_, err := pushSvc.Push(context.Background())
			require.NoError(t, err)

			targetRepo := target + "/" + repo

			legacyDesc := mustGet(t, targetRepo+":sha256-"+digestA.Hex+".att")
			assert.Equal(t, mustDigest(t, legacyA), legacyDesc.Digest)

			for _, tc := range []struct {
				subject v1.Hash
				index   v1.ImageIndex
			}{{digestA, indexA}, {digestB, indexB}} {
				indexDesc := mustGet(t, targetRepo+":sha256-"+tc.subject.Hex)
				assert.True(t, indexDesc.MediaType.IsIndex())
				assert.Equal(t, mustDigest(t, tc.index), indexDesc.Digest, "the index must arrive byte for byte")

				srcManifest, err := tc.index.IndexManifest()
				require.NoError(t, err)

				subject, err := name.NewDigest(targetRepo+"@"+tc.subject.String(), name.Insecure)
				require.NoError(t, err)

				referrers, err := remote.Referrers(subject, remote.WithFilter("artifactType", vextest.BundleMediaType))
				require.NoError(t, err)
				referrersManifest, err := referrers.IndexManifest()
				require.NoError(t, err)

				require.Len(t, referrersManifest.Manifests, 1, "the attestation must be found by artifactType")
				assert.Equal(t, srcManifest.Manifests[0].Digest, referrersManifest.Manifests[0].Digest)

				mustGet(t, targetRepo+"@"+tc.subject.String())
				mustGet(t, targetRepo+"@"+srcManifest.Manifests[0].Digest.String())
			}
		})
	}
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

func mustGet(t *testing.T, reference string) *remote.Descriptor {
	t.Helper()

	ref, err := name.ParseReference(reference, name.Insecure)
	require.NoError(t, err)

	desc, err := remote.Get(ref)
	require.NoErrorf(t, err, "%s must exist in the target", reference)

	return desc
}
