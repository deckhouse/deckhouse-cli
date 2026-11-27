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
	"bytes"
	"context"
	"fmt"
	"io"
	golog "log"
	"net/http"
	"net/http/httptest"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	ggcrregistry "github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	regclient "github.com/deckhouse/deckhouse/pkg/registry/client"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/vex/vextest"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

// TestPullPush_VEXRoundTrip checks legacy and indexed attestations of module
// images and the indexed attestation of a multi-platform installer.
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

	amd64 := upfake.NewImageBuilder().WithFile("deckhouse/candi/images_digests.json", "{}").MustBuild()
	arm64 := upfake.NewImageBuilder().WithFile("bin", "arm64").MustBuild()
	installer := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: amd64, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}},
		mutate.IndexAddendum{Add: arm64, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "arm64"}}},
	)
	digestInstaller := mustDigest(t, installer)
	indexInstaller, err := vextest.ReferrersIndex(installer, `{"vex":"installer"}`)
	require.NoError(t, err)
	reg.MustAddImage("", "main", version)
	reg.MustAddImage("release-channel", "main", version)
	reg.MustAddIndex("install", "main", installer)
	reg.MustAddIndex("install", "sha256-"+digestInstaller.Hex, indexInstaller)

	bundleDir := t.TempDir()
	pullSvc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "main", &PullServiceOptions{
		BundleDir:     bundleDir,
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
			srv := httptest.NewServer(requirePublishedSubject(ggcrregistry.New(
				ggcrregistry.WithReferrersSupport(referrersAPI),
				ggcrregistry.Logger(golog.New(io.Discard, "", 0)),
			)))
			t.Cleanup(srv.Close)

			target := strings.TrimPrefix(srv.URL, "http://") + "/deckhouse/ee"
			pushSvc := NewPushService(
				pkgclient.NewFromOptions(target, regclient.WithInsecure(true)),
				&PushServiceOptions{
					Packages: []string{
						filepath.Join(bundleDir, "platform.tar"),
						filepath.Join(bundleDir, "module-"+module+".tar"),
					},
					WorkingDir: t.TempDir(),
				},
				pullSvc.logger, pullSvc.userLogger,
			)

			_, err := pushSvc.Push(context.Background())
			require.NoError(t, err)

			moduleRepo := target + "/" + repo
			installRepo := target + "/install"
			assert.Equal(t, digestInstaller, mustGet(t, installRepo+":main").Digest)

			legacyDesc := mustGet(t, moduleRepo+":sha256-"+digestA.Hex+".att")
			assert.Equal(t, mustDigest(t, legacyA), legacyDesc.Digest)

			for _, tc := range []struct {
				repo    string
				subject v1.Hash
				index   v1.ImageIndex
				payload string
			}{
				{moduleRepo, digestA, indexA, `{"vex":"a"}`},
				{moduleRepo, digestB, indexB, `{"vex":"b"}`},
				{installRepo, digestInstaller, indexInstaller, `{"vex":"installer"}`},
			} {
				indexDesc := mustGet(t, tc.repo+":sha256-"+tc.subject.Hex)
				assert.True(t, indexDesc.MediaType.IsIndex())
				assert.Equal(t, mustDigest(t, tc.index), indexDesc.Digest, "the index must arrive byte for byte")

				srcManifest, err := tc.index.IndexManifest()
				require.NoError(t, err)

				subject, err := name.NewDigest(tc.repo+"@"+tc.subject.String(), name.Insecure)
				require.NoError(t, err)

				referrers, err := remote.Referrers(subject, remote.WithFilter("artifactType", vextest.BundleMediaType))
				require.NoError(t, err)
				referrersManifest, err := referrers.IndexManifest()
				require.NoError(t, err)

				require.Len(t, referrersManifest.Manifests, 1, "the attestation must be found by artifactType")
				assert.Equal(t, srcManifest.Manifests[0].Digest, referrersManifest.Manifests[0].Digest)

				mustGet(t, tc.repo+"@"+tc.subject.String())
				att, err := mustGet(t, tc.repo+"@"+srcManifest.Manifests[0].Digest.String()).Image()
				require.NoError(t, err)
				manifest, err := att.Manifest()
				require.NoError(t, err)
				require.NotNil(t, manifest.Subject)
				assert.Equal(t, tc.subject, manifest.Subject.Digest)

				layers, err := att.Layers()
				require.NoError(t, err)
				require.Len(t, layers, 1)
				layer, err := layers[0].Compressed()
				require.NoError(t, err)
				payload, err := io.ReadAll(layer)
				require.NoError(t, layer.Close())
				require.NoError(t, err)
				assert.Equal(t, tc.payload, string(payload))
			}
		})
	}
}

// requirePublishedSubject rejects a manifest whose subject is missing from its repository.
func requirePublishedSubject(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || !strings.Contains(r.URL.Path, "/manifests/") {
			next.ServeHTTP(w, r)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		manifest, err := v1.ParseManifest(bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if manifest.Subject != nil {
			subjectPath := path.Join(path.Dir(r.URL.Path), manifest.Subject.Digest.String())
			res := httptest.NewRecorder()
			next.ServeHTTP(res, httptest.NewRequest(http.MethodHead, subjectPath, nil))
			if res.Code != http.StatusOK {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, `{"errors":[{"code":"MANIFEST_BLOB_UNKNOWN","message":"missing subject %s"}]}`, manifest.Subject.Digest)
				return
			}
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		next.ServeHTTP(w, r)
	})
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
