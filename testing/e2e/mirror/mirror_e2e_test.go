/*
Copyright 2024 Flant JSC

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
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/auth"
)

func TestMirrorE2E(t *testing.T) {
	t.SkipNow()
}

func createDeckhouseReleaseChannelsInRegistry(t *testing.T, repo string) {
	t.Helper()

	createDeckhouseReleaseChannelImageInRegistry(t, repo+"/release-channel", "alpha", "v1.56.5")
	createDeckhouseReleaseChannelImageInRegistry(t, repo+"/release-channel", "beta", "v1.56.5")
	createDeckhouseReleaseChannelImageInRegistry(t, repo+"/release-channel", "early-access", "v1.55.7")
	createDeckhouseReleaseChannelImageInRegistry(t, repo+"/release-channel", "stable", "v1.55.7")
	createDeckhouseReleaseChannelImageInRegistry(t, repo+"/release-channel", "rock-solid", "v1.55.7")
	createDeckhouseReleaseChannelImageInRegistry(t, repo+"/release-channel", "v1.55.7", "v1.55.7")
	createDeckhouseReleaseChannelImageInRegistry(t, repo+"/release-channel", "v1.56.5", "v1.56.5")
}

func createTrivyVulnerabilityDatabasesInRegistry(t *testing.T, repo string, insecure, useTLS bool) {
	t.Helper()
	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(authn.Anonymous, insecure, useTLS)

	images := []string{
		repo + "/security/trivy-db:2",
		repo + "/security/trivy-bdu:1",
		repo + "/security/trivy-java-db:1",
		repo + "/security/trivy-checks:0",
	}

	for _, image := range images {
		ref, err := name.ParseReference(image, nameOpts...)
		require.NoError(t, err)
		wantImage, err := random.Image(256, 1)
		require.NoError(t, err)
		require.NoError(t, remote.Write(ref, wantImage, remoteOpts...))
	}
}

func createDeckhouseControllersAndInstallersInRegistry(t *testing.T, repo string) {
	t.Helper()

	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(nil, true, false)

	createRandomImageInRegistry(t, repo+":alpha")
	createRandomImageInRegistry(t, repo+":beta")
	createRandomImageInRegistry(t, repo+":early-access")
	createRandomImageInRegistry(t, repo+":stable")
	createRandomImageInRegistry(t, repo+":rock-solid")
	createRandomImageInRegistry(t, repo+":v1.56.5")
	createRandomImageInRegistry(t, repo+":v1.55.7")

	installers := map[string]v1.Image{
		"v1.56.5": createSyntheticInstallerImage(t, "v1.56.5", repo),
		"v1.55.7": createSyntheticInstallerImage(t, "v1.55.7", repo),
	}
	installers["alpha"] = installers["v1.56.5"]
	installers["beta"] = installers["v1.56.5"]
	installers["early-access"] = installers["v1.55.7"]
	installers["stable"] = installers["v1.55.7"]
	installers["rock-solid"] = installers["v1.55.7"]

	for shortTag, installer := range installers {
		ref, err := name.ParseReference(repo+"/install:"+shortTag, nameOpts...)
		require.NoError(t, err)

		err = remote.Write(ref, installer, remoteOpts...)
		require.NoError(t, err)

		ref, err = name.ParseReference(repo+"/install-standalone:"+shortTag, nameOpts...)
		require.NoError(t, err)

		err = remote.Write(ref, installer, remoteOpts...)
		require.NoError(t, err)
	}
}

func createSyntheticInstallerImage(t *testing.T, version, repo string) v1.Image {
	t.Helper()

	// FROM scratch
	base := empty.Image
	layers := make([]v1.Layer, 0)

	// COPY ./version /deckhouse/version
	// COPY ./images_digests.json /deckhouse/candi/images_digests.json
	imagesDigests, err := json.Marshal(
		map[string]map[string]string{
			"common": {
				"alpine": createRandomImageInRegistry(t, repo+":alpine"+version),
			},
			"nodeManager": {
				"bashibleApiserver": createRandomImageInRegistry(t, repo+":bashibleApiserver"+version),
			},
		})
	require.NoError(t, err)
	l, err := crane.Layer(map[string][]byte{
		"deckhouse/version":                   []byte(version),
		"deckhouse/candi/images_digests.json": imagesDigests,
	})
	require.NoError(t, err)
	layers = append(layers, l)

	img, err := mutate.AppendLayers(base, layers...)
	require.NoError(t, err)

	// ENTRYPOINT ["/bin/bash"]
	img, err = mutate.Config(img, v1.Config{
		Entrypoint: []string{"/bin/bash"},
	})
	require.NoError(t, err)

	return img
}

func createRandomImageInRegistry(t *testing.T, tag string) (digest string) {
	t.Helper()

	img, err := random.Image(int64(rand.Intn(1024)+1), int64(rand.Intn(5)+1))
	require.NoError(t, err)

	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(nil, true, false)
	ref, err := name.ParseReference(tag, nameOpts...)
	require.NoError(t, err)

	err = remote.Write(ref, img, remoteOpts...)
	require.NoError(t, err)

	digestHash, err := img.Digest()
	require.NoError(t, err)

	return digestHash.String()
}

func createDeckhouseReleaseChannelImageInRegistry(t *testing.T, repo, tag, version string) (digest string) {
	t.Helper()

	// FROM scratch
	base := empty.Image
	layers := make([]v1.Layer, 0)

	// COPY ./version.json /version.json
	changelog, err := yaml.JSONToYAML([]byte(`{"candi":{"fixes":[{"summary":"Fix deckhouse containerd start after installing new containerd-deckhouse package.","pull_request":"https://github.com/deckhouse/deckhouse/pull/6329"}]}}`))
	require.NoError(t, err)
	versionInfo := fmt.Sprintf(
		`{"disruptions":{"1.56":["ingressNginx"]},"requirements":{"containerdOnAllNodes":"true","ingressNginx":"1.1","k8s":"1.23.0","nodesMinimalOSVersionUbuntu":"18.04"},"version":%q}`,
		"v"+version,
	)
	l, err := crane.Layer(map[string][]byte{
		"version.json":   []byte(versionInfo),
		"changelog.yaml": changelog,
	})
	layers = append(layers, l)

	img, err := mutate.AppendLayers(base, layers...)
	require.NoError(t, err)

	nameOpts, remoteOpts := auth.MakeRemoteRegistryRequestOptions(nil, true, false)
	ref, err := name.ParseReference(repo+":"+tag, nameOpts...)
	require.NoError(t, err)

	err = remote.Write(ref, img, remoteOpts...)
	require.NoError(t, err)

	digestHash, err := img.Digest()
	require.NoError(t, err)

	return digestHash.String()
}

func validateDeckhouseReleasesManifests(t *testing.T, pullCtx *params.PullParams, versions []semver.Version) {
	t.Helper()
	deckhouseReleasesManifestsFilepath := filepath.Join(pullCtx.BundleDir, "deckhousereleases.yaml")
	actualManifests, err := os.ReadFile(deckhouseReleasesManifestsFilepath)
	require.NoError(t, err)

	expectedManifests := strings.Builder{}
	for _, version := range versions {
		expectedManifests.WriteString(fmt.Sprintf(`---
apiVersion: deckhouse.io/v1alpha1
approved: false
kind: DeckhouseRelease
metadata:
  creationTimestamp: null
  name: v%[1]s
spec:
  changelog:
    candi:
      fixes:
      - summary: Fix deckhouse containerd start after installing new containerd-deckhouse package.
        pull_request: https://github.com/deckhouse/deckhouse/pull/6329
  changelogLink: https://github.com/deckhouse/deckhouse/releases/tag/v%[1]s
  disruptions:
  - ingressNginx
  requirements:
    containerdOnAllNodes: 'true'
    ingressNginx: '1.1'
    k8s: 1.23.0
    nodesMinimalOSVersionUbuntu: '18.04'
  version: v%[1]s
status:
  approved: false
  message: ""
  transitionTime: "0001-01-01T00:00:00Z"
`, version.String()))
	}

	require.FileExists(t, deckhouseReleasesManifestsFilepath, "deckhousereleases.yaml should be generated next tar bundle")
	require.YAMLEq(t, expectedManifests.String(), string(actualManifests))
}
