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

package fake

import (
	"fmt"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	localreg "github.com/deckhouse/deckhouse/pkg/registry"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

// defaultSource is the registry root used by NewRegistryClientStub.
const defaultSource = "registry.deckhouse.ru/deckhouse/fe"

// releaseChannelData maps a release-channel tag to the version its image
// carries in version.json.
var releaseChannelData = map[string]string{
	"alpha":        "v1.72.10",
	"beta":         "v1.71.0",
	"early-access": "v1.70.0",
	"stable":       "v1.69.0",
	"rock-solid":   "v1.68.0",
}

// changelogYAML is the sample changelog file embedded in every stub image.
const changelogYAML = `candi:
  fixes:
  - summary: "Fix deckhouse containerd start after installing new containerd-deckhouse package."
    pull_request: "https://github.com/deckhouse/deckhouse/pull/6329"
`

// imagesDigestsJSON is the sample images-tags file embedded in stub version images.
const imagesDigestsJSON = `{}`

// NewRegistryClientStub creates a [localreg.Client] pre-populated with
// Deckhouse-shaped registry data that mirrors the structure expected by the
// platform test suite.
//
// The stub exposes a registry at [defaultSource]
// ("registry.deckhouse.ru/deckhouse/fe") with the following structure:
//
//   - root repository (empty path): tags alpha, beta, early-access, stable,
//     rock-solid, v1.72.10, v1.71.0, v1.70.0, v1.69.0, v1.68.0, pr12345.
//
//   - "release-channel" repository: tags alpha, beta, early-access, stable,
//     rock-solid.  Each image carries version.json with the channel's current
//     version (e.g. alpha → v1.72.10).
//
//   - "install" and "install-standalone" repositories: same tags as root.
func NewRegistryClientStub() localreg.Client {
	reg := upfake.NewRegistry(defaultSource)

	// ---- release-channel repository ----
	for channel, version := range releaseChannelData {
		img := releaseChannelImage(version)
		reg.MustAddImage("release-channel", channel, img)
		// Version-tagged release-channel images are required by non-DryRun full-discovery pull.
		reg.MustAddImage("release-channel", version, img)
	}

	// ---- root-level and installer repositories ----
	rootTags := []struct {
		tag     string
		version string
	}{
		{"alpha", "v1.72.10"},
		{"beta", "v1.71.0"},
		{"early-access", "v1.70.0"},
		{"stable", "v1.69.0"},
		{"rock-solid", "v1.68.0"},
		{"v1.72.10", "v1.72.10"},
		{"v1.71.0", "v1.71.0"},
		{"v1.70.0", "v1.70.0"},
		{"v1.69.0", "v1.69.0"},
		{"v1.68.0", "v1.68.0"},
		{"pr12345", "v1.72.10"}, // custom non-semver tag
	}

	for _, rt := range rootTags {
		img := platformImage(rt.version)
		reg.MustAddImage("", rt.tag, img)
		reg.MustAddImage("install", rt.tag, img)
		reg.MustAddImage("install-standalone", rt.tag, img)
	}

	return pkgclient.Adapt(upfake.NewClient(reg))
}

// platformImage creates a stub v1.Image for the root (edition) repository
// containing the files that the deckhouse platform service reads during
// version discovery.
func platformImage(version string) v1.Image {
	return upfake.NewImageBuilder().
		WithFile("version.json", fmt.Sprintf(`{"version":%q}`, version)).
		WithFile("changelog.yaml", changelogYAML).
		WithFile("deckhouse/candi/images_digests.json", imagesDigestsJSON).
		WithLabel("org.opencontainers.image.version", version).
		MustBuild()
}

// releaseChannelImage creates a stub v1.Image for the release-channel
// repository containing version.json that DeckhouseReleaseService reads.
func releaseChannelImage(version string) v1.Image {
	return upfake.NewImageBuilder().
		WithFile("version.json", fmt.Sprintf(`{"version":%q}`, version)).
		MustBuild()
}
