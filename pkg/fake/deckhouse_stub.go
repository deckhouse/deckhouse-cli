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
	"encoding/base64"
	"fmt"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"

	localreg "github.com/deckhouse/deckhouse/pkg/registry"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

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

// stubModuleVersion is the version the stub module's stable channel points at.
const stubModuleVersion = "v0.5.0"

// stubPluginContract makes the stub plugin auto-selected whenever the
// cert-manager module is mirrored.
const stubPluginContract = `{
	"name": "cert-manager-tool", "version": "v1.0.0",
	"requirements": {"modules": {"mandatory": [{"name": "cert-manager", "constraint": ">=0.1.0"}]}}
}`

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
//
//   - "modules" catalog with the cert-manager module: one version
//     (stubModuleVersion) reachable via its stable release channel.
//
//   - "deckhouse-cli/plugins" catalog with the cert-manager-tool plugin
//     (v1.0.0), whose contract requires the cert-manager module - so a pull
//     that mirrors the module auto-selects the plugin.
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
		reg.MustAddImage("installer", rt.tag, img)
	}

	// ---- security databases ----
	// Each entry represents a well-known Trivy DB segment together with the
	// fixed tag that FillSecurityImages hard-codes in security/layout.go.
	securityImages := []struct {
		segment string
		tag     string
	}{
		{"security/trivy-db", "2"},
		{"security/trivy-bdu", "1"},
		{"security/trivy-java-db", "1"},
		{"security/trivy-checks", "0"},
	}
	for _, si := range securityImages {
		reg.MustAddImage(si.segment, si.tag, securityImage())
	}

	// ---- modules ----
	// cert-manager carries one pullable version via its stable channel, so
	// command-level tests exercise the modules phase and the module-driven
	// plugin selection.
	reg.MustAddImage("modules", "cert-manager", moduleImage(stubModuleVersion))
	reg.MustAddImage("modules/cert-manager", stubModuleVersion, moduleImage(stubModuleVersion))
	reg.MustAddImage("modules/cert-manager/release", "stable", moduleImage(stubModuleVersion))
	reg.MustAddImage("modules/cert-manager/release", stubModuleVersion, moduleImage(stubModuleVersion))

	// ---- plugins catalog ----
	// The catalog name index is directory-as-tags: a tag per plugin name on
	// the catalog repo, next to the per-plugin version repos.
	reg.MustAddImage("deckhouse-cli/plugins", "cert-manager-tool",
		upfake.NewImageBuilder().WithFile("name", "cert-manager-tool").MustBuild())
	reg.MustAddImage("deckhouse-cli/plugins/cert-manager-tool", "v1.0.0",
		pluginImage("cert-manager-tool", "v1.0.0", stubPluginContract))

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

// securityImage creates a minimal stub v1.Image for a security database
// repository (trivy-db, trivy-bdu, trivy-java-db, trivy-checks).
func securityImage() v1.Image {
	return upfake.NewImageBuilder().MustBuild()
}

// moduleImage creates a stub v1.Image for module repositories: version.json
// (read during module version discovery) plus the OCI version label.
func moduleImage(version string) v1.Image {
	return upfake.NewImageBuilder().
		WithFile("version.json", fmt.Sprintf(`{"version":%q}`, version)).
		WithLabel("org.opencontainers.image.version", version).
		MustBuild()
}

// pluginImage creates a stub v1.Image for a plugin version: the contract JSON
// is base64-encoded into the "contract" annotation the plugins catalog reads.
func pluginImage(name, tag, contractJSON string) v1.Image {
	img := upfake.NewImageBuilder().WithFile("plugin", "binary-"+name+"-"+tag).MustBuild()

	encoded := base64.StdEncoding.EncodeToString([]byte(contractJSON))

	return mutate.Annotations(img, map[string]string{"contract": encoded}).(v1.Image)
}
