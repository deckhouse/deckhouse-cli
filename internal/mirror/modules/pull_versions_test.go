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
	"log/slog"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

const (
	pullVersionsTestHost   = "fake.registry"
	pullVersionsTestModule = "console"
)

// TestPullSingleModule_SemverConstraintExpandsToRegistryTags is a regression test
// for the bug where `--include-module <name>@<semver>` ignored the registry's
// list of module version tags and produced a download set containing only the
// versions currently advertised on release channels.
//
// Before the fix, pullSingleModule built `&Module{Name, RegistryPath}` with
// Releases == nil and passed it to Filter.VersionsToMirror. The semver branch
// iterates mod.Releases, which was empty, so VersionsToMirror returned nil and
// only the channel version made it into downloadList.Module.
//
// After the fix, pullSingleModule fetches the full tag list via
// modulesService.Module(name).ListTags(ctx) and stores it in Module.Releases,
// so the semver constraint matches every version in the registry that falls in
// range.
func TestPullSingleModule_SemverConstraintExpandsToRegistryTags(t *testing.T) {
	const channelVersion = "v1.45.2"

	// Versions present in the fake registry. v1.39.0 is intentionally below the
	// semver lower bound (^1.40.0 → >=1.40.0) and must be excluded by the filter.
	allVersions := []string{
		"v1.39.0",
		"v1.40.0",
		"v1.40.1",
		"v1.41.0",
		"v1.42.0",
		"v1.43.0",
		"v1.44.0",
		"v1.45.2",
	}

	reg := upfake.NewRegistry(pullVersionsTestHost)

	// Top-level "modules" repo: tags are module names.
	// modulesService.ListTags returns ["console"].
	reg.MustAddImage("modules", pullVersionsTestModule, pullVersionsImage(channelVersion))

	// Per-module repo: tags are the versions available in the registry.
	// modulesService.Module("console").ListTags returns the full version list.
	for _, v := range allVersions {
		reg.MustAddImage("modules/"+pullVersionsTestModule, v, pullVersionsImage(v))
	}

	// Release-channel repo: 5 channels, each pointing at the same channel version.
	// extractVersionsFromReleaseChannels reads version.json from each.
	for _, ch := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
		reg.MustAddImage("modules/"+pullVersionsTestModule+"/release", ch, pullVersionsImage(channelVersion))
	}

	stubClient := pkgclient.Adapt(upfake.NewClient(reg))

	// Whitelist filter: console@1.40.0 → semver ^1.40.0 (>=1.40.0 <2.0.0).
	filter, err := NewFilter([]string{pullVersionsTestModule + "@1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	regSvc := registryservice.NewService(stubClient, pkg.NoEdition, logger)

	svc := NewService(
		regSvc,
		t.TempDir(),
		&Options{
			BundleDir:     t.TempDir(),
			Filter:        filter,
			SkipVexImages: true, // VEX discovery is unrelated to this test.
		},
		logger,
		userLogger,
	)

	require.NoError(t, svc.PullModules(context.Background()))

	// After PullModules the per-module download list must contain all versions
	// that match the filter, not only the channel version.
	moduleDL := svc.modulesDownloadList.list[pullVersionsTestModule]
	require.NotNil(t, moduleDL, "expected download list entry for module %q", pullVersionsTestModule)

	got := make([]string, 0, len(moduleDL.Module))
	for ref := range moduleDL.Module {
		// extractInternalDigestImages can add @sha256: refs - we only care about
		// version-tagged refs in this assertion.
		if strings.Contains(ref, "@sha256:") {
			continue
		}
		got = append(got, ref)
	}

	want := []string{
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.40.0",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.40.1",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.41.0",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.42.0",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.43.0",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.44.0",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.45.2",
	}

	assert.ElementsMatch(t, want, got,
		"semver ^1.40.0 must expand to every matching tag in the registry, not only the channel version")

	// v1.39.0 is below the lower bound; it must not be pulled.
	assert.NotContains(t, got, pullVersionsTestHost+"/modules/"+pullVersionsTestModule+":v1.39.0",
		"v1.39.0 is below ^1.40.0 lower bound and must be excluded")
}

// pullVersionsImage builds a minimal v1.Image with version.json so that
// extractVersionsFromReleaseChannels and extractVersionJSON have something to
// read. images_digests.json and extra_images.json are intentionally omitted -
// downstream code tolerates missing files (debug-log + skip).
func pullVersionsImage(version string) v1.Image {
	return upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+version+`"}`).
		WithLabel("org.opencontainers.image.version", version).
		MustBuild()
}
