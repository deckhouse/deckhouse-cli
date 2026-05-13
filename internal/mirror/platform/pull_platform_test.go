// Copyright 2025 Flant JSC
// SPDX-License-Identifier: Apache-2.0

package platform

import (
"context"
"fmt"
"log/slog"
"os"
"path/filepath"
"testing"

"github.com/Masterminds/semver/v3"
"github.com/google/go-containerregistry/pkg/v1/layout"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"

dkplog "github.com/deckhouse/deckhouse/pkg/log"

"github.com/deckhouse/deckhouse-cli/internal"
"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
mlayouts "github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
localreg "github.com/deckhouse/deckhouse/pkg/registry"
"github.com/deckhouse/deckhouse-cli/pkg"
registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"
	localfake "github.com/deckhouse/deckhouse-cli/pkg/fake"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

// stubRootURL is the host used by NewRegistryClientStub.
const stubRootURL = "registry.deckhouse.ru/deckhouse/fe"

// newDryRunService builds a Service in DryRun mode backed by stubClient.
// layout and pullerService are intentionally nil — DryRun never uses them.
func newDryRunService(
stubClient localreg.Client,
options *Options,
logger *dkplog.Logger,
userLogger *log.SLogger,
) *Service {
	if options == nil {
		options = &Options{}
	}
	options.DryRun = true
	return &Service{
		deckhouseService: registryservice.NewDeckhouseService(stubClient, logger),
		downloadList:     NewImageDownloadList(stubClient.GetRegistry()),
		options:          options,
		logger:           logger,
		userLogger:       userLogger,
	}
}

// suspendedStub returns a stub where the alpha release channel is suspended
// (version.json contains "suspend": true).
func suspendedStub() localreg.Client {
	// reuse the full stub but replace the alpha release-channel image
	reg := upfake.NewRegistry(stubRootURL)

	// Standard channels — alpha is suspended.
	channels := map[string]struct {
		version  string
		suspend  bool
	}{
		"alpha":        {version: "v1.72.10", suspend: true},
		"beta":         {version: "v1.71.0"},
		"early-access": {version: "v1.70.0"},
		"stable":       {version: "v1.69.0"},
		"rock-solid":   {version: "v1.68.0"},
	}
	for ch, data := range channels {
		content := fmt.Sprintf(`{"version":%q,"suspend":%v}`, data.version, data.suspend)
		img := upfake.NewImageBuilder().WithFile("version.json", content).MustBuild()
		reg.MustAddImage("release-channel", ch, img)
	}

	// Root + installer repos (same as default stub).
	versionTags := []string{"alpha", "beta", "early-access", "stable", "rock-solid",
		"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0", "pr12345"}
	for _, tag := range versionTags {
		img := upfake.NewImageBuilder().
			WithFile("version.json", `{"version":"v1.72.10"}`).
			WithFile("deckhouse/candi/images_digests.json", `{}`).
			MustBuild()
		reg.MustAddImage("", tag, img)
		reg.MustAddImage("install", tag, img)
		reg.MustAddImage("install-standalone", tag, img)
	}
	return pkgclient.Adapt(upfake.NewClient(reg))
}

// ---- validatePlatformAccess behaviour ----

func TestPullPlatform_ErrorWhenRegistryEmpty(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(emptyStub(), nil, logger, userLogger)
	err := svc.PullPlatform(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate platform access")
}

func TestPullPlatform_ErrorWhenSemverTagMissingInRegistry(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(localfake.NewRegistryClientStub(), &Options{TargetTag: "v9.99.0"}, logger, userLogger)
	err := svc.PullPlatform(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate platform access")
	assert.Contains(t, err.Error(), "v9.99.0")
}

func TestPullPlatform_ErrorWhenCustomTagMissingInRegistry(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(localfake.NewRegistryClientStub(), &Options{TargetTag: "no-such-tag"}, logger, userLogger)
	err := svc.PullPlatform(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate platform access")
}

func TestPullPlatform_ErrorWhenSuspendedWithoutIgnore(t *testing.T) {
	// Alpha channel is suspended; full discovery without IgnoreSuspend must fail.
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(suspendedStub(), nil, logger, userLogger)
	err := svc.PullPlatform(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "suspended")
}

// ---- IgnoreSuspend ----

func TestPullPlatform_IgnoreSuspend_SucceedsWithSuspendedChannel(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(suspendedStub(), &Options{IgnoreSuspend: true}, logger, userLogger)
	err := svc.PullPlatform(context.Background())

	require.NoError(t, err)
	// All 5 channel versions + 5 channel aliases should be present.
	assert.GreaterOrEqual(t, len(svc.downloadList.Deckhouse), 5)
}

// ---- TargetTag: specific semver ----

func TestPullPlatform_DryRun_SemverTag_PopulatesDownloadList(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(localfake.NewRegistryClientStub(), &Options{TargetTag: "v1.69.0"}, logger, userLogger)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	rootURL := stubRootURL
	// Version tag must appear in every image set.
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":v1.69.0",
"expected Deckhouse entry for requested version")
	assert.Contains(t, svc.downloadList.DeckhouseInstall, rootURL+"/install:v1.69.0",
"expected installer entry for requested version")
	assert.Contains(t, svc.downloadList.DeckhouseInstallStandalone, rootURL+"/install-standalone:v1.69.0",
"expected standalone installer entry for requested version")
	// Channel aliases live only in the release-channel layout; the main
	// Deckhouse repo carries only the version tag (see FillForChannels).
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel, rootURL+"/release-channel:stable",
"expected release-channel alias for matched channel")
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":stable",
"Deckhouse repo must not carry channel aliases")
	assert.NotContains(t, svc.downloadList.DeckhouseInstall, rootURL+"/install:stable",
"Deckhouse installer must not carry channel aliases")
	// Other versions must NOT appear.
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":v1.72.10")
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":v1.68.0")
}

func TestPullPlatform_DryRun_AlphaVersionTag_MatchesAlphaChannel(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(localfake.NewRegistryClientStub(), &Options{TargetTag: "v1.72.10"}, logger, userLogger)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	rootURL := stubRootURL
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":v1.72.10")
	// alpha is only an alias and must live in release-channel, not in <root>.
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel, rootURL+"/release-channel:alpha",
"alpha alias expected in release-channel because v1.72.10 is the alpha version")
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":alpha",
"Deckhouse repo must not carry channel aliases")
	// Stable and rock-solid must NOT appear anywhere for this target.
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":stable")
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":rock-solid")
	assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel, rootURL+"/release-channel:stable")
	assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel, rootURL+"/release-channel:rock-solid")
}

// ---- TargetTag: channel name ----

func TestPullPlatform_DryRun_ChannelTag_PopulatesVersionAndAlias(t *testing.T) {
	tests := []struct {
		channel        string
		expectedSemver string // version that channel points to
	}{
		{"alpha", "v1.72.10"},
		{"beta", "v1.71.0"},
		{"early-access", "v1.70.0"},
		{"stable", "v1.69.0"},
		{"rock-solid", "v1.68.0"},
	}

	for _, tt := range tests {
		t.Run("channel="+tt.channel, func(t *testing.T) {
logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
userLogger := log.NewSLogger(slog.LevelWarn)

svc := newDryRunService(localfake.NewRegistryClientStub(), &Options{TargetTag: tt.channel}, logger, userLogger)
			err := svc.PullPlatform(context.Background())
			require.NoError(t, err)

			rootURL := stubRootURL
			assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+tt.expectedSemver,
"semver entry expected for channel %s", tt.channel)
			// The channel alias must live only in release-channel, not in <root>.
			assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
				rootURL+"/release-channel:"+tt.channel,
"release-channel alias entry expected for channel %s", tt.channel)
			assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":"+tt.channel,
"Deckhouse repo must not carry channel aliases")
			assert.NotContains(t, svc.downloadList.DeckhouseInstall,
				rootURL+"/install:"+tt.channel,
"Deckhouse installer must not carry channel aliases")
			// Exactly the single semver tag should be in Deckhouse.
			assert.Len(t, svc.downloadList.Deckhouse, 1,
"only the matched version should be in Deckhouse")
		})
	}
}

// ---- TargetTag: custom non-semver tag ----

func TestPullPlatform_DryRun_CustomTag_OnlyThatTagInDownloadList(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(localfake.NewRegistryClientStub(), &Options{TargetTag: "pr12345"}, logger, userLogger)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	rootURL := stubRootURL
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":pr12345")
	assert.Contains(t, svc.downloadList.DeckhouseInstall, rootURL+"/install:pr12345")
	assert.Contains(t, svc.downloadList.DeckhouseInstallStandalone, rootURL+"/install-standalone:pr12345")
	// No channels or semver versions should appear.
	assert.Len(t, svc.downloadList.Deckhouse, 1,
"only the custom tag should appear in Deckhouse list")
	assert.Len(t, svc.downloadList.DeckhouseInstall, 1)
	assert.Len(t, svc.downloadList.DeckhouseInstallStandalone, 1)
}

// ---- No TargetTag: full discovery ----

func TestPullPlatform_DryRun_FullDiscovery_AllVersionsAndChannels(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(localfake.NewRegistryClientStub(), nil, logger, userLogger)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	rootURL := stubRootURL
	// Every semver tag from the stub must appear.
	for _, ver := range []string{"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0"} {
		assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+ver,
"expected version %s in Deckhouse list", ver)
		assert.Contains(t, svc.downloadList.DeckhouseInstall, rootURL+"/install:"+ver,
"expected installer entry for %s", ver)
		assert.Contains(t, svc.downloadList.DeckhouseInstallStandalone, rootURL+"/install-standalone:"+ver,
"expected standalone installer entry for %s", ver)
	}
	// Channel aliases must be confined to the release-channel layout.
	for _, ch := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
		assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
			rootURL+"/release-channel:"+ch,
"expected release-channel alias for %s", ch)
		assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":"+ch,
"Deckhouse repo must not carry channel alias %s", ch)
		assert.NotContains(t, svc.downloadList.DeckhouseInstall,
			rootURL+"/install:"+ch,
"Deckhouse installer must not carry channel alias %s", ch)
	}
	// All image sets in <root>/<install>/<install-standalone> are semver-only.
	assert.Len(t, svc.downloadList.Deckhouse, 5)
	assert.Len(t, svc.downloadList.DeckhouseInstall, 5)
	assert.Len(t, svc.downloadList.DeckhouseInstallStandalone, 5)
}

// ---- SinceVersion filter ----

func TestPullPlatform_DryRun_SinceVersion_FiltersOlderChannels(t *testing.T) {
	// SinceVersion=1.70.0 means stable(v1.69.0) and rock-solid(v1.68.0) are excluded.
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	since := semver.MustParse("1.70.0")
	svc := newDryRunService(localfake.NewRegistryClientStub(), &Options{SinceVersion: since}, logger, userLogger)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	rootURL := stubRootURL
	// Versions at or above 1.70.0 must be present.
	for _, ver := range []string{"v1.72.10", "v1.71.0", "v1.70.0"} {
		assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+ver)
	}
	// Versions below 1.70.0 must be absent.
	for _, ver := range []string{"v1.69.0", "v1.68.0"} {
		assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":"+ver,
"version %s is below SinceVersion and must be excluded", ver)
	}
	// Channel-aliased release channels below SinceVersion must be absent.
	assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel,
		rootURL+"/release-channel:stable")
	assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel,
		rootURL+"/release-channel:rock-solid")
	// Channel-aliased release channels at or above SinceVersion must be present.
	for _, ch := range []string{"alpha", "beta", "early-access"} {
		assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
			rootURL+"/release-channel:"+ch)
		assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":"+ch,
"Deckhouse repo must not carry channel alias %s", ch)
	}
}

func TestPullPlatform_DryRun_SinceVersion_EqualToAlpha_OnlyAlpha(t *testing.T) {
	// SinceVersion=1.72.10 means only the alpha version (== newest) survives.
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	since := semver.MustParse("1.72.10")
	svc := newDryRunService(localfake.NewRegistryClientStub(), &Options{SinceVersion: since}, logger, userLogger)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	rootURL := stubRootURL
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":v1.72.10")
	// alpha must be present in release-channel only.
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
		rootURL+"/release-channel:alpha")
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":alpha")
	// All older channels must be gone everywhere.
	for _, ch := range []string{"beta", "early-access", "stable", "rock-solid"} {
		assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":"+ch)
		assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel,
			rootURL+"/release-channel:"+ch)
	}
}

// ---- LTS channel fallback ----

func TestPullPlatform_DryRun_LTSRegistry_WithSemverTargetTag(t *testing.T) {
	// ltsOnlyStub has only an LTS channel; a semver TargetTag should still work
	// because access validation checks the tag directly (not via channels).
	// However the stub's root repo has no tags so the check will fail.
// Use a custom stub that has the LTS channel AND a semver tag in the root repo.
reg := upfake.NewRegistry(stubRootURL)

img := upfake.NewImageBuilder().
WithFile("version.json", `{"version":"v1.68.0"}`).
WithFile("deckhouse/candi/images_digests.json", `{}`).
MustBuild()
reg.MustAddImage("release-channel", "lts", img)
reg.MustAddImage("", "v1.68.0", img)
reg.MustAddImage("install", "v1.68.0", img)
reg.MustAddImage("install-standalone", "v1.68.0", img)

client := pkgclient.Adapt(upfake.NewClient(reg))
logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
userLogger := log.NewSLogger(slog.LevelWarn)

svc := newDryRunService(client, &Options{TargetTag: "v1.68.0"}, logger, userLogger)
err := svc.PullPlatform(context.Background())
require.NoError(t, err)

assert.Contains(t, svc.downloadList.Deckhouse, stubRootURL+":v1.68.0")
}

// ---- ReleaseChannel download list ----

func TestPullPlatform_DryRun_FullDiscovery_ReleaseChannelListPopulated(t *testing.T) {
logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
userLogger := log.NewSLogger(slog.LevelWarn)

svc := newDryRunService(localfake.NewRegistryClientStub(), nil, logger, userLogger)
err := svc.PullPlatform(context.Background())
require.NoError(t, err)

rootURL := stubRootURL
// Version-tagged release-channel entries from FillDeckhouseImages.
for _, ver := range []string{"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0"} {
assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
rootURL+"/release-channel:"+ver,
"expected release-channel entry for version %s", ver)
}
// Channel-tagged entries from FillForChannels.
for _, ch := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
rootURL+"/release-channel:"+ch,
"expected release-channel entry for channel %s", ch)
}
}

func TestPullPlatform_DryRun_SemverTag_ReleaseChannelListContainsVersionAndChannel(t *testing.T) {
logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
userLogger := log.NewSLogger(slog.LevelWarn)

svc := newDryRunService(localfake.NewRegistryClientStub(), &Options{TargetTag: "v1.69.0"}, logger, userLogger)
err := svc.PullPlatform(context.Background())
require.NoError(t, err)

rootURL := stubRootURL
assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel, rootURL+"/release-channel:v1.69.0")
assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel, rootURL+"/release-channel:stable")
}

// ---- Multiple TargetTag: custom + semver combined ----

func TestPullPlatform_DryRun_AllSemverTags_HaveInstallerEntries(t *testing.T) {
// Confirm that every version pulled also appears in both installer lists.
logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
userLogger := log.NewSLogger(slog.LevelWarn)

svc := newDryRunService(localfake.NewRegistryClientStub(), nil, logger, userLogger)
err := svc.PullPlatform(context.Background())
require.NoError(t, err)

rootURL := stubRootURL
for deckhouseKey := range svc.downloadList.Deckhouse {
// Extract the tag part after the last ":"
tag := deckhouseKey[len(rootURL)+1:]
v, parseErr := semver.NewVersion(tag)
if parseErr != nil {
// Not a semver tag (e.g. channel alias) — skip installer-standalone check.
continue
}
vTag := "v" + v.String()
assert.Contains(t, svc.downloadList.DeckhouseInstallStandalone,
rootURL+"/install-standalone:"+vTag,
"standalone installer must have entry for semver version %s", vTag)
}
}

// ---- Channel alias propagation (regression for d8 mirror pull → push → pull) ----

// ltsOnlySourceStub builds a CSE-like registry that only exposes the LTS channel:
//   - release-channel:lts, release-channel:<ver>
//   - <root>:lts, <root>:<ver>
//   - install:lts, install:<ver>
//   - install-standalone:lts, install-standalone:<ver>
//
// In particular it does NOT expose alpha/beta/early-access/stable/rock-solid in any
// of the four repositories — which matches what registry-cse.deckhouse.ru returns.
func ltsOnlySourceStub(ver string) localreg.Client {
	reg := upfake.NewRegistry(stubRootURL)

	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+ver+`"}`).
		WithFile("deckhouse/candi/images_digests.json", `{}`).
		MustBuild()

	for _, tag := range []string{"lts", ver} {
		reg.MustAddImage("release-channel", tag, img)
		reg.MustAddImage("", tag, img)
		reg.MustAddImage("install", tag, img)
		reg.MustAddImage("install-standalone", tag, img)
	}

	return pkgclient.Adapt(upfake.NewClient(reg))
}

// TestPullPlatform_LTSPull_ChannelAliasesLiveOnlyInReleaseChannel pins down the
// shape of the bundle produced by `d8 mirror pull --deckhouse-tag <tag>` against
// a CSE-like (LTS-only) registry. The invariant we lock in here is:
//
//   - release-channel layout carries every default channel alias
//     (alpha/beta/early-access/stable/rock-solid) on top of the version tag and
//     the originally requested `lts` tag. They are added by the propagation
//     block in pullDeckhousePlatform.
//   - The main Deckhouse, DeckhouseInstall and DeckhouseInstallStandalone
//     layouts carry ONLY semver/`lts`-style tags. They MUST NOT contain channel
//     aliases — those are just pointers to the version tag and duplicating them
//     in the main repos created an inconsistency that broke
//     `d8 mirror push` → `d8 mirror pull` (full discovery) cycles. See
//     TestPullPlatform_RePullFromBundleLikeRegistry_FullDiscovery for the
//     end-to-end variant of the regression.
func TestPullPlatform_LTSPull_ChannelAliasesLiveOnlyInReleaseChannel(t *testing.T) {
	const ver = "v1.73.5"

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	workingDir := t.TempDir()
	bundleDir := t.TempDir()

	svc := NewService(
		registryservice.NewService(ltsOnlySourceStub(ver), pkg.NoEdition, logger),
		workingDir,
		&Options{
			TargetTag: "lts",
			BundleDir: bundleDir,
		},
		logger,
		userLogger,
	)

	require.NoError(t, svc.PullPlatform(context.Background()))

	// pullDeckhousePlatform packs the populated workingDir into platform.tar at
	// the end, which is destructive (`bundle.Pack` removes files as it streams).
	// Unpack the produced tar into a fresh directory to inspect the bundle the
	// way `d8 mirror push` would see it.
	tarPath := filepath.Join(bundleDir, "platform.tar")
	tarFile, err := os.Open(tarPath)
	require.NoError(t, err)
	defer tarFile.Close()

	unpackDir := t.TempDir()
	require.NoError(t, bundle.Unpack(context.Background(), tarFile, unpackDir, "platform"))

	// 1. release-channel MUST carry every default channel alias.
	releaseChannelPath := layout.Path(filepath.Join(unpackDir, internal.ReleaseChannelSegment))
	for _, ch := range internal.GetAllDefaultReleaseChannels() {
		_, err := mlayouts.FindImageDescriptorByTag(releaseChannelPath, ch)
		assert.NoErrorf(t, err,
			"DeckhouseReleaseChannel layout must contain alias for channel %q after pull with --deckhouse-tag=lts",
			ch)
	}

	// 2. The main Deckhouse / Install / InstallStandalone layouts MUST NOT
	//    contain any channel aliases.
	mainLayouts := []struct {
		name       string
		layoutPath layout.Path
	}{
		{name: "Deckhouse", layoutPath: layout.Path(unpackDir)},
		{name: "DeckhouseInstall", layoutPath: layout.Path(filepath.Join(unpackDir, internal.InstallSegment))},
		{name: "DeckhouseInstallStandalone", layoutPath: layout.Path(filepath.Join(unpackDir, internal.InstallStandaloneSegment))},
	}
	for _, target := range mainLayouts {
		for _, ch := range internal.GetAllDefaultReleaseChannels() {
			_, err := mlayouts.FindImageDescriptorByTag(target.layoutPath, ch)
			assert.Errorf(t, err,
				"%s layout must NOT contain channel alias %q (channels live only in release-channel)",
				target.name, ch)
		}
	}
}

// TestPullPlatform_RePullFromBundleLikeRegistry_FullDiscovery is the end-to-end
// regression for the d8 mirror pull → push → pull cycle.
//
// Steps:
//  1. Build a CSE-like source registry (only LTS exists in every repo).
//  2. Pull with --deckhouse-tag=lts. The propagation block in
//     pullDeckhousePlatform writes alpha/beta/early-access/stable/rock-solid
//     aliases into the release-channel layout (and only there).
//  3. Replay the resulting bundle into a fresh "intermediate" registry the way
//     `d8 mirror push` would: add every (segment,tag) descriptor from the
//     unpacked layouts as an image in the registry.
//  4. Run a second PullPlatform against that intermediate registry with no
//     TargetTag (full-discovery), reproducing the user-reported scenario.
//
// Before the fix step (4) failed inside "Pull release channels and installers"
// with `pull deckhouse images: get digest for image <channel>: ... 404` because
// FillForChannels enqueued <root>:<channel> and <root>/install:<channel> entries
// that did not exist in the intermediate registry. After the fix
// FillForChannels keeps channel aliases scoped to the release-channel layout
// and the second pull completes successfully.
func TestPullPlatform_RePullFromBundleLikeRegistry_FullDiscovery(t *testing.T) {
	const ver = "v1.73.5"

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	// --- step 1+2: first pull from a CSE-like source ---
	workingDir1 := t.TempDir()
	bundleDir1 := t.TempDir()
	svc1 := NewService(
		registryservice.NewService(ltsOnlySourceStub(ver), pkg.NoEdition, logger),
		workingDir1,
		&Options{TargetTag: "lts", BundleDir: bundleDir1},
		logger,
		userLogger,
	)
	require.NoError(t, svc1.PullPlatform(context.Background()))

	// --- step 3: replay bundle into an intermediate registry ---
	tarPath := filepath.Join(bundleDir1, "platform.tar")
	tarFile, err := os.Open(tarPath)
	require.NoError(t, err)
	defer tarFile.Close()

	unpackDir := t.TempDir()
	require.NoError(t, bundle.Unpack(context.Background(), tarFile, unpackDir, "platform"))

	intermediate := buildRegistryFromUnpackedBundle(t, unpackDir)

	// --- step 4: full-discovery re-pull from the intermediate registry ---
	workingDir2 := t.TempDir()
	bundleDir2 := t.TempDir()
	svc2 := NewService(
		registryservice.NewService(intermediate, pkg.NoEdition, logger),
		workingDir2,
		// Empty TargetTag mirrors what the user reports running: plain
		// `d8 mirror pull` against the intermediate registry, no flags.
		&Options{BundleDir: bundleDir2},
		logger,
		userLogger,
	)
	require.NoError(t, svc2.PullPlatform(context.Background()),
		"re-pull from a bundle-derived registry must not fail on missing channel aliases")
}

// buildRegistryFromUnpackedBundle reads every OCI layout (segment) under root
// and republishes its descriptors into a fresh fake registry under the same
// segments and tags. It is intentionally a minimal stand-in for `d8 mirror
// push` — enough to reproduce the bug, not a full pusher reimplementation.
func buildRegistryFromUnpackedBundle(t *testing.T, root string) localreg.Client {
	t.Helper()

	reg := upfake.NewRegistry(stubRootURL)

	// Map of relative-path (segment) → set of tags. Empty segment means root.
	segments := map[string]layout.Path{
		"":                             layout.Path(root),
		internal.ReleaseChannelSegment: layout.Path(filepath.Join(root, internal.ReleaseChannelSegment)),
		internal.InstallSegment:        layout.Path(filepath.Join(root, internal.InstallSegment)),
		internal.InstallStandaloneSegment: layout.Path(filepath.Join(root, internal.InstallStandaloneSegment)),
	}

	for segment, lp := range segments {
		index, err := lp.ImageIndex()
		if err != nil {
			// segment may not exist in the bundle (e.g. when nothing was pulled
			// into it) — skip silently.
			continue
		}
		manifest, err := index.IndexManifest()
		require.NoError(t, err)

		for _, desc := range manifest.Manifests {
			img, err := index.Image(desc.Digest)
			require.NoError(t, err)

			tag := desc.Annotations["io.deckhouse.image.short_tag"]
			if tag == "" {
				continue
			}
			reg.MustAddImage(segment, tag, img)
		}
	}

	return pkgclient.Adapt(upfake.NewClient(reg))
}
