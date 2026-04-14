// Copyright 2025 Flant JSC
// SPDX-License-Identifier: Apache-2.0

package platform

import (
"context"
"fmt"
"log/slog"
"testing"

"github.com/Masterminds/semver/v3"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"

dkplog "github.com/deckhouse/deckhouse/pkg/log"

"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
localreg "github.com/deckhouse/deckhouse/pkg/registry"
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
	// Channel alias must also appear because v1.69.0 == stable channel version.
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":stable",
"expected Deckhouse channel alias for matched channel")
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
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":alpha",
"alpha channel alias expected because v1.72.10 is the alpha version")
	// Stable and rock-solid must NOT appear.
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":stable")
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":rock-solid")
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
			assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+tt.channel,
"channel alias entry expected")
			// Exactly 2 entries: one semver + one channel alias.
			assert.Len(t, svc.downloadList.Deckhouse, 2,
"only the matched version and channel alias should be in Deckhouse")
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
	// Every channel alias must appear in Deckhouse.
	for _, ch := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
		assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+ch,
"expected channel alias %s in Deckhouse list", ch)
		assert.Contains(t, svc.downloadList.DeckhouseInstall, rootURL+"/install:"+ch,
"expected channel installer entry for %s", ch)
	}
	// 5 versions + 5 channel aliases = 10 entries.
	assert.Len(t, svc.downloadList.Deckhouse, 10)
	assert.Len(t, svc.downloadList.DeckhouseInstall, 10)
	// DeckhouseInstallStandalone receives only version tags (not channel aliases).
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
	// Channels below SinceVersion must be absent.
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":stable")
	assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":rock-solid")
	// Channels at or above SinceVersion must be present.
	for _, ch := range []string{"alpha", "beta", "early-access"} {
		assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+ch)
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
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":alpha")
	// All older channels must be gone.
	for _, ch := range []string{"beta", "early-access", "stable", "rock-solid"} {
		assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":"+ch)
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
