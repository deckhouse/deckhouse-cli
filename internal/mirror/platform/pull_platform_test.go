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
"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
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

// suspendedStubWith returns a stub with the five standard release channels
// where every channel listed in suspendedChannels carries "suspend": true in
// its version.json. extraTags are published to the root, install and
// install-standalone repositories only, so they match no channel version.
func suspendedStubWith(suspendedChannels []string, extraTags ...string) localreg.Client {
	reg := upfake.NewRegistry(stubRootURL)

	suspended := make(map[string]bool, len(suspendedChannels))
	for _, ch := range suspendedChannels {
		suspended[ch] = true
	}

	channels := map[string]string{
		"alpha":        "v1.72.10",
		"beta":         "v1.71.0",
		"early-access": "v1.70.0",
		"stable":       "v1.69.0",
		"rock-solid":   "v1.68.0",
	}
	for ch, version := range channels {
		content := fmt.Sprintf(`{"version":%q,"suspend":%v}`, version, suspended[ch])
		img := upfake.NewImageBuilder().WithFile("version.json", content).MustBuild()
		reg.MustAddImage("release-channel", ch, img)
	}

	// Root + installer repos (same as default stub).
	versionTags := []string{"alpha", "beta", "early-access", "stable", "rock-solid",
		"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0", "pr12345"}
	versionTags = append(versionTags, extraTags...)
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

// suspendedStub returns a stub where the alpha release channel is suspended
// (version.json contains "suspend": true).
func suspendedStub() localreg.Client {
	return suspendedStubWith([]string{"alpha"})
}

// suspendedLTSOnlyStub builds a CSE-like registry whose only release channel
// is a suspended LTS. extraTags are published to the root, install and
// install-standalone repositories alongside the LTS version tag.
func suspendedLTSOnlyStub(ltsVersion string, extraTags ...string) localreg.Client {
	reg := upfake.NewRegistry(stubRootURL)

	channelImg := upfake.NewImageBuilder().
		WithFile("version.json", fmt.Sprintf(`{"version":%q,"suspend":true}`, ltsVersion)).
		MustBuild()
	reg.MustAddImage("release-channel", "lts", channelImg)

	tags := append([]string{ltsVersion}, extraTags...)
	for _, tag := range tags {
		img := upfake.NewImageBuilder().
			WithFile("version.json", fmt.Sprintf(`{"version":%q}`, ltsVersion)).
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

// ---- TargetTag vs suspended channels ----

// TestPullPlatform_DryRun_SemverTag_UnrelatedChannelSuspended_Succeeds is the
// regression for `d8 mirror pull --deckhouse-tag vX.Y.Z` aborting with
//
//	failed to get release channel version from registry for channel rock-solid:
//	source registry contains suspended release channel "rock-solid", try again later
//
// when the requested version does not belong to the suspended channel at all.
// A suspension may only block pulls that resolve to the suspended channel;
// pinning an unrelated tag must succeed and must not enqueue the suspended
// channel alias.
func TestPullPlatform_DryRun_SemverTag_UnrelatedChannelSuspended_Succeeds(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	// v1.71.5 exists in the registry but is not the current version of any channel.
	svc := newDryRunService(
		suspendedStubWith([]string{"rock-solid"}, "v1.71.5"),
		&Options{TargetTag: "v1.71.5"},
		logger,
		userLogger,
	)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err,
		"a suspended channel unrelated to the requested tag must not abort the pull")

	rootURL := stubRootURL
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":v1.71.5")
	assert.Len(t, svc.downloadList.Deckhouse, 1,
		"only the requested tag must be enqueued for download")
	assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel,
		rootURL+"/release-channel:rock-solid",
		"suspended channel alias must not be enqueued")
}

// TestPullPlatform_DryRun_SemverTag_MatchingSuspendedChannel_Fails pins the
// conservative half of the contract: a tag equal to a suspended channel's
// current version resolves to that channel (its alias would be bundled), so
// the pull is refused unless --ignore-suspend is given.
func TestPullPlatform_DryRun_SemverTag_MatchingSuspendedChannel_Fails(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	// v1.72.10 is the current version of the suspended alpha channel.
	svc := newDryRunService(suspendedStub(), &Options{TargetTag: "v1.72.10"}, logger, userLogger)
	err := svc.PullPlatform(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "suspended")
	assert.Contains(t, err.Error(), `"alpha"`)
}

func TestPullPlatform_DryRun_ChannelTag_SuspendedChannel_Fails(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(suspendedStub(), &Options{TargetTag: "alpha"}, logger, userLogger)
	err := svc.PullPlatform(context.Background())

	require.Error(t, err, "requesting a suspended channel by name must be refused")
	assert.Contains(t, err.Error(), "suspended")
	assert.Contains(t, err.Error(), `"alpha"`)
}

func TestPullPlatform_DryRun_ChannelTag_OtherChannelSuspended_Succeeds(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	// stable is healthy; only alpha is suspended.
	svc := newDryRunService(suspendedStub(), &Options{TargetTag: "stable"}, logger, userLogger)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err,
		"a suspended channel must not block pulling another, healthy channel")

	rootURL := stubRootURL
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":v1.69.0")
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
		rootURL+"/release-channel:stable")
	assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel,
		rootURL+"/release-channel:alpha",
		"suspended channel alias must not be enqueued")
}

func TestPullPlatform_DryRun_CustomTag_ChannelSuspended_Succeeds(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(suspendedStub(), &Options{TargetTag: "pr12345"}, logger, userLogger)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err,
		"custom tags match no channel and must not be affected by suspensions")

	assert.Contains(t, svc.downloadList.Deckhouse, stubRootURL+":pr12345")
	assert.Len(t, svc.downloadList.Deckhouse, 1)
}

func TestPullPlatform_DryRun_IgnoreSuspend_TagMatchingSuspendedChannel_Succeeds(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(
		suspendedStub(),
		&Options{TargetTag: "v1.72.10", IgnoreSuspend: true},
		logger,
		userLogger,
	)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err,
		"--ignore-suspend must override the suspension on the matched channel")

	rootURL := stubRootURL
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":v1.72.10")
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
		rootURL+"/release-channel:alpha",
		"with --ignore-suspend the matched channel alias is enqueued as usual")
}

// TestPullPlatform_DryRun_FullDiscovery_SuspendedLTS_Fails pins two contracts
// at once: a suspended LTS still counts as present, so missing default
// channels stay non-fatal (CSE registries), and full discovery resolves to
// every fetched channel, so the suspension itself is what aborts the pull.
func TestPullPlatform_DryRun_FullDiscovery_SuspendedLTS_Fails(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(suspendedLTSOnlyStub("v1.68.0"), nil, logger, userLogger)
	err := svc.PullPlatform(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), `suspended release channel "lts"`,
		"the failure must come from the suspension, not from the missing default channels")
}

// TestPullPlatform_DryRun_SuspendedLTS_UnrelatedTag_Succeeds covers the CSE
// variant of the unrelated-suspension scenario: the only channel (LTS) is
// suspended, the requested tag does not match it, so the pull must succeed.
func TestPullPlatform_DryRun_SuspendedLTS_UnrelatedTag_Succeeds(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(
		suspendedLTSOnlyStub("v1.68.0", "v1.71.5"),
		&Options{TargetTag: "v1.71.5"},
		logger,
		userLogger,
	)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err)

	assert.Contains(t, svc.downloadList.Deckhouse, stubRootURL+":v1.71.5")
	assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel,
		stubRootURL+"/release-channel:lts",
		"suspended LTS alias must not be enqueued")
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

// devRegistryStub mirrors the shape of a Deckhouse dev/CI registry such as
// dev-registry.deckhouse.io/sys/deckhouse-oss: only a single PR-style tag is
// published in the root, install, install-standalone repositories, and no
// release-channel images exist at all (no LTS, no rock-solid, no stable, no
// alpha/beta/early-access).
//
// It is intentionally narrower than ltsOnlySourceStub: this is the scenario
// users hit when testing pre-release builds with `--deckhouse-tag prNNNNN`
// against a registry that has never published any release-channel manifest.
func devRegistryStub(prTag string) localreg.Client {
	reg := upfake.NewRegistry(stubRootURL)

	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+prTag+`"}`).
		WithFile("deckhouse/candi/images_digests.json", `{}`).
		MustBuild()

	reg.MustAddImage("", prTag, img)
	reg.MustAddImage("install", prTag, img)
	reg.MustAddImage("install-standalone", prTag, img)

	return pkgclient.Adapt(upfake.NewClient(reg))
}

// TestPullPlatform_DryRun_CustomTag_NoReleaseChannelsInRegistry is the
// regression for d8 mirror pull failing with
//
//	get rock-solid release version from registry: get rock-solid release channel data:
//	GET .../release-channel/manifests/rock-solid: MANIFEST_UNKNOWN
//
// when invoked as
//
//	d8 mirror pull --deckhouse-tag prNNNNN --source dev-registry.../deckhouse-oss ...
//
// against a registry whose release-channel/ repository is empty.
//
// Before the fix `findTagsToMirror`/`versionsToMirrorFunc` (the legacy
// implementation in v0.27.0) always iterated through every default channel
// and bubbled up the missing rock-solid channel even though the user had
// already pinned the exact tag with --deckhouse-tag. Pinning the tag must
// short-circuit channel discovery entirely: the user has explicitly told the
// CLI which build to mirror.
//
// Post-fix invariants exercised here:
//   - PullPlatform must not return an error when no release-channel images
//     are published and a custom tag is requested.
//   - The downloadList must contain exactly the requested tag in Deckhouse,
//     DeckhouseInstall and DeckhouseInstallStandalone.
//   - DeckhouseReleaseChannel is allowed to be empty: there is nothing to
//     download from release-channel/ when the registry never published any.
func TestPullPlatform_DryRun_CustomTag_NoReleaseChannelsInRegistry(t *testing.T) {
	const prTag = "pr17405"

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(devRegistryStub(prTag), &Options{TargetTag: prTag}, logger, userLogger)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err,
		"--deckhouse-tag must short-circuit release-channel discovery; missing channels are not fatal")

	rootURL := stubRootURL
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+prTag)
	assert.Contains(t, svc.downloadList.DeckhouseInstall, rootURL+"/install:"+prTag)
	assert.Contains(t, svc.downloadList.DeckhouseInstallStandalone, rootURL+"/install-standalone:"+prTag)
	assert.Len(t, svc.downloadList.Deckhouse, 1,
		"only the requested tag must be enqueued for download")
}

// TestPullPlatform_DryRun_SemverTag_NoReleaseChannelsInRegistry is the same
// regression as TestPullPlatform_DryRun_CustomTag_NoReleaseChannelsInRegistry
// but for the semver-shaped --deckhouse-tag value (e.g. v1.69.0). The legacy
// v0.27.0 code failed identically for both tag shapes because the bug lived in
// the unconditional channel loop, not in tag classification.
func TestPullPlatform_DryRun_SemverTag_NoReleaseChannelsInRegistry(t *testing.T) {
	const semverTag = "v1.69.0"

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(devRegistryStub(semverTag), &Options{TargetTag: semverTag}, logger, userLogger)
	err := svc.PullPlatform(context.Background())
	require.NoError(t, err,
		"--deckhouse-tag must short-circuit release-channel discovery for semver tags too")

	rootURL := stubRootURL
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+semverTag)
	assert.Contains(t, svc.downloadList.DeckhouseInstall, rootURL+"/install:"+semverTag)
	assert.Contains(t, svc.downloadList.DeckhouseInstallStandalone, rootURL+"/install-standalone:"+semverTag)
}

// TestService_versionsToMirror_CustomTag_NoReleaseChannels asserts at the
// versionsToMirror level (one layer below PullPlatform) that requesting a
// custom tag never propagates ErrSomeChannelsFailed as a hard error. This
// pins down the contract independently of validatePlatformAccess so that
// future refactors of the access check cannot mask a regression in the
// channel-discovery short-circuit.
func TestService_versionsToMirror_CustomTag_NoReleaseChannels(t *testing.T) {
	const prTag = "pr17405"

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(devRegistryStub(prTag), &Options{TargetTag: prTag}, logger, userLogger)

	result, err := svc.versionsToMirror(context.Background(), []string{prTag})
	require.NoError(t, err,
		"versionsToMirror with explicit tag must tolerate registries without any release channels")
	require.NotNil(t, result)

	assert.Empty(t, result.Versions, "no semver versions should be discovered when channels are absent")
	assert.Empty(t, result.Channels, "no channels should be matched when the registry has none")
	assert.Equal(t, []string{prTag}, result.CustomTags,
		"the requested non-semver tag must be propagated as a custom tag")
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

// ---- --include-platform: semver constraint ----

// includePlatformConstraint is a tiny test helper that constructs the same
// VersionConstraint the CLI would build out of --include-platform.
func includePlatformConstraint(t *testing.T, expr string) modules.VersionConstraint {
	t.Helper()
	c, err := modules.ParseVersionConstraint(expr)
	require.NoError(t, err, "constraint %q must parse", expr)
	return c
}

// TestPullPlatform_DryRun_IncludePlatform_RangeFiltersChannelsAndVersions
// covers the user-facing scenario from the original feature request: pull
// an inclusive [1.69, 1.71] window of platform releases (the equivalent of
// "1.64..1.68" against the stub registry whose newest tag is v1.72.10).
// Channel snapshots above the upper bound (alpha → v1.72.10) and below the
// lower bound (rock-solid → v1.68.0) must be excluded; everything in between
// must be present, plus the latest patch per minor inside the range.
func TestPullPlatform_DryRun_IncludePlatform_RangeFiltersChannelsAndVersions(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(
		localfake.NewRegistryClientStub(),
		&Options{IncludeConstraint: includePlatformConstraint(t, ">=1.69 <=1.71")},
		logger,
		userLogger,
	)
	require.NoError(t, svc.PullPlatform(context.Background()))

	rootURL := stubRootURL
	for _, ver := range []string{"v1.71.0", "v1.70.0", "v1.69.0"} {
		assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+ver,
			"version %s is within the include-platform window and must be included", ver)
	}
	for _, ver := range []string{"v1.72.10", "v1.68.0"} {
		assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":"+ver,
			"version %s is outside the include-platform window and must be excluded", ver)
	}
	for _, ch := range []string{"beta", "early-access", "stable"} {
		assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
			rootURL+"/release-channel:"+ch,
			"channel %s points inside the include-platform window and must survive", ch)
	}
	for _, ch := range []string{"alpha", "rock-solid"} {
		assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel,
			rootURL+"/release-channel:"+ch,
			"channel %s points outside the include-platform window and must be dropped", ch)
	}
}

// TestPullPlatform_DryRun_IncludePlatform_TildeKeepsSingleMinor exercises the
// tilde shorthand (~X.Y.Z → >=X.Y.Z <X.(Y+1).0) so the constraint dialect we
// expose matches what --include-module accepts.
func TestPullPlatform_DryRun_IncludePlatform_TildeKeepsSingleMinor(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(
		localfake.NewRegistryClientStub(),
		&Options{IncludeConstraint: includePlatformConstraint(t, "~1.69.0")},
		logger,
		userLogger,
	)
	require.NoError(t, svc.PullPlatform(context.Background()))

	rootURL := stubRootURL
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":v1.69.0",
		"v1.69.0 is the only minor that satisfies ~1.69.0 in the stub")
	for _, ver := range []string{"v1.72.10", "v1.71.0", "v1.70.0", "v1.68.0"} {
		assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":"+ver,
			"version %s does not satisfy ~1.69.0 and must be excluded", ver)
	}
}

// TestPullPlatform_DryRun_IncludePlatform_InclusiveAnchorPreserved encodes the
// "anchors round-trip" contract for the platform path. With a stub that does
// not expose multiple patches per minor the latest-patch filter is a no-op,
// but exercising the anchor extractor here means a future bug that drops
// `>=` literals will surface as a clear failure.
func TestPullPlatform_DryRun_IncludePlatform_InclusiveAnchorPreserved(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(
		localfake.NewRegistryClientStub(),
		&Options{IncludeConstraint: includePlatformConstraint(t, ">=1.69.0 <=1.70.0")},
		logger,
		userLogger,
	)
	require.NoError(t, svc.PullPlatform(context.Background()))

	rootURL := stubRootURL
	for _, ver := range []string{"v1.69.0", "v1.70.0"} {
		assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":"+ver,
			"anchor version %s must be preserved by include-platform", ver)
	}
	for _, ver := range []string{"v1.72.10", "v1.71.0", "v1.68.0"} {
		assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":"+ver,
			"version %s is outside the anchored window", ver)
	}
}

// TestPullPlatform_DryRun_IncludePlatform_ExactTagBehavesLikeTargetTag pins
// down the contract that =vX.Y.Z is operationally identical to
// --deckhouse-tag=vX.Y.Z. The exact-tag synthesis happens in PullPlatform
// before validation, so the test asserts both the version download list
// (only the pinned tag is present) and the channel propagation block (every
// default channel points at the pinned tag, mirroring --deckhouse-tag).
func TestPullPlatform_DryRun_IncludePlatform_ExactTagBehavesLikeTargetTag(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	svc := newDryRunService(
		localfake.NewRegistryClientStub(),
		&Options{IncludeConstraint: includePlatformConstraint(t, "=v1.69.0")},
		logger,
		userLogger,
	)
	require.NoError(t, svc.PullPlatform(context.Background()))

	rootURL := stubRootURL
	assert.Contains(t, svc.downloadList.Deckhouse, rootURL+":v1.69.0")
	for _, ver := range []string{"v1.72.10", "v1.71.0", "v1.70.0", "v1.68.0"} {
		assert.NotContains(t, svc.downloadList.Deckhouse, rootURL+":"+ver,
			"only the exactly-pinned tag must be enqueued for download")
	}
	// Synthesized TargetTag flows through findTagsToMirror which matches the
	// pinned tag against channel snapshots; stable points at v1.69.0 in the
	// stub so it must show up in the release-channel layout.
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
		rootURL+"/release-channel:stable",
		"=v1.69.0 must propagate to stable because v1.69.0 is the stable channel version in the stub")
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

// TestPullPlatform_FEEdition_DownloadListUsesEditionRoot is the regression for
// the report that `d8 mirror pull --source=.../deckhouse/fe` enqueues entries
// like "registry.deckhouse.ru/deckhouse/release-channel:<ch>" (without the
// "fe" segment) alongside the correct
// "registry.deckhouse.ru/deckhouse/fe/release-channel:<ch>" ones.
//
// The root cause was platform.NewService seeding the downloadList rootURL
// from registryService.GetRoot() (non-edition root) while
// getReleaseChannelInfo fed the same map with edition-scoped
// keys obtained through deckhouseService. The fix is to seed the rootURL
// with registryService.GetEditionRoot() so every key in the downloadList
// carries the edition segment.
//
// We exercise the contract directly at the platform.NewService boundary so
// the test does not depend on the fake registry's WithSegment semantics
// (the in-memory fake intentionally exposes only the host via GetRegistry,
// which would mask both the bug and the fix).
func TestPullPlatform_FEEdition_DownloadListUsesEditionRoot(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	c := pkgclient.NewFromOptions("registry.deckhouse.ru/deckhouse")
	regSvc := registryservice.NewService(c, pkg.FEEdition, logger)

	svc := NewService(
		regSvc,
		t.TempDir(),
		&Options{BundleDir: t.TempDir(), DryRun: true},
		logger,
		userLogger,
	)

	const editionRoot = "registry.deckhouse.ru/deckhouse/fe"

	// The downloadList rootURL must point at the edition sub-tree. We probe it
	// through FillDeckhouseImages, which uses rootURL to build the entry keys.
	const probe = "v1.69.0"
	svc.downloadList.FillDeckhouseImages([]string{probe})

	assert.Contains(t, svc.downloadList.Deckhouse, editionRoot+":"+probe,
		"FE main Deckhouse entry must live under the edition sub-tree")
	assert.Contains(t, svc.downloadList.DeckhouseInstall,
		editionRoot+"/install:"+probe,
		"FE install entry must live under the edition sub-tree")
	assert.Contains(t, svc.downloadList.DeckhouseInstallStandalone,
		editionRoot+"/install-standalone:"+probe,
		"FE standalone install entry must live under the edition sub-tree")
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
		editionRoot+"/release-channel:"+probe,
		"FE release-channel version tag must live under the edition sub-tree")

	const bareRoot = "registry.deckhouse.ru/deckhouse"
	// Spot-check that no key was written under the bare (non-edition) root.
	// These are the exact shapes that surfaced as duplicate pulls in the bug report.
	assert.NotContains(t, svc.downloadList.Deckhouse, bareRoot+":"+probe)
	assert.NotContains(t, svc.downloadList.DeckhouseInstall, bareRoot+"/install:"+probe)
	assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel,
		bareRoot+"/release-channel:"+probe)

	// FillForChannels populates channel aliases — same rootURL must apply.
	svc.downloadList.FillForChannels([]string{"stable", "alpha"})
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
		editionRoot+"/release-channel:stable",
		"FE release-channel alias must live under the edition sub-tree")
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
		editionRoot+"/release-channel:alpha",
		"FE release-channel alias must live under the edition sub-tree")
	assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel,
		bareRoot+"/release-channel:stable",
		"non-edition-scoped duplicate must not be enqueued")
}

// TestPullPlatform_NoEdition_DownloadListUsesBareRoot is the companion of
// TestPullPlatform_FEEdition_DownloadListUsesEditionRoot for the NoEdition
// case. When no edition is configured GetEditionRoot must collapse to the
// bare root so the downloadList keeps producing the original key shape used
// by community / CSE sources.
func TestPullPlatform_NoEdition_DownloadListUsesBareRoot(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	const bareRoot = "registry.example.com/deckhouse"
	c := pkgclient.NewFromOptions(bareRoot)
	regSvc := registryservice.NewService(c, pkg.NoEdition, logger)

	svc := NewService(
		regSvc,
		t.TempDir(),
		&Options{BundleDir: t.TempDir(), DryRun: true},
		logger,
		userLogger,
	)

	const probe = "v1.69.0"
	svc.downloadList.FillDeckhouseImages([]string{probe})

	assert.Contains(t, svc.downloadList.Deckhouse, bareRoot+":"+probe,
		"with NoEdition the main Deckhouse entry must live at the bare root")
	assert.Contains(t, svc.downloadList.DeckhouseInstall,
		bareRoot+"/install:"+probe,
		"with NoEdition install entries must live under the bare root")
	assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
		bareRoot+"/release-channel:"+probe,
		"with NoEdition release-channel entries must live under the bare root")
}

// TestPullPlatform_AllEditions_DownloadListUsesEditionRoot sweeps every
// concrete edition so that any future addition to pkg.Edition that is not
// wired through to platform.NewService surfaces here as an explicit failure.
func TestPullPlatform_AllEditions_DownloadListUsesEditionRoot(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	const bareRoot = "registry.deckhouse.ru/deckhouse"
	editions := []pkg.Edition{
		pkg.FEEdition,
		pkg.EEEdition,
		pkg.SEEdition,
		pkg.SEPlusEdition,
		pkg.BEEdition,
		pkg.CEEdition,
	}

	const probe = "v1.69.0"

	for _, edition := range editions {
		t.Run("edition="+edition.String(), func(t *testing.T) {
			c := pkgclient.NewFromOptions(bareRoot)
			regSvc := registryservice.NewService(c, edition, logger)
			svc := NewService(
				regSvc,
				t.TempDir(),
				&Options{BundleDir: t.TempDir(), DryRun: true},
				logger,
				userLogger,
			)

			svc.downloadList.FillDeckhouseImages([]string{probe})
			svc.downloadList.FillForChannels([]string{"stable"})

			editionRoot := bareRoot + "/" + edition.String()
			assert.Contains(t, svc.downloadList.Deckhouse, editionRoot+":"+probe)
			assert.Contains(t, svc.downloadList.DeckhouseInstall,
				editionRoot+"/install:"+probe)
			assert.Contains(t, svc.downloadList.DeckhouseInstallStandalone,
				editionRoot+"/install-standalone:"+probe)
			assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
				editionRoot+"/release-channel:"+probe)
			assert.Contains(t, svc.downloadList.DeckhouseReleaseChannel,
				editionRoot+"/release-channel:stable")

			assert.NotContains(t, svc.downloadList.Deckhouse, bareRoot+":"+probe,
				"%s edition must not leak a duplicate at the bare root", edition)
			assert.NotContains(t, svc.downloadList.DeckhouseReleaseChannel,
				bareRoot+"/release-channel:stable",
				"%s edition must not leak a duplicate channel alias at the bare root", edition)
		})
	}
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
