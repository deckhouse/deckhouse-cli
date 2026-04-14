// Copyright 2026 Flant JSC
// SPDX-License-Identifier: Apache-2.0

package mirror

import (
	"context"
	"log/slog"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	localreg "github.com/deckhouse/deckhouse-cli/pkg/registry"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"
	localfake "github.com/deckhouse/deckhouse-cli/pkg/fake"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

// pullStubRootURL matches the default host used by NewRegistryClientStub.
const pullStubRootURL = "registry.deckhouse.ru/deckhouse/fe"

// newPullService builds a PullService backed by the given stub client using
// pkg.NoEdition (the stub's root URL already includes the edition).
func newPullService(
	t *testing.T,
	stubClient localreg.Client,
	targetTag string,
	options *PullServiceOptions,
) *PullService {
	t.Helper()
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)
	regSvc := registryservice.NewService(stubClient, pkg.NoEdition, logger)
	return NewPullService(
		regSvc,
		t.TempDir(),
		targetTag,
		options,
		logger,
		userLogger,
	)
}

// fullStub returns a stub that has data in all four service areas:
//   - platform (root, release-channel, install, install-standalone)
//   - installer  ("installer" repo at root, tag "latest")
//   - security   ("security/trivy-db" repo, tag "2")
//   - modules    ("modules" repo with two module names as tags)
func fullStub() localreg.Client {
	reg := upfake.NewRegistry(pullStubRootURL)

	// ---- platform: release-channel ----
	channels := map[string]string{
		"alpha":        "v1.72.10",
		"beta":         "v1.71.0",
		"early-access": "v1.70.0",
		"stable":       "v1.69.0",
		"rock-solid":   "v1.68.0",
	}
	for ch, ver := range channels {
		img := upfake.NewImageBuilder().
			WithFile("version.json", `{"version":"`+ver+`"}`).
			MustBuild()
		reg.MustAddImage("release-channel", ch, img)
		// Version-tagged release-channel images are required by non-DryRun full-discovery pull.
		reg.MustAddImage("release-channel", ver, img)
	}

	// ---- platform: root + install + install-standalone ----
	platformImg := func(ver string) v1.Image {
		return upfake.NewImageBuilder().
			WithFile("version.json", `{"version":"`+ver+`"}`).
			WithFile("deckhouse/candi/images_digests.json", `{}`).
			MustBuild()
	}
	for _, rt := range []struct{ tag, ver string }{
		{"alpha", "v1.72.10"}, {"beta", "v1.71.0"}, {"early-access", "v1.70.0"},
		{"stable", "v1.69.0"}, {"rock-solid", "v1.68.0"},
		{"v1.72.10", "v1.72.10"}, {"v1.71.0", "v1.71.0"}, {"v1.70.0", "v1.70.0"},
		{"v1.69.0", "v1.69.0"}, {"v1.68.0", "v1.68.0"},
	} {
		img := platformImg(rt.ver)
		reg.MustAddImage("", rt.tag, img)
		reg.MustAddImage("install", rt.tag, img)
		reg.MustAddImage("install-standalone", rt.tag, img)
	}

	// ---- installer: "installer" repo (used by Service.InstallerService) ----
	installerImg := upfake.NewImageBuilder().MustBuild()
	reg.MustAddImage("installer", "latest", installerImg)
	reg.MustAddImage("installer", "v1.72.10", installerImg)
	reg.MustAddImage("installer", "v1.69.0", installerImg)

	// ---- security: trivy-db tag "2" ----
	trivyImg := upfake.NewImageBuilder().MustBuild()
	reg.MustAddImage("security/trivy-db", "2", trivyImg)

	// ---- modules: two module names as tags ----
	modImg := upfake.NewImageBuilder().MustBuild()
	reg.MustAddImage("modules", "cert-manager", modImg)
	reg.MustAddImage("modules", "ingress-nginx", modImg)

	return pkgclient.Adapt(upfake.NewClient(reg))
}

// ---------------------------------------------------------------------------
// Error path tests
// ---------------------------------------------------------------------------

// TestPull_EmptyRegistry_ReturnsPlatformError verifies that Pull returns an
// error wrapping "pull platform" when the registry is empty and SkipPlatform
// is false.
func TestPull_EmptyRegistry_ReturnsPlatformError(t *testing.T) {
	emptyStub := pkgclient.Adapt(upfake.NewClient(upfake.NewRegistry(pullStubRootURL)))

	svc := newPullService(t, emptyStub, "v1.69.0", &PullServiceOptions{
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
	})

	err := svc.Pull(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pull platform")
}

// TestPull_MissingTargetTag_ReturnsPlatformError verifies that Pull wraps a
// platform error that names the missing tag.
func TestPull_MissingTargetTag_ReturnsPlatformError(t *testing.T) {
	svc := newPullService(t, localfake.NewRegistryClientStub(), "v9.99.0", &PullServiceOptions{
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
	})

	err := svc.Pull(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pull platform")
	assert.Contains(t, err.Error(), "v9.99.0")
}

// TestPull_SuspendedChannel_ReturnsError verifies Pull returns an error
// wrapping "suspended" when a release channel is suspended and IgnoreSuspend
// is false.
func TestPull_SuspendedChannel_ReturnsError(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	reg.MustAddImage("release-channel", "alpha",
		upfake.NewImageBuilder().
			WithFile("version.json", `{"version":"v1.72.10","suspend":true}`).
			MustBuild(),
	)
	for _, ch := range []string{"beta", "early-access", "stable", "rock-solid"} {
		reg.MustAddImage("release-channel", ch,
			upfake.NewImageBuilder().WithFile("version.json", `{"version":"v1.69.0"}`).MustBuild(),
		)
	}
	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.72.10"}`).
		WithFile("deckhouse/candi/images_digests.json", `{}`).
		MustBuild()
	for _, tag := range []string{"alpha", "v1.72.10", "v1.69.0"} {
		reg.MustAddImage("", tag, img)
		reg.MustAddImage("install", tag, img)
		reg.MustAddImage("install-standalone", tag, img)
	}

	svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
		IgnoreSuspend: false,
	})

	err := svc.Pull(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pull platform")
	assert.Contains(t, err.Error(), "suspended")
}

// ---------------------------------------------------------------------------
// Success path tests
// ---------------------------------------------------------------------------

// TestPull_DefaultStub_Succeeds verifies Pull returns nil using the
// default stub with a specific semver tag and security/modules/installer
// skipped (those repos are absent from the default stub).
func TestPull_DefaultStub_Succeeds(t *testing.T) {
	svc := newPullService(t, localfake.NewRegistryClientStub(), "v1.69.0", &PullServiceOptions{
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_AllChannelTags verifies Pull succeeds for each of the five
// standard release-channel tags as TargetTag.
func TestPull_AllChannelTags(t *testing.T) {
	for _, ch := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
		ch := ch
		t.Run(ch, func(t *testing.T) {
			svc := newPullService(t, localfake.NewRegistryClientStub(), ch, &PullServiceOptions{
				SkipSecurity:  true,
				SkipModules:   true,
				SkipInstaller: true,
			})
			require.NoError(t, svc.Pull(context.Background()))
		})
	}
}

// TestPull_EmptyTargetTag_FullDiscovery verifies full-discovery mode
// (no TargetTag) returns nil.
func TestPull_EmptyTargetTag_FullDiscovery(t *testing.T) {
	svc := newPullService(t, localfake.NewRegistryClientStub(), "", &PullServiceOptions{
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_IgnoreSuspend_AllowsSuspendedChannel verifies that
// IgnoreSuspend=true makes Pull succeed even when a channel is suspended.
func TestPull_IgnoreSuspend_AllowsSuspendedChannel(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	channelVersions := map[string]struct {
		ver     string
		suspend bool
	}{
		"alpha":        {"v1.72.10", true},
		"beta":         {"v1.71.0", false},
		"early-access": {"v1.70.0", false},
		"stable":       {"v1.69.0", false},
		"rock-solid":   {"v1.68.0", false},
	}
	for ch, data := range channelVersions {
		content := `{"version":"` + data.ver + `"}`
		if data.suspend {
			content = `{"version":"` + data.ver + `","suspend":true}`
		}
		img := upfake.NewImageBuilder().WithFile("version.json", content).MustBuild()
		reg.MustAddImage("release-channel", ch, img)
		// Version-tagged release-channel images are required by non-DryRun full-discovery pull.
		reg.MustAddImage("release-channel", data.ver, img)
	}
	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.72.10"}`).
		WithFile("deckhouse/candi/images_digests.json", `{}`).
		MustBuild()
	for _, tag := range []string{"alpha", "beta", "early-access", "stable", "rock-solid",
		"v1.72.10", "v1.71.0", "v1.70.0", "v1.69.0", "v1.68.0"} {
		reg.MustAddImage("", tag, img)
		reg.MustAddImage("install", tag, img)
		reg.MustAddImage("install-standalone", tag, img)
	}

	svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
		IgnoreSuspend: true,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_SkipPlatform_AlwaysSucceeds verifies that SkipPlatform=true
// makes Pull succeed even with an empty registry.
func TestPull_SkipPlatform_AlwaysSucceeds(t *testing.T) {
	emptyStub := pkgclient.Adapt(upfake.NewClient(upfake.NewRegistry(pullStubRootURL)))

	svc := newPullService(t, emptyStub, "v1.69.0", &PullServiceOptions{
		SkipPlatform:  true,
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_SkipAll_EmptyRegistry verifies Pull succeeds when every
// service is skipped, regardless of registry contents.
func TestPull_SkipAll_EmptyRegistry(t *testing.T) {
	emptyStub := pkgclient.Adapt(upfake.NewClient(upfake.NewRegistry(pullStubRootURL)))

	svc := newPullService(t, emptyStub, "", &PullServiceOptions{
		SkipPlatform:  true,
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_InstallerGracefulSkip verifies that when the "installer"
// repo is absent Pull still succeeds (PullInstaller logs a warning and returns nil).
func TestPull_InstallerGracefulSkip(t *testing.T) {
	// The default stub has no "installer" repo; installer access validation
	// returns ErrImageNotFound which PullInstaller treats as a graceful skip.
	svc := newPullService(t, localfake.NewRegistryClientStub(), "v1.69.0", &PullServiceOptions{
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: false,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_SecurityGracefulSkip verifies that when the security repo is
// absent Pull still succeeds (validateSecurityAccess gracefully skips on
// ErrImageNotFound).
func TestPull_SecurityGracefulSkip(t *testing.T) {
	svc := newPullService(t, localfake.NewRegistryClientStub(), "v1.69.0", &PullServiceOptions{
		SkipPlatform:  true,
		SkipModules:   true,
		SkipInstaller: true,
		SkipSecurity:  false,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_ModulesGracefulSkip verifies that when no modules exist in
// the registry Pull still succeeds (PullModules logs a warning and returns nil).
func TestPull_ModulesGracefulSkip(t *testing.T) {
	svc := newPullService(t, localfake.NewRegistryClientStub(), "v1.69.0", &PullServiceOptions{
		SkipPlatform:  true,
		SkipSecurity:  true,
		SkipInstaller: true,
		SkipModules:   false,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_SkipVexImages_DoesNotError verifies the SkipVexImages flag
// does not cause Pull to fail.
func TestPull_SkipVexImages_DoesNotError(t *testing.T) {
	svc := newPullService(t, localfake.NewRegistryClientStub(), "v1.69.0", &PullServiceOptions{
		SkipVexImages: true,
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// ---------------------------------------------------------------------------
// Full-stub tests — all four services active simultaneously
// ---------------------------------------------------------------------------

// TestPull_FullStub_AllServicesActive verifies Pull returns nil when
// all four services are enabled and the stub has data for each.
func TestPull_FullStub_AllServicesActive(t *testing.T) {
	svc := newPullService(t, fullStub(), "v1.69.0", &PullServiceOptions{
		SkipVexImages: true,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_FullStub_FullDiscovery verifies full-discovery mode (empty
// TargetTag) succeeds with all services enabled.
func TestPull_FullStub_FullDiscovery(t *testing.T) {
	svc := newPullService(t, fullStub(), "", &PullServiceOptions{
		SkipVexImages: true,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_FullStub_CustomTag verifies Pull succeeds with a custom
// (non-semver, non-channel) tag when that tag exists in the registry.
func TestPull_FullStub_CustomTag(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.72.10"}`).
		WithFile("deckhouse/candi/images_digests.json", `{}`).
		MustBuild()
	reg.MustAddImage("", "pr12345", img)
	reg.MustAddImage("install", "pr12345", img)
	reg.MustAddImage("install-standalone", "pr12345", img)

	svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "pr12345", &PullServiceOptions{
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: true,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_FullStub_InstallerPresent verifies that when the "installer"
// repo exists Pull runs PullInstaller successfully.
func TestPull_FullStub_InstallerPresent(t *testing.T) {
	svc := newPullService(t, fullStub(), "v1.69.0", &PullServiceOptions{
		InstallerTag:  "v1.69.0",
		SkipSecurity:  true,
		SkipModules:   true,
		SkipInstaller: false,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_FullStub_SecurityPresent verifies that when the security
// repo exists Pull runs PullSecurity successfully.
func TestPull_FullStub_SecurityPresent(t *testing.T) {
	svc := newPullService(t, fullStub(), "v1.69.0", &PullServiceOptions{
		SkipPlatform:  true,
		SkipModules:   true,
		SkipInstaller: true,
		SkipSecurity:  false,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_FullStub_ModulesPresent verifies that when modules exist in
// the registry Pull runs PullModules successfully.
func TestPull_FullStub_ModulesPresent(t *testing.T) {
	svc := newPullService(t, fullStub(), "", &PullServiceOptions{
		SkipPlatform:  true,
		SkipSecurity:  true,
		SkipInstaller: true,
		SkipModules:   false,
	})

	require.NoError(t, svc.Pull(context.Background()))
}

// TestPull_FullStub_AllServices verifies Pull returns nil when all four
// services are enabled with installer tag set.
func TestPull_FullStub_AllServices(t *testing.T) {
	svc := newPullService(t, fullStub(), "v1.69.0", &PullServiceOptions{
		InstallerTag:  "v1.69.0",
		SkipVexImages: true,
	})

	require.NoError(t, svc.Pull(context.Background()))
}
