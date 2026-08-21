// Copyright 2026 Flant JSC
// SPDX-License-Identifier: Apache-2.0

package mirror

// End-to-end tests for the plugins leg of the pull pipeline. Unlike the unit
// suites in internal/mirror/plugins, these run the whole PullService.Pull:
// the modules phase discovers module versions from release channels, records
// them in its stats, and the plugins phase resolves the catalog against those
// stats. The tests pin the cross-phase handoff and the produced artifacts
// (bundle tars, stats, summary provenance), not the resolver internals -
// those are covered by internal/mirror/plugins/resolver_test.go.

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
)

// pluginsCatalogRepo is where PluginsService looks for the catalog, relative
// to the registry root (plugins are not edition-scoped).
const pluginsCatalogRepo = "deckhouse-cli/plugins"

// e2eBuiltins mirrors pluginBuiltinCommands from the cmd layer: built-in d8
// commands that satisfy a same-named plugin dependency without being pulled.
var e2eBuiltins = []string{"delivery-kit", "package"}

// ---------------------------------------------------------------------------
// Contracts
// ---------------------------------------------------------------------------

// The two postgresql-mgr versions split the module version range: v1.1.0
// serves postgresql <1.10.0, v1.2.0 serves >=1.10.0.
const mgrContractForOldPostgres = `{
	"name": "postgresql-mgr", "version": "v1.1.0",
	"requirements": {"modules": {"mandatory": [{"name": "postgresql", "constraint": ">=1.0.0 <1.10.0"}]}}
}`

const mgrContractForNewPostgres = `{
	"name": "postgresql-mgr", "version": "v1.2.0",
	"requirements": {"modules": {"mandatory": [{"name": "postgresql", "constraint": ">=1.10.0"}]}}
}`

// pg-mgr depends on another catalog plugin and on a built-in d8 command.
const pgMgrContractWithDeps = `{
	"name": "pg-mgr", "version": "v1.2.0",
	"requirements": {
		"modules": {"mandatory": [{"name": "postgresql", "constraint": ">=1.0.0"}]},
		"plugins": {"mandatory": [{"name": "pg-backup", "constraint": ">=1.0.0"}, {"name": "delivery-kit"}]}
	}
}`

const pgBackupContract = `{"name": "pg-backup", "version": "v1.0.5"}`

// cond-tool only conditionally mentions postgresql: a conditional mention
// must never trigger auto-selection.
const condToolContract = `{
	"name": "cond-tool", "version": "v1.0.0",
	"requirements": {"modules": {"conditional": [{"name": "postgresql", "constraint": ">=1.0.0"}]}}
}`

// old-mgr is triggered by postgresql but no version pairs with v1.5.0.
const oldMgrContract = `{
	"name": "old-mgr", "version": "v0.9.0",
	"requirements": {"modules": {"mandatory": [{"name": "postgresql", "constraint": "<1.0.0"}]}}
}`

const standaloneToolStableContract = `{"name": "standalone-tool", "version": "v1.0.0"}`

const standaloneToolRCContract = `{"name": "standalone-tool", "version": "v2.0.0-rc.1"}`

// dh-tool is triggered by postgresql but also constrains the platform version.
const dhToolContract = `{
	"name": "dh-tool", "version": "v1.0.0",
	"requirements": {
		"modules": {"mandatory": [{"name": "postgresql", "constraint": ">=1.0.0"}]},
		"deckhouse": {"constraint": ">=1.70.0"}
	}
}`

// ---------------------------------------------------------------------------
// Tests: module versions reach the plugin resolver through Pull
// ---------------------------------------------------------------------------

// TestPullE2E_ModuleVersionsReachPluginResolver pins the modules->plugins
// handoff: the module versions discovered from release channels by the
// modules phase must reach the plugin resolver, which pairs each bundled
// module version with a compatible plugin version. Both selected versions
// must land in one plugin tar with module provenance in the summary.
func TestPullE2E_ModuleVersionsReachPluginResolver(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	addModule(reg, "postgresql", map[string]string{"stable": "v1.5.0", "alpha": "v1.10.0"})
	addPluginVersion(reg, "postgresql-mgr", "v1.1.0", mgrContractForOldPostgres)
	addPluginVersion(reg, "postgresql-mgr", "v1.2.0", mgrContractForNewPostgres)

	bundleDir := t.TempDir()
	svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		SkipPlatform:   true,
		SkipSecurity:   true,
		SkipInstaller:  true,
		SkipVexImages:  true,
		BundleDir:      bundleDir,
		PluginBuiltins: e2eBuiltins,
	})

	summary, err := svc.Pull(context.Background())
	require.NoError(t, err)
	require.NotNil(t, summary)

	// The modules phase recorded both channel versions.
	require.Len(t, summary.Modules.Modules, 1)
	assert.Equal(t, "postgresql", summary.Modules.Modules[0].Name)
	assert.ElementsMatch(t, []string{"v1.5.0", "v1.10.0"}, summary.Modules.Modules[0].Versions)

	// The plugins phase paired each bundled module version with a plugin version.
	assert.True(t, summary.Plugins.Attempted)
	assert.False(t, summary.Plugins.Skipped)
	require.Len(t, summary.Plugins.Plugins, 1)

	plugin := summary.Plugins.Plugins[0]
	assert.Equal(t, "postgresql-mgr", plugin.Name)
	assert.Equal(t, 2, plugin.Images)
	assert.Equal(t, 2, summary.Plugins.TotalImages)

	require.Len(t, plugin.Versions, 2, "one plugin version per bundled module version")
	assert.Equal(t, "v1.2.0", plugin.Versions[0].Version, "versions are newest first")
	assert.Equal(t, []PluginReason{{Kind: "module", Subject: "postgresql", Constraint: ">=1.10.0"}},
		plugin.Versions[0].Reasons)
	assert.Equal(t, "v1.1.0", plugin.Versions[1].Version)
	assert.Equal(t, []PluginReason{{Kind: "module", Subject: "postgresql", Constraint: ">=1.0.0 <1.10.0"}},
		plugin.Versions[1].Reasons)

	// Artifacts: the module tar and one plugin tar holding exactly the
	// selected versions.
	assert.FileExists(t, filepath.Join(bundleDir, "module-postgresql.tar"))

	pluginTar := filepath.Join(bundleDir, "plugin-postgresql-mgr.tar")
	require.FileExists(t, pluginTar)
	assert.ElementsMatch(t, []string{"v1.1.0", "v1.2.0"}, pluginTarShortTags(t, pluginTar),
		"the bundle must hold exactly the versions the resolver picked")
}

// TestPullE2E_DependencyChainAndBuiltin verifies that PluginBuiltins wiring
// reaches the resolver through PullServiceOptions: a mandatory plugin
// dependency is pulled with dependency provenance, while a same-named
// built-in command dependency is satisfied by presence and produces no tar.
func TestPullE2E_DependencyChainAndBuiltin(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	addModule(reg, "postgresql", map[string]string{"stable": "v1.5.0"})
	addPluginVersion(reg, "pg-mgr", "v1.2.0", pgMgrContractWithDeps)
	addPluginVersion(reg, "pg-backup", "v1.0.5", pgBackupContract)

	bundleDir := t.TempDir()
	svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		SkipPlatform:   true,
		SkipSecurity:   true,
		SkipInstaller:  true,
		SkipVexImages:  true,
		BundleDir:      bundleDir,
		PluginBuiltins: e2eBuiltins,
	})

	summary, err := svc.Pull(context.Background())
	require.NoError(t, err)

	require.Len(t, summary.Plugins.Plugins, 2, "the triggered plugin and its dependency")

	backup, mgr := summary.Plugins.Plugins[0], summary.Plugins.Plugins[1]
	assert.Equal(t, "pg-backup", backup.Name, "plugins are sorted by name")
	assert.Equal(t, "pg-mgr", mgr.Name)

	require.Len(t, mgr.Versions, 1)
	assert.Equal(t, "v1.2.0", mgr.Versions[0].Version)
	assert.Equal(t, []PluginReason{{Kind: "module", Subject: "postgresql", Constraint: ">=1.0.0"}},
		mgr.Versions[0].Reasons)

	require.Len(t, backup.Versions, 1)
	assert.Equal(t, "v1.0.5", backup.Versions[0].Version)
	assert.Equal(t, []PluginReason{{Kind: "dependency", Subject: "pg-mgr@v1.2.0", Constraint: ">=1.0.0"}},
		backup.Versions[0].Reasons)

	assert.Equal(t, 2, summary.Plugins.TotalImages)

	assert.FileExists(t, filepath.Join(bundleDir, "plugin-pg-mgr.tar"))
	assert.FileExists(t, filepath.Join(bundleDir, "plugin-pg-backup.tar"))
	assert.NoFileExists(t, filepath.Join(bundleDir, "plugin-delivery-kit.tar"),
		"a built-in command dependency must not be pulled")
}

// TestPullE2E_NothingExtraAndSkipReporting pins the "nothing extra"
// principle at the pipeline boundary: a conditional-only mention does not
// select a plugin, and a triggered plugin with no compatible version is
// reported in the summary as skipped, with no tar written.
func TestPullE2E_NothingExtraAndSkipReporting(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	addModule(reg, "postgresql", map[string]string{"stable": "v1.5.0"})
	addPluginVersion(reg, "cond-tool", "v1.0.0", condToolContract)
	addPluginVersion(reg, "old-mgr", "v0.9.0", oldMgrContract)

	bundleDir := t.TempDir()
	svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		SkipPlatform:   true,
		SkipSecurity:   true,
		SkipInstaller:  true,
		SkipVexImages:  true,
		BundleDir:      bundleDir,
		PluginBuiltins: e2eBuiltins,
	})

	summary, err := svc.Pull(context.Background())
	require.NoError(t, err)

	assert.True(t, summary.Plugins.Attempted)
	assert.Empty(t, summary.Plugins.Plugins, "neither plugin qualifies for the bundle")

	// old-mgr was triggered by postgresql but has no compatible version: the
	// summary must spell out why it is missing from an air-gapped bundle.
	require.Len(t, summary.Plugins.SkippedPlugins, 1)
	assert.Equal(t, "old-mgr", summary.Plugins.SkippedPlugins[0].Name)
	assert.NotEmpty(t, summary.Plugins.SkippedPlugins[0].Reason)

	// cond-tool is simply irrelevant: not selected, not reported as skipped.
	for _, skip := range summary.Plugins.SkippedPlugins {
		assert.NotEqual(t, "cond-tool", skip.Name, "a conditional mention must not put the plugin in play")
	}

	pluginTars, err := filepath.Glob(filepath.Join(bundleDir, "plugin-*.tar"))
	require.NoError(t, err)
	assert.Empty(t, pluginTars, "no plugin tars when nothing is selected")
}

// ---------------------------------------------------------------------------
// Tests: phase interactions and options wiring
// ---------------------------------------------------------------------------

// TestPullE2E_ExplicitIncludePlugin verifies the PluginFilter wiring: an
// exact-pin --include-plugin entry selects the pinned pre-release (which
// auto-selection can never reach) without any module trigger.
func TestPullE2E_ExplicitIncludePlugin(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	addPluginVersion(reg, "standalone-tool", "v1.0.0", standaloneToolStableContract)
	addPluginVersion(reg, "standalone-tool", "v2.0.0-rc.1", standaloneToolRCContract)

	filter, err := modules.NewFilter([]string{"standalone-tool@=v2.0.0-rc.1"}, modules.FilterTypeWhitelist)
	require.NoError(t, err)

	bundleDir := t.TempDir()
	svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		SkipPlatform:   true,
		SkipSecurity:   true,
		SkipInstaller:  true,
		SkipModules:    true,
		SkipVexImages:  true,
		BundleDir:      bundleDir,
		PluginFilter:   filter,
		PluginBuiltins: e2eBuiltins,
	})

	summary, err := svc.Pull(context.Background())
	require.NoError(t, err)

	require.Len(t, summary.Plugins.Plugins, 1)
	plugin := summary.Plugins.Plugins[0]
	assert.Equal(t, "standalone-tool", plugin.Name)

	require.Len(t, plugin.Versions, 1, "only the pinned version, not the newest stable")
	assert.Equal(t, "v2.0.0-rc.1", plugin.Versions[0].Version)
	assert.Equal(t, []PluginReason{{Kind: "explicit", Subject: "--include-plugin standalone-tool", Constraint: "=v2.0.0-rc.1"}},
		plugin.Versions[0].Reasons)

	pluginTar := filepath.Join(bundleDir, "plugin-standalone-tool.tar")
	require.FileExists(t, pluginTar)
	assert.ElementsMatch(t, []string{"v2.0.0-rc.1"}, pluginTarShortTags(t, pluginTar))
}

// TestPullE2E_SkipModules_NoPluginAutoSelection: with the modules phase
// skipped there are no modules in the bundle, so a relevant catalog plugin
// must not be selected - nothing extra.
func TestPullE2E_SkipModules_NoPluginAutoSelection(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	addModule(reg, "postgresql", map[string]string{"stable": "v1.5.0"})
	addPluginVersion(reg, "postgresql-mgr", "v1.1.0", mgrContractForOldPostgres)

	bundleDir := t.TempDir()
	svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		SkipPlatform:   true,
		SkipSecurity:   true,
		SkipInstaller:  true,
		SkipModules:    true,
		SkipVexImages:  true,
		BundleDir:      bundleDir,
		PluginBuiltins: e2eBuiltins,
	})

	summary, err := svc.Pull(context.Background())
	require.NoError(t, err)

	assert.True(t, summary.Plugins.Attempted)
	assert.Empty(t, summary.Plugins.Plugins)
	assert.Empty(t, summary.Plugins.SkippedPlugins)

	pluginTars, err := filepath.Glob(filepath.Join(bundleDir, "plugin-*.tar"))
	require.NoError(t, err)
	assert.Empty(t, pluginTars)
}

// TestPullE2E_DeckhouseConstraint verifies the platform->plugins handoff: a
// contract's deckhouse constraint is enforced against the platform versions
// in the bundle, and not enforced when the platform phase is skipped.
func TestPullE2E_DeckhouseConstraint(t *testing.T) {
	buildRegistry := func() *upfake.Registry {
		reg := upfake.NewRegistry(pullStubRootURL)
		addPlatform(reg, "v1.69.0")
		addModule(reg, "postgresql", map[string]string{"stable": "v1.5.0"})
		addPluginVersion(reg, "dh-tool", "v1.0.0", dhToolContract)

		return reg
	}

	t.Run("enforced against bundled platform version", func(t *testing.T) {
		bundleDir := t.TempDir()
		svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(buildRegistry())), "v1.69.0", &PullServiceOptions{
			SkipSecurity:   true,
			SkipInstaller:  true,
			SkipVexImages:  true,
			BundleDir:      bundleDir,
			PluginBuiltins: e2eBuiltins,
		})

		summary, err := svc.Pull(context.Background())
		require.NoError(t, err)

		assert.Empty(t, summary.Plugins.Plugins, "dh-tool requires deckhouse >=1.70.0, bundle has v1.69.0")
		require.Len(t, summary.Plugins.SkippedPlugins, 1)
		assert.Equal(t, "dh-tool", summary.Plugins.SkippedPlugins[0].Name)
		assert.NotEmpty(t, summary.Plugins.SkippedPlugins[0].Reason)
	})

	t.Run("not enforced without platform in bundle", func(t *testing.T) {
		bundleDir := t.TempDir()
		svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(buildRegistry())), "", &PullServiceOptions{
			SkipPlatform:   true,
			SkipSecurity:   true,
			SkipInstaller:  true,
			SkipVexImages:  true,
			BundleDir:      bundleDir,
			PluginBuiltins: e2eBuiltins,
		})

		summary, err := svc.Pull(context.Background())
		require.NoError(t, err)

		require.Len(t, summary.Plugins.Plugins, 1,
			"without platform versions the deckhouse constraint is not enforced at mirror time")
		assert.Equal(t, "dh-tool", summary.Plugins.Plugins[0].Name)
	})
}

// TestPullE2E_DryRun_ResolutionParityNoFiles: dry-run resolves plugins with
// the same versions and provenance as a real pull, but writes nothing.
func TestPullE2E_DryRun_ResolutionParityNoFiles(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	addModule(reg, "postgresql", map[string]string{"stable": "v1.5.0", "alpha": "v1.10.0"})
	addPluginVersion(reg, "postgresql-mgr", "v1.1.0", mgrContractForOldPostgres)
	addPluginVersion(reg, "postgresql-mgr", "v1.2.0", mgrContractForNewPostgres)

	bundleDir := t.TempDir()
	svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		SkipPlatform:   true,
		SkipSecurity:   true,
		SkipInstaller:  true,
		SkipVexImages:  true,
		BundleDir:      bundleDir,
		DryRun:         true,
		PluginBuiltins: e2eBuiltins,
	})

	summary, err := svc.Pull(context.Background())
	require.NoError(t, err)
	assert.True(t, summary.DryRun)

	// Same resolution as the real pull in TestPullE2E_ModuleVersionsReachPluginResolver.
	require.Len(t, summary.Plugins.Plugins, 1)
	plugin := summary.Plugins.Plugins[0]
	assert.Equal(t, "postgresql-mgr", plugin.Name)
	require.Len(t, plugin.Versions, 2)
	assert.Equal(t, "v1.2.0", plugin.Versions[0].Version)
	assert.Equal(t, "v1.1.0", plugin.Versions[1].Version)
	assert.NotEmpty(t, plugin.Versions[0].Reasons)

	assert.Zero(t, plugin.Images, "dry-run pulls no images")
	assert.Zero(t, summary.Plugins.TotalImages)

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write bundle files")
}

// TestPullE2E_OnlyExtraImages_PluginsSkipped: --only-extra-images skips the
// plugins phase entirely and the summary reports it as skipped.
func TestPullE2E_OnlyExtraImages_PluginsSkipped(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	addModule(reg, "postgresql", map[string]string{"stable": "v1.5.0"})
	addPluginVersion(reg, "postgresql-mgr", "v1.1.0", mgrContractForOldPostgres)

	bundleDir := t.TempDir()
	svc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		SkipPlatform:    true,
		SkipSecurity:    true,
		SkipInstaller:   true,
		SkipVexImages:   true,
		BundleDir:       bundleDir,
		OnlyExtraImages: true,
		PluginBuiltins:  e2eBuiltins,
	})

	summary, err := svc.Pull(context.Background())
	require.NoError(t, err)

	assert.True(t, summary.Plugins.Skipped)
	assert.False(t, summary.Plugins.Attempted)

	pluginTars, err := filepath.Glob(filepath.Join(bundleDir, "plugin-*.tar"))
	require.NoError(t, err)
	assert.Empty(t, pluginTars)
}

// ---------------------------------------------------------------------------
// Tests: pull -> push roundtrip
// ---------------------------------------------------------------------------

// TestPullE2E_RoundTrip_PullThenPushPlugins carries plugins through the whole
// mirror path: pull from the source registry, pack into bundle tars, push the
// tars into a target registry with an edition segment. The plugin repository
// must land at deckhouse-cli/plugins under the root above that edition, with
// its discovery tag, while --modules-path-suffix moves modules only and they
// stay under the edition.
func TestPullE2E_RoundTrip_PullThenPushPlugins(t *testing.T) {
	reg := upfake.NewRegistry(pullStubRootURL)
	addModule(reg, "postgresql", map[string]string{"stable": "v1.5.0", "alpha": "v1.10.0"})
	addPluginVersion(reg, "postgresql-mgr", "v1.1.0", mgrContractForOldPostgres)
	addPluginVersion(reg, "postgresql-mgr", "v1.2.0", mgrContractForNewPostgres)

	bundleDir := t.TempDir()
	pullSvc := newPullService(t, pkgclient.Adapt(upfake.NewClient(reg)), "", &PullServiceOptions{
		SkipPlatform:   true,
		SkipSecurity:   true,
		SkipInstaller:  true,
		SkipVexImages:  true,
		BundleDir:      bundleDir,
		PluginBuiltins: e2eBuiltins,
	})

	ctx := context.Background()

	_, err := pullSvc.Pull(ctx)
	require.NoError(t, err)

	tars, err := filepath.Glob(filepath.Join(bundleDir, "*.tar"))
	require.NoError(t, err)
	require.NotEmpty(t, tars)

	destReg := upfake.NewRegistry("registry.example.com")
	hostClient := pkgclient.Adapt(upfake.NewClient(destReg))

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	pushSvc := NewPushService(hostClient, &PushServiceOptions{
		TargetPath: "/deckhouse/ee",
		Packages:   tars,
		WorkingDir: t.TempDir(),
		// A moved modules path must not touch plugins.
		ModulesPathSuffix: "/my/mods",
	}, logger, userLogger)

	pushSummary, err := pushSvc.Push(ctx)
	require.NoError(t, err)

	assert.Equal(t, 1, pushSummary.Plugins, "one plugin repository pushed")
	assert.Equal(t, 1, pushSummary.Modules)
	assert.Equal(t, "registry.example.com/deckhouse/deckhouse-cli/plugins", pushSvc.pluginsPath.Path)

	// Plugins are rooted above the edition: registry.example.com/deckhouse,
	// not .../deckhouse/ee. Both resolved plugin versions land there.
	rootClient := hostClient.WithSegment("deckhouse")
	destClient := rootClient.WithSegment("ee")

	pluginClient := rootClient.WithSegment("deckhouse-cli", "plugins", "postgresql-mgr")
	assert.NoError(t, pluginClient.CheckImageExists(ctx, "v1.1.0"))
	assert.NoError(t, pluginClient.CheckImageExists(ctx, "v1.2.0"))

	// The discovery tag makes the plugin visible to catalog listing.
	catalogTags, err := rootClient.WithSegment("deckhouse-cli", "plugins").ListTags(ctx)
	require.NoError(t, err)
	assert.Contains(t, catalogTags, "postgresql-mgr")

	// Nothing plugin-related is left under the edition.
	underEdition := destClient.WithSegment("deckhouse-cli", "plugins", "postgresql-mgr")
	assert.Error(t, underEdition.CheckImageExists(ctx, "v1.1.0"),
		"plugins must not be written under the edition segment")

	// The module moved with the suffix and stayed under the edition.
	movedModule := destClient.WithSegment("my", "mods", "postgresql")
	assert.NoError(t, movedModule.CheckImageExists(ctx, "v1.5.0"))
	assert.NoError(t, movedModule.CheckImageExists(ctx, "v1.10.0"))

	defaultModule := destClient.WithSegment("modules", "postgresql")
	assert.Error(t, defaultModule.CheckImageExists(ctx, "v1.5.0"),
		"a moved modules path must hold nothing at the default location")
}

// ---------------------------------------------------------------------------
// Registry fixture builders
// ---------------------------------------------------------------------------

// addPlatform publishes the minimal platform refs a --deckhouse-tag pinned
// pull needs: the root, install and install-standalone images for one
// version. Channel discovery is short-circuited by the pinned tag, so no
// release-channel refs are required.
func addPlatform(reg *upfake.Registry, version string) {
	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+version+`"}`).
		WithFile("deckhouse/candi/images_digests.json", `{}`).
		MustBuild()

	for _, repo := range []string{"", "install", "install-standalone"} {
		reg.MustAddImage(repo, version, img)
	}
}

// versionImage builds a minimal module image the modules phase can read:
// version.json plus the OCI version label.
func versionImage(version string) v1.Image {
	return upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+version+`"}`).
		WithLabel("org.opencontainers.image.version", version).
		MustBuild()
}

// addModule populates the registry with one module's worth of refs, the same
// shape the modules phase discovers against a real registry:
//
//	modules:<name>                    - modules-list entry
//	modules/<name>:<v>                - one image per distinct channel version
//	modules/<name>/release:<channel>  - the given release channels
//	modules/<name>/release:<v>        - version-tagged release images
//
// channels maps a release-channel tag to the version it points at; the
// module's bundled versions are exactly the distinct channel versions.
// Channels absent from the map are tolerated by the modules phase.
func addModule(reg *upfake.Registry, name string, channels map[string]string) {
	versions := make(map[string]struct{}, len(channels))

	for channel, version := range channels {
		reg.MustAddImage("modules/"+name+"/release", channel, versionImage(version))
		versions[version] = struct{}{}
	}

	for version := range versions {
		reg.MustAddImage("modules/"+name, version, versionImage(version))
		reg.MustAddImage("modules/"+name+"/release", version, versionImage(version))
	}

	reg.MustAddImage("modules", name, upfake.NewImageBuilder().WithFile("name", name).MustBuild())
}

// addPluginVersion publishes one plugin version with the given contract JSON
// (base64-encoded into the "contract" annotation) plus the catalog's
// directory-as-tags name entry. Takes no *testing.T so registry builders
// without one (fullStub) can use it; the type assertion cannot fail for a
// v1.Image input.
func addPluginVersion(reg *upfake.Registry, name, tag, contractJSON string) {
	img := upfake.NewImageBuilder().WithFile("plugin", "binary-"+name+"-"+tag).MustBuild()

	encoded := base64.StdEncoding.EncodeToString([]byte(contractJSON))
	annotated := mutate.Annotations(img, map[string]string{"contract": encoded}).(v1.Image)

	reg.MustAddImage(pluginsCatalogRepo+"/"+name, tag, annotated)
	reg.MustAddImage(pluginsCatalogRepo, name, upfake.NewImageBuilder().WithFile("name", name).MustBuild())
}

// ---------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------

// pluginTarShortTags reads a plugin bundle tar and returns the
// io.deckhouse.image.short_tag annotations of its OCI index - the version
// tags that actually made it into the bundle.
func pluginTarShortTags(t *testing.T, tarPath string) []string {
	t.Helper()

	f, err := os.Open(tarPath)
	require.NoError(t, err)
	defer f.Close()

	var indexJSON []byte

	tr := tar.NewReader(f)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		if strings.HasSuffix(header.Name, "/index.json") {
			indexJSON, err = io.ReadAll(tr)
			require.NoError(t, err)
		}
	}

	require.NotEmpty(t, indexJSON, "bundle tar must contain an OCI index.json")

	var index struct {
		Manifests []struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"manifests"`
	}
	require.NoError(t, json.Unmarshal(indexJSON, &index))

	tags := make([]string, 0, len(index.Manifests))
	for _, m := range index.Manifests {
		tags = append(tags, m.Annotations["io.deckhouse.image.short_tag"])
	}

	return tags
}
