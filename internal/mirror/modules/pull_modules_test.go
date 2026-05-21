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
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	golayout "github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// =============================================================================
// Shared fixtures
// =============================================================================

const (
	testHost       = "fake.registry"
	testModuleName = "console"
	channelVersion = "v1.45.2" // version every release channel points at
)

// defaultRegistryVersions is a small but representative tag set for tests
// that don't care about the exact list - a few patches and a few minors.
var defaultRegistryVersions = []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.45.2"}

// =============================================================================
// Tests: which versions get pulled
// =============================================================================

// Regression for --include-module <name>@<semver>: every (major, minor) bucket
// in the registry that satisfies the semver constraint must contribute exactly
// its highest patch to the download list. This pins down two contracts at the
// pull-flow boundary:
//
//  1. Constraints reject everything outside their range (negative side).
//  2. Constraints collapse same-minor patches to the latest one (the issue #220
//     fix: without this, `module@v1.6.0` would pull v1.6.0..v1.6.5 verbatim).
//
// The version pinned by the release channel snapshot (`channelVersion`) is
// always pulled in addition to the constraint-derived set, so it must appear
// in every wantTags below.
func TestPullModules_SemverConstraintPullsAllMatchingTags(t *testing.T) {
	registryVersions := []string{
		"v1.39.0",
		// Two patches in 1.40.x to exercise the per-minor collapse.
		"v1.40.0", "v1.40.1",
		"v1.41.0", "v1.42.0", "v1.43.0",
		channelVersion,
	}

	cases := []struct {
		name        string
		constraint  string   // text after "<module>@" in --include-module
		wantTags    []string // tags expected to land in the download list
		rejectTags  []string // tags present in the registry that the constraint must reject (out of range or older patch in same minor)
	}{
		{
			// 1.40.x collapses to v1.40.1 (latest patch). 1.41/1.42/1.43 each
			// have a single tag, so they survive untouched.
			name:       "implicit caret (1.40.0 -> ^1.40.0) keeps only latest patch per minor",
			constraint: "1.40.0",
			wantTags:   []string{"v1.40.1", "v1.41.0", "v1.42.0", "v1.43.0", channelVersion},
			rejectTags: []string{"v1.39.0", "v1.40.0"},
		},
		{
			name:       "explicit caret (^1.40.0) keeps only latest patch per minor",
			constraint: "^1.40.0",
			wantTags:   []string{"v1.40.1", "v1.41.0", "v1.42.0", "v1.43.0", channelVersion},
			rejectTags: []string{"v1.39.0", "v1.40.0"},
		},
		{
			// Tilde collapses everything to a single (1.40.x) bucket; only
			// v1.40.1 (the latest patch) makes it through.
			name:       "tilde (~1.40.0 - patch only) keeps only latest patch",
			constraint: "~1.40.0",
			wantTags:   []string{"v1.40.1", channelVersion},
			rejectTags: []string{"v1.40.0", "v1.41.0"},
		},
		{
			// `>=1.40.0` literally names v1.40.0 — the equality is part of
			// the operator and the user's explicit ask MUST be honoured.
			// v1.40.x has two patches: the anchor v1.40.0 stays AND the
			// latest patch v1.40.1 stays. Other minors collapse to their
			// latest patch as usual; v1.43.0 is excluded by the strict <
			// upper bound.
			name:       "explicit range (>=1.40.0 <1.43.0) preserves >= anchor and keeps latest patch per minor",
			constraint: ">=1.40.0 <1.43.0",
			wantTags:   []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", channelVersion},
			rejectTags: []string{"v1.43.0"},
		},
		{
			// Bare `>=1.40.0` with no upper bound. Anchor v1.40.0 is kept,
			// 1.40.x latest patch v1.40.1 is also kept (per-minor rule),
			// 1.41/1.42/1.43 collapse to their (only) tag.
			name:       "bare >= preserves anchor and keeps latest patch in same minor",
			constraint: ">=1.40.0",
			wantTags:   []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", "v1.43.0", channelVersion},
			rejectTags: []string{"v1.39.0"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := singleModuleRegistry(testModuleName, channelVersion, registryVersions)
			filter := mustNewFilter(t, FilterTypeWhitelist, testModuleName+"@"+tc.constraint)
			svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), filter)

			require.NoError(t, svc.PullModules(context.Background()))

			got := pulledModuleVersionRefs(t, svc, testModuleName)
			assert.ElementsMatch(t, taggedModuleRefs(testModuleName, tc.wantTags), got)
			for _, rejected := range tc.rejectTags {
				assert.NotContains(t, got, taggedModuleRef(testModuleName, rejected),
					"constraint %q must reject %s (out-of-range or older patch in same minor)", tc.constraint, rejected)
			}
		})
	}
}

// Each --include-module flag carries its own constraint - the matcher must
// scope to the named module and not leak across modules. Mixes a semver and
// an exact constraint to exercise both filter branches in one shot.
//
// console is wired with two patches in the same minor (v1.40.0, v1.40.1) so
// that the per-minor collapse rule is observable: only the latest patch
// (v1.40.1) survives.
func TestPullModules_PerModuleConstraintIsolation(t *testing.T) {
	const (
		consoleName   = "console"
		commanderName = "commander"
	)

	reg := upfake.NewRegistry(testHost)
	addModule(reg, consoleName, "v1.40.1", []string{"v1.40.0", "v1.40.1", "v1.41.0", channelVersion})
	addModule(reg, commanderName, "v0.5.1", []string{"v0.5.0", "v0.5.1", "v0.6.0"})

	filter := mustNewFilter(t, FilterTypeWhitelist,
		consoleName+"@~1.40.0",   // tilde matches v1.40.x; collapses to latest patch v1.40.1
		commanderName+"@=v0.6.0", // exact tag
	)
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), filter)

	require.NoError(t, svc.PullModules(context.Background()))

	assert.ElementsMatch(t,
		taggedModuleRefs(consoleName, []string{"v1.40.1"}),
		pulledModuleVersionRefs(t, svc, consoleName),
		"console: tilde must match only v1.40.x and collapse to the latest patch (v1.40.1)")
	assert.NotContains(t, pulledModuleVersionRefs(t, svc, consoleName),
		taggedModuleRef(consoleName, "v1.40.0"),
		"console: older patch v1.40.0 must be dropped by the per-minor latest-patch filter")
	assert.ElementsMatch(t,
		taggedModuleRefs(commanderName, []string{"v0.6.0"}),
		pulledModuleVersionRefs(t, svc, commanderName),
		"commander: exact must match only v0.6.0 - no leak from console's tilde")
}

// TestPullModules_Issue220_LatestPatchPerMinor reproduces the exact registry
// shape from https://github.com/deckhouse/deckhouse-cli/issues/220:
//
//	d8 mirror pull --include-module code@v1.6.0
//
// against a registry that publishes
//
//	v1.6.0, v1.6.1, v1.6.2, v1.6.3, v1.6.4, v1.6.5,
//	v1.7.0, v1.7.1
//
// for the `code` module. Before the filterOnlyLatestPatches policy was
// applied at the module-filter level, the implicit caret expansion of
// `v1.6.0` (~ ^v1.6.0 ~ >=1.6.0 <2.0.0) made the puller download all eight
// version-tagged release images. The user observed exactly this: 8 version
// tags + 5 channel aliases = 13 release-channel pulls per module.
//
// Post-fix invariants:
//   - The version-tagged module list MUST contain only the latest patch in
//     each (major, minor) — v1.6.5 and v1.7.1.
//   - Older patches in the same minor (v1.6.0..v1.6.4, v1.7.0) MUST be
//     absent, because the user can re-pull them deliberately with
//     `--include-module code@=v1.6.2`.
//   - The release-channel set is a separate concern and is *not* re-asserted
//     here; it is covered by TestPullModules_PerModuleConstraintIsolation
//     and the LTS test below.
func TestPullModules_Issue220_LatestPatchPerMinor(t *testing.T) {
	const (
		moduleName = "code"
		// Channel snapshot points at the latest 1.6.x patch, the way a real
		// dev registry would. This rules out the channel snapshot accidentally
		// dragging an older patch back into the pull list.
		issueChannelVersion = "v1.6.5"
	)
	registryVersions := []string{
		"v1.6.0", "v1.6.1", "v1.6.2", "v1.6.3", "v1.6.4", "v1.6.5",
		"v1.7.0", "v1.7.1",
	}

	reg := singleModuleRegistry(moduleName, issueChannelVersion, registryVersions)
	filter := mustNewFilter(t, FilterTypeWhitelist, moduleName+"@v1.6.0")
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), filter)

	require.NoError(t, svc.PullModules(context.Background()))

	got := pulledModuleVersionRefs(t, svc, moduleName)

	// Expected: only the latest patch per (major, minor). The channel snapshot
	// (v1.6.5) coincides with the 1.6.x latest patch, so deduplication folds
	// it in transparently.
	wantLatestPatches := []string{"v1.6.5", "v1.7.1"}
	assert.ElementsMatch(t,
		taggedModuleRefs(moduleName, wantLatestPatches),
		got,
		"issue #220: --include-module code@v1.6.0 must collapse to one tag per minor")

	// Negative side: every older patch the user reported as wasted MUST be
	// absent from the pull list. This is the headline regression — losing
	// any of these assertions means the optimization was undone.
	for _, dropped := range []string{"v1.6.0", "v1.6.1", "v1.6.2", "v1.6.3", "v1.6.4", "v1.7.0"} {
		assert.NotContains(t, got, taggedModuleRef(moduleName, dropped),
			"issue #220: older patch %s must be dropped by the latest-patch-per-minor filter", dropped)
	}
}

// =============================================================================
// Tests: per-module ListTags policy
// =============================================================================

// Per-module ListTags is only needed for non-exact constraints. The baseline
// cost of a default pull or an exact-tag pull must not regress by adding an
// unconditional per-module call.
func TestPullModules_PerModuleListTagsCallCount(t *testing.T) {
	// PullModules lists tags at the registry root twice:
	//   - validateModulesAccess (reachability check)
	//   - pullModules (module-name enumeration)
	const baselineRootCalls int64 = 2

	cases := []struct {
		name        string
		filterType  FilterType
		filterExprs []string
		wantExtra   int64 // extra ListTags calls on top of the baseline
	}{
		{
			name:        "exact constraint skips per-module ListTags",
			filterType:  FilterTypeWhitelist,
			filterExprs: []string{testModuleName + "@=v1.40.0"},
			wantExtra:   0,
		},
		{
			name:        "blacklist filter (no constraint) skips per-module ListTags",
			filterType:  FilterTypeBlacklist,
			filterExprs: nil,
			wantExtra:   0,
		},
		{
			name:        "semver constraint triggers one per-module ListTags",
			filterType:  FilterTypeWhitelist,
			filterExprs: []string{testModuleName + "@1.40.0"},
			wantExtra:   1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := singleModuleRegistry(testModuleName, channelVersion, defaultRegistryVersions)
			counter := newListTagsCounter(upfake.NewClient(reg))
			filter := mustNewFilter(t, tc.filterType, tc.filterExprs...)
			svc := newService(t, pkgclient.Adapt(counter), filter)

			require.NoError(t, svc.PullModules(context.Background()))

			assert.Equal(t, baselineRootCalls+tc.wantExtra, counter.calls.Load())
		})
	}
}

// Per-module ListTags reuses validateModulesAccess's error policy:
//   - ErrImageNotFound: warn-and-skip (the module repo simply isn't there).
//   - any other error:  fail-fast (we cannot verify the constraint and refuse
//     to silently produce a partial bundle).
func TestPullModules_PerModuleListTagsErrorHandling(t *testing.T) {
	transientErr := errors.New("simulated registry 503")

	cases := []struct {
		name     string
		injected error
		wantErr  error    // nil = pull must succeed
		wantTags []string // checked only on success - the channel snapshot is the sole contributor when per-module ListTags is skipped
	}{
		{
			name:     "ErrImageNotFound is warned and skipped",
			injected: dkpreg.ErrImageNotFound,
			wantTags: []string{channelVersion},
		},
		{
			name:     "transient error fails fast",
			injected: transientErr,
			wantErr:  transientErr,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := singleModuleRegistry(testModuleName, channelVersion, defaultRegistryVersions)
			client := newListTagsErrAtModule(upfake.NewClient(reg), tc.injected)
			// Semver constraint to actually trigger the per-module ListTags.
			filter := mustNewFilter(t, FilterTypeWhitelist, testModuleName+"@1.40.0")
			svc := newService(t, pkgclient.Adapt(client), filter)

			err := svc.PullModules(context.Background())

			if tc.wantErr == nil {
				require.NoError(t, err)
				assert.ElementsMatch(t,
					taggedModuleRefs(testModuleName, tc.wantTags),
					pulledModuleVersionRefs(t, svc, testModuleName))
				return
			}

			require.Error(t, err)
			assert.ErrorIs(t, err, tc.wantErr)
			assert.Contains(t, err.Error(), "list tags for module "+testModuleName)
		})
	}
}

// =============================================================================
// Service & filter builders
// =============================================================================

// newService wires a Service against the given fake client, with logs muted.
func newService(t *testing.T, client dkpreg.Client, filter *Filter) *Service {
	t.Helper()

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)
	regSvc := registryservice.NewService(client, pkg.NoEdition, logger)

	return NewService(
		regSvc,
		t.TempDir(),
		&Options{
			BundleDir:     t.TempDir(),
			Filter:        filter,
			SkipVexImages: true,
		},
		logger,
		userLogger,
	)
}

func mustNewFilter(t *testing.T, ftype FilterType, exprs ...string) *Filter {
	t.Helper()
	f, err := NewFilter(exprs, ftype)
	require.NoError(t, err)
	return f
}

// =============================================================================
// Registry fixture builders
// =============================================================================

// addModule populates a fake registry with one module's worth of refs:
//
//	modules:<name>                    - modules-list entry, points at channelVer
//	modules/<name>:<v>                - one image per version
//	modules/<name>/release:<channel>  - 5 release channels, all pointing at channelVer
func addModule(reg *upfake.Registry, name, channelVer string, versions []string) {
	reg.MustAddImage("modules", name, versionImage(channelVer))
	for _, v := range versions {
		reg.MustAddImage("modules/"+name, v, versionImage(v))
		reg.MustAddImage("modules/"+name+"/release", v, versionImage(v))
	}
	for _, ch := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
		reg.MustAddImage("modules/"+name+"/release", ch, versionImage(channelVer))
	}
}

// singleModuleRegistry builds a fake registry containing exactly one module.
func singleModuleRegistry(name, channelVer string, versions []string) *upfake.Registry {
	reg := upfake.NewRegistry(testHost)
	addModule(reg, name, channelVer, versions)
	return reg
}

// addLTSReleaseChannel adds the optional LTS release channel (CSE editions).
func addLTSReleaseChannel(reg *upfake.Registry, name, channelVer string) {
	reg.MustAddImage("modules/"+name+"/release", internal.LTSChannel, versionImage(channelVer))
}

// versionImage builds a v1.Image carrying only version.json. Missing
// images_digests.json / extra_images.json is tolerated downstream.
func versionImage(version string) v1.Image {
	return upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+version+`"}`).
		WithLabel("org.opencontainers.image.version", version).
		MustBuild()
}

// =============================================================================
// Assertion helpers
// =============================================================================

// taggedModuleRef is the registry URL the production code uses for a single
// version-tagged module image.
func taggedModuleRef(moduleName, version string) string {
	return testHost + "/modules/" + moduleName + ":" + version
}

func moduleReleaseChannelRef(moduleName, channel string) string {
	return testHost + "/modules/" + moduleName + "/release:" + channel
}

func taggedModuleRefs(moduleName string, versions []string) []string {
	refs := make([]string, 0, len(versions))
	for _, v := range versions {
		refs = append(refs, taggedModuleRef(moduleName, v))
	}
	return refs
}

// pulledModuleVersionRefs returns the version-tagged refs the service recorded
// for the given module, dropping @sha256: refs (these are added by extra-image
// resolution and are not relevant to constraint tests).
func pulledModuleVersionRefs(t *testing.T, svc *Service, moduleName string) []string {
	t.Helper()
	dl := svc.modulesDownloadList.Module(moduleName)
	require.NotNil(t, dl, "no download list recorded for module %s", moduleName)

	refs := make([]string, 0, len(dl.Module))
	for ref := range dl.Module {
		if strings.Contains(ref, "@sha256:") {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

// =============================================================================
// Test doubles
// =============================================================================

// listTagsCounter counts every ListTags call on this client and on any
// sub-client spawned via WithSegment (the counter is shared across the chain).
type listTagsCounter struct {
	dkpreg.Client
	calls *atomic.Int64
}

func newListTagsCounter(c dkpreg.Client) *listTagsCounter {
	return &listTagsCounter{Client: c, calls: new(atomic.Int64)}
}

func (c *listTagsCounter) WithSegment(segments ...string) dkpreg.Client {
	return &listTagsCounter{Client: c.Client.WithSegment(segments...), calls: c.calls}
}

func (c *listTagsCounter) ListTags(ctx context.Context, opts ...dkpreg.ListTagsOption) ([]string, error) {
	c.calls.Add(1)
	return c.Client.ListTags(ctx, opts...)
}

// listTagsErrAtModule returns the configured error from ListTags only at the
// per-module path (modules/<name>, two segments below root). Root-level
// ListTags - validateModulesAccess and module-name enumeration - pass through.
type listTagsErrAtModule struct {
	dkpreg.Client
	depth int
	err   error
}

// perModuleDepth is the WithSegment depth at which a client points at
// modules/<name>: root -> "modules" -> "<name>".
const perModuleDepth = 2

func newListTagsErrAtModule(c dkpreg.Client, err error) *listTagsErrAtModule {
	return &listTagsErrAtModule{Client: c, depth: 0, err: err}
}

func (c *listTagsErrAtModule) WithSegment(segments ...string) dkpreg.Client {
	return &listTagsErrAtModule{
		Client: c.Client.WithSegment(segments...),
		depth:  c.depth + len(segments),
		err:    c.err,
	}
}

func (c *listTagsErrAtModule) ListTags(ctx context.Context, opts ...dkpreg.ListTagsOption) ([]string, error) {
	if c.depth == perModuleDepth {
		return nil, c.err
	}
	return c.Client.ListTags(ctx, opts...)
}

// =============================================================================
// Tests: empty module layouts are not packed into tars
// =============================================================================

// TestPullModules_EmptyModuleNotPacked verifies that when a module is present
// in the registry listing but has no pullable images (all release channels are
// missing and no version tags match), packModules skips it and does not create
// an empty stub tar in the bundle directory.
//
// This is a regression guard for the scenario where `d8 mirror pull` is
// interrupted or a module has no images for the mirrored version: previously
// a ~5 KB skeleton tar (oci-layout + index.json + empty blobs/) was always
// written for every discovered module.
func TestPullModules_EmptyModuleNotPacked(t *testing.T) {
	// Registry has the module listed but zero version images.
	// The release-channel images are absent too (AllowMissingTags is true
	// for channels, so the puller won't error — it just pulls nothing).
	reg := upfake.NewRegistry(testHost)
	reg.MustAddImage("modules", testModuleName, versionImage(channelVersion))
	// Intentionally: no modules/<name>:<version> and no modules/<name>/release:<channel>.

	bundleDir := t.TempDir()
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
	svc.options.BundleDir = bundleDir

	require.NoError(t, svc.PullModules(context.Background()))

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries,
		"bundle dir must stay empty when module has no images; got: %v", entries)
}

// TestPackModules_EmptyModuleIsSilent guards the user-facing log output for
// modules whose layout has no pulled images: there must be no "Pack
// module-<name>.tar" header, no "Skipping ... no images were pulled" line,
// and no "succeeded in ..." footer. Together those three lines used to fire
// for every empty module discovered during a partial or no-op pull, drowning
// the real progress in screenfuls of noise (see d8pull output: dozens of
// "Skipping module-<x>.tar: no images were pulled" entries for modules that
// were never actually packed).
//
// The test wires an Info-level user logger bound to a captured stdout so that
// any Process()/Infof() call inside packModules would actually be observable;
// the populated module's Pack header is asserted as a positive control to
// prove the capture pipeline is wired up.
func TestPackModules_EmptyModuleIsSilent(t *testing.T) {
	const (
		fullMod  = "with-images"
		emptyMod = "no-images"
	)

	reg := upfake.NewRegistry(testHost)
	addModule(reg, fullMod, channelVersion, defaultRegistryVersions)
	// Modules-list entry only - no version tags and no release channels.
	// The puller will discover the module name but find nothing to pull,
	// so the layout ends up empty and packModules must skip it silently.
	reg.MustAddImage("modules", emptyMod, versionImage(channelVersion))

	bundleDir := t.TempDir()
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
	svc.options.BundleDir = bundleDir

	// Capture user-facing log output by redirecting os.Stdout, then build a
	// fresh Info-level SLogger bound to the redirected stdout. Constructing
	// the logger AFTER the redirect is required because NewSLogger captures
	// os.Stdout into its slog handler at construction time.
	stdoutSave := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() { os.Stdout = stdoutSave }()

	// Drain the pipe concurrently so PullModules can't deadlock on a full
	// pipe buffer if it logs more than the kernel buffer can hold.
	var captured strings.Builder
	drained := make(chan struct{})
	go func() {
		_, _ = io.Copy(&captured, r)
		close(drained)
	}()

	svc.userLogger = log.NewSLogger(slog.LevelInfo)

	runErr := svc.PullModules(context.Background())

	require.NoError(t, w.Close())
	<-drained
	os.Stdout = stdoutSave
	require.NoError(t, runErr)

	out := captured.String()

	// Positive control: the populated module went through the Pack pipeline,
	// proving the log capture is wired up correctly. If this fails, the
	// negative assertions below are vacuously true and the test is useless.
	assert.Contains(t, out, "Pack module-"+fullMod+".tar",
		"log-capture sanity: pack header for populated module must appear in captured output; got:\n%s", out)

	// Headline regression: zero noise for the empty module.
	emptyTar := "module-" + emptyMod + ".tar"
	assert.NotContains(t, out, emptyTar,
		"empty module must not appear anywhere in user-facing logs; got:\n%s", out)
	assert.NotContains(t, out, "no images were pulled",
		"the legacy 'Skipping ... no images were pulled' line must not appear; got:\n%s", out)
	assert.NotContains(t, out, "Skipping module-",
		"no per-module 'Skipping module-...' line must appear; got:\n%s", out)
}

// TestPullModules_NonEmptyModulePacked is the positive counterpart: a module
// with at least one pulled image must produce a non-empty tar in the bundle dir.
func TestPullModules_NonEmptyModulePacked(t *testing.T) {
	reg := singleModuleRegistry(testModuleName, channelVersion, defaultRegistryVersions)
	bundleDir := t.TempDir()

	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
	svc.options.BundleDir = bundleDir

	require.NoError(t, svc.PullModules(context.Background()))

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "bundle dir must contain a tar for a module with images")

	for _, e := range entries {
		info, err := e.Info()
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(5120),
			"tar %q must be larger than 5120 bytes (the empty-layout skeleton)", e.Name())
	}
}

// TestPullModules_InterruptedPullDoesNotProduceEmptyStubTars is the headline
// regression for the user-reported bug: pulling a registry that lists many
// modules, then cancelling mid-flight, must NOT leave behind a stub
// ~5120-byte module-<name>.tar for every module that did not actually
// download.  Before the fix, AllowMissingTags=true silently swallowed
// context.Canceled returned by GetDigest, so the pull loop processed every
// remaining module as a no-op and the pack phase produced one empty-skeleton
// tar per module.
func TestPullModules_InterruptedPullDoesNotProduceEmptyStubTars(t *testing.T) {
	const (
		earlyMod = "aaa-pulled-first"  // alphabetically first, will fully pull
		lateMod  = "zzz-never-touched" // alphabetically last, will be cancelled out
	)

	reg := upfake.NewRegistry(testHost)
	addModule(reg, earlyMod, channelVersion, defaultRegistryVersions)
	addModule(reg, lateMod, channelVersion, defaultRegistryVersions)

	bundleDir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())

	// Wire the fake client through a wrapper that cancels the user's context
	// the moment a request hits the *second* module's path. That simulates
	// the user pressing Ctrl+C while the pull moves from earlyMod onto
	// lateMod — exactly the timing that produced the empty stub tars.
	client := &cancelOnSecondModule{
		Client:   upfake.NewClient(reg),
		cancel:   cancel,
		trigger:  "modules/" + lateMod,
		canceled: new(atomic.Bool),
	}

	svc := newService(t, pkgclient.Adapt(client), nil)
	svc.options.BundleDir = bundleDir

	err := svc.PullModules(ctx)
	require.NoError(t, err,
		"PullModules must finish gracefully after cancellation (packing what was downloaded)")
	require.True(t, client.canceled.Load(), "test bug: cancellation trigger never fired")

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)

	var sawEarly bool
	for _, e := range entries {
		name := e.Name()

		// The cancelled module must not produce any artifact - no .tar, no
		// .tar.tmp left behind.
		require.NotContains(t, name, lateMod,
			"cancelled module %q must not appear in the bundle dir, found %q", lateMod, name)

		// Anything that does end up in the bundle must be a real tar - the
		// 5120-byte stub artifact that the user reported is exactly what we
		// regress against here.
		info, err := e.Info()
		require.NoError(t, err)
		require.Greater(t, info.Size(), int64(5120),
			"bundle file %q has stub size %d (must be > 5120 bytes; that's the empty-layout skeleton)",
			name, info.Size())

		if name == "module-"+earlyMod+".tar" {
			sawEarly = true
		}
	}

	require.True(t, sawEarly,
		"%s should have been pulled and packed before cancellation; entries=%v", earlyMod, entries)
}

// cancelOnSecondModule is a registry client wrapper that fires the supplied
// cancel func the first time a method touches the configured trigger path.
// Used by the interrupted-pull regression to simulate a user hitting Ctrl+C
// at a well-defined point in the pull sequence.
type cancelOnSecondModule struct {
	dkpreg.Client
	cancel   context.CancelFunc
	trigger  string
	scope    string
	canceled *atomic.Bool
}

func (c *cancelOnSecondModule) WithSegment(segments ...string) dkpreg.Client {
	next := c.scope
	for _, s := range segments {
		if next == "" {
			next = s
		} else {
			next = next + "/" + s
		}
	}
	return &cancelOnSecondModule{
		Client:   c.Client.WithSegment(segments...),
		cancel:   c.cancel,
		trigger:  c.trigger,
		scope:    next,
		canceled: c.canceled,
	}
}

func (c *cancelOnSecondModule) maybeCancel() {
	if c.canceled.Load() {
		return
	}
	if strings.HasPrefix(c.scope, c.trigger) {
		c.canceled.Store(true)
		c.cancel()
	}
}

func (c *cancelOnSecondModule) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	c.maybeCancel()
	return c.Client.GetDigest(ctx, tag)
}

func (c *cancelOnSecondModule) GetImage(ctx context.Context, tag string, opts ...dkpreg.ImageGetOption) (dkpreg.Image, error) {
	c.maybeCancel()
	return c.Client.GetImage(ctx, tag, opts...)
}

func (c *cancelOnSecondModule) CheckImageExists(ctx context.Context, tag string) error {
	c.maybeCancel()
	return c.Client.CheckImageExists(ctx, tag)
}

func (c *cancelOnSecondModule) ListTags(ctx context.Context, opts ...dkpreg.ListTagsOption) ([]string, error) {
	c.maybeCancel()
	return c.Client.ListTags(ctx, opts...)
}

// TestImageLayouts_HasImages_Empty verifies HasImages returns false when no
// images have been appended to any of the module's sub-layouts.
func TestImageLayouts_HasImages_Empty(t *testing.T) {
	layouts, err := createOCIImageLayoutsForModule(t.TempDir())
	require.NoError(t, err)
	assert.False(t, layouts.HasImages(),
		"HasImages must return false for a freshly created (empty) layout")
}

// TestImageLayouts_HasImages_WithImage verifies HasImages returns true once an
// image has been appended to one of the module's sub-layouts.  The test does
// not go through the full pull flow (which destroys layouts during packing)
// but instead appends an image directly to the OCI layout path.
func TestImageLayouts_HasImages_WithImage(t *testing.T) {
	dir := t.TempDir()

	layouts, err := createOCIImageLayoutsForModule(dir)
	require.NoError(t, err)

	assert.False(t, layouts.HasImages(), "freshly created layout must have no images before any append")

	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.0.0"}`).
		MustBuild()
	err = layouts.Modules.Path().AppendImage(img,
		golayout.WithAnnotations(map[string]string{"io.deckhouse.image.short_tag": "v1.0.0"}))
	require.NoError(t, err)

	assert.True(t, layouts.HasImages(),
		"HasImages must return true after at least one image is appended")
}

// =============================================================================
// Tests: LTS release channel
// =============================================================================

// CSE editions expose an optional LTS release channel in addition to the five
// default channels. discoverChannelVersions must detect it and include it in
// the pull without failing.
func TestPullModules_LTSChannel(t *testing.T) {
	reg := singleModuleRegistry(testModuleName, channelVersion, defaultRegistryVersions)
	addLTSReleaseChannel(reg, testModuleName, channelVersion)

	bundleDir := t.TempDir()
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
	svc.options.BundleDir = bundleDir

	require.NoError(t, svc.PullModules(context.Background()))

	moduleDL := svc.modulesDownloadList.Module(testModuleName)
	require.NotNil(t, moduleDL, "modulesDownloadList must have an entry for module %q", testModuleName)

	ltsRef := moduleReleaseChannelRef(testModuleName, internal.LTSChannel)
	_, ok := moduleDL.ModuleReleaseChannels[ltsRef]
	assert.True(t, ok,
		"ModuleReleaseChannels should contain LTS ref %q; actual keys: %v",
		ltsRef, moduleDL.ModuleReleaseChannels)

	assert.Contains(t, pulledModuleVersionRefs(t, svc, testModuleName), taggedModuleRef(testModuleName, channelVersion),
		"LTS channel must contribute %s to the pulled module versions", channelVersion)

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "bundle dir must contain a tar when LTS channel pull succeeds")
}
