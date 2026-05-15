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
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	golayout "github.com/google/go-containerregistry/pkg/v1/layout"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	dkpreg "github.com/deckhouse/deckhouse/pkg/registry"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

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

// Regression for --include-module <name>@<semver>: every tag in the registry
// that satisfies the semver constraint must end up in the download list,
// not only the tag pinned by the release channel snapshot.
func TestPullModules_SemverConstraintPullsAllMatchingTags(t *testing.T) {
	registryVersions := []string{
		"v1.39.0",
		"v1.40.0", "v1.40.1",
		"v1.41.0", "v1.42.0", "v1.43.0",
		channelVersion,
	}

	cases := []struct {
		name       string
		constraint string   // text after "<module>@" in --include-module
		wantTags   []string // tags expected to land in the download list
		rejectTag  string   // tag present in the registry that the constraint must reject
	}{
		{
			name:       "implicit caret (1.40.0 -> ^1.40.0)",
			constraint: "1.40.0",
			wantTags:   []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", "v1.43.0", channelVersion},
			rejectTag:  "v1.39.0",
		},
		{
			name:       "explicit caret (^1.40.0)",
			constraint: "^1.40.0",
			wantTags:   []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", "v1.43.0", channelVersion},
			rejectTag:  "v1.39.0",
		},
		{
			name:       "tilde (~1.40.0 - patch only)",
			constraint: "~1.40.0",
			wantTags:   []string{"v1.40.0", "v1.40.1", channelVersion},
			rejectTag:  "v1.41.0",
		},
		{
			name:       "explicit range (>=1.40.0 <1.43.0)",
			constraint: ">=1.40.0 <1.43.0",
			wantTags:   []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", channelVersion},
			rejectTag:  "v1.43.0",
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
			assert.NotContains(t, got, taggedModuleRef(testModuleName, tc.rejectTag),
				"constraint %q must reject %s", tc.constraint, tc.rejectTag)
		})
	}
}

// Each --include-module flag carries its own constraint - the matcher must
// scope to the named module and not leak across modules. Mixes a semver and
// an exact constraint to exercise both filter branches in one shot.
func TestPullModules_PerModuleConstraintIsolation(t *testing.T) {
	const (
		consoleName   = "console"
		commanderName = "commander"
	)

	reg := upfake.NewRegistry(testHost)
	addModule(reg, consoleName, "v1.40.1", []string{"v1.40.0", "v1.40.1", "v1.41.0", channelVersion})
	addModule(reg, commanderName, "v0.5.1", []string{"v0.5.0", "v0.5.1", "v0.6.0"})

	filter := mustNewFilter(t, FilterTypeWhitelist,
		consoleName+"@~1.40.0",   // tilde matches v1.40.x only
		commanderName+"@=v0.6.0", // exact tag
	)
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), filter)

	require.NoError(t, svc.PullModules(context.Background()))

	assert.ElementsMatch(t,
		taggedModuleRefs(consoleName, []string{"v1.40.0", "v1.40.1"}),
		pulledModuleVersionRefs(t, svc, consoleName),
		"console: tilde must match only v1.40.x")
	assert.ElementsMatch(t,
		taggedModuleRefs(commanderName, []string{"v0.6.0"}),
		pulledModuleVersionRefs(t, svc, commanderName),
		"commander: exact must match only v0.6.0 - no leak from console's tilde")
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
