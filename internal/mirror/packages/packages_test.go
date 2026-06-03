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

package packages

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// =============================================================================
// Shared fixtures
// =============================================================================

const (
	testHost        = "fake.registry"
	testPackageName = "console"
	channelVersion  = "v1.45.2" // version every version-channel points at
)

// defaultRegistryVersions is a small but representative tag set for tests
// that don't care about the exact list - a few patches and a few minors.
var defaultRegistryVersions = []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.45.2"}

// =============================================================================
// Tests: which versions get pulled
// =============================================================================

// Regression for --include-package <name>@<semver>: every (major, minor) bucket
// in the registry that satisfies the semver constraint must contribute exactly
// its highest patch to the download list. Packages reuse the modules filter, so
// this pins down that the same latest-patch-per-minor + inclusive-anchor policy
// is wired through the package pull flow.
//
// The version pinned by the version-channel snapshot (`channelVersion`) is
// always pulled in addition to the constraint-derived set, so it must appear
// in every wantTags below.
func TestPullPackages_SemverConstraintPullsAllMatchingTags(t *testing.T) {
	registryVersions := []string{
		"v1.39.0",
		// Two patches in 1.40.x to exercise the per-minor collapse.
		"v1.40.0", "v1.40.1",
		"v1.41.0", "v1.42.0", "v1.43.0",
		channelVersion,
	}

	cases := []struct {
		name       string
		constraint string   // text after "<package>@" in --include-package
		wantTags   []string // tags expected to land in the download list
		rejectTags []string // tags present in the registry that the constraint must reject
	}{
		{
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
			name:       "tilde (~1.40.0 - patch only) keeps only latest patch",
			constraint: "~1.40.0",
			wantTags:   []string{"v1.40.1", channelVersion},
			rejectTags: []string{"v1.40.0", "v1.41.0"},
		},
		{
			name:       "explicit range (>=1.40.0 <1.43.0) preserves >= anchor and keeps latest patch per minor",
			constraint: ">=1.40.0 <1.43.0",
			wantTags:   []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", channelVersion},
			rejectTags: []string{"v1.43.0"},
		},
		{
			name:       "bare >= preserves anchor and keeps latest patch in same minor",
			constraint: ">=1.40.0",
			wantTags:   []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", "v1.43.0", channelVersion},
			rejectTags: []string{"v1.39.0"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := singlePackageRegistry(testPackageName, channelVersion, registryVersions)
			filter := mustNewFilter(t, modules.FilterTypeWhitelist, testPackageName+"@"+tc.constraint)
			svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), filter)

			require.NoError(t, svc.PullPackages(context.Background()))

			got := pulledPackageVersionRefs(t, svc, testPackageName)
			assert.ElementsMatch(t, packageRefs(testPackageName, tc.wantTags), got)
			for _, rejected := range tc.rejectTags {
				assert.NotContains(t, got, packageRef(testPackageName, rejected),
					"constraint %q must reject %s (out-of-range or older patch in same minor)", tc.constraint, rejected)
			}
		})
	}
}

// Each --include-package flag carries its own constraint - the matcher must
// scope to the named package and not leak across packages. Mixes a semver and
// an exact constraint to exercise both filter branches in one shot.
func TestPullPackages_PerPackageConstraintIsolation(t *testing.T) {
	const (
		consoleName = "console"
		scannerName = "scanner"
	)

	reg := upfake.NewRegistry(testHost)
	addPackage(reg, consoleName, "v1.40.1", []string{"v1.40.0", "v1.40.1", "v1.41.0", channelVersion})
	addPackage(reg, scannerName, "v0.5.1", []string{"v0.5.0", "v0.5.1", "v0.6.0"})

	filter := mustNewFilter(t, modules.FilterTypeWhitelist,
		consoleName+"@~1.40.0", // tilde matches v1.40.x; collapses to latest patch v1.40.1
		scannerName+"@=v0.6.0", // exact tag
	)
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), filter)

	require.NoError(t, svc.PullPackages(context.Background()))

	assert.ElementsMatch(t,
		packageRefs(consoleName, []string{"v1.40.1"}),
		pulledPackageVersionRefs(t, svc, consoleName),
		"console: tilde must match only v1.40.x and collapse to the latest patch (v1.40.1)")
	assert.NotContains(t, pulledPackageVersionRefs(t, svc, consoleName),
		packageRef(consoleName, "v1.40.0"),
		"console: older patch v1.40.0 must be dropped by the per-minor latest-patch filter")
	assert.ElementsMatch(t,
		packageRefs(scannerName, []string{"v0.6.0"}),
		pulledPackageVersionRefs(t, svc, scannerName),
		"scanner: exact must match only v0.6.0 - no leak from console's tilde")
}

// TestPullPackages_LatestPatchPerMinor verifies the latest-patch-per-minor
// collapse for packages: --include-package code@v1.6.0 against a registry that
// publishes six patches in 1.6.x and two in 1.7.x must collapse to a single
// (highest) patch per (major, minor).
func TestPullPackages_LatestPatchPerMinor(t *testing.T) {
	const (
		packageName         = "code"
		issueChannelVersion = "v1.6.5"
	)
	registryVersions := []string{
		"v1.6.0", "v1.6.1", "v1.6.2", "v1.6.3", "v1.6.4", "v1.6.5",
		"v1.7.0", "v1.7.1",
	}

	reg := singlePackageRegistry(packageName, issueChannelVersion, registryVersions)
	filter := mustNewFilter(t, modules.FilterTypeWhitelist, packageName+"@v1.6.0")
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), filter)

	require.NoError(t, svc.PullPackages(context.Background()))

	got := pulledPackageVersionRefs(t, svc, packageName)

	wantLatestPatches := []string{"v1.6.5", "v1.7.1"}
	assert.ElementsMatch(t,
		packageRefs(packageName, wantLatestPatches),
		got,
		"--include-package code@v1.6.0 must collapse to one tag per minor")

	for _, dropped := range []string{"v1.6.0", "v1.6.1", "v1.6.2", "v1.6.3", "v1.6.4", "v1.7.0"} {
		assert.NotContains(t, got, packageRef(packageName, dropped),
			"older patch %s must be dropped by the latest-patch-per-minor filter", dropped)
	}
}

// =============================================================================
// Tests: per-package ListTags policy
// =============================================================================

// Per-package ListTags is only needed for non-exact constraints. The baseline
// cost of a default pull or an exact-tag pull must not regress by adding an
// unconditional per-package call.
func TestPullPackages_PerPackageListTagsCallCount(t *testing.T) {
	// PullPackages lists tags at the packages root twice:
	//   - validatePackagesAccess (reachability check)
	//   - pullPackages (package-name enumeration)
	const baselineRootCalls int64 = 2

	cases := []struct {
		name        string
		filterType  modules.FilterType
		filterExprs []string
		wantExtra   int64 // extra ListTags calls on top of the baseline
	}{
		{
			name:        "exact constraint skips per-package ListTags",
			filterType:  modules.FilterTypeWhitelist,
			filterExprs: []string{testPackageName + "@=v1.40.0"},
			wantExtra:   0,
		},
		{
			name:        "blacklist filter (no constraint) skips per-package ListTags",
			filterType:  modules.FilterTypeBlacklist,
			filterExprs: nil,
			wantExtra:   0,
		},
		{
			name:        "semver constraint triggers one per-package ListTags",
			filterType:  modules.FilterTypeWhitelist,
			filterExprs: []string{testPackageName + "@1.40.0"},
			wantExtra:   1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := singlePackageRegistry(testPackageName, channelVersion, defaultRegistryVersions)
			counter := newListTagsCounter(upfake.NewClient(reg))
			filter := mustNewFilter(t, tc.filterType, tc.filterExprs...)
			svc := newService(t, pkgclient.Adapt(counter), filter)

			require.NoError(t, svc.PullPackages(context.Background()))

			assert.Equal(t, baselineRootCalls+tc.wantExtra, counter.calls.Load())
		})
	}
}

// Per-package ListTags reuses validatePackagesAccess's error policy:
//   - ErrImageNotFound: warn-and-skip (the package repo simply isn't there).
//   - any other error:  fail-fast (we cannot verify the constraint and refuse
//     to silently produce a partial bundle).
func TestPullPackages_PerPackageListTagsErrorHandling(t *testing.T) {
	transientErr := errors.New("simulated registry 503")

	cases := []struct {
		name     string
		injected error
		wantErr  error    // nil = pull must succeed
		wantTags []string // checked only on success - the channel snapshot is the sole contributor when per-package ListTags is skipped
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
			reg := singlePackageRegistry(testPackageName, channelVersion, defaultRegistryVersions)
			client := newListTagsErrAtPackage(upfake.NewClient(reg), tc.injected)
			// Semver constraint to actually trigger the per-package ListTags.
			filter := mustNewFilter(t, modules.FilterTypeWhitelist, testPackageName+"@1.40.0")
			svc := newService(t, pkgclient.Adapt(client), filter)

			err := svc.PullPackages(context.Background())

			if tc.wantErr == nil {
				require.NoError(t, err)
				assert.ElementsMatch(t,
					packageRefs(testPackageName, tc.wantTags),
					pulledPackageVersionRefs(t, svc, testPackageName))
				return
			}

			require.Error(t, err)
			assert.ErrorIs(t, err, tc.wantErr)
			assert.Contains(t, err.Error(), "list tags for package "+testPackageName)
		})
	}
}

// TestPullPackages_MissingPackagesRepoSkipsGracefully is the regression test for
// the bug where a registry without a /packages repository failed the entire
// pull instead of skipping the packages phase. The public registry reports the
// missing repo as a NAME_UNKNOWN transport error (not ErrImageNotFound), so both
// root-level ListTags probes - validatePackagesAccess and discoverPackageNames -
// must treat either signal as "no packages here".
func TestPullPackages_MissingPackagesRepoSkipsGracefully(t *testing.T) {
	cases := map[string]error{
		"NAME_UNKNOWN transport error": errors.New("NAME_UNKNOWN: repository name not known to registry"),
		"ErrImageNotFound":             dkpreg.ErrImageNotFound,
	}

	for name, injected := range cases {
		t.Run(name, func(t *testing.T) {
			reg := upfake.NewRegistry(testHost)
			client := &listTagsAlwaysErr{Client: upfake.NewClient(reg), err: injected}

			svc := newService(t, pkgclient.Adapt(client), nil)

			require.NoError(t, svc.PullPackages(context.Background()),
				"a registry without a /packages repo must skip the phase, not fail the pull")
		})
	}
}

// =============================================================================
// Tests: empty package layouts are not packed into tars
// =============================================================================

// TestPullPackages_EmptyPackageNotPacked verifies that when a package is present
// in the registry listing but has no pullable images (all version channels are
// missing and no version tags match), packPackages skips it and does not create
// an empty stub tar in the bundle directory.
func TestPullPackages_EmptyPackageNotPacked(t *testing.T) {
	// Registry has the package listed but zero version images.
	reg := upfake.NewRegistry(testHost)
	reg.MustAddImage(internal.PackagesSegment, testPackageName, versionImage(channelVersion))
	// Intentionally: no packages/<name>:<version> and no packages/<name>/version:<channel>.

	bundleDir := t.TempDir()
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
	svc.options.BundleDir = bundleDir

	require.NoError(t, svc.PullPackages(context.Background()))

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries,
		"bundle dir must stay empty when package has no images; got: %v", entries)
}

// TestPackPackages_EmptyPackageIsSilent guards the user-facing log output for
// packages whose layout has no pulled images: there must be no "Pack
// package-<name>.tar" header for the empty package. The populated package's
// Pack header is asserted as a positive control to prove the capture pipeline
// is wired up.
func TestPackPackages_EmptyPackageIsSilent(t *testing.T) {
	const (
		fullPkg  = "with-images"
		emptyPkg = "no-images"
	)

	reg := upfake.NewRegistry(testHost)
	addPackage(reg, fullPkg, channelVersion, defaultRegistryVersions)
	// Packages-list entry only - no version tags and no version channels.
	reg.MustAddImage(internal.PackagesSegment, emptyPkg, versionImage(channelVersion))

	bundleDir := t.TempDir()
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
	svc.options.BundleDir = bundleDir

	// Capture user-facing log output by redirecting os.Stdout, then build a
	// fresh Info-level SLogger bound to the redirected stdout.
	stdoutSave := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	defer func() { os.Stdout = stdoutSave }()

	var captured strings.Builder
	drained := make(chan struct{})
	go func() {
		_, _ = io.Copy(&captured, r)
		close(drained)
	}()

	svc.userLogger = log.NewSLogger(slog.LevelInfo)

	runErr := svc.PullPackages(context.Background())

	require.NoError(t, w.Close())
	<-drained
	os.Stdout = stdoutSave
	require.NoError(t, runErr)

	out := captured.String()

	// Positive control: the populated package went through the Pack pipeline.
	assert.Contains(t, out, "Pack package-"+fullPkg+".tar",
		"log-capture sanity: pack header for populated package must appear in captured output; got:\n%s", out)

	// Headline: zero noise for the empty package.
	emptyTar := "package-" + emptyPkg + ".tar"
	assert.NotContains(t, out, emptyTar,
		"empty package must not appear anywhere in user-facing logs; got:\n%s", out)
}

// TestPullPackages_NonEmptyPackagePacked is the positive counterpart: a package
// with at least one pulled image must produce a non-empty tar in the bundle dir.
func TestPullPackages_NonEmptyPackagePacked(t *testing.T) {
	reg := singlePackageRegistry(testPackageName, channelVersion, defaultRegistryVersions)
	bundleDir := t.TempDir()

	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
	svc.options.BundleDir = bundleDir

	require.NoError(t, svc.PullPackages(context.Background()))

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "bundle dir must contain a tar for a package with images")

	for _, e := range entries {
		info, err := e.Info()
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(5120),
			"tar %q must be larger than 5120 bytes (the empty-layout skeleton)", e.Name())
	}
}

// TestPullPackages_InterruptedPullDoesNotProduceEmptyStubTars is the regression
// for the cancellation path: pulling a registry that lists many packages, then
// cancelling mid-flight, must NOT leave behind a stub ~5120-byte
// package-<name>.tar for every package that did not actually download.
func TestPullPackages_InterruptedPullDoesNotProduceEmptyStubTars(t *testing.T) {
	const (
		earlyPkg = "aaa-pulled-first"  // alphabetically first, will fully pull
		latePkg  = "zzz-never-touched" // alphabetically last, will be cancelled out
	)

	reg := upfake.NewRegistry(testHost)
	addPackage(reg, earlyPkg, channelVersion, defaultRegistryVersions)
	addPackage(reg, latePkg, channelVersion, defaultRegistryVersions)

	bundleDir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())

	// Wire the fake client through a wrapper that cancels the user's context
	// the moment a request hits the *second* package's path.
	client := &cancelOnSecondPackage{
		Client:   upfake.NewClient(reg),
		cancel:   cancel,
		trigger:  internal.PackagesSegment + "/" + latePkg,
		canceled: new(atomic.Bool),
	}

	svc := newService(t, pkgclient.Adapt(client), nil)
	svc.options.BundleDir = bundleDir

	err := svc.PullPackages(ctx)
	require.NoError(t, err,
		"PullPackages must finish gracefully after cancellation (packing what was downloaded)")
	require.True(t, client.canceled.Load(), "test bug: cancellation trigger never fired")

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)

	var sawEarly bool
	for _, e := range entries {
		name := e.Name()

		require.NotContains(t, name, latePkg,
			"cancelled package %q must not appear in the bundle dir, found %q", latePkg, name)

		info, err := e.Info()
		require.NoError(t, err)
		require.Greater(t, info.Size(), int64(5120),
			"bundle file %q has stub size %d (must be > 5120 bytes; that's the empty-layout skeleton)",
			name, info.Size())

		if name == "package-"+earlyPkg+".tar" {
			sawEarly = true
		}
	}

	require.True(t, sawEarly,
		"%s should have been pulled and packed before cancellation; entries=%v", earlyPkg, entries)
}

// =============================================================================
// Tests: image layout HasImages helper
// =============================================================================

// TestImageLayouts_HasImages_Empty verifies HasImages returns false when no
// images have been appended to any of the package's sub-layouts.
func TestImageLayouts_HasImages_Empty(t *testing.T) {
	layouts, err := createOCIImageLayoutsForPackage(t.TempDir())
	require.NoError(t, err)
	assert.False(t, layouts.HasImages(),
		"HasImages must return false for a freshly created (empty) layout")
}

// TestImageLayouts_HasImages_WithImage verifies HasImages returns true once an
// image has been appended to one of the package's sub-layouts.
func TestImageLayouts_HasImages_WithImage(t *testing.T) {
	dir := t.TempDir()

	layouts, err := createOCIImageLayoutsForPackage(dir)
	require.NoError(t, err)

	assert.False(t, layouts.HasImages(), "freshly created layout must have no images before any append")

	img := upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"v1.0.0"}`).
		MustBuild()
	err = layouts.Packages.Path().AppendImage(img,
		golayout.WithAnnotations(map[string]string{"io.deckhouse.image.short_tag": "v1.0.0"}))
	require.NoError(t, err)

	assert.True(t, layouts.HasImages(),
		"HasImages must return true after at least one image is appended")
}

// =============================================================================
// Tests: version channel download list & LTS channel
// =============================================================================

// TestPackageVersionChannelDownloadList_FilledCorrectly verifies that
// PullPackages populates packagesDownloadList with the expected version-channel
// image reference keys. Each key is constructed by discoverChannelVersions as:
//
//	rootURL + "/packages/" + packageName + "/version:" + channel
func TestPackageVersionChannelDownloadList_FilledCorrectly(t *testing.T) {
	const channelVer = "v1.45.0"

	reg := singlePackageRegistry(testPackageName, channelVer, []string{channelVer})
	svc := newServiceOpts(t, pkgclient.Adapt(upfake.NewClient(reg)), &Options{
		DryRun:        true,
		SkipVexImages: true,
	})

	require.NoError(t, svc.PullPackages(context.Background()))

	packageDL := svc.packagesDownloadList.Package(testPackageName)
	require.NotNil(t, packageDL, "packagesDownloadList must have an entry for package %q", testPackageName)

	for _, channel := range internal.GetAllDefaultReleaseChannels() {
		expectedRef := packageVersionChannelRef(testPackageName, channel)
		_, ok := packageDL.PackageVersionChannels[expectedRef]
		assert.True(t, ok,
			"packagesDownloadList[%q].PackageVersionChannels should contain ref %q; actual keys: %v",
			testPackageName, expectedRef, packageDL.PackageVersionChannels)
	}
}

// TestPullPackages_LTSChannel verifies the optional LTS version channel (CSE
// editions) is detected by discoverChannelVersions and included in the pull.
func TestPullPackages_LTSChannel(t *testing.T) {
	reg := singlePackageRegistry(testPackageName, channelVersion, defaultRegistryVersions)
	addLTSVersionChannel(reg, testPackageName, channelVersion)

	bundleDir := t.TempDir()
	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
	svc.options.BundleDir = bundleDir

	require.NoError(t, svc.PullPackages(context.Background()))

	packageDL := svc.packagesDownloadList.Package(testPackageName)
	require.NotNil(t, packageDL, "packagesDownloadList must have an entry for package %q", testPackageName)

	ltsRef := packageVersionChannelRef(testPackageName, internal.LTSChannel)
	_, ok := packageDL.PackageVersionChannels[ltsRef]
	assert.True(t, ok,
		"PackageVersionChannels should contain LTS ref %q; actual keys: %v",
		ltsRef, packageDL.PackageVersionChannels)

	assert.Contains(t, pulledPackageVersionRefs(t, svc, testPackageName), packageRef(testPackageName, channelVersion),
		"LTS channel must contribute %s to the pulled package versions", channelVersion)

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "bundle dir must contain a tar when LTS channel pull succeeds")
}

// =============================================================================
// Tests: dry-run never writes bundles
// =============================================================================

// TestDryRun_NoBundleFilesWritten verifies that PullPackages in dry-run mode does
// not write any tar bundles to the bundle directory.
func TestDryRun_NoBundleFilesWritten(t *testing.T) {
	reg := singlePackageRegistry(testPackageName, channelVersion, defaultRegistryVersions)
	bundleDir := t.TempDir()

	svc := newServiceOpts(t, pkgclient.Adapt(upfake.NewClient(reg)), &Options{
		BundleDir:     bundleDir,
		DryRun:        true,
		SkipVexImages: true,
	})

	require.NoError(t, svc.PullPackages(context.Background()))

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run must not write any files to the bundle directory; found: %v", entries)
}

// =============================================================================
// Tests: validate packages access
// =============================================================================

func TestValidatePackagesAccess(t *testing.T) {
	t.Run("packages present in registry returns no error", func(t *testing.T) {
		reg := upfake.NewRegistry(testHost)
		placeholder := upfake.NewImageBuilder().MustBuild()
		reg.MustAddImage(internal.PackagesSegment, "console", placeholder)
		reg.MustAddImage(internal.PackagesSegment, "scanner", placeholder)

		svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
		require.NoError(t, svc.validatePackagesAccess(context.Background()))
	})

	t.Run("packages repository absent in registry returns no error", func(t *testing.T) {
		// Empty registry – the "packages" repo does not exist; access check
		// must treat this as a graceful skip rather than a hard failure.
		reg := upfake.NewRegistry(testHost)
		svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
		require.NoError(t, svc.validatePackagesAccess(context.Background()))
	})

	t.Run("proxy registry skips the listing access check", func(t *testing.T) {
		reg := upfake.NewRegistry(testHost)
		svc := newServiceOpts(t, pkgclient.Adapt(upfake.NewClient(reg)), &Options{
			ProxyRegistry: true,
			Filter:        mustNewFilter(t, modules.FilterTypeWhitelist, testPackageName+"@1.0.0"),
		})
		require.NoError(t, svc.validatePackagesAccess(context.Background()))
	})
}

// TestPullPackages_NoPackagesFound is a smoke test: an empty registry must
// finish without error and write nothing into the bundle directory.
func TestPullPackages_NoPackagesFound(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	bundleDir := t.TempDir()

	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), nil)
	svc.options.BundleDir = bundleDir

	require.NoError(t, svc.PullPackages(context.Background()))

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "empty registry must not produce any bundle files; found: %v", entries)
}

// =============================================================================
// Tests: edition-scoped rootURL
// =============================================================================

// TestPackagesService_RootURL_UsesEditionSegment is the regression for the
// "missing edition segment" report applied to the packages service: the rootURL
// used to compose per-package registry paths must include the edition segment.
func TestPackagesService_RootURL_UsesEditionSegment(t *testing.T) {
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)

	const bareRoot = "registry.deckhouse.ru/deckhouse"
	c := pkgclient.NewFromOptions(bareRoot)
	regSvc := registryservice.NewService(c, pkg.FEEdition, logger)

	svc := NewService(
		regSvc,
		t.TempDir(),
		&Options{BundleDir: t.TempDir(), DryRun: true},
		logger,
		userLogger,
	)

	const editionRoot = bareRoot + "/fe"
	assert.Equal(t, editionRoot, svc.rootURL,
		"packages Service.rootURL must be the edition-scoped root")

	composed := svc.packageRef(testPackageName, channelVersion)
	assert.Equal(t, editionRoot+"/packages/"+testPackageName+":"+channelVersion, composed,
		"per-package registry path must live under the edition sub-tree")
}

// TestPackagesService_RootURL_NoEdition pins down that with no edition the
// rootURL collapses to the bare host.
func TestPackagesService_RootURL_NoEdition(t *testing.T) {
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

	assert.Equal(t, bareRoot, svc.rootURL,
		"packages Service.rootURL must equal the bare root when no edition is set")
}

// =============================================================================
// Tests: PullPackageVersions (shared package-versions.tar)
// =============================================================================

// TestPullPackageVersions_PacksReleaseImages verifies that PullPackageVersions
// clones the version/release-image catalog of every package into a single
// shared package-versions.tar.
func TestPullPackageVersions_PacksReleaseImages(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addPackage(reg, "console", channelVersion, defaultRegistryVersions)
	addPackage(reg, "scanner", "v0.5.0", []string{"v0.5.0"})

	bundleDir := t.TempDir()
	svc := newServiceOpts(t, pkgclient.Adapt(upfake.NewClient(reg)), &Options{
		BundleDir:     bundleDir,
		SkipVexImages: true,
	})

	require.NoError(t, svc.PullPackageVersions(context.Background()))

	assert.FileExists(t, filepath.Join(bundleDir, "package-versions.tar"),
		"PullPackageVersions must produce a package-versions.tar with the release images")
}

// TestPullPackageVersions_NoPackagesIsNoop verifies that a registry without a
// packages catalog produces no package-versions.tar and no error.
func TestPullPackageVersions_NoPackagesIsNoop(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	bundleDir := t.TempDir()

	svc := newServiceOpts(t, pkgclient.Adapt(upfake.NewClient(reg)), &Options{
		BundleDir:     bundleDir,
		SkipVexImages: true,
	})

	require.NoError(t, svc.PullPackageVersions(context.Background()))

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "no packages must produce no package-versions.tar; found: %v", entries)
}

// TestPullPackageVersions_DryRun verifies that PullPackageVersions in dry-run
// mode writes nothing into the bundle directory.
func TestPullPackageVersions_DryRun(t *testing.T) {
	reg := singlePackageRegistry(testPackageName, channelVersion, defaultRegistryVersions)
	bundleDir := t.TempDir()

	svc := newServiceOpts(t, pkgclient.Adapt(upfake.NewClient(reg)), &Options{
		BundleDir:     bundleDir,
		DryRun:        true,
		SkipVexImages: true,
	})

	require.NoError(t, svc.PullPackageVersions(context.Background()))

	entries, err := os.ReadDir(bundleDir)
	require.NoError(t, err)
	assert.Empty(t, entries, "dry-run PullPackageVersions must not write any files; found: %v", entries)
}

// =============================================================================
// Tests: proxy-registry tag probing
// =============================================================================

// TestPullPackages_ProxyRegistryProbesTags verifies that with --proxy-registry
// the package names come straight from the whitelist and per-tag probing
// (CheckImageExists) replaces catalog enumeration. The registry root catalog is
// never listed.
func TestPullPackages_ProxyRegistryProbesTags(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	// Only the per-package main images exist; the registry refuses catalog
	// enumeration (no "packages" list entry, no version channels).
	for _, v := range []string{"v1.40.0", "v1.40.1", "v1.41.0"} {
		reg.MustAddImage(internal.PackagesSegment+"/"+testPackageName, v, versionImage(v))
	}

	counter := newListTagsCounter(upfake.NewClient(reg))
	svc := newServiceOpts(t, pkgclient.Adapt(counter), &Options{
		ProxyRegistry: true,
		SkipVexImages: true,
		Filter:        mustNewFilter(t, modules.FilterTypeWhitelist, testPackageName+"@>=1.40.0"),
	})

	require.NoError(t, svc.PullPackages(context.Background()))

	assert.Zero(t, counter.calls.Load(),
		"proxy-registry mode must never enumerate the registry catalog via ListTags")

	got := pulledPackageVersionRefs(t, svc, testPackageName)
	assert.ElementsMatch(t,
		packageRefs(testPackageName, []string{"v1.40.0", "v1.40.1", "v1.41.0"}),
		got,
		"proxy probe must discover every served tag satisfying the constraint (with >= anchor restored)")
}

// =============================================================================
// Service & filter builders
// =============================================================================

// newServiceOpts wires a Service against the given fake client, with logs muted.
// A default BundleDir is supplied when the caller leaves it empty.
func newServiceOpts(t *testing.T, client dkpreg.Client, opts *Options) *Service {
	t.Helper()

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)
	regSvc := registryservice.NewService(client, pkg.NoEdition, logger)

	if opts == nil {
		opts = &Options{}
	}
	if opts.BundleDir == "" {
		opts.BundleDir = t.TempDir()
	}

	return NewService(regSvc, t.TempDir(), opts, logger, userLogger)
}

// newService wires a Service with a filter and VEX pulls disabled.
func newService(t *testing.T, client dkpreg.Client, filter *modules.Filter) *Service {
	t.Helper()
	return newServiceOpts(t, client, &Options{Filter: filter, SkipVexImages: true})
}

func mustNewFilter(t *testing.T, ftype modules.FilterType, exprs ...string) *modules.Filter {
	t.Helper()
	f, err := modules.NewFilter(exprs, ftype)
	require.NoError(t, err)
	return f
}

// =============================================================================
// Registry fixture builders
// =============================================================================

// addPackage populates a fake registry with one package's worth of refs:
//
//	packages:<name>                    - packages-list entry, points at channelVer
//	packages/<name>:<v>                - one main image per version
//	packages/<name>/version:<v>        - version-tagged release metadata
//	packages/<name>/version:<channel>  - 5 version channels, all pointing at channelVer
func addPackage(reg *upfake.Registry, name, channelVer string, versions []string) {
	reg.MustAddImage(internal.PackagesSegment, name, versionImage(channelVer))
	for _, v := range versions {
		reg.MustAddImage(internal.PackagesSegment+"/"+name, v, versionImage(v))
		reg.MustAddImage(internal.PackagesSegment+"/"+name+"/"+internal.PackagesVersionSegment, v, versionImage(v))
	}
	for _, ch := range internal.GetAllDefaultReleaseChannels() {
		reg.MustAddImage(internal.PackagesSegment+"/"+name+"/"+internal.PackagesVersionSegment, ch, versionImage(channelVer))
	}
}

// singlePackageRegistry builds a fake registry containing exactly one package.
func singlePackageRegistry(name, channelVer string, versions []string) *upfake.Registry {
	reg := upfake.NewRegistry(testHost)
	addPackage(reg, name, channelVer, versions)
	return reg
}

// addLTSVersionChannel adds the optional LTS version channel (CSE editions).
func addLTSVersionChannel(reg *upfake.Registry, name, channelVer string) {
	reg.MustAddImage(internal.PackagesSegment+"/"+name+"/"+internal.PackagesVersionSegment, internal.LTSChannel, versionImage(channelVer))
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

// packageRef is the registry URL the production code uses for a single
// version-tagged main package image.
func packageRef(packageName, version string) string {
	return testHost + "/" + internal.PackagesSegment + "/" + packageName + ":" + version
}

// packageVersionChannelRef is the registry URL for a package version-channel.
func packageVersionChannelRef(packageName, channel string) string {
	return testHost + "/" + internal.PackagesSegment + "/" + packageName + "/" + internal.PackagesVersionSegment + ":" + channel
}

func packageRefs(packageName string, versions []string) []string {
	refs := make([]string, 0, len(versions))
	for _, v := range versions {
		refs = append(refs, packageRef(packageName, v))
	}
	return refs
}

// pulledPackageVersionRefs returns the version-tagged refs the service recorded
// for the given package, dropping @sha256: refs (these are added by extra-image
// and internal-digest resolution and are not relevant to constraint tests).
func pulledPackageVersionRefs(t *testing.T, svc *Service, packageName string) []string {
	t.Helper()
	dl := svc.packagesDownloadList.Package(packageName)
	require.NotNil(t, dl, "no download list recorded for package %s", packageName)

	refs := make([]string, 0, len(dl.Package))
	for ref := range dl.Package {
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

// listTagsErrAtPackage returns the configured error from ListTags only at the
// per-package path (packages/<name>, two segments below root). Root-level
// ListTags - validatePackagesAccess and package-name enumeration - pass through.
type listTagsErrAtPackage struct {
	dkpreg.Client
	depth int
	err   error
}

// perPackageDepth is the WithSegment depth at which a client points at
// packages/<name>: root -> "packages" -> "<name>".
const perPackageDepth = 2

func newListTagsErrAtPackage(c dkpreg.Client, err error) *listTagsErrAtPackage {
	return &listTagsErrAtPackage{Client: c, depth: 0, err: err}
}

func (c *listTagsErrAtPackage) WithSegment(segments ...string) dkpreg.Client {
	return &listTagsErrAtPackage{
		Client: c.Client.WithSegment(segments...),
		depth:  c.depth + len(segments),
		err:    c.err,
	}
}

func (c *listTagsErrAtPackage) ListTags(ctx context.Context, opts ...dkpreg.ListTagsOption) ([]string, error) {
	if c.depth == perPackageDepth {
		return nil, c.err
	}
	return c.Client.ListTags(ctx, opts...)
}

// listTagsAlwaysErr returns a fixed error from every ListTags call, at any depth.
// It simulates a registry whose packages repository does not exist: the root
// packages ListTags (validatePackagesAccess, then discoverPackageNames) is the
// first thing PullPackages probes.
type listTagsAlwaysErr struct {
	dkpreg.Client
	err error
}

func (c *listTagsAlwaysErr) WithSegment(segments ...string) dkpreg.Client {
	return &listTagsAlwaysErr{Client: c.Client.WithSegment(segments...), err: c.err}
}

func (c *listTagsAlwaysErr) ListTags(_ context.Context, _ ...dkpreg.ListTagsOption) ([]string, error) {
	return nil, c.err
}

// cancelOnSecondPackage is a registry client wrapper that fires the supplied
// cancel func the first time a method touches the configured trigger path.
// Used by the interrupted-pull regression to simulate a user hitting Ctrl+C
// at a well-defined point in the pull sequence.
type cancelOnSecondPackage struct {
	dkpreg.Client
	cancel   context.CancelFunc
	trigger  string
	scope    string
	canceled *atomic.Bool
}

func (c *cancelOnSecondPackage) WithSegment(segments ...string) dkpreg.Client {
	next := c.scope
	for _, s := range segments {
		if next == "" {
			next = s
		} else {
			next = next + "/" + s
		}
	}
	return &cancelOnSecondPackage{
		Client:   c.Client.WithSegment(segments...),
		cancel:   c.cancel,
		trigger:  c.trigger,
		scope:    next,
		canceled: c.canceled,
	}
}

func (c *cancelOnSecondPackage) maybeCancel() {
	if c.canceled.Load() {
		return
	}
	if strings.HasPrefix(c.scope, c.trigger) {
		c.canceled.Store(true)
		c.cancel()
	}
}

func (c *cancelOnSecondPackage) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	c.maybeCancel()
	return c.Client.GetDigest(ctx, tag)
}

func (c *cancelOnSecondPackage) GetImage(ctx context.Context, tag string, opts ...dkpreg.ImageGetOption) (dkpreg.Image, error) {
	c.maybeCancel()
	return c.Client.GetImage(ctx, tag, opts...)
}

func (c *cancelOnSecondPackage) CheckImageExists(ctx context.Context, tag string) error {
	c.maybeCancel()
	return c.Client.CheckImageExists(ctx, tag)
}

func (c *cancelOnSecondPackage) ListTags(ctx context.Context, opts ...dkpreg.ListTagsOption) ([]string, error) {
	c.maybeCancel()
	return c.Client.ListTags(ctx, opts...)
}
