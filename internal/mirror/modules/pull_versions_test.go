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
	"sync/atomic"
	"testing"

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

// Test fixture identifiers and shared registry contents. Tests that need
// extra version tags pass their own list to buildTestRegistry; otherwise
// defaultModuleVersions is used.
const (
	testHost       = "fake.registry"
	testModule     = "console"
	channelVersion = "v1.45.2"
)

var defaultModuleVersions = []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.45.2"}

// ListTags is called twice on the root level for every pullModules run:
// - once by validateModulesAccess to smoke-check registry credentials,
// - once by pullModules itself to enumerate all modules in the registry.
// On top of this baseline, pullSingleModule calls ListTags once per module
// that has a non-exact (semver) constraint - exact-tag and blacklist modules
// short-circuit before this call.
const (
	baselineListTagsCalls int64 = 2
	perModuleListTagsCall int64 = 1
)

// Regression: --include-module <name>@<semver> must pull every matching tag
// in the registry, not only the version currently on a release channel.
// Each subtest uses the same registry layout but a different constraint;
// the spread of registry versions is wide enough to make the expected sets
// of caret / tilde / range distinguishable.
func TestPullSingleModule_SemverConstraintMatchesRegistryTags(t *testing.T) {
	registryVersions := []string{
		"v1.39.0",
		"v1.40.0", "v1.40.1",
		"v1.41.0",
		"v1.42.0",
		"v1.43.0",
		channelVersion, // v1.45.2 - the version every release channel points at
	}

	cases := []struct {
		name           string
		constraint     string   // value placed after "<module>@" in the filter
		expected       []string // module versions expected in the download list
		mustNotContain string   // a version that MUST be excluded (sanity check)
	}{
		{
			// Implicit caret: bare version is expanded to ^X.Y.Z for legacy
			// backward compatibility (see filter.go:parseVersionConstraint).
			name:           "implicit_caret",
			constraint:     "1.40.0",
			expected:       []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", "v1.43.0", channelVersion},
			mustNotContain: "v1.39.0",
		},
		{
			// Explicit caret: same range as implicit. Verifies the parser
			// treats both forms identically.
			name:           "explicit_caret",
			constraint:     "^1.40.0",
			expected:       []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", "v1.43.0", channelVersion},
			mustNotContain: "v1.39.0",
		},
		{
			// Tilde: pinned to the same minor (>=1.40.0 <1.41.0).
			// channelVersion is unrelated to the constraint but is always
			// added to the download list because release channels are still
			// pulled for any non-exact constraint.
			name:           "tilde",
			constraint:     "~1.40.0",
			expected:       []string{"v1.40.0", "v1.40.1", channelVersion},
			mustNotContain: "v1.41.0",
		},
		{
			// Explicit range. The upper bound is exclusive, so v1.43.0
			// must not appear in the result.
			name:           "explicit_range",
			constraint:     ">=1.40.0 <1.43.0",
			expected:       []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", channelVersion},
			mustNotContain: "v1.43.0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filter, err := NewFilter([]string{testModule + "@" + tc.constraint}, FilterTypeWhitelist)
			require.NoError(t, err)

			svc := newService(t, pkgclient.Adapt(upfake.NewClient(buildTestRegistry(registryVersions))), filter)
			require.NoError(t, svc.PullModules(context.Background()))

			dl := svc.modulesDownloadList.list[testModule]
			require.NotNil(t, dl)

			got := extractTaggedRefs(dl)
			assert.ElementsMatch(t, taggedModuleRefs(testModule, tc.expected), got)
			assert.NotContains(t, got, taggedRef(testModule, tc.mustNotContain),
				"constraint %q must exclude %s", tc.constraint, tc.mustNotContain)
		})
	}
}

// Multiple --include-module flags must each apply their own constraint
// independently; constraints on one module must not leak into another.
// This case mixes a semver-constraint module with an exact-tag module to
// also verify the two filter branches coexist correctly in one pull run.
func TestPullSingleModule_MultipleModulesIndependentConstraints(t *testing.T) {
	const (
		consoleName   = "console"
		commanderName = "commander"
	)

	reg := upfake.NewRegistry(testHost)
	addModule(reg, consoleName, "v1.40.1", []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.45.2"})
	addModule(reg, commanderName, "v0.5.1", []string{"v0.5.0", "v0.5.1", "v0.6.0"})

	filter, err := NewFilter(
		[]string{
			consoleName + "@~1.40.0",    // tilde: only v1.40.x + channel snapshot
			commanderName + "@=v0.6.0", // exact: only v0.6.0, no channels
		},
		FilterTypeWhitelist,
	)
	require.NoError(t, err)

	svc := newService(t, pkgclient.Adapt(upfake.NewClient(reg)), filter)
	require.NoError(t, svc.PullModules(context.Background()))

	consoleDL := svc.modulesDownloadList.list[consoleName]
	require.NotNil(t, consoleDL)
	assert.ElementsMatch(t,
		taggedModuleRefs(consoleName, []string{"v1.40.0", "v1.40.1"}),
		extractTaggedRefs(consoleDL),
		"tilde constraint on console must match only v1.40.x (channel v1.40.1 dedups)")

	commanderDL := svc.modulesDownloadList.list[commanderName]
	require.NotNil(t, commanderDL)
	assert.ElementsMatch(t,
		taggedModuleRefs(commanderName, []string{"v0.6.0"}),
		extractTaggedRefs(commanderDL),
		"exact constraint on commander must match only v0.6.0 and skip channels")
}

// Exact-tag constraint takes a Filter branch that never reads Module.Releases,
// so per-module ListTags must be skipped - only the baseline calls happen.
func TestPullSingleModule_ExactTagConstraintSkipsPerModuleListTags(t *testing.T) {
	filter, err := NewFilter([]string{testModule + "@=v1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	counter := runPullWithCounter(t, filter)

	assert.Equal(t, baselineListTagsCalls, counter.calls.Load(),
		"exact-tag constraint must skip per-module ListTags")
}

// In blacklist mode (no --include-module) modules without a filter entry
// short-circuit in Filter.VersionsToMirror, so per-module ListTags must be
// skipped. Saves N requests for N non-excluded modules.
func TestPullSingleModule_BlacklistModeSkipsPerModuleListTags(t *testing.T) {
	filter, err := NewFilter(nil, FilterTypeBlacklist)
	require.NoError(t, err)

	counter := runPullWithCounter(t, filter)

	assert.Equal(t, baselineListTagsCalls, counter.calls.Load(),
		"blacklist mode must skip per-module ListTags for non-excluded modules")
}

// Counterpart to the two skip tests: semver constraint MUST trigger per-module
// ListTags, otherwise the original bug resurfaces.
func TestPullSingleModule_SemverConstraintTriggersPerModuleListTags(t *testing.T) {
	filter, err := NewFilter([]string{testModule + "@1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	counter := runPullWithCounter(t, filter)

	assert.Equal(t, baselineListTagsCalls+perModuleListTagsCall, counter.calls.Load(),
		"semver constraint must trigger per-module ListTags exactly once")
}

// addModule populates a single module's images in the given registry,
// mirroring the production "modules/<name>" + "modules/<name>/release/<channel>"
// layout. All 5 release channels point at channelVer.
func addModule(reg *upfake.Registry, name, channelVer string, versions []string) {
	reg.MustAddImage("modules", name, makeVersionImage(channelVer))

	for _, v := range versions {
		reg.MustAddImage("modules/"+name, v, makeVersionImage(v))
	}

	for _, ch := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
		reg.MustAddImage("modules/"+name+"/release", ch, makeVersionImage(channelVer))
	}
}

// buildTestRegistry returns a fake registry containing a single module
// (testModule) with the given versions and channelVersion on every channel.
// For multi-module fixtures, build the registry by hand and call addModule
// for each module.
func buildTestRegistry(moduleVersions []string) *upfake.Registry {
	reg := upfake.NewRegistry(testHost)
	addModule(reg, testModule, channelVersion, moduleVersions)
	return reg
}

// makeVersionImage builds a v1.Image with only version.json populated.
// images_digests.json and extra_images.json are intentionally omitted -
// downstream code tolerates missing files.
func makeVersionImage(version string) v1.Image {
	return upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+version+`"}`).
		WithLabel("org.opencontainers.image.version", version).
		MustBuild()
}

// taggedRef returns the registry URL the production code uses for a single
// version-tagged module image.
func taggedRef(moduleName, version string) string {
	return testHost + "/modules/" + moduleName + ":" + version
}

// taggedModuleRefs converts a list of versions for a given module into the
// registry URLs the regression assertions expect to see in the download list.
func taggedModuleRefs(moduleName string, versions []string) []string {
	refs := make([]string, 0, len(versions))
	for _, v := range versions {
		refs = append(refs, taggedRef(moduleName, v))
	}
	return refs
}

// extractTaggedRefs returns version-tagged module image refs from the
// download list, filtering out @sha256: refs added by extra-image
// resolution (those are not relevant to constraint tests).
func extractTaggedRefs(dl *ImageDownloadList) []string {
	refs := make([]string, 0, len(dl.Module))
	for ref := range dl.Module {
		if strings.Contains(ref, "@sha256:") {
			continue
		}
		refs = append(refs, ref)
	}
	return refs
}

// listTagsCounter wraps a Client and counts ListTags calls. The counter is
// shared across all sub-clients produced by WithSegment, so scoped calls
// increment the same counter as the parent.
type listTagsCounter struct {
	dkpreg.Client
	calls *atomic.Int64
}

func newListTagsCounter(c dkpreg.Client) *listTagsCounter {
	return &listTagsCounter{Client: c, calls: new(atomic.Int64)}
}

func (c *listTagsCounter) WithSegment(segments ...string) dkpreg.Client {
	return &listTagsCounter{
		Client: c.Client.WithSegment(segments...),
		calls:  c.calls,
	}
}

func (c *listTagsCounter) ListTags(ctx context.Context, opts ...dkpreg.ListTagsOption) ([]string, error) {
	c.calls.Add(1)
	return c.Client.ListTags(ctx, opts...)
}

// newService wires a Service against the given registry client and filter
// with logging muted to warn-level. Used by tests that inspect either the
// download list or the ListTags call counter.
func newService(t *testing.T, stubClient dkpreg.Client, filter *Filter) *Service {
	t.Helper()

	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	userLogger := log.NewSLogger(slog.LevelWarn)
	regSvc := registryservice.NewService(stubClient, pkg.NoEdition, logger)

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

// runPullWithCounter runs PullModules against a fresh fake registry built
// with defaultModuleVersions and returns the ListTags call counter.
func runPullWithCounter(t *testing.T, filter *Filter) *listTagsCounter {
	t.Helper()

	counter := newListTagsCounter(upfake.NewClient(buildTestRegistry(defaultModuleVersions)))
	svc := newService(t, pkgclient.Adapt(counter), filter)
	require.NoError(t, svc.PullModules(context.Background()))
	return counter
}
