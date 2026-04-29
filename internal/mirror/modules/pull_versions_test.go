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

// Shared test-fixture identifiers.
const (
	testHost       = "fake.registry"
	testModule     = "console"
	channelVersion = "v1.45.2"
)

var defaultModuleVersions = []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.45.2"}

// Each pullModules run calls ListTags twice at the root (validateModulesAccess
// + module enumeration). Per-module ListTags adds one more call, but only for
// non-exact (semver) constraints - exact and blacklist short-circuit.
const (
	baselineListTagsCalls int64 = 2
	perModuleListTagsCall int64 = 1
)

// Regression: --include-module <name>@<semver> must pull every matching tag
// from the registry, not only the version currently on a release channel.
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
			// Bare version expands to ^X.Y.Z (legacy backward compat in parseVersionConstraint).
			name:           "implicit_caret",
			constraint:     "1.40.0",
			expected:       []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", "v1.43.0", channelVersion},
			mustNotContain: "v1.39.0",
		},
		{
			// Explicit caret must produce the same range as implicit.
			name:           "explicit_caret",
			constraint:     "^1.40.0",
			expected:       []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.42.0", "v1.43.0", channelVersion},
			mustNotContain: "v1.39.0",
		},
		{
			// Tilde pins to the same minor; channelVersion still arrives via release channels.
			name:           "tilde",
			constraint:     "~1.40.0",
			expected:       []string{"v1.40.0", "v1.40.1", channelVersion},
			mustNotContain: "v1.41.0",
		},
		{
			// Explicit range with exclusive upper bound.
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

// Constraints on different --include-module flags must not leak across modules.
// Mixes semver and exact constraints to also exercise both filter branches in one pull.
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

// Exact-tag branch never reads Module.Releases, so per-module ListTags is skipped.
func TestPullSingleModule_ExactTagConstraintSkipsPerModuleListTags(t *testing.T) {
	filter, err := NewFilter([]string{testModule + "@=v1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	counter := runPullWithCounter(t, filter)

	assert.Equal(t, baselineListTagsCalls, counter.calls.Load(),
		"exact-tag constraint must skip per-module ListTags")
}

// Blacklist mode short-circuits VersionsToMirror, so per-module ListTags is skipped.
func TestPullSingleModule_BlacklistModeSkipsPerModuleListTags(t *testing.T) {
	filter, err := NewFilter(nil, FilterTypeBlacklist)
	require.NoError(t, err)

	counter := runPullWithCounter(t, filter)

	assert.Equal(t, baselineListTagsCalls, counter.calls.Load(),
		"blacklist mode must skip per-module ListTags for non-excluded modules")
}

// Counter-check: semver constraint MUST trigger per-module ListTags, or the original bug returns.
func TestPullSingleModule_SemverConstraintTriggersPerModuleListTags(t *testing.T) {
	filter, err := NewFilter([]string{testModule + "@1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	counter := runPullWithCounter(t, filter)

	assert.Equal(t, baselineListTagsCalls+perModuleListTagsCall, counter.calls.Load(),
		"semver constraint must trigger per-module ListTags exactly once")
}

// Missing module repo is warned and skipped (same policy as validateModulesAccess),
// not propagated as a fatal error.
func TestPullSingleModule_PerModuleListTagsImageNotFoundIsSkipped(t *testing.T) {
	failingClient := newFailingListTagsClient(
		upfake.NewClient(buildTestRegistry(defaultModuleVersions)),
		[]string{"modules", testModule},
		dkpreg.ErrImageNotFound,
	)

	filter, err := NewFilter([]string{testModule + "@1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	svc := newService(t, pkgclient.Adapt(failingClient), filter)
	require.NoError(t, svc.PullModules(context.Background()),
		"pull must not fail when per-module ListTags reports the module repo is missing")

	dl := svc.modulesDownloadList.list[testModule]
	require.NotNil(t, dl)

	// With per-module tag list skipped, only the channel snapshot contributes.
	assert.ElementsMatch(t,
		taggedModuleRefs(testModule, []string{channelVersion}),
		extractTaggedRefs(dl),
		"bundle should contain only the channel snapshot")
}

// Anything other than ErrImageNotFound is fail-fast: we cannot verify the
// constraint and refuse to produce a silently-wrong partial bundle.
func TestPullSingleModule_PerModuleListTagsTransientErrorFailsFast(t *testing.T) {
	transientErr := errors.New("simulated registry 503")
	failingClient := newFailingListTagsClient(
		upfake.NewClient(buildTestRegistry(defaultModuleVersions)),
		[]string{"modules", testModule},
		transientErr,
	)

	filter, err := NewFilter([]string{testModule + "@1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	svc := newService(t, pkgclient.Adapt(failingClient), filter)
	pullErr := svc.PullModules(context.Background())

	require.Error(t, pullErr, "non-NotFound errors from per-module ListTags must propagate")
	assert.ErrorIs(t, pullErr, transientErr, "wrapped error must preserve the underlying cause")
	assert.Contains(t, pullErr.Error(), "list tags for module "+testModule)
}

// addModule mirrors the production "modules/<name>" + "modules/<name>/release/<channel>"
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

// buildTestRegistry builds a single-module fixture. Multi-module tests should
// call addModule directly on a fresh upfake.NewRegistry.
func buildTestRegistry(moduleVersions []string) *upfake.Registry {
	reg := upfake.NewRegistry(testHost)
	addModule(reg, testModule, channelVersion, moduleVersions)
	return reg
}

// makeVersionImage builds a v1.Image carrying only version.json. Missing
// images_digests.json / extra_images.json is tolerated downstream.
func makeVersionImage(version string) v1.Image {
	return upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+version+`"}`).
		WithLabel("org.opencontainers.image.version", version).
		MustBuild()
}

// taggedRef builds the registry URL production code uses for a single version-tagged image.
func taggedRef(moduleName, version string) string {
	return testHost + "/modules/" + moduleName + ":" + version
}

// taggedModuleRefs is taggedRef applied to a slice; convenient for ElementsMatch assertions.
func taggedModuleRefs(moduleName string, versions []string) []string {
	refs := make([]string, 0, len(versions))
	for _, v := range versions {
		refs = append(refs, taggedRef(moduleName, v))
	}
	return refs
}

// extractTaggedRefs returns tagged refs from the download list, dropping
// @sha256: refs added by extra-image resolution (irrelevant to constraint tests).
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

// listTagsCounter counts ListTags calls. The counter is shared across
// sub-clients spawned by WithSegment, so all scoped calls increment one counter.
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

// failingListTagsClient replaces ListTags with failErr only when the accumulated
// WithSegment chain matches failOn exactly. Other paths pass through unchanged.
type failingListTagsClient struct {
	dkpreg.Client
	segments []string
	failOn   []string
	failErr  error
}

func newFailingListTagsClient(c dkpreg.Client, failOn []string, failErr error) *failingListTagsClient {
	return &failingListTagsClient{Client: c, failOn: failOn, failErr: failErr}
}

func (c *failingListTagsClient) WithSegment(segments ...string) dkpreg.Client {
	next := make([]string, 0, len(c.segments)+len(segments))
	next = append(next, c.segments...)
	next = append(next, segments...)
	return &failingListTagsClient{
		Client:   c.Client.WithSegment(segments...),
		segments: next,
		failOn:   c.failOn,
		failErr:  c.failErr,
	}
}

func (c *failingListTagsClient) ListTags(ctx context.Context, opts ...dkpreg.ListTagsOption) ([]string, error) {
	if segmentsEqual(c.segments, c.failOn) {
		return nil, c.failErr
	}
	return c.Client.ListTags(ctx, opts...)
}

func segmentsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// newService wires a Service against the given client and filter, muting logs.
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

// runPullWithCounter runs PullModules against the default fixture, returning the call counter.
func runPullWithCounter(t *testing.T, filter *Filter) *listTagsCounter {
	t.Helper()

	counter := newListTagsCounter(upfake.NewClient(buildTestRegistry(defaultModuleVersions)))
	svc := newService(t, pkgclient.Adapt(counter), filter)
	require.NoError(t, svc.PullModules(context.Background()))
	return counter
}
