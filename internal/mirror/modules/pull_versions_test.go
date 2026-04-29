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
func TestPullSingleModule_SemverConstraintExpandsToRegistryTags(t *testing.T) {
	// v1.39.0 is below the ^1.40.0 lower bound and must be excluded by the filter.
	registryVersions := []string{
		"v1.39.0",
		"v1.40.0", "v1.40.1",
		"v1.41.0", "v1.42.0", "v1.43.0", "v1.44.0",
		"v1.45.2",
	}
	expectedVersions := []string{
		"v1.40.0", "v1.40.1",
		"v1.41.0", "v1.42.0", "v1.43.0", "v1.44.0",
		"v1.45.2",
	}

	filter, err := NewFilter([]string{testModule + "@1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	svc := newService(t, pkgclient.Adapt(upfake.NewClient(buildTestRegistry(registryVersions))), filter)
	require.NoError(t, svc.PullModules(context.Background()))

	dl := svc.modulesDownloadList.list[testModule]
	require.NotNil(t, dl)

	got := extractTaggedRefs(dl)
	assert.ElementsMatch(t, taggedModuleRefs(expectedVersions), got)
	assert.NotContains(t, got, taggedRef("v1.39.0"),
		"v1.39.0 is below ^1.40.0 lower bound and must be excluded")
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

// buildTestRegistry returns a fake registry that mirrors the production
// "modules/<name>" + "modules/<name>/release/<channel>" layout.
// Pass moduleVersions to control the set of version-tagged module images.
func buildTestRegistry(moduleVersions []string) *upfake.Registry {
	reg := upfake.NewRegistry(testHost)

	reg.MustAddImage("modules", testModule, makeVersionImage(channelVersion))

	for _, v := range moduleVersions {
		reg.MustAddImage("modules/"+testModule, v, makeVersionImage(v))
	}

	for _, ch := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
		reg.MustAddImage("modules/"+testModule+"/release", ch, makeVersionImage(channelVersion))
	}

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
func taggedRef(version string) string {
	return testHost + "/modules/" + testModule + ":" + version
}

// taggedModuleRefs converts a list of versions into the registry URLs the
// regression assertions expect to see in the download list.
func taggedModuleRefs(versions []string) []string {
	refs := make([]string, 0, len(versions))
	for _, v := range versions {
		refs = append(refs, taggedRef(v))
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
