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

const (
	pullVersionsTestHost   = "fake.registry"
	pullVersionsTestModule = "console"
)

// Regression: --include-module <name>@<semver> must pull every matching tag
// in the registry, not only the version currently on a release channel.
func TestPullSingleModule_SemverConstraintExpandsToRegistryTags(t *testing.T) {
	const channelVersion = "v1.45.2"

	// v1.39.0 is below the ^1.40.0 lower bound and must be excluded by the filter.
	allVersions := []string{
		"v1.39.0",
		"v1.40.0",
		"v1.40.1",
		"v1.41.0",
		"v1.42.0",
		"v1.43.0",
		"v1.44.0",
		"v1.45.2",
	}

	reg := upfake.NewRegistry(pullVersionsTestHost)
	reg.MustAddImage("modules", pullVersionsTestModule, pullVersionsImage(channelVersion))
	for _, v := range allVersions {
		reg.MustAddImage("modules/"+pullVersionsTestModule, v, pullVersionsImage(v))
	}
	for _, ch := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
		reg.MustAddImage("modules/"+pullVersionsTestModule+"/release", ch, pullVersionsImage(channelVersion))
	}

	filter, err := NewFilter([]string{pullVersionsTestModule + "@1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	svc := newPullVersionsService(t, pkgclient.Adapt(upfake.NewClient(reg)), filter)
	require.NoError(t, svc.PullModules(context.Background()))

	moduleDL := svc.modulesDownloadList.list[pullVersionsTestModule]
	require.NotNil(t, moduleDL)

	got := make([]string, 0, len(moduleDL.Module))
	for ref := range moduleDL.Module {
		// Skip @sha256: refs added by extractInternalDigestImages - the assertion is about version-tagged refs.
		if strings.Contains(ref, "@sha256:") {
			continue
		}
		got = append(got, ref)
	}

	want := []string{
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.40.0",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.40.1",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.41.0",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.42.0",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.43.0",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.44.0",
		pullVersionsTestHost + "/modules/" + pullVersionsTestModule + ":v1.45.2",
	}

	assert.ElementsMatch(t, want, got)
	assert.NotContains(t, got, pullVersionsTestHost+"/modules/"+pullVersionsTestModule+":v1.39.0")
}

// Exact-tag constraint takes a Filter branch that never reads Module.Releases,
// so per-module ListTags must be skipped. Baseline = 2 (validateModulesAccess
// + pullModules root listing).
func TestPullSingleModule_ExactTagConstraintSkipsPerModuleListTags(t *testing.T) {
	filter, err := NewFilter([]string{pullVersionsTestModule + "@=v1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	counter := runPullModulesWithCounter(t, filter)

	assert.Equal(t, int64(2), counter.calls.Load(),
		"exact-tag constraint must skip per-module ListTags")
}

// In blacklist mode (no --include-module) modules without a filter entry
// short-circuit in Filter.VersionsToMirror, so per-module ListTags must be
// skipped. Saves N requests for N non-excluded modules.
func TestPullSingleModule_BlacklistModeSkipsPerModuleListTags(t *testing.T) {
	filter, err := NewFilter(nil, FilterTypeBlacklist)
	require.NoError(t, err)

	counter := runPullModulesWithCounter(t, filter)

	assert.Equal(t, int64(2), counter.calls.Load(),
		"blacklist mode must skip per-module ListTags for non-excluded modules")
}

// Counterpart to the two skip tests: semver constraint MUST trigger per-module
// ListTags, otherwise the original bug resurfaces.
func TestPullSingleModule_SemverConstraintTriggersPerModuleListTags(t *testing.T) {
	filter, err := NewFilter([]string{pullVersionsTestModule + "@1.40.0"}, FilterTypeWhitelist)
	require.NoError(t, err)

	counter := runPullModulesWithCounter(t, filter)

	assert.Equal(t, int64(3), counter.calls.Load(),
		"semver constraint must trigger per-module ListTags exactly once")
}

// pullVersionsImage builds a v1.Image with only version.json.
// images_digests.json and extra_images.json are intentionally omitted -
// downstream code tolerates missing files.
func pullVersionsImage(version string) v1.Image {
	return upfake.NewImageBuilder().
		WithFile("version.json", `{"version":"`+version+`"}`).
		WithLabel("org.opencontainers.image.version", version).
		MustBuild()
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

// pullVersionsBuildRegistry mirrors the production modules layout: top-level
// "modules" repo, per-module repo with version tags, per-module "release" repo
// with channel tags.
func pullVersionsBuildRegistry() *upfake.Registry {
	const channelVersion = "v1.45.2"

	reg := upfake.NewRegistry(pullVersionsTestHost)

	reg.MustAddImage("modules", pullVersionsTestModule, pullVersionsImage(channelVersion))

	for _, v := range []string{"v1.40.0", "v1.40.1", "v1.41.0", "v1.45.2"} {
		reg.MustAddImage("modules/"+pullVersionsTestModule, v, pullVersionsImage(v))
	}

	for _, ch := range []string{"alpha", "beta", "early-access", "stable", "rock-solid"} {
		reg.MustAddImage("modules/"+pullVersionsTestModule+"/release", ch, pullVersionsImage(channelVersion))
	}

	return reg
}

// newPullVersionsService wires a Service against the given registry client and
// filter with logging muted to warn-level. Used by tests that inspect the
// resulting download list.
func newPullVersionsService(t *testing.T, stubClient dkpreg.Client, filter *Filter) *Service {
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

// runPullModulesWithCounter runs PullModules against a fresh fake registry
// and returns the counter for ListTags assertions.
func runPullModulesWithCounter(t *testing.T, filter *Filter) *listTagsCounter {
	t.Helper()

	counter := newListTagsCounter(upfake.NewClient(pullVersionsBuildRegistry()))
	svc := newPullVersionsService(t, pkgclient.Adapt(counter), filter)
	require.NoError(t, svc.PullModules(context.Background()))
	return counter
}
