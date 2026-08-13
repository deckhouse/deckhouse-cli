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

package plugins

import (
	"context"
	"encoding/base64"
	"log/slog"
	"testing"

	"github.com/Masterminds/semver/v3"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	upfake "github.com/deckhouse/deckhouse/pkg/registry/fake"

	"github.com/deckhouse/deckhouse-cli/pkg"
	pkgclient "github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

const testHost = "fake.registry"

const pluginsCatalogRepo = "deckhouse-cli/plugins"

// newTestCatalog builds a Catalog over a fake registry, wired through the
// real registry service so the deckhouse-cli/plugins scoping is exercised too.
func newTestCatalog(t *testing.T, reg *upfake.Registry) Catalog {
	t.Helper()

	stub := pkgclient.Adapt(upfake.NewClient(reg))
	logger := dkplog.NewLogger(dkplog.WithLevel(slog.LevelWarn))
	regSvc := registryservice.NewService(stub, pkg.NoEdition, logger)

	return NewCatalog(regSvc.PluginService(), logger)
}

// addPluginVersion publishes one plugin version image into the fake registry:
// tagged in the plugin repo and, when contractJSON is non-empty, annotated
// with the base64 contract. The catalog name index gets the plugin name tag.
func addPluginVersion(t *testing.T, reg *upfake.Registry, name, tag, contractJSON string) {
	t.Helper()

	img := upfake.NewImageBuilder().WithFile("plugin", "binary-"+name+"-"+tag).MustBuild()

	if contractJSON != "" {
		encoded := base64.StdEncoding.EncodeToString([]byte(contractJSON))
		annotated, ok := mutate.Annotations(img, map[string]string{"contract": encoded}).(v1.Image)
		require.True(t, ok, "mutate.Annotations on an image must return an image")
		img = annotated
	}

	reg.MustAddImage(pluginsCatalogRepo+"/"+name, tag, img)
	reg.MustAddImage(pluginsCatalogRepo, name, upfake.NewImageBuilder().WithFile("name", name).MustBuild())
}

// addRawContract publishes a plugin version whose contract annotation is the
// given raw string (not JSON-encoded here), for malformed-contract cases.
func addRawContract(t *testing.T, reg *upfake.Registry, name, tag, rawAnnotation string) {
	t.Helper()

	img := upfake.NewImageBuilder().WithFile("plugin", "binary").MustBuild()
	annotated, ok := mutate.Annotations(img, map[string]string{"contract": rawAnnotation}).(v1.Image)
	require.True(t, ok)

	reg.MustAddImage(pluginsCatalogRepo+"/"+name, tag, annotated)
}

func TestCatalog_PluginNames(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addPluginVersion(t, reg, "postgresql-mgr", "v1.0.0", "")
	addPluginVersion(t, reg, "db-connector", "v0.9.0", "")

	catalog := newTestCatalog(t, reg)

	names, err := catalog.PluginNames(context.Background())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"postgresql-mgr", "db-connector"}, names,
		"plugin names must come from the catalog directory-as-tags index")
}

// TestCatalog_PluginVersions_FiltersAndSorts pins the stable-version notion:
// werf build junk (non-semver tags) is dropped, genuine pre-releases are
// dropped, "-main" CI markers stay, and the result is newest first.
func TestCatalog_PluginVersions_FiltersAndSorts(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	for _, tag := range []string{
		"v1.0.0", "v1.1.0", "v1.2.0-main",
		"v2.0.0-rc.1",       // genuine pre-release: dropped
		"main-linux-amd64",  // werf junk: dropped
		"meta-89584285_abc", // werf junk: dropped
	} {
		addPluginVersion(t, reg, "postgresql-mgr", tag, "")
	}

	catalog := newTestCatalog(t, reg)

	versions, err := catalog.PluginVersions(context.Background(), "postgresql-mgr")
	require.NoError(t, err)

	got := make([]string, 0, len(versions))
	for _, v := range versions {
		got = append(got, v.Original())
	}

	assert.Equal(t, []string{"v1.2.0-main", "v1.1.0", "v1.0.0"}, got)
}

// TestCatalog_PluginVersions_Memoized: the second call must not see registry
// changes - the version list is fetched once per pull.
func TestCatalog_PluginVersions_Memoized(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addPluginVersion(t, reg, "postgresql-mgr", "v1.0.0", "")

	catalog := newTestCatalog(t, reg)

	first, err := catalog.PluginVersions(context.Background(), "postgresql-mgr")
	require.NoError(t, err)
	require.Len(t, first, 1)

	addPluginVersion(t, reg, "postgresql-mgr", "v9.9.9", "")

	second, err := catalog.PluginVersions(context.Background(), "postgresql-mgr")
	require.NoError(t, err)
	assert.Len(t, second, 1, "memoized version list must not refresh mid-pull")
}

func TestCatalog_Contract_FullV2Schema(t *testing.T) {
	const contractJSON = `{
		"name": "postgresql-mgr",
		"version": "v1.2.3",
		"description": "manage postgresql",
		"requirements": {
			"deckhouse": {"constraint": ">=1.70"},
			"modules": {"mandatory": [{"name": "postgresql", "constraint": ">=1.0.0"}]},
			"plugins": {"mandatory": [{"name": "db-connector", "constraint": ">=0.9.0"}]}
		}
	}`

	reg := upfake.NewRegistry(testHost)
	addPluginVersion(t, reg, "postgresql-mgr", "v1.2.3", contractJSON)

	catalog := newTestCatalog(t, reg)

	contract, err := catalog.Contract(context.Background(), "postgresql-mgr", semver.MustParse("v1.2.3"))
	require.NoError(t, err)

	assert.Equal(t, "postgresql-mgr", contract.Name)
	assert.Equal(t, "v1.2.3", contract.Version)
	assert.Equal(t, ">=1.70", contract.Requirements.Deckhouse.Constraint)

	require.Len(t, contract.Requirements.Modules.Mandatory, 1)
	assert.Equal(t, "postgresql", contract.Requirements.Modules.Mandatory[0].Name)
	assert.Equal(t, ">=1.0.0", contract.Requirements.Modules.Mandatory[0].Constraint)

	require.Len(t, contract.Requirements.Plugins.Mandatory, 1)
	assert.Equal(t, "db-connector", contract.Requirements.Plugins.Mandatory[0].Name)
}

// TestCatalog_Contract_NoAnnotation: a contract-less image is not an error -
// only name and version are known, there is nothing to enforce.
func TestCatalog_Contract_NoAnnotation(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addPluginVersion(t, reg, "plain", "v1.0.0", "")

	catalog := newTestCatalog(t, reg)

	contract, err := catalog.Contract(context.Background(), "plain", semver.MustParse("v1.0.0"))
	require.NoError(t, err)

	assert.Equal(t, "plain", contract.Name)
	assert.Equal(t, "v1.0.0", contract.Version)
	assert.Empty(t, contract.Requirements.Modules.Mandatory)
}

// TestCatalog_Contract_ContentErrors: every deterministic content problem
// must carry the ErrInvalidContract sentinel, so the resolver can skip the
// version instead of failing the pull.
func TestCatalog_Contract_ContentErrors(t *testing.T) {
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	cases := []struct {
		name       string
		annotation string
	}{
		{"broken base64", "%%%not-base64%%%"},
		{"malformed JSON", b64("{{{")},
		{"v1 array schema rejected", b64(`{"name":"x","version":"v1.0.0","requirements":{"modules":[{"name":"m"}]}}`)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := upfake.NewRegistry(testHost)
			addRawContract(t, reg, "broken", "v1.0.0", tc.annotation)

			catalog := newTestCatalog(t, reg)

			_, err := catalog.Contract(context.Background(), "broken", semver.MustParse("v1.0.0"))
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidContract)
		})
	}
}

// TestCatalog_Contract_TransportErrorIsNotContentError: a missing tag must
// come back as a plain error WITHOUT the ErrInvalidContract sentinel - the
// resolver treats it as operational, not as a broken published version.
func TestCatalog_Contract_TransportErrorIsNotContentError(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addPluginVersion(t, reg, "present", "v1.0.0", "")

	catalog := newTestCatalog(t, reg)

	_, err := catalog.Contract(context.Background(), "present", semver.MustParse("v9.9.9"))
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrInvalidContract)
}

// TestCatalog_Contract_Memoized: re-publishing a tag mid-pull must not change
// the already-fetched contract.
func TestCatalog_Contract_Memoized(t *testing.T) {
	reg := upfake.NewRegistry(testHost)
	addPluginVersion(t, reg, "postgresql-mgr", "v1.0.0", `{"name":"postgresql-mgr","version":"v1.0.0","description":"first"}`)

	catalog := newTestCatalog(t, reg)

	first, err := catalog.Contract(context.Background(), "postgresql-mgr", semver.MustParse("v1.0.0"))
	require.NoError(t, err)
	require.Equal(t, "first", first.Description)

	addPluginVersion(t, reg, "postgresql-mgr", "v1.0.0", `{"name":"postgresql-mgr","version":"v1.0.0","description":"second"}`)

	second, err := catalog.Contract(context.Background(), "postgresql-mgr", semver.MustParse("v1.0.0"))
	require.NoError(t, err)
	assert.Equal(t, "first", second.Description, "memoized contract must not refresh mid-pull")
}
