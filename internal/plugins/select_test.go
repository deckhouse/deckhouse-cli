/*
Copyright 2025 Flant JSC

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
	"fmt"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal"
	d8flags "github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/requirements"
)

// fakeSelectSource is a pluginSource returning fixed tags and per-tag contracts,
// counting GetPluginContract calls so cache behaviour can be asserted.
type fakeSelectSource struct {
	tags          []string
	contracts     map[string]*internal.Plugin
	contractCalls map[string]int
}

func (f *fakeSelectSource) ListPluginTags(context.Context, string) ([]string, error) {
	return f.tags, nil
}

func (f *fakeSelectSource) GetPluginContract(_ context.Context, _, tag string) (*internal.Plugin, error) {
	if f.contractCalls != nil {
		f.contractCalls[tag]++
	}

	contract, ok := f.contracts[tag]
	if !ok {
		return nil, fmt.Errorf("no contract for %s", tag)
	}

	return contract, nil
}

func (f *fakeSelectSource) ExtractPlugin(context.Context, string, string, string) error {
	return nil
}

func k8sContract(version, constraint string) *internal.Plugin {
	return &internal.Plugin{
		Name:         "p",
		Version:      version,
		Requirements: internal.Requirements{Kubernetes: internal.KubernetesRequirement{Constraint: constraint}},
	}
}

func TestSortedSemverDesc(t *testing.T) {
	got := sortedSemverDesc([]string{"v1.0.0", "v2.0.0", "latest", "v1.5.0"})

	require.Len(t, got, 3, "non-semver tags are dropped")
	assert.Equal(t, "2.0.0", got[0].String())
	assert.Equal(t, "1.5.0", got[1].String())
	assert.Equal(t, "1.0.0", got[2].String())
}

func TestSelectLatestCompatiblePicksNewestCompatible(t *testing.T) {
	tags := []string{"v1.0.0", "v1.1.0", "v1.2.0"}
	m := testManager()
	m.service = &fakeSelectSource{tags: tags, contracts: map[string]*internal.Plugin{
		"v1.2.0": k8sContract("v1.2.0", ">= 99.0"), // needs a newer cluster - skipped
		"v1.1.0": k8sContract("v1.1.0", ">= 1.20"), // compatible
		"v1.0.0": k8sContract("v1.0.0", ""),
	}}
	m.clusterStateCache = &requirements.ClusterState{Kubernetes: semver.MustParse("v1.28.3")}

	got, err := m.selectLatestCompatible(context.Background(), "p", tags)
	require.NoError(t, err)
	assert.Equal(t, "v1.1.0", got.Original(), "skips the too-new v1.2.0, picks newest compatible")
}

func TestSelectLatestCompatibleNewestWhenAllCompatible(t *testing.T) {
	tags := []string{"v1.0.0", "v1.2.0", "v1.1.0"}
	m := testManager()
	m.service = &fakeSelectSource{tags: tags, contracts: map[string]*internal.Plugin{
		"v1.2.0": k8sContract("v1.2.0", ">= 1.20"),
		"v1.1.0": k8sContract("v1.1.0", ">= 1.20"),
		"v1.0.0": k8sContract("v1.0.0", ">= 1.20"),
	}}
	m.clusterStateCache = &requirements.ClusterState{Kubernetes: semver.MustParse("v1.28.3")}

	got, err := m.selectLatestCompatible(context.Background(), "p", tags)
	require.NoError(t, err)
	assert.Equal(t, "v1.2.0", got.Original())
}

func TestSelectLatestCompatibleNoneCompatible(t *testing.T) {
	tags := []string{"v1.0.0", "v1.1.0"}
	m := testManager()
	m.service = &fakeSelectSource{tags: tags, contracts: map[string]*internal.Plugin{
		"v1.1.0": k8sContract("v1.1.0", ">= 99.0"),
		"v1.0.0": k8sContract("v1.0.0", ">= 99.0"),
	}}
	m.clusterStateCache = &requirements.ClusterState{Kubernetes: semver.MustParse("v1.28.3")}

	_, err := m.selectLatestCompatible(context.Background(), "p", tags)
	require.Error(t, err)
}

func TestSelectLatestCompatibleSkipChecksPicksNewest(t *testing.T) {
	prev := d8flags.SkipClusterChecks
	t.Cleanup(func() { d8flags.SkipClusterChecks = prev })
	d8flags.SkipClusterChecks = true

	tags := []string{"v1.0.0", "v2.0.0"}
	m := testManager()
	m.service = &fakeSelectSource{tags: tags, contracts: map[string]*internal.Plugin{
		"v2.0.0": k8sContract("v2.0.0", ">= 99.0"), // would be incompatible if checked
		"v1.0.0": k8sContract("v1.0.0", ""),
	}}
	// No clusterStateCache and no cluster: skip must avoid consulting it at all.

	got, err := m.selectLatestCompatible(context.Background(), "p", tags)
	require.NoError(t, err)
	assert.Equal(t, "v2.0.0", got.Original())
}

func TestSelectLatestCompatibleExcludesPrereleases(t *testing.T) {
	// The newest tag is a genuine pre-release; the default pick must skip it.
	tags := []string{"v1.0.0", "v2.0.0-rc.1"}
	m := testManager()
	m.service = &fakeSelectSource{tags: tags, contracts: map[string]*internal.Plugin{
		"v2.0.0-rc.1": {Name: "p", Version: "v2.0.0-rc.1"},
		"v1.0.0":      {Name: "p", Version: "v1.0.0"},
	}}

	got, err := m.selectLatestCompatible(context.Background(), "p", tags)
	require.NoError(t, err)
	assert.Equal(t, "v1.0.0", got.Original(), "pre-release excluded from default pick")
}

func TestSelectLatestCompatibleMalformedContractHardStops(t *testing.T) {
	// A malformed constraint in the NEWEST contract is operational: selection must
	// hard-stop, not silently downgrade to an older version.
	tags := []string{"v1.0.0", "v2.0.0"}
	m := testManager()
	m.service = &fakeSelectSource{tags: tags, contracts: map[string]*internal.Plugin{
		"v2.0.0": k8sContract("v2.0.0", "garbage-constraint"),
		"v1.0.0": k8sContract("v1.0.0", ">= 1.0"),
	}}
	m.clusterStateCache = &requirements.ClusterState{Kubernetes: semver.MustParse("v1.28.3")}

	_, err := m.selectLatestCompatible(context.Background(), "p", tags)
	require.Error(t, err, "operational error must not be masked as incompatibility")
}

func TestPluginContractCachesPerTag(t *testing.T) {
	calls := map[string]int{}
	m := testManager()
	m.service = &fakeSelectSource{
		contracts:     map[string]*internal.Plugin{"v1.0.0": {Name: "p", Version: "v1.0.0"}},
		contractCalls: calls,
	}

	for range 3 {
		_, err := m.PluginContract(context.Background(), "p", "v1.0.0")
		require.NoError(t, err)
	}

	assert.Equal(t, 1, calls["v1.0.0"], "contract fetched once, then served from cache")
}
