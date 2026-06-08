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
	"strings"
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/requirements"
)

func envValue(env []string, key string) (string, bool) {
	for _, kv := range env {
		if name, val, ok := strings.Cut(kv, "="); ok && name == key {
			return val, true
		}
	}

	return "", false
}

func TestPluginRunEnvInjectsRequestedVars(t *testing.T) {
	m := testManager()
	contract := &internal.Plugin{
		Name: "p",
		Env: []internal.EnvVar{
			{Name: "KUBECONFIG"},
			{Name: "PLUGINS_CALLER"},
			{Name: "MODULE_CONFIG_INFO"}, // deferred - must not crash, not injected
		},
	}

	env := m.pluginRunEnv(contract)

	kubeconfig, ok := envValue(env, "KUBECONFIG")
	assert.True(t, ok, "KUBECONFIG is injected when requested")
	assert.NotEmpty(t, kubeconfig)

	caller, ok := envValue(env, "PLUGINS_CALLER")
	assert.True(t, ok, "PLUGINS_CALLER is injected when requested")
	assert.NotEmpty(t, caller, "PLUGINS_CALLER points at the d8 executable")

	_, ok = envValue(env, "MODULE_CONFIG_INFO")
	assert.False(t, ok, "MODULE_CONFIG_INFO is deferred, not injected")
}

func TestPluginRunEnvNilContractIsInherited(t *testing.T) {
	m := testManager()
	env := m.pluginRunEnv(nil)
	assert.NotEmpty(t, env, "a nil contract yields the inherited environment")
}

func TestPluginRunEnvOnlyRequestedVars(t *testing.T) {
	m := testManager()
	// A contract that requests nothing must not inject PLUGINS_CALLER/KUBECONFIG.
	env := m.pluginRunEnv(&internal.Plugin{Name: "p"})
	_, ok := envValue(env, "PLUGINS_CALLER")
	assert.False(t, ok, "PLUGINS_CALLER is injected only when the contract requests it")
}

func TestIsLocalPluginInvocation(t *testing.T) {
	for _, args := range [][]string{
		{"--help"}, {"-h"}, {"--version"}, {"-v"}, {"help"}, {"completion", "bash"}, {"__complete"},
		// `--help` after a subcommand is still a help query.
		{"server", "--help"}, {"status", "-h"},
	} {
		assert.True(t, isLocalPluginInvocation(args), "%v is local", args)
	}

	for _, args := range [][]string{
		{}, {"secret", "put"},
		// past a literal `--` a help token is plugin payload, not a flag.
		{"run", "--", "--help"},
	} {
		assert.False(t, isLocalPluginInvocation(args), "%v needs the gate", args)
	}
}

func TestEnsurePluginRequirementsNoRequirements(t *testing.T) {
	m := testManager()
	m.pluginDirectory = t.TempDir()

	// A contract with no cluster/plugin requirements passes without cluster access.
	require.NoError(t, m.ensurePluginRequirements(context.Background(), &internal.Plugin{Name: "p"}))
}

func TestEnsurePluginRequirementsBlocksOnViolation(t *testing.T) {
	m := testManager()
	m.pluginDirectory = t.TempDir()
	m.clusterStateCache = &requirements.ClusterState{Kubernetes: semver.MustParse("v1.28.3")}

	contract := &internal.Plugin{
		Name:         "p",
		Requirements: internal.Requirements{Kubernetes: internal.KubernetesRequirement{Constraint: ">= 1.30"}},
	}

	err := m.ensurePluginRequirements(context.Background(), contract)
	require.Error(t, err, "an unsatisfied Kubernetes requirement blocks the run")
	assert.Contains(t, err.Error(), "1.30")
}

func TestEnsurePluginRequirementsReportsMissingDependency(t *testing.T) {
	m := testManager()
	m.pluginDirectory = t.TempDir()

	contract := &internal.Plugin{
		Name: "p",
		Requirements: internal.Requirements{
			Plugins: internal.PluginRequirementsGroup{
				Mandatory: []internal.PluginRequirement{{Name: "delivery", Constraint: ">= 1.0.0"}},
			},
		},
	}

	err := m.ensurePluginRequirements(context.Background(), contract)
	require.Error(t, err, "a missing mandatory plugin dependency blocks the run")
	assert.Contains(t, err.Error(), "delivery")
	assert.Contains(t, err.Error(), "not installed")
}

func TestEnsurePluginRequirementsPassesWhenSatisfied(t *testing.T) {
	m := testManager()
	m.pluginDirectory = t.TempDir()
	m.clusterStateCache = &requirements.ClusterState{Kubernetes: semver.MustParse("v1.31.0")}

	contract := &internal.Plugin{
		Name:         "p",
		Requirements: internal.Requirements{Kubernetes: internal.KubernetesRequirement{Constraint: ">= 1.30"}},
	}

	assert.NoError(t, m.ensurePluginRequirements(context.Background(), contract))
}
