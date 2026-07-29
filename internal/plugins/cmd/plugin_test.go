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

package pluginscmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/plugins/flags"
)

func TestWithContractHelp(t *testing.T) {
	contract := &internal.Plugin{
		Name:        "p",
		Description: "does things",
		Flags:       []internal.Flag{{Name: "--my-feature-flag"}},
		Env:         []internal.EnvVar{{Name: "KUBECONFIG"}},
	}

	help := withContractHelp("does things", contract)

	assert.Contains(t, help, "does things")
	assert.Contains(t, help, "--my-feature-flag")
	assert.Contains(t, help, "Flags forwarded to the plugin:")
	assert.Contains(t, help, "Environment requested by the plugin:")
	assert.Contains(t, help, "KUBECONFIG (provided by d8)")
}

func TestWithContractHelpMarksUnprovidedEnv(t *testing.T) {
	contract := &internal.Plugin{
		Name: "p",
		Env:  []internal.EnvVar{{Name: "MODULE_CONFIG_INFO"}},
	}

	help := withContractHelp("desc", contract)
	assert.Contains(t, help, "MODULE_CONFIG_INFO (not provided by d8 yet")
}

func TestWithContractHelpNoFlagsOrEnv(t *testing.T) {
	help := withContractHelp("just a description", &internal.Plugin{Name: "p"})
	assert.Equal(t, "just a description", help, "no extra sections when the contract declares none")
}

func TestNewPluginCommandReturnsCommandWhenInstallRootFails(t *testing.T) {
	// Make EnsureInstallRoot fail with a non-permission error (a path component is a
	// regular file -> ENOTDIR), so no home fallback is attempted and the test is hermetic.
	tmp := t.TempDir()
	blocker := filepath.Join(tmp, "blocker")
	require.NoError(t, os.WriteFile(blocker, nil, 0o644))

	orig := flags.DeckhousePluginsDir
	flags.DeckhousePluginsDir = filepath.Join(blocker, "sub")
	t.Cleanup(func() { flags.DeckhousePluginsDir = orig })

	cmd := NewPluginCommand("system", "Operate system options", []string{"s"}, dkplog.NewNop())
	require.NotNil(t, cmd, "a failed install root must not yield a nil command (nil panics cobra.AddCommand)")
	assert.Equal(t, "system", cmd.Use)
}
