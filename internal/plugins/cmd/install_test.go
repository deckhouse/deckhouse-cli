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
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// installFlagSet mirrors the flags newInstallCommand registers, so validation can
// be exercised without building a Manager (which would want a plugins root).
func installFlagSet(t *testing.T, changed ...string) *cobra.Command {
	t.Helper()

	cmd := &cobra.Command{Use: "install"}
	cmd.Flags().String("version", "", "")
	cmd.Flags().Int("use-major", -1, "")
	cmd.Flags().Bool("force", false, "")
	cmd.Flags().Bool("all", false, "")

	for _, name := range changed {
		require.NoError(t, cmd.Flags().Set(name, flagValueFor(name)))
	}

	return cmd
}

func flagValueFor(name string) string {
	switch name {
	case "version":
		return "v1.2.3"
	case "use-major":
		return "2"
	default:
		return "true"
	}
}

func TestValidateInstallArgsSinglePlugin(t *testing.T) {
	assert.NoError(t, validateInstallArgs(installFlagSet(t), []string{"stronghold"}, false))
}

// A bare `install` names nothing to install and must say what to do, rather than
// failing somewhere deeper on an empty plugin name.
func TestValidateInstallArgsRequiresNameOrAll(t *testing.T) {
	err := validateInstallArgs(installFlagSet(t), nil, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "--all")
}

func TestValidateInstallArgsAllTakesNoName(t *testing.T) {
	err := validateInstallArgs(installFlagSet(t, "all"), []string{"stronghold"}, true)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "takes no plugin name")
}

// --version and --use-major pin one plugin to one version, which cannot mean
// anything applied to every installed plugin at once.
func TestValidateInstallArgsAllRejectsPinningFlags(t *testing.T) {
	for _, flag := range []string{"version", "use-major"} {
		t.Run(flag, func(t *testing.T) {
			err := validateInstallArgs(installFlagSet(t, "all", flag), nil, true)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "--"+flag)
			assert.Contains(t, err.Error(), "--all")
		})
	}
}

// --force applies uniformly (re-pull every plugin), so it is the one option that
// combines with --all.
func TestValidateInstallArgsAllAllowsForce(t *testing.T) {
	assert.NoError(t, validateInstallArgs(installFlagSet(t, "all", "force"), nil, true))
}
