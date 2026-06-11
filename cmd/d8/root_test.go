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

package main

import (
	"io"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopLevelCommandName(t *testing.T) {
	root := &cobra.Command{Use: "d8"}
	cli := &cobra.Command{Use: "cli"}
	update := &cobra.Command{Use: "update"}
	cli.AddCommand(update)
	root.AddCommand(cli)

	assert.Equal(t, "", topLevelCommandName(nil))
	assert.Equal(t, "", topLevelCommandName(root), "the root itself has no top-level subcommand")
	assert.Equal(t, "cli", topLevelCommandName(cli))
	assert.Equal(t, "cli", topLevelCommandName(update), "nested commands resolve to their first-level ancestor")
}

// TestTopLevelCommandNameSurvivesPersistentFlags pins the reason the gating is
// derived from the resolved command and not from os.Args: with a global
// persistent flag in front of the subcommand, args[1] is the flag - an
// args-based check would silently stop matching.
func TestTopLevelCommandNameSurvivesPersistentFlags(t *testing.T) {
	root := &cobra.Command{Use: "d8", SilenceUsage: true, SilenceErrors: true}
	root.PersistentFlags().String("log-level", "", "")

	plugins := &cobra.Command{Use: "plugins"}
	list := &cobra.Command{Use: "list", RunE: func(*cobra.Command, []string) error { return nil }}
	plugins.AddCommand(list)
	root.AddCommand(plugins)

	root.SetArgs([]string{"--log-level=debug", "plugins", "list"})
	root.SetOut(io.Discard)

	executed, err := root.ExecuteC()
	require.NoError(t, err)

	top := topLevelCommandName(executed)
	assert.Equal(t, "plugins", top)
	assert.True(t, isPluginManagementCommand(top))
	assert.False(t, isSelfUpdateCommand(top))
	assert.False(t, skipUpdateNotify(top))
}

func TestSkipUpdateNotify(t *testing.T) {
	for _, topLevel := range []string{"", "help", "completion", cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd} {
		assert.True(t, skipUpdateNotify(topLevel), "%q must be side-effect-free", topLevel)
	}

	for _, topLevel := range []string{"mirror", "kubectl", "plugins", "cli", "system"} {
		assert.False(t, skipUpdateNotify(topLevel), "%q is an ordinary command for the notify hook", topLevel)
	}
}
