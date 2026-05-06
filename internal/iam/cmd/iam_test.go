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

package cmd

import (
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIAMTree locks down the public command surface of `d8 iam` so that
// the kubectl-style top-level get/list verbs are wired through and the
// pre-refactor wrappers (user get/list, group get/list, access list,
// access rules list/get) stay removed.
//
// We verify by walking the cobra tree, not by scanning source, so that
// renames inside subpackages don't cause silent UX regressions: if any
// listed path goes missing, the assertion fires here with a readable diff.
func TestIAMTree(t *testing.T) {
	root := NewCommand()

	mustExist := []string{
		// Mutating verbs stay where they were.
		"iam user create",
		"iam user delete",
		"iam user reset-password",
		"iam user reset2fa",
		"iam user lock",
		"iam user unlock",
		"iam group create",
		"iam group delete",
		"iam group add-member",
		"iam group remove-member",
		"iam access grant",
		"iam access revoke",

		// New top-level read verbs (kubectl-style).
		"iam get user",
		"iam get group",
		"iam get rule",
		"iam list users",
		"iam list groups",
		"iam list rules",
	}

	mustNotExist := []string{
		// Old per-resource read wrappers — fully removed (no Hidden alias).
		"iam user get",
		"iam user list",
		"iam group get",
		"iam group list",
		"iam access list",
		"iam access rules",
		// "explain" was folded into "iam get user/group" — same warnings,
		// one command instead of two.
		"iam access explain",
	}

	have := collectPaths(root)

	for _, p := range mustExist {
		assert.Contains(t, have, p, "expected command %q to exist", p)
	}
	for _, p := range mustNotExist {
		assert.NotContains(t, have, p, "command %q should be removed", p)
	}
}

// TestGetListAcceptSingularAndPlural makes sure both `iam list users` and
// `iam list user` (and the same for groups/rules) resolve to the same
// command, since several places in our docs and muscle memory use the
// singular form.
func TestGetListAcceptSingularAndPlural(t *testing.T) {
	root := NewCommand()

	pairs := [][2]string{
		// list <plural> is canonical, list <singular> must alias to it.
		{"list users", "list user"},
		{"list groups", "list group"},
		{"list rules", "list rule"},
		// get <singular> is canonical, get <plural> must alias to it.
		{"get user", "get users"},
		{"get group", "get groups"},
		{"get rule", "get rules"},
	}

	for _, p := range pairs {
		canonical := mustFind(t, root, p[0])
		alias := mustFind(t, root, p[1])
		require.Equal(t, canonical, alias,
			"alias %q must resolve to the same cobra.Command as %q", p[1], p[0])
	}
}

func collectPaths(c *cobra.Command) []string {
	var out []string
	var walk func(node *cobra.Command, prefix string)
	walk = func(node *cobra.Command, prefix string) {
		path := prefix
		if path != "" {
			path += " "
		}
		path += node.Name()
		out = append(out, path)
		for _, child := range node.Commands() {
			walk(child, path)
		}
	}
	walk(c, "")
	sort.Strings(out)
	return out
}

func mustFind(t *testing.T, root *cobra.Command, args string) *cobra.Command {
	t.Helper()
	cmd, _, err := root.Find(strings.Fields(args))
	require.NoErrorf(t, err, "find %q", args)
	require.NotNilf(t, cmd, "command %q must exist", args)
	require.NotEqualf(t, root.Name(), cmd.Name(),
		"command %q resolved to root, meaning the path is unknown", args)
	return cmd
}
