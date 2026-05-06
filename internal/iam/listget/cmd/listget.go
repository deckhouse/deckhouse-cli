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

// Package listget hosts the top-level read-only commands `d8 iam get` and
// `d8 iam list`. It is intentionally a thin composition layer: every
// subcommand here is a factory exported by package iam/access/cmd, which
// owns the actual aggregation pipeline. The split exists so that the
// kubectl-style `iam get / iam list` UX can sit at the top of the tree
// without dragging the access aggregation code through an import cycle.
package listget

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	access "github.com/deckhouse/deckhouse-cli/internal/iam/access/cmd"
	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
)

var getLong = templates.LongDesc(`
Show a single Deckhouse IAM object with its effective context.

Subcommands:
  user <name>   show effective access for a user (groups, grants, summary)
  group <name>  show effective access for a group (members, grants, summary)
  rule REF      show one ClusterAuthorizationRule or AuthorizationRule

Aliases users / groups / rules are accepted for symmetry with "d8 iam list".

© Flant JSC 2026`)

var listLong = templates.LongDesc(`
List Deckhouse IAM objects with their effective context.

Subcommands:
  users   list every user with effective-access summary (groups, grants count, level)
  groups  list every group with effective-access summary (members, grants count, level)
  rules   list every ClusterAuthorizationRule and AuthorizationRule

Aliases user / group / rule are accepted so both forms work as muscle memory.

© Flant JSC 2026`)

// NewGetCommand returns the "d8 iam get" parent command.
func NewGetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "get (user|group|rule) NAME",
		Short:         "Show a single Deckhouse IAM object",
		Long:          getLong,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	flags.AddPersistentFlags(cmd)

	cmd.AddCommand(
		access.NewGetUserCommand(),
		access.NewGetGroupCommand(),
		access.NewGetRuleCommand(),
	)

	return cmd
}

// NewListCommand returns the "d8 iam list" parent command.
func NewListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "list (users|groups|rules)",
		Short:         "List Deckhouse IAM objects",
		Long:          listLong,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	flags.AddPersistentFlags(cmd)

	cmd.AddCommand(
		access.NewListUsersCommand(),
		access.NewListGroupsCommand(),
		access.NewListRulesCommand(),
	)

	return cmd
}
