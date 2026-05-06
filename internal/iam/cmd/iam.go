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
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	access "github.com/deckhouse/deckhouse-cli/internal/iam/access/cmd"
	group "github.com/deckhouse/deckhouse-cli/internal/iam/group/cmd"
	listget "github.com/deckhouse/deckhouse-cli/internal/iam/listget/cmd"
	user "github.com/deckhouse/deckhouse-cli/internal/iam/user/cmd"
)

var iamLong = templates.LongDesc(`
Manage Deckhouse identity and access: users, groups, and access grants.

Subcommands:
  user    — local users (user-authn Dex): create/delete/reset-password/
            reset2fa/lock/unlock.
  group   — local groups: create/delete and add-member/remove-member.
  access  — grant and revoke (current authz model).
  get     — show one user/group/rule with effective access and warnings.
  list    — list users/groups/rules with effective access.

All subcommands accept the standard --kubeconfig / --context flags.

© Flant JSC 2026`)

// NewCommand returns the "d8 iam" parent command that composes the user,
// group, access, and the kubectl-style get/list subcommands.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "iam",
		Short:         "Manage Deckhouse users, groups, and access grants",
		Long:          iamLong,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	cmd.AddCommand(
		user.NewCommand(),
		group.NewCommand(),
		access.NewCommand(),
		listget.NewGetCommand(),
		listget.NewListCommand(),
	)

	return cmd
}
