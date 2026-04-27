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
	user "github.com/deckhouse/deckhouse-cli/internal/iam/user/cmd"
)

var iamLong = templates.LongDesc(`
Manage Deckhouse identity and access: users, groups, and access grants.

Subcommands:
  user    — manage local static users (user-authn Dex).
  group   — manage local groups and their membership (user-authn).
  access  — grant, revoke, list, and explain permissions backed by
            AuthorizationRule and ClusterAuthorizationRule CRs (user-authz).

Each subcommand accepts the standard --kubeconfig / --context flags
(short: -k / --context) inherited from its own persistent flag set.

© Flant JSC 2026`)

// NewCommand returns the "d8 iam" parent command that composes the user,
// group, and access subcommands under a single namespace.
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
	)

	return cmd
}
