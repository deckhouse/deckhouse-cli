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

package group

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
)

var groupLong = templates.LongDesc(`
Manage Deckhouse local groups (user-authn).

This command provides lifecycle operations for local Group CRs:
Create, Delete, Get, List, and membership management via add-member / remove-member.

© Flant JSC 2026`)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "group",
		Aliases:       []string{"groups"},
		Short:         "Manage Deckhouse local groups (user-authn)",
		Long:          groupLong,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	flags.AddPersistentFlags(cmd)

	cmd.AddCommand(
		newCreateCommand(),
		newDeleteCommand(),
		newGetCommand(),
		newListCommand(),
		newAddMemberCommand(),
		newRemoveMemberCommand(),
	)

	return cmd
}
