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

package user

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
)

var userOperationLong = templates.LongDesc(`
Manage Deckhouse local static users (user-authn).

This command provides lifecycle operations for Dex local users:
Create, Delete, Get, List, ResetPassword, Reset2FA, Lock, Unlock.

© Flant JSC 2026`)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "user",
		Aliases:       []string{"userop"},
		Short:         "Manage Deckhouse users (user-authn)",
		Long:          userOperationLong,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	// Reuse standard kubeconfig/context flags (same as `d8 system ...`).
	flags.AddPersistentFlags(cmd)

	cmd.AddCommand(
		newCreateCommand(),
		newDeleteCommand(),
		newGetCommand(),
		newListCommand(),
		newReset2FACommand(),
		newResetPasswordCommand(),
		newLockCommand(),
		newUnlockCommand(),
	)

	return cmd
}
