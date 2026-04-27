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

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func newResetPasswordCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "reset-password <username> <bcryptHash>",
		Aliases:           []string{"resetpass"},
		Short:             "Reset local user's password in Dex (requires bcrypt hash)",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeUserNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}
			return runUserOperation(cmd, dyn, userOpRequest{
				NamePrefix: "op-resetpass-",
				OpType:     "ResetPassword",
				User:       args[0],
				ExtraSpec: map[string]any{
					"resetPassword": map[string]any{"newPasswordHash": args[1]},
				},
			})
		},
	}

	cmd.Long = "Reset local user's password in Dex.\n\nThe second argument must be a bcrypt hash (e.g. produced by `htpasswd -BinC 10`)."
	addWaitFlags(cmd)
	return cmd
}
