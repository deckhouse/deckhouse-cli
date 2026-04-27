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

func newUnlockCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "unlock <username>",
		Short:             "Unlock local user in Dex",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeUserNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}
			return runUserOperation(cmd, dyn, userOpRequest{
				NamePrefix: "op-unlock-",
				OpType:     "Unlock",
				User:       args[0],
			})
		},
	}

	cmd.Long = "Unlock local user in Dex.\n\nThis requests a UserOperation of type Unlock and waits for completion by default."
	addWaitFlags(cmd)
	return cmd
}
