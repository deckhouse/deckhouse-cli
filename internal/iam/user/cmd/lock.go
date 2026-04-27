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
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

func newLockCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "lock <username> <lockDuration>",
		Short:             "Lock local user in Dex for a period of time",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeUserNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			username := args[0]
			lockDuration := args[1]
			if _, err := time.ParseDuration(lockDuration); err != nil {
				return fmt.Errorf("invalid lockDuration %q: %w", lockDuration, err)
			}

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			return runUserOperation(cmd, dyn, userOpRequest{
				NamePrefix: "op-lock-",
				OpType:     "Lock",
				User:       username,
				ExtraSpec: map[string]any{
					"lock": map[string]any{"for": lockDuration},
				},
			})
		},
	}

	cmd.Long = "Lock local user in Dex for a period of time.\n\nThe lockDuration argument must be a duration string (e.g. 30s, 10m, 1h)."
	addWaitFlags(cmd)
	return cmd
}
