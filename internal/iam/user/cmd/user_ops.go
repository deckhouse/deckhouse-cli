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

// userOpDef declares one UserOperation-backed command. The factory below
// (newUserOpCommand) turns each entry into a concrete cobra command, which
// keeps lock / unlock / reset-2fa to ~10 lines each instead of a full file
// per command.
//
// Reset-password is intentionally NOT covered by this factory: it needs the
// full password-input flow (prompt / stdin / generate / hash) and the extra
// flags would be confusing on the simpler ops.
type userOpDef struct {
	Use        string
	Aliases    []string
	Short      string
	Long       string
	NamePrefix string
	OpType     string
	Args       cobra.PositionalArgs
	// BuildExtraSpec returns the operation-specific spec.<field> map (e.g. spec.lock).
	// Returning a nil map is fine for ops that need no extra spec (Unlock, Reset2FA).
	BuildExtraSpec func(args []string) (map[string]any, error)
}

// userOpDefs is the table of UserOperation commands that need only a
// positional argument (the username) and an optional simple validation step.
// Keep entries sorted by NamePrefix so registration order stays stable.
var userOpDefs = []userOpDef{
	{
		Use:        "lock <username> <lockDuration>",
		Short:      "Lock local user in Dex for a period of time",
		Long:       "Lock local user in Dex for a period of time.\n\nThe lockDuration argument must be a duration string (e.g. 30s, 10m, 1h).",
		NamePrefix: "op-lock-",
		OpType:     "Lock",
		Args:       cobra.ExactArgs(2),
		BuildExtraSpec: func(args []string) (map[string]any, error) {
			if _, err := time.ParseDuration(args[1]); err != nil {
				return nil, fmt.Errorf("invalid lockDuration %q: %w", args[1], err)
			}
			return map[string]any{
				"lock": map[string]any{"for": args[1]},
			}, nil
		},
	},
	{
		Use:        "unlock <username>",
		Short:      "Unlock local user in Dex",
		Long:       "Unlock local user in Dex.\n\nThis requests a UserOperation of type Unlock and waits for completion by default.",
		NamePrefix: "op-unlock-",
		OpType:     "Unlock",
		Args:       cobra.ExactArgs(1),
	},
	{
		Use:        "reset2fa <username>",
		Short:      "Reset local user's 2FA (TOTP) in Dex",
		Long:       "Reset local user's 2FA (TOTP) in Dex.\n\nThis requests a UserOperation of type Reset2FA and waits for completion by default.",
		NamePrefix: "op-reset2fa-",
		OpType:     "Reset2FA",
		Args:       cobra.ExactArgs(1),
	},
}

func newUserOpCommand(def userOpDef) *cobra.Command {
	cmd := &cobra.Command{
		Use:               def.Use,
		Aliases:           def.Aliases,
		Short:             def.Short,
		Long:              def.Long,
		Args:              def.Args,
		ValidArgsFunction: completeUserNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE: func(cmd *cobra.Command, args []string) error {
			var extra map[string]any
			if def.BuildExtraSpec != nil {
				e, err := def.BuildExtraSpec(args)
				if err != nil {
					return err
				}
				extra = e
			}
			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}
			return runUserOperation(cmd, dyn, userOpRequest{
				NamePrefix: def.NamePrefix,
				OpType:     def.OpType,
				User:       args[0],
				ExtraSpec:  extra,
			})
		},
	}
	addWaitFlags(cmd)
	return cmd
}
