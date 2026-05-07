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
	"strings"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

// NewDeprecatedTopLevelCommand returns a hidden top-level "user" command that
// preserves backward compatibility for the four UserOperation-issuing commands
// that existed at the root before the iam refactor:
//
//	d8 user lock <user> <duration>
//	d8 user unlock <user>
//	d8 user reset-password <user> <bcryptHash>   (legacy 2-positional form)
//	d8 user reset-2fa <user>                     (alias of reset2fa)
//
// lock / unlock / reset2fa are reused verbatim from the in-package factory
// (newUserOpCommand) so their flags / completion / wait semantics stay
// byte-identical with the new `d8 iam user ...` counterparts.
//
// reset-password is the one shape we cannot reuse: the legacy command took a
// positional bcrypt hash, while the modern iam form expects --password-hash.
// We rebuild the legacy 2-arg signature explicitly here and delegate the
// actual UserOperation wire shape to runUserOperation (the same helper the
// modern command uses), so the only difference is the input-parsing layer.
//
// Other former d8 user surfaces (create / delete / get / list) are NOT
// re-exposed here — they were either added in this PR (no BC obligation) or
// never existed at the root (so there is nothing to break).
func NewDeprecatedTopLevelCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "user",
		Short:         "Deprecated: use 'd8 iam user' instead",
		Hidden:        true,
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	flags.AddPersistentFlags(cmd)

	cmd.AddCommand(newDeprecatedResetPasswordCommand())

	for _, def := range userOpDefs {
		sub := newUserOpCommand(def)
		// Use head-of-Use as the canonical name segment for the new-path
		// hint; e.g. "lock <username> <lockDuration>" -> "lock".
		newPath := "d8 iam user " + strings.SplitN(sub.Use, " ", 2)[0]
		cmd.AddCommand(deprecateForward(sub, newPath))
	}

	// reset-2fa: hyphenated form is the historical d8 user spelling; we keep
	// it as a hidden alias on the underlying reset2fa command so old scripts
	// keep working. Cobra resolves aliases identically to Use, so the banner
	// (whose new-path hint comes from cmd.Use head segment) still names
	// reset2fa, which matches the canonical iam form.
	if reset2fa, _, err := cmd.Find([]string{"reset2fa"}); err == nil && reset2fa != nil {
		reset2fa.Aliases = append(reset2fa.Aliases, "reset-2fa")
	}

	return cmd
}

// newDeprecatedResetPasswordCommand re-creates the legacy
//
//	d8 user reset-password <username> <bcryptHash>
//
// shape verbatim. The hash is forwarded raw into
// spec.resetPassword.newPasswordHash, matching the pre-refactor behaviour
// (no validation, no base64 wrap, no interactive prompt). Operators who want
// the new prompt / generate / stdin flow are pointed at the modern command
// in the deprecation banner.
func newDeprecatedResetPasswordCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "reset-password <username> <bcryptHash>",
		Aliases:           []string{"resetpass"},
		Short:             "Deprecated: use 'd8 iam user reset-password' instead",
		Long:              "Deprecated. Reset a local user's password in Dex by submitting a UserOperation.\n\nThe second positional argument is a raw bcrypt hash (e.g. produced by `htpasswd -BinC 10`). For interactive prompt, --password-stdin or --generate-password support, use 'd8 iam user reset-password' instead.",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completeUserNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprintln(cmd.ErrOrStderr(),
				"Warning: 'd8 user reset-password <user> <hash>' is deprecated and will be removed in a future release; "+
					"use 'd8 iam user reset-password <user> --password-hash <hash>' instead.")
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			username, bcryptHash := args[0], args[1]

			dyn, err := utilk8s.NewDynamicClient(cmd)
			if err != nil {
				return err
			}

			return runUserOperation(cmd, dyn, userOpRequest{
				NamePrefix: "op-resetpass-",
				OpType:     "ResetPassword",
				User:       username,
				ExtraSpec: map[string]any{
					"resetPassword": map[string]any{
						"newPasswordHash": bcryptHash,
					},
				},
			})
		},
	}
	addWaitFlags(cmd)
	return cmd
}

// deprecateForward attaches a stderr deprecation banner to sub, emitted before
// the original PreRunE/RunE chain executes. The banner format is intentionally
// stable so external tooling can grep it.
func deprecateForward(sub *cobra.Command, newPath string) *cobra.Command {
	origPre := sub.PreRunE
	sub.PreRunE = func(cmd *cobra.Command, args []string) error {
		// "d8 user <name>" reflects how the operator invoked the legacy
		// command; cmd.Name() is the leaf command name regardless of which
		// alias resolved to it.
		fmt.Fprintf(cmd.ErrOrStderr(),
			"Warning: 'd8 user %s' is deprecated and will be removed in a future release; use '%s' instead.\n",
			cmd.Name(), newPath)
		if origPre != nil {
			return origPre(cmd, args)
		}
		return nil
	}
	return sub
}
