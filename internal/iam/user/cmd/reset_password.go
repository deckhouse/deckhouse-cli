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

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/utilk8s"
)

var resetPasswordLong = templates.LongDesc(`
Reset a local static user's password in Dex.

The new password is read from one of:

  --password-prompt          (default if stdin is a terminal)
  --password-stdin           (echo "newpw" | d8 iam user reset-password ...)
  --generate-password        (auto-generate, shown once on stderr)
  --password-hash STRING     (pre-computed bcrypt hash, must start with $2)

This is the same flag layout as 'd8 iam user create', so muscle memory and
shell autocompletion transfer between the two commands.

Internally a UserOperation of type ResetPassword is created with the raw
bcrypt hash in spec.resetPassword.newPasswordHash. The user-authn hook
validates the prefix and bcrypt.Cost, base64-encodes the hash, and applies
it to the Dex Password CR. We never write the plaintext to the API.

© Flant JSC 2026`)

var resetPasswordExample = templates.Examples(`
  # Interactive prompt (default if stdin is a terminal)
  d8 iam user reset-password anton

  # Pipe from a CI pipeline
  echo "newpw" | d8 iam user reset-password anton --password-stdin

  # Auto-generate a strong password (shown once on stderr)
  d8 iam user reset-password anton --generate-password

  # Apply a pre-computed bcrypt hash (e.g. produced by 'htpasswd -BinC 10')
  d8 iam user reset-password anton --password-hash '$2y$10$abcdef...'`)

func newResetPasswordCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "reset-password <username>",
		Aliases:           []string{"resetpass"},
		Short:             "Reset local user's password in Dex",
		Long:              resetPasswordLong,
		Example:           resetPasswordExample,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeUserNames,
		SilenceErrors:     true,
		SilenceUsage:      true,
		RunE:              runResetPassword,
	}

	addPasswordFlags(cmd)
	addWaitFlags(cmd)
	return cmd
}

func runResetPassword(cmd *cobra.Command, args []string) error {
	username := args[0]

	res, mode, err := resolvePasswordInput(cmd)
	if err != nil {
		return err
	}

	rawHash, err := res.rawBcryptHash()
	if err != nil {
		return err
	}

	dyn, err := utilk8s.NewDynamicClient(cmd)
	if err != nil {
		return err
	}

	if err := runUserOperation(cmd, dyn, userOpRequest{
		NamePrefix: "op-resetpass-",
		OpType:     "ResetPassword",
		User:       username,
		// UserOperation.spec.resetPassword.newPasswordHash expects a raw bcrypt
		// hash (starts with $2). The user-authn hook (get_dex_user_operation_crds)
		// validates the prefix and base64-wraps the value before applying it
		// to Dex's Password CR. Do NOT base64 encode here — that would produce
		// a double-wrapped value that the hook silently accepts at the apiserver
		// boundary but then rejects when it tries to bcrypt-verify the hash.
		ExtraSpec: map[string]any{
			"resetPassword": map[string]any{"newPasswordHash": rawHash},
		},
	}); err != nil {
		return err
	}

	if mode == passwordModeGenerate {
		fmt.Fprintf(cmd.ErrOrStderr(), "Generated new password (shown once): %s\n", res.Plain)
	}
	return nil
}
