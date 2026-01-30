package useroperation

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
)

var userOperationLong = templates.LongDesc(`
Request local user operations (ResetPassword/Reset2FA/Lock/Unlock) in the Deckhouse user-authn module.

The command creates a UserOperation custom resource and (optionally) waits for completion.

© Flant JSC 2026`)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "user-operation",
		Aliases:       []string{"userop", "uo"},
		Short:         "Request local user operations in user-authn module",
		Long:          userOperationLong,
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	// Reuse standard kubeconfig/context flags (same as `d8 system ...`).
	flags.AddPersistentFlags(cmd)

	cmd.AddCommand(
		newReset2FACommand(),
		newResetPasswordCommand(),
		newLockCommand(),
		newUnlockCommand(),
	)

	return cmd
}
