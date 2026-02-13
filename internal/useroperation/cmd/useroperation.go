package useroperation

import (
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/system/flags"
)

var userOperationLong = templates.LongDesc(`
Manage Deckhouse users (user-authn).

This command provides admin operations for Dex local users via UserOperation custom resources:
ResetPassword, Reset2FA, Lock, Unlock.

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
		newReset2FACommand(),
		newResetPasswordCommand(),
		newLockCommand(),
		newUnlockCommand(),
	)

	return cmd
}
