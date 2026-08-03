package verify

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify"
)

var (
	// hideWarnings controls whether warning-level verification messages are hidden.
	hideWarnings bool
	// showIgnored controls whether ignored-level verification messages are shown.
	showIgnored bool

	// lintConfig is an optional .pkglint.yaml path used instead of package-relative discovery.
	lintConfig string
)

// NewCmdVerify creates a command that checks package structure and linter
// configuration, then reports verification errors and warnings. Without a
// subcommand it verifies the current package directory; the remote subcommand
// verifies a published package instead.
func NewCmdVerify() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify a package structure and configuration",
		Long: `Verify a package structure and configuration.

Use 'package verify' to verify the current package directory against the
'static' linter settings.

Use 'package verify remote <registry-path> <package>' to verify a published
package instead.

Linter configuration can be customized via .pkglint.yaml in the package root.`,
		Example: `
  # Verify the current package
  package verify

  # Verify with only errors shown
  package verify --hide-warnings

  # Verify and show ignored-level messages
  package verify --show-ignored

  # Verify with an explicit lint config
  package verify --lint-config ./configs/pkglint.yaml

  # Verify the latest published version of a package
  package verify remote registry.io/packages app`,
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return verify.Verify(cmd.Context(), options(verify.RemoteTarget{}))
		},
	}

	// Display and config flags are persistent so the remote subcommand accepts them too.
	cmd.PersistentFlags().BoolVar(&hideWarnings, "hide-warnings", false, "Hide warning-level messages")
	cmd.PersistentFlags().BoolVar(&showIgnored, "show-ignored", false, "Show ignored-level messages")
	cmd.PersistentFlags().StringVar(&lintConfig, "lint-config", "", "Path to lint config file")

	cmd.AddCommand(NewCmdVerifyRemote())

	return cmd
}

// options builds the verify options shared by both modes from the persistent flags,
// leaving the target to the caller.
func options(target verify.RemoteTarget) verify.Options {
	return verify.Options{
		HideWarnings: hideWarnings,
		ShowIgnored:  showIgnored,
		LintConfig:   lintConfig,
		Remote:       target,
	}
}
