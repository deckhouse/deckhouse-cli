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

	// remote is an optional image reference to verify instead of the local package directory.
	remote string
	// lintConfig is an optional .pkglint.yaml path used instead of package-relative discovery.
	lintConfig string
)

// NewCmdVerify creates a command that checks package structure and linter
// configuration, then reports verification errors and warnings.
func NewCmdVerify() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify a package structure and configuration",
		Long: `Verify a package structure and configuration.

Use 'package verify' to verify the current package directory.

Linter configuration can be customized via .pkglint.yaml in the package root.`,
		Example: `
  # Verify a package
  package verify

  # Verify with only errors shown
  package verify --hide-warnings

  # Verify and show ignored-level messages
  package verify --show-ignored

  # Verify a package image from a registry
  package verify --remote registry.io/packages/app:v1.0.0

  # Verify with an explicit lint config
  package verify --lint-config ./configs/pkglint.yaml`,
		Args:         cobra.MaximumNArgs(0),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := verify.Options{
				HideWarnings: hideWarnings,
				ShowIgnored:  showIgnored,
				Remote:       remote,
				LintConfig:   lintConfig,
			}

			ctx := cmd.Context()

			return verify.Verify(ctx, opts)
		},
	}

	cmd.Flags().BoolVar(&hideWarnings, "hide-warnings", false, "Hide warning-level messages")
	cmd.Flags().BoolVar(&showIgnored, "show-ignored", false, "Show ignored-level messages")
	cmd.Flags().StringVarP(&remote, "remote", "r", "", "Verify a package image from a registry reference")
	cmd.Flags().StringVar(&lintConfig, "lint-config", "", "Path to lint config file")

	return cmd
}
