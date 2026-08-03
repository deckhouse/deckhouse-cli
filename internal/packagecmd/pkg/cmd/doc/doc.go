package doc

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/verify/lint/doc"
)

// rule optionally narrows the output to a single rule of the requested linter.
var rule string

// NewCmdDoc creates a command that prints the reference documentation of the
// verify linters and their rules.
func NewCmdDoc() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doc [linter]",
		Short: "Show documentation for verify linters and rules",
		Long: `Show documentation for the linters run by 'package verify'.

Use 'package doc' to list every linter.

Use 'package doc <linter>' to see what a linter checks, the rules it runs with
their default severities, and the .pkglint.yaml keys that configure it.

Use 'package doc <linter> --rule <rule>' to see what makes a single rule report a
finding and how to satisfy it.

A rule's impact caps the severity of its findings: ignored hides them, warn
reports them without failing, error fails verification.`,
		Example: `
  # List every linter
  package doc

  # Show what the package linter checks
  package doc package

  # Show a single rule
  package doc package --rule has_changelog

  # Show a templates rule
  package doc templates --rule vpa`,
		Args:         cobra.MaximumNArgs(1),
		ValidArgs:    doc.LinterIDs(),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := doc.Options{Rule: rule}
			if len(args) > 0 {
				opts.Linter = args[0]
			}

			return doc.Print(cmd.OutOrStdout(), opts)
		},
	}

	cmd.Flags().StringVar(&rule, "rule", "", "Show documentation for a single rule of the linter")

	// Completing --rule needs the linter from the positional argument, so the
	// completion is registered here rather than derived from ValidArgs.
	_ = cmd.RegisterFlagCompletionFunc("rule",
		func(_ *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
			if len(args) == 0 {
				return nil, cobra.ShellCompDirectiveNoFileComp
			}

			return doc.RuleIDs(args[0]), cobra.ShellCompDirectiveNoFileComp
		})

	return cmd
}
