package bootstrap

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
)

// NewCmdBootstrapApplication creates a command that generates an application
// package with application metadata, hooks, and optional Werf or extended files.
func NewCmdBootstrapApplication() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "application <name>",
		Short: "Bootstrap a new application package",
		Long: `Bootstrap a new application package from templates.

  • Creates a package with type Application in package.yaml
  • Includes hooks
  • Initializes a git repository with an initial commit
`,
		Example: `
  package bootstrap application my-app
  package bootstrap application my-app --output /path/to/my-app
  package bootstrap application my-app --hooks --extended --werf
`,
		Args: cobra.ExactArgs(1),
		RunE: runBootstrap(packages.TypeApplication),
	}

	cmd.Flags().StringVarP(&bootstrapPath, "output", "o", "", "Custom path for the package (default: <cwd>/<name>)")
	cmd.Flags().BoolVar(&genHooks, "hooks", false, "Generate hooks in the package")
	cmd.Flags().BoolVar(&extended, "extended", false, "Generate extended files in the package")
	cmd.Flags().BoolVar(&useWerf, "werf", false, "Use werf file for images")

	return cmd
}
