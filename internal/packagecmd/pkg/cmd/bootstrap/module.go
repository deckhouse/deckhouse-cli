package bootstrap

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/packages"
)

// NewCmdBootstrapModule creates a command that generates a module package with
// module metadata, hooks, CRD templates, and optional Werf or extended files.
func NewCmdBootstrapModule() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "module <name>",
		Short: "Bootstrap a new module package",
		Long: `Bootstrap a new module package from templates.

  • Creates a package with type Module in package.yaml
  • Includes hooks and CRDs; templates use package name
  • Initializes a git repository with an initial commit
`,
		Example: `
  package bootstrap module my-module
  package bootstrap module my-module --output /path/to/my-module --hooks
  package bootstrap module my-module --extended --werf
`,
		Args: cobra.ExactArgs(1),
		RunE: runBootstrap(packages.TypeModule),
	}

	cmd.Flags().StringVarP(&bootstrapPath, "output", "o", "", "Custom path for the package (default: <cwd>/<name>)")
	cmd.Flags().BoolVar(&genHooks, "hooks", false, "Generate hooks in the package")
	cmd.Flags().BoolVar(&extended, "extended", false, "Generate extended files in the package")
	cmd.Flags().BoolVar(&useWerf, "werf", false, "Use werf file for images")

	return cmd
}
