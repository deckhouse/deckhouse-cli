package bootstrap

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/bootstrap"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/internal/utils/logs"
)

var (
	// bootstrapPath stores the destination path for the generated package.
	bootstrapPath string
	// genHooks controls whether hook templates are generated.
	genHooks bool
	// extended controls whether optional extended templates are generated.
	extended bool
	// useWerf controls whether Werf image files are generated.
	useWerf bool
)

// NewCmdBootstrap creates a command that generates new package projects from
// templates through module and application subcommands.
func NewCmdBootstrap() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap a new package (module or application)",
		Long: `Bootstrap a new package from templates.

Use 'bootstrap module <name>' for a module (with hooks, CRDs).
Use 'bootstrap application <name>' for an application package (per-instance resources).
`,
	}

	cmd.AddCommand(NewCmdBootstrapModule())
	cmd.AddCommand(NewCmdBootstrapApplication())

	return cmd
}

// runBootstrap returns a command handler that bootstraps a package of packageType.
func runBootstrap(packageType string) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		logger := logs.New(true)
		packageName := args[0]

		destPath := bootstrapPath
		if destPath == "" {
			cwd, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("failed to get current directory: %w", err)
			}

			destPath = filepath.Join(cwd, packageName)
		}

		opts := bootstrap.Options{
			GenerateHook: genHooks,
			Werf:         useWerf,
			Extended:     extended,
			Data: bootstrap.Data{
				PackageName: packageName,
				PackageType: packageType,
			},
		}

		if err := bootstrap.Bootstrap(destPath, opts, logger); err != nil {
			return fmt.Errorf("bootstrap failed: %w", err)
		}

		return nil
	}
}
