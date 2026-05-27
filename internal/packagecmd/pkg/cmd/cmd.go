package cmd

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/pkg/cmd/bootstrap"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/pkg/cmd/build"
	"github.com/deckhouse/deckhouse-cli/internal/packagecmd/pkg/cmd/verify"
)

// NewCmdRoot creates the root command for bootstrapping, building, verifying,
// and inspecting package plugin metadata.
func NewCmdRoot() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "package",
		Short: "Package build and bootstrap tool for containerized packages",
		Long: `A plugin for building and bootstrapping packages.

This plugin helps you:
	• Bootstrap new packages from templates with registry configuration
	• Build containerized packages and push them to container registries
	• Manage package lifecycle with semantic versioning
`,
	}

	cmd.AddCommand(bootstrap.NewCmdBootstrap())
	cmd.AddCommand(build.NewCmdBuild())
	cmd.AddCommand(verify.NewCmdVerify())

	return cmd
}
