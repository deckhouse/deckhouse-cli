package packagecmd

import (
	"github.com/spf13/cobra"

	pkgcmd "github.com/deckhouse/deckhouse-cli/internal/packagecmd/pkg/cmd"
)

// NewCommand returns the root "d8 package" command. Thin adapter so the rest of
// d8-cli (cmd/d8/root.go) can register via the standard NewCommand() entry
// point used by other built-ins; the actual command tree lives under pkg/cmd
// to mirror the original d8-package-plugin layout.
//
// Two things from the upstream plugin are deliberately left out: its standalone
// entry point (cmd/package/main.go) and the "version" subcommand, whose value is
// injected by the plugin's own ldflags. d8 reports its version itself.
//
// Vendored from d8-package-plugin v0.0.25 (4458426). Keep this in sync when
// re-syncing internal/, pkg/ and templates/ from upstream.
func NewCommand() *cobra.Command {
	return pkgcmd.NewCmdRoot()
}
