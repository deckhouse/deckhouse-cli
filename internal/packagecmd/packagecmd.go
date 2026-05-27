package packagecmd

import (
	"github.com/spf13/cobra"

	pkgcmd "github.com/deckhouse/deckhouse-cli/internal/packagecmd/pkg/cmd"
)

// NewCommand returns the root "d8 package" command. Thin adapter so the rest of
// d8-cli (cmd/d8/root.go) can register via the standard NewCommand() entry
// point used by other built-ins; the actual command tree lives under pkg/cmd
// to mirror the original d8-package-plugin layout.
func NewCommand() *cobra.Command {
	return pkgcmd.NewCmdRoot()
}
