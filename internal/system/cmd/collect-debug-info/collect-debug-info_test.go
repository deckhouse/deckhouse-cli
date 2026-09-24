package collectdebuginfo

import (
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// TestSubcommandTypoIsRejected guards the Args: cobra.NoArgs on the parent
// command. Without it cobra accepts an unknown positional argument silently
// (its "unknown command" check only fires for the root command) and the full
// cluster-wide collection runs instead of the requested subcommand.
func TestSubcommandTypoIsRejected(t *testing.T) {
	root := &cobra.Command{Use: "d8", SilenceErrors: true, SilenceUsage: true}
	system := &cobra.Command{Use: "system"}
	root.AddCommand(system)
	system.AddCommand(NewCommand())

	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"system", "collect-debug-info", "virtualisation"})

	err := root.Execute()
	if err == nil {
		t.Fatal("a misspelled subcommand was accepted, the full collection would have run")
	}

	if !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("error = %v, want it to mention an unknown command", err)
	}
}
