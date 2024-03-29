package cmd

import (
	"fmt"
	"github.com/deckhouse/deckhouse-cli/internal/virtualization"
	"os"
	"path/filepath"
)

func init() {
	virtualizationCmd, _ := virtualization.NewCommand(fmt.Sprintf("%s virtualziation", filepath.Base(os.Args[0])))
	virtualizationCmd.Use = "v"
	virtualizationCmd.Aliases = []string{"virtualization"}

	rootCmd.AddCommand(virtualizationCmd)
}
