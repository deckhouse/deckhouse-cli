package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/virtualization"
)

func init() {
	virtualizationCmd, _ := virtualization.NewCommand(fmt.Sprintf("%s v", filepath.Base(os.Args[0])))
	virtualizationCmd.Use = "v"
	virtualizationCmd.Aliases = []string{"virtualization"}

	rootCmd.AddCommand(virtualizationCmd)
}
