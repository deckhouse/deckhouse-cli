package cmd

import (
	"fmt"
	"github.com/deckhouse/deckhouse-cli/pkg/virtualization"
	"os"
	"path/filepath"
)

func init() {
	virtualizationCmd, _ := virtualization.NewCommand(fmt.Sprintf("%s virtualziation", filepath.Base(os.Args[0])))
	virtualizationCmd.Use = "virtualization"
	virtualizationCmd.Aliases = []string{"v"}

	rootCmd.AddCommand(virtualizationCmd)
}
