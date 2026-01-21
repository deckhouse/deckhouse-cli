/*
Copyright 2025 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package calculatefromfile

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest"
)

var calculateFromFileLong = templates.LongDesc(`
Calculate GOST R 34.11-2012 (Streebog-256) digest for a file.

Use '-' to read from stdin.

Example:
  d8 tools imagedigest calculate-from-file /path/to/file
  d8 tools imagedigest calculate-from-file -
  cat file.tar | d8 tools imagedigest calculate-from-file -`)

func NewCommand() *cobra.Command {
	calculateFromFileCmd := &cobra.Command{
		Use:           "calculate-from-file <file>",
		Short:         "Calculate GOST digest for a file (use '-' for stdin)",
		Long:          calculateFromFileLong,
		SilenceErrors: true,
		SilenceUsage:  true,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("this command requires exactly 1 argument (file path or '-' for stdin), got %d", len(args))
			}
			return nil
		},
		RunE: runCalculateFromFile,
	}

	return calculateFromFileCmd
}

func runCalculateFromFile(cmd *cobra.Command, args []string) error {
	filename := args[0]

	reader := os.Stdin
	if filename != "-" {
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()
		reader = file
	}

	digest, err := imagedigest.CalculateFromReader(reader)
	if err != nil {
		return fmt.Errorf("failed to calculate GOST digest: %w", err)
	}

	fmt.Println(hex.EncodeToString(digest))

	return nil
}
