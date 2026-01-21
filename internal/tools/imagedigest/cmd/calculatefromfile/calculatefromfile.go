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

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest"
)

func NewCommand() *cobra.Command {
	calculateFromFileCmd := &cobra.Command{
		Use:   "calculate-from-file <file>",
		Short: "Calculating the file digest according to the GOST standard Streebog (GOST R 34.11-2012). For stdin use '-'",
		Long:  `Calculating the file digest according to the GOST standard Streebog (GOST R 34.11-2012)`,
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.MinimumNArgs(1)(cmd, args); err != nil {
				return err
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
			log.Err(err).Msg("failed to open file")
			os.Exit(1)
		}
		defer file.Close()
		reader = file
	}

	sum, err := imagedigest.CalculateGostHashFromReader(reader)
	if err != nil {
		log.Err(err).Msg("failed to calculate GOST digest")
		os.Exit(2)
	}

	fmt.Println(hex.EncodeToString(sum))

	return nil
}
