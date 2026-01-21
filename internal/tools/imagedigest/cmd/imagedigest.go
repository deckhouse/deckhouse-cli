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

package cmd

import (
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest/cmd/add"
	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest/cmd/calculate"
	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest/cmd/calculatefromfile"
	"github.com/deckhouse/deckhouse-cli/internal/tools/imagedigest/cmd/validate"
)

func NewCommand() *cobra.Command {
	imagedigestCmd := &cobra.Command{
		Use:   "imagedigest",
		Short: "",
		Long:  "",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			ojson, _ := cmd.Flags().GetBool("json")
			debug, _ := cmd.Flags().GetBool("debug")

			zerolog.SetGlobalLevel(zerolog.InfoLevel)
			if !ojson {
				log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
			}
			if debug {
				zerolog.SetGlobalLevel(zerolog.DebugLevel)
			}
		},
	}

	imagedigestCmd.PersistentFlags().BoolP("insecure", "i", false, "Allow insecure connections to registries (skip TLS verification)")
	imagedigestCmd.PersistentFlags().BoolP("json", "", false, "Use JSON formatter for output logs")
	imagedigestCmd.PersistentFlags().BoolP("debug", "d", false, "Enable debug logging")

	imagedigestCmd.AddCommand(
		calculate.NewCommand(),
		calculatefromfile.NewCommand(),
		add.NewCommand(),
		validate.NewCommand(),
	)

	return imagedigestCmd
}
