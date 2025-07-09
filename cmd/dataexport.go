/*
Copyright 2024 Flant JSC

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

	"github.com/spf13/cobra"

	deCreate "github.com/deckhouse/deckhouse-cli/internal/dataexport/cmd/create"
	deDelete "github.com/deckhouse/deckhouse-cli/internal/dataexport/cmd/delete"
	deDownload "github.com/deckhouse/deckhouse-cli/internal/dataexport/cmd/download"
	deList "github.com/deckhouse/deckhouse-cli/internal/dataexport/cmd/list"
)

const (
	cmdName = "data"
)

func init() {
	dataCmd := &cobra.Command{
		Use:           cmdName,
		Aliases:       []string{"de", "dataexport"},
		Short:         "Provides volume resources data from kubernetes cluster",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			cmd.Help()
		},
	}

	dataCmd.SetOut(os.Stdout)

	dataCmd.AddCommand(
		deCreate.NewCommand(),
		deDelete.NewCommand(),
		deDownload.NewCommand(),
		//deGet.NewCommand(),
		deList.NewCommand(),
	)

	rootCmd.AddCommand(dataCmd)
}
