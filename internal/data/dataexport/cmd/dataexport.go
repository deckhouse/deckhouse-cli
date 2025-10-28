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
	"context"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	deCreate "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/cmd/create"
	deDelete "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/cmd/delete"
	deDownload "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/cmd/download"
	deList "github.com/deckhouse/deckhouse-cli/internal/data/dataexport/cmd/list"
	"github.com/deckhouse/deckhouse-cli/internal/data/dataexport/util"
)

const (
	cmdName = "export"
)

func NewCommand() *cobra.Command {
	dataCmd := &cobra.Command{
		Use:           cmdName,
		Aliases:       []string{"de"},
		Short:         "Provides volume resources data from kubernetes cluster",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			cmd.Help()
		},
	}

	dataCmd.SetOut(os.Stdout)

	ctx := context.Background()

	logger := util.SetupLogger()
	if logger == nil {
		logger = slog.Default()
	}

	dataCmd.AddCommand(
		deCreate.NewCommand(ctx, logger),
		deDelete.NewCommand(ctx, logger),
		deDownload.NewCommand(ctx, logger),
		deList.NewCommand(ctx, logger),
	)

	return dataCmd
}
