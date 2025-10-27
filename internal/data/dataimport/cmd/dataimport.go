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

	diCreate "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/cmd/create"
	diDelete "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/cmd/delete"
	diUpload "github.com/deckhouse/deckhouse-cli/internal/data/dataimport/cmd/upload"
)

const (
	cmdName = "import"
)

func NewCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           cmdName,
		Aliases:       []string{"di"},
		Short:         "Create and manage DataImport resources, upload files",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			cmd.Help()
		},
	}

	root.SetOut(os.Stdout)

	ctx := context.Background()
	logger := slog.Default()

	root.AddCommand(
		diCreate.NewCommand(ctx, logger),
		diDelete.NewCommand(ctx, logger),
		diUpload.NewCommand(ctx, logger),
	)

	return root
}
