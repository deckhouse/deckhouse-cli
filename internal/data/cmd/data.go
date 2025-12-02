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

	"github.com/deckhouse/deckhouse-cli/internal/data/cmd/dataimport"
	"github.com/deckhouse/deckhouse-cli/internal/data/cmd/export"
)

const cmdName = "data"

// NewCommand creates the data parent command with export and import subcommands
func NewCommand() *cobra.Command {
	ctx := context.Background()
	logger := slog.Default()

	root := &cobra.Command{
		Use:           cmdName,
		Short:         "Data operations (export/import)",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			_ = cmd.Help()
		},
	}

	root.SetOut(os.Stdout)

	root.AddCommand(
		export.NewCommand(ctx, logger),
		dataimport.NewCommand(ctx, logger),
	)

	return root
}
