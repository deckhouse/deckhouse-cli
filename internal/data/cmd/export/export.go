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

package export

import (
	"context"
	"log/slog"

	"github.com/spf13/cobra"
)

// NewCommand creates the export parent command
func NewCommand(ctx context.Context, log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "export",
		Short:         "Export data (DataExport)",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run:           func(cmd *cobra.Command, _ []string) { _ = cmd.Help() },
	}

	cmd.AddCommand(
		NewCreateCommand(ctx, log),
		NewDeleteCommand(ctx, log),
		NewDownloadCommand(ctx, log),
		NewListCommand(ctx, log),
	)

	return cmd
}

