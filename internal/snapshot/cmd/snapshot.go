/*
Copyright 2026 Flant JSC

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

package snapshot

import (
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/cmd/download"
	restorecmd "github.com/deckhouse/deckhouse-cli/internal/snapshot/cmd/restore"
)

// NewCommand returns the root cobra command for the `d8 snapshot` command group.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "snapshot",
		Short:         "Snapshot operations (download, restore)",
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			_ = cmd.Help()
		},
	}

	log := slog.Default()

	cmd.AddCommand(download.NewCommand(log))
	cmd.AddCommand(restorecmd.NewCommand(log))

	return cmd
}
