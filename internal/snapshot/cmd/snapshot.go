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

package cmd

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/cmd/download"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/cmd/list"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/cmd/tree"
)

const snapshotLong = `Manage Deckhouse namespace snapshots.

The snapshot command lets you list, inspect, and download namespace manifests
captured by the state-snapshotter module.

  list     - list Snapshot CRs in the cluster (all namespaces or one with -n)
  tree     - show the node tree and objects of a single Snapshot CR
  download - download snapshot manifests and volume data to a local directory`

// NewCommand returns the top-level snapshot cobra command (alias: snap).
func NewCommand() *cobra.Command {
	snapshotCmd := &cobra.Command{
		Use:           "snapshot",
		Aliases:       []string{"snap"},
		Short:         "Snapshot operations (list, tree, download)",
		Long:          snapshotLong,
		SilenceUsage:  true,
		SilenceErrors: true,
		Run: func(cmd *cobra.Command, _ []string) {
			_ = cmd.Help()
		},
	}

	snapshotCmd.AddCommand(
		list.NewCommand(),
		tree.NewCommand(),
		download.NewCommand(),
	)

	return snapshotCmd
}
