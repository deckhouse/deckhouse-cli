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

package local

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/localscan"
)

// NewGetCommand builds the `d8 snapshot local get <DIR>` cobra command.
// The command is fully offline: it scans the archive directory with localscan
// and prints a brief one-line summary of the root node without contacting a cluster.
func NewGetCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "get <DIR>",
		Short:         "Print a one-line summary of a locally downloaded snapshot archive",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		Example: `  # Summarise a downloaded snapshot archive
  d8 snapshot local get ./my-snapshot`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGet(log, cmd, args)
		},
	}

	return cmd
}

// runGet resolves the archive directory, scans it offline with localscan, and
// prints one summary line: kind/name  namespace=<ns|->  children=N  volumes=M.
func runGet(log *slog.Logger, cmd *cobra.Command, args []string) error {
	dir, err := filepath.Abs(args[0])
	if err != nil {
		return fmt.Errorf("resolving directory %q: %w", args[0], err)
	}

	log.Debug("scanning local snapshot", slog.String("dir", dir))

	node, err := localscan.Scan(dir)
	if err != nil {
		return fmt.Errorf("scanning snapshot at %s: %w", dir, err)
	}

	ns := node.Namespace
	if ns == "" {
		ns = "-"
	}

	fmt.Fprintf(cmd.OutOrStdout(), "%s/%s  namespace=%s  children=%d  volumes=%d\n",
		node.Kind, node.Name, ns, len(node.Children), node.VolumeCount())

	return nil
}
