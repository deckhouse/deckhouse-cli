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

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/archive"
	"github.com/deckhouse/deckhouse-cli/internal/snapshot/localscan"
)

const flagAllowUnauthenticatedLegacy = "allow-unauthenticated-legacy"

// NewGetCommand builds the `d8 snapshot local get <DIR>` cobra command.
// The command is fully offline: it scans the archive directory with localscan
// and prints a brief one-line summary of the root node without contacting a cluster.
func NewGetCommand(log *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <DIR>",
		Short: "Print a one-line summary of a locally downloaded snapshot archive",
		Long: `Print a one-line summary of a locally downloaded snapshot archive.

By default, every node's content checksum and authenticated snapshot.yaml metadata are
verified before any output is rendered. Legacy archives without formatVersion and
metadataChecksum are rejected because their identity and volume metadata are unauthenticated.
Use --allow-unauthenticated-legacy only to inspect a trusted pre-version archive; the command
emits a warning because compatibility mode cannot distinguish a genuine legacy archive from a
downgraded and tampered current archive.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		Example: `  # Summarise a downloaded snapshot archive
  d8 snapshot local get ./my-snapshot`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runGet(log, cmd, args)
		},
	}

	cmd.Flags().Bool(
		flagAllowUnauthenticatedLegacy,
		false,
		"allow trusted pre-version archives with unauthenticated snapshot.yaml metadata (unsafe compatibility mode)",
	)

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

	allowUnauthenticatedLegacy, err := cmd.Flags().GetBool(flagAllowUnauthenticatedLegacy)
	if err != nil {
		return fmt.Errorf("reading --%s flag: %w", flagAllowUnauthenticatedLegacy, err)
	}

	if allowUnauthenticatedLegacy {
		log.Warn("allowing unauthenticated legacy snapshot metadata",
			slog.String("warning",
				"legacy compatibility cannot distinguish a genuine pre-version archive from a downgraded tampered archive"))
	}

	node, err := localscan.ScanVerifiedWithOptions(dir, archive.SnapshotYAMLReadOptions{
		AllowUnauthenticatedLegacy: allowUnauthenticatedLegacy,
	})
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
