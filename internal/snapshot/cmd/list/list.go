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

package list

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/listing"
	snapshotlog "github.com/deckhouse/deckhouse-cli/internal/snapshot/log"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdLong = `List the snapshot node tree (and optionally the manifest objects) from a
live cluster Snapshot CR or a local archive directory produced by "d8 snapshot download".

Cluster mode (default): requires a reachable cluster and a Ready Snapshot CR.
Archive mode (--archive): reads from a local directory, no cluster connection needed.

Output format defaults to human-readable text; use -o json or -o yaml for structured output.`

	cmdExample = `  # List snapshot node tree from the cluster
  d8 snapshot list snap-test my-snap

  # List with per-node object counts and manifest details
  d8 snapshot list snap-test my-snap --objects

  # List only a subtree
  d8 snapshot list snap-test my-snap --node Snapshot--child-snap

  # List from a local archive directory
  d8 snapshot list --archive ./snap-test-my-snap

  # List archive objects in JSON
  d8 snapshot list --archive ./snap-test-my-snap --objects -o json`
)

// NewCommand returns the cobra command for `d8 snapshot list`.
func NewCommand() *cobra.Command {

	cmd := &cobra.Command{
		Use:     "list [<namespace> <snapshot>]",
		Aliases: []string{"ls"},
		Short:   "List snapshot nodes and objects",
		Long:    cmdLong,
		Example: cmdExample,
		Args:    args,
		RunE:    run,
	}

	cmd.Flags().String("archive", "", "path to a local archive directory (offline; no cluster required)")
	cmd.Flags().String("node", "", "show only the subtree rooted at this node ID")
	cmd.Flags().Bool("objects", false, "include per-node manifest object listing")
	cmd.Flags().StringP("output", "o", listing.FormatHuman, "output format: human, json, yaml")

	return cmd
}

func args(cmd *cobra.Command, args []string) error {
	if cmd.Flags().Changed("archive") {
		return cobra.NoArgs(cmd, args)
	}

	return cobra.ExactArgs(2)(cmd, args)
}

func run(cmd *cobra.Command, args []string) error {
	log := snapshotlog.New()
	opts, format := newOptions(cmd, args)

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	var tree *listing.Tree

	// list from archive
	if opts.ArchiveDir != "" {
		t, err := listing.BuildFromArchive(opts, log)
		if err != nil {
			return fmt.Errorf("read archive: %w", err)
		}

		tree = t
	}

	// list from cluster
	if tree == nil {
		opts.Namespace, opts.SnapshotName = args[0], args[1]

		safeClient.SupportNoAuth = false

		sc, err := safeClient.NewSafeClient(cmd.PersistentFlags())
		if err != nil {
			return fmt.Errorf("build kube client: %w", err)
		}

		rtClient, err := sc.NewRTClient()
		if err != nil {
			return fmt.Errorf("build runtime client: %w", err)
		}

		t, err := listing.BuildFromCluster(ctx, sc, rtClient, opts, log)
		if err != nil {
			return err
		}

		tree = t
	}

	return listing.Render(cmd.OutOrStdout(), tree, format)
}

func newOptions(cmd *cobra.Command, args []string) (listing.Options, string) {
	archiveDir, _ := cmd.Flags().GetString("archive")
	nodeFilter, _ := cmd.Flags().GetString("node")
	withObjects, _ := cmd.Flags().GetBool("objects")
	format, _ := cmd.Flags().GetString("output")

	opts := listing.Options{
		ArchiveDir:  archiveDir,
		NodeFilter:  nodeFilter,
		WithObjects: withObjects,
	}

	if archiveDir == "" {
		opts.Namespace, opts.SnapshotName = args[0], args[1]
	}

	return opts, format
}
