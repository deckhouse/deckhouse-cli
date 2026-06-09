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

package tree

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/snapshot/listing"
	snapshotlog "github.com/deckhouse/deckhouse-cli/internal/snapshot/log"
	safeClient "github.com/deckhouse/deckhouse-cli/pkg/libsaferequest/client"
)

const (
	cmdLong = `Show the snapshot node tree and manifest objects from a live cluster Snapshot CR
or a local archive directory produced by "d8 snapshot download".

By default, per-node manifest objects are included in the output.
Use --no-objects to show only the node tree without object details.

Cluster mode (default): requires a reachable cluster and a Ready Snapshot CR.
Archive mode (--archive): reads from a local directory, no cluster connection needed.

Output format defaults to human-readable text; use -o json or -o yaml for structured output.`

	cmdExample = `  # Show snapshot node tree with objects from the cluster (current kubeconfig namespace)
  d8 snapshot tree my-snap

  # Show snapshot node tree with objects from a specific namespace
  d8 snapshot tree my-snap -n snap-test

  # Show only the node tree (no object details)
  d8 snapshot tree my-snap -n snap-test --no-objects

  # Show only a subtree
  d8 snapshot tree my-snap -n snap-test --node Snapshot--child-snap

  # Show from a local archive directory
  d8 snapshot tree --archive ./snap-test-my-snap

  # Show archive tree without objects in JSON
  d8 snapshot tree --archive ./snap-test-my-snap --no-objects -o json`
)

// NewCommand returns the cobra command for `d8 snapshot tree`.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "tree <snapshot>",
		Short:   "Show snapshot node tree and objects",
		Long:    cmdLong,
		Example: cmdExample,
		Args:    args,
		RunE:    run,
	}

	cmd.Flags().StringP("namespace", "n", "", "namespace of the snapshot (default: current kubeconfig namespace)")
	cmd.Flags().String("archive", "", "path to a local archive directory (offline; no cluster required)")
	cmd.Flags().String("node", "", "show only the subtree rooted at this node ID")
	cmd.Flags().Bool("no-objects", false, "show only the node tree, omit per-node manifest objects")
	cmd.Flags().StringP("output", "o", listing.FormatHuman, "output format: human, json, yaml")

	return cmd
}

func args(cmd *cobra.Command, args []string) error {
	if cmd.Flags().Changed("archive") {
		return cobra.NoArgs(cmd, args)
	}

	return cobra.ExactArgs(1)(cmd, args)
}

func run(cmd *cobra.Command, positional []string) error {
	log := snapshotlog.New()
	opts, format := newOptions(cmd, positional)

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	var tree *listing.Tree

	// tree from archive
	if opts.ArchiveDir != "" {
		t, err := listing.BuildFromArchive(opts, log)
		if err != nil {
			return fmt.Errorf("read archive: %w", err)
		}

		tree = t
	}

	// tree from cluster
	if tree == nil {
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

func newOptions(cmd *cobra.Command, positional []string) (listing.Options, string) {
	archiveDir, _ := cmd.Flags().GetString("archive")
	nodeFilter, _ := cmd.Flags().GetString("node")
	noObjects, _ := cmd.Flags().GetBool("no-objects")
	format, _ := cmd.Flags().GetString("output")

	opts := listing.Options{
		ArchiveDir:  archiveDir,
		NodeFilter:  nodeFilter,
		WithObjects: !noObjects,
	}

	if archiveDir == "" {
		opts.SnapshotName = positional[0]

		namespace, _ := cmd.Flags().GetString("namespace")
		if namespace == "" {
			namespace = safeClient.DefaultNamespace()
		}

		opts.Namespace = namespace
	}

	return opts, format
}
