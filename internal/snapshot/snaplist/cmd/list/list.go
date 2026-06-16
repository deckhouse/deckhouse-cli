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

// Package list implements "d8 snapshot list": render a snapshot hierarchy as a tree, either from a
// live cluster (the stable server-side /view subresource) or from a downloaded bundle's view.json.
// The CLI renders only the stable SnapshotView projection and never parses the internal index.json.
package list

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"

	snaputil "github.com/deckhouse/deckhouse-cli/internal/snapshot/util"
)

// NewCommand builds "d8 snapshot list".
func NewCommand(ctx context.Context, _ *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list [<snapshot-name>]",
		Short: "List a snapshot hierarchy as a tree (in-cluster /view or a local bundle's view.json)",
		Long: "List a snapshot hierarchy as a tree.\n\n" +
			"In-cluster:  d8 snapshot list <snapshot-name> [-n ns] [--kind Snapshot] [--api-version ...]\n" +
			"Local bundle: d8 snapshot list --dir <bundle-dir>",
		Args:          cobra.MaximumNArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(ctx, cmd, args)
		},
	}
	cmd.Flags().StringP("namespace", "n", "", "namespace of the snapshot (default: "+snaputil.DefaultNamespace+")")
	cmd.Flags().StringP("dir", "d", "", "render a local bundle's view.json instead of querying the cluster")
	cmd.Flags().String("kind", snaputil.DefaultSnapshotKind, "kind of the snapshot (in-cluster mode)")
	cmd.Flags().String("api-version", snaputil.DefaultSnapshotAPIVersion, "apiVersion of the snapshot (in-cluster mode)")
	return cmd
}

func run(ctx context.Context, cmd *cobra.Command, args []string) error {
	dir, _ := cmd.Flags().GetString("dir")
	out := cmd.OutOrStdout()

	if dir != "" {
		if len(args) > 0 {
			return fmt.Errorf("--dir lists a local bundle and takes no <snapshot-name> argument")
		}
		raw, err := os.ReadFile(snaputil.ViewPath(dir))
		if err != nil {
			return fmt.Errorf("read view.json: %w (run 'd8 snapshot export download' to produce it)", err)
		}
		view, err := snaputil.ParseView(raw)
		if err != nil {
			return err
		}
		snaputil.RenderView(out, view)
		return nil
	}

	if len(args) != 1 {
		return fmt.Errorf("a <snapshot-name> argument is required (or use --dir <bundle-dir> for a local bundle)")
	}
	name := args[0]
	ns := snaputil.ResolveNamespace(cmd)
	kind, _ := cmd.Flags().GetString("kind")
	apiVersion, _ := cmd.Flags().GetString("api-version")

	_, rt, api, err := snaputil.NewClients(cmd)
	if err != nil {
		return err
	}
	resource, err := snaputil.ResolveResource(rt, apiVersion, kind)
	if err != nil {
		return err
	}
	raw, err := api.Get(ctx, snaputil.ViewAbsPath(ns, resource, name))
	if err != nil {
		return fmt.Errorf("get view for %s %q: %w", kind, name, err)
	}
	view, err := snaputil.ParseView(raw)
	if err != nil {
		return err
	}
	snaputil.RenderView(out, view)
	return nil
}
