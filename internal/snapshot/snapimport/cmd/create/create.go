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

package create

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/cobra"

	snaputil "github.com/deckhouse/deckhouse-cli/internal/snapshot/util"
)

// NewCommand builds "d8 snapshot import create".
func NewCommand(ctx context.Context, _ *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "create <import-name> --snapshot <snapshot-name>",
		Short:         "Create a SnapshotImport that prepares upload endpoints for a snapshot bundle",
		Args:          cobra.ExactArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(ctx, cmd, args[0])
		},
	}
	cmd.Flags().StringP("namespace", "n", "", "namespace to import into (default: "+snaputil.DefaultNamespace+")")
	cmd.Flags().String("snapshot", "", "desired name of the root Snapshot to (re)create (required)")
	cmd.Flags().StringArray("storage-class-map", nil, "remap a source StorageClass to a target one, as src=dst (repeatable)")
	cmd.Flags().Bool("publish", false, "expose upload endpoints outside the cluster")
	_ = cmd.MarkFlagRequired("snapshot")
	return cmd
}

func run(ctx context.Context, cmd *cobra.Command, name string) error {
	ns := snaputil.ResolveNamespace(cmd)
	snapshot, _ := cmd.Flags().GetString("snapshot")
	publish, _ := cmd.Flags().GetBool("publish")
	mapEntries, _ := cmd.Flags().GetStringArray("storage-class-map")

	scMapping, err := parseStorageClassMap(mapEntries)
	if err != nil {
		return err
	}

	_, rt, _, err := snaputil.NewClients(cmd)
	if err != nil {
		return err
	}
	created, err := snaputil.EnsureSnapshotImport(ctx, rt, ns, name, snapshot, scMapping, publish)
	if err != nil {
		return err
	}
	if created {
		fmt.Fprintf(cmd.OutOrStdout(), "SnapshotImport %s/%s created (snapshot %q); upload the bundle with 'd8 snapshot import upload %s -d <dir>'\n",
			ns, name, snapshot, name)
		return nil
	}
	// The object already existed and its spec was NOT updated; do not claim the new flags took effect.
	fmt.Fprintf(cmd.OutOrStdout(), "SnapshotImport %s/%s already exists; its spec was left unchanged. To apply a different --snapshot or --storage-class-map, delete it first: 'd8 snapshot import delete %s'\n",
		ns, name, name)
	return nil
}

func parseStorageClassMap(entries []string) (map[string]string, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	m := make(map[string]string, len(entries))
	for _, e := range entries {
		k, v, ok := strings.Cut(e, "=")
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if !ok || k == "" || v == "" {
			return nil, fmt.Errorf("invalid --storage-class-map %q, expected src=dst", e)
		}
		if _, dup := m[k]; dup {
			return nil, fmt.Errorf("duplicate --storage-class-map source %q", k)
		}
		m[k] = v
	}
	return m, nil
}
