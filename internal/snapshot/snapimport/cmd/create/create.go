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

	v1alpha1 "github.com/deckhouse/deckhouse-cli/internal/snapshot/api/v1alpha1"
	snaputil "github.com/deckhouse/deckhouse-cli/internal/snapshot/util"
)

// NewCommand builds "d8 snapshot import create".
func NewCommand(ctx context.Context, _ *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "create <import-name> --target <name>",
		Short:         "Create a SnapshotImport that prepares upload endpoints for a snapshot bundle",
		Args:          cobra.ExactArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(ctx, cmd, args[0])
		},
	}
	cmd.Flags().StringP("namespace", "n", "", "namespace to import into (default: "+snaputil.DefaultNamespace+")")
	cmd.Flags().String("target", "", "desired name of the recreated (root or re-rooted child) snapshot (required)")
	cmd.Flags().String("child-name", "", "import only this child snapshot from the uploaded bundle (server-side re-root)")
	cmd.Flags().String("child-kind", "", "kind of the child snapshot to import (with --child-name)")
	cmd.Flags().String("child-api-version", "", "apiVersion of the child snapshot to import (with --child-name)")
	cmd.Flags().String("ttl", "30m", "idle time-to-live for the import's upload endpoints")
	cmd.Flags().StringArray("storage-class-map", nil, "remap a source StorageClass to a target one, as src=dst (repeatable)")
	cmd.Flags().Bool("publish", false, "expose upload endpoints outside the cluster")
	_ = cmd.MarkFlagRequired("target")
	return cmd
}

func run(ctx context.Context, cmd *cobra.Command, name string) error {
	ns := snaputil.ResolveNamespace(cmd)
	target, _ := cmd.Flags().GetString("target")
	ttl, _ := cmd.Flags().GetString("ttl")
	publish, _ := cmd.Flags().GetBool("publish")
	mapEntries, _ := cmd.Flags().GetStringArray("storage-class-map")

	child, err := childRef(cmd)
	if err != nil {
		return err
	}
	scMapping, err := parseStorageClassMap(mapEntries)
	if err != nil {
		return err
	}

	_, rt, _, err := snaputil.NewClients(cmd)
	if err != nil {
		return err
	}
	created, err := snaputil.EnsureSnapshotImport(ctx, rt, ns, name, target, child, ttl, scMapping, publish)
	if err != nil {
		return err
	}
	if created {
		fmt.Fprintf(cmd.OutOrStdout(), "SnapshotImport %s/%s created (target %q%s); upload the bundle with 'd8 snapshot import upload %s -d <dir>'\n",
			ns, name, target, childDesc(child), name)
		return nil
	}
	// The object already existed and its spec was NOT updated; do not claim the new flags took effect.
	fmt.Fprintf(cmd.OutOrStdout(), "SnapshotImport %s/%s already exists; its spec was left unchanged. To apply different flags, delete it first: 'd8 snapshot import delete %s'\n",
		ns, name, name)
	return nil
}

// childRef builds the optional spec.childSnapshot from the --child-* flags. --child-name is the
// trigger: without it no re-root is requested; with it the kind/apiVersion default server-side.
func childRef(cmd *cobra.Command) (*v1alpha1.SnapshotReference, error) {
	childName, _ := cmd.Flags().GetString("child-name")
	childKind, _ := cmd.Flags().GetString("child-kind")
	childAPIVersion, _ := cmd.Flags().GetString("child-api-version")
	if childName == "" {
		if childKind != "" || childAPIVersion != "" {
			return nil, fmt.Errorf("--child-kind/--child-api-version require --child-name")
		}
		return nil, nil
	}
	return &v1alpha1.SnapshotReference{APIVersion: childAPIVersion, Kind: childKind, Name: childName}, nil
}

func childDesc(child *v1alpha1.SnapshotReference) string {
	if child == nil {
		return ""
	}
	kind := child.Kind
	if kind == "" {
		kind = "child"
	}
	return fmt.Sprintf(", re-rooted at %s %q", kind, child.Name)
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
