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

package delete

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"

	snaputil "github.com/deckhouse/deckhouse-cli/internal/snapshot/util"
)

// NewCommand builds "d8 snapshot import delete".
func NewCommand(ctx context.Context, _ *slog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:           "delete <import-name>",
		Short:         "Delete a SnapshotImport (the pre-provisioned snapshot tree is retained)",
		Args:          cobra.ExactArgs(1),
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			ns := snaputil.ResolveNamespace(cmd)
			_, rt, _, err := snaputil.NewClients(cmd)
			if err != nil {
				return err
			}
			existed, err := snaputil.DeleteSnapshotImport(ctx, rt, ns, args[0])
			if err != nil {
				return err
			}
			if existed {
				fmt.Fprintf(cmd.OutOrStdout(), "SnapshotImport %s/%s deleted\n", ns, args[0])
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "SnapshotImport %s/%s not found (nothing to delete)\n", ns, args[0])
			}
			return nil
		},
	}
	cmd.Flags().StringP("namespace", "n", "", "namespace of the import (default: "+snaputil.DefaultNamespace+")")
	return cmd
}
