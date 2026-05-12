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

package fs

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/imagefs"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/output"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

func newExtractCmd(opts *registry.Options) *cobra.Command {
	var outputDir string
	cmd := &cobra.Command{
		Use:   "extract IMAGE",
		Short: "Extract a container image filesystem to a local directory",
		Long: `Extract the merged filesystem of a container image into --output. To
inspect or copy a single file, use "fs cat"; to list a subpath, use
"fs ls IMAGE PATH". An extraction summary is printed when finished.

On interruption (Ctrl+C): partially-written files stay in --output for inspection.
Rerun extract to overwrite them; there is no per-file skip, the full filesystem is
re-materialized from layer 0.`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion.ImageRef(),
		RunE: func(cmd *cobra.Command, args []string) error {
			img, err := registry.Fetch(cmd.Context(), args[0], opts)
			if err != nil {
				return err
			}

			stats, err := imagefs.ExtractMerged(cmd.Context(), img, outputDir)
			if err != nil {
				return err
			}

			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "Extracted to %s\n", outputDir)
			fmt.Fprintf(w, "  files:     %d\n", stats.Files)
			fmt.Fprintf(w, "  dirs:      %d\n", stats.Dirs)
			fmt.Fprintf(w, "  symlinks:  %d\n", stats.Symlinks)
			fmt.Fprintf(w, "  hardlinks: %d\n", stats.Hardlinks)
			fmt.Fprintf(w, "  total:     %s (%d bytes)\n", output.HumanSize(stats.TotalSize), stats.TotalSize)
			return nil
		},
	}
	cmd.Flags().StringVarP(&outputDir, "output", "o", "", "Write the filesystem into this directory")
	_ = cmd.MarkFlagRequired("output")
	return cmd
}
