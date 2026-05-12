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
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/imagefs"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/output"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

func newLsCmd(opts *registry.Options) *cobra.Command {
	var longForm bool
	cmd := &cobra.Command{
		Use:   "ls IMAGE [PATH]",
		Short: "List files inside a container image",
		Long: `List files inside a container image.

PATH limits output to that path and its descendants. Leading "./" or "/" is
stripped, so "/etc" and "etc" are equivalent. Output paths are tar-relative
(no leading "/"), matching "crane export | tar tf -".`,
		Args:              cobra.RangeArgs(1, 2),
		ValidArgsFunction: completion.ImageThenInImagePath(),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref := args[0]
			subpath := ""
			if len(args) == 2 {
				subpath = args[1]
			}

			img, err := registry.Fetch(cmd.Context(), ref, opts)
			if err != nil {
				return err
			}

			entries, err := imagefs.MergedFS(img)
			if err != nil {
				return err
			}
			entries = imagefs.FilterBySubpath(entries, subpath)

			return output.WriteEntriesText(cmd.OutOrStdout(), entries, longForm)
		},
	}
	cmd.Flags().BoolVarP(&longForm, "long", "l", false, "Long format with mode and size")
	return cmd
}
