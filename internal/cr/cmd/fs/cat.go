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
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

func newCatCmd(opts *registry.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cat IMAGE PATH",
		Short: "Print a file from a container image",
		Long: `Print a file from a container image to stdout.

Only regular files are supported. Directories, symlinks, hardlinks and
other non-regular entries return an error. Reads the merged filesystem -
files deleted by upper layers are reported as not-found.`,
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completion.ImageThenInImagePath(),
		RunE: func(cmd *cobra.Command, args []string) error {
			ref, filePath := args[0], args[1]

			img, err := registry.Fetch(cmd.Context(), ref, opts)
			if err != nil {
				return err
			}

			content, err := imagefs.ReadFile(img, filePath)
			if err != nil {
				return err
			}
			_, err = cmd.OutOrStdout().Write(content)
			return err
		},
	}
	return cmd
}
