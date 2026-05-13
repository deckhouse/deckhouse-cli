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

// Package fs implements the `d8 cr fs` subtree - filesystem inspection of
// container images. Kept separate from crane-style `ls` (which lists tags)
// to avoid the semantic collision between crane and artship/CEK tooling.
package fs

import (
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

// NewCommand returns the `d8 cr fs` subtree. All subcommands share opts
// populated by the root command's PersistentPreRunE.
func NewCommand(opts *registry.Options) *cobra.Command {
	fsCmd := &cobra.Command{
		Use:   "fs",
		Short: "Inspect or extract files inside a container image",
		Long: `Inspect or extract files inside a container image without running it.

Subcommands show the merged filesystem - what a running container would see,
with deleted files hidden.

Path conventions:
  - Input PATH is tolerant: "/etc/nginx", "etc/nginx", and "./etc/nginx"
    are all accepted and refer to the same entry.
  - Output paths are tar-relative (no leading "/"), matching the convention
    of "crane export IMAGE - | tar tf -" and POSIX tar archives. JSON output
    uses the same tar-relative form.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	fsCmd.AddCommand(
		newLsCmd(opts),
		newCatCmd(opts),
		newTreeCmd(opts),
		newExtractCmd(opts),
	)

	return fsCmd
}
