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

package basic

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

func NewCatalogCmd(opts *registry.Options) *cobra.Command {
	var fullRef bool
	cmd := &cobra.Command{
		Use:               "catalog REGISTRY",
		Short:             "List the repositories in a registry",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion.RegistryHost(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCatalog(cmd.Context(), cmd.OutOrStdout(), args[0], fullRef, opts)
		},
	}
	cmd.Flags().BoolVar(&fullRef, "full-ref", false, "Print the full repository reference (registry/repo)")
	return cmd
}

func runCatalog(ctx context.Context, w io.Writer, src string, fullRef bool, opts *registry.Options) error {
	return registry.ListCatalog(ctx, src, opts, func(repos []string) error {
		for _, repo := range repos {
			line := repo
			if fullRef {
				line = path.Join(src, repo)
			}
			if _, err := fmt.Fprintln(w, line); err != nil {
				return err
			}
		}
		return nil
	})
}
