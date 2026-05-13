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
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

const digestTagPrefix = "sha256-"

func NewLsCmd(opts *registry.Options) *cobra.Command {
	var (
		fullRef        bool
		omitDigestTags bool
	)
	cmd := &cobra.Command{
		Use:               "ls REPO",
		Short:             "List the tags in a repository",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completion.RepoRef(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLs(cmd.Context(), cmd.OutOrStdout(), args[0], fullRef, omitDigestTags, opts)
		},
	}
	cmd.Flags().BoolVar(&fullRef, "full-ref", false, "Print the full image reference (registry/repo:tag)")
	cmd.Flags().BoolVarP(&omitDigestTags, "omit-digest-tags", "O", false, "Skip digest-based tags (sha256-*) created by signing tools")
	return cmd
}

func runLs(ctx context.Context, w io.Writer, src string, fullRef, omitDigestTags bool, opts *registry.Options) error {
	var repo name.Repository
	if fullRef {
		r, err := name.NewRepository(src, opts.Name...)
		if err != nil {
			return fmt.Errorf("parse repository %q: %w", src, err)
		}
		repo = r
	}

	return registry.ListTags(ctx, src, opts, func(tags []string) error {
		for _, tag := range tags {
			if omitDigestTags && strings.HasPrefix(tag, digestTagPrefix) {
				continue
			}
			line := tag
			if fullRef {
				line = repo.Tag(tag).String()
			}
			if _, err := fmt.Fprintln(w, line); err != nil {
				return err
			}
		}
		return nil
	})
}
