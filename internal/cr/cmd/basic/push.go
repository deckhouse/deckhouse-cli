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
	"os"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/imageio"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

func NewPushCmd(opts *registry.Options) *cobra.Command {
	var (
		asIndex       bool
		imageRefsPath string
	)
	cmd := &cobra.Command{
		Use:   "push PATH IMAGE",
		Short: "Push a local image to a registry",
		Long: `A directory PATH is read as an OCI image layout; a file is treated as a
docker-style tarball. Multi-manifest OCI layouts must be pushed with --index.

Prints the pushed reference (with digest) to stdout; use --image-refs to also
write it to a file. --image-refs overwrites the target file if it exists.`,
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: completion.PathThenImage(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPush(cmd.Context(), cmd.OutOrStdout(), args[0], args[1], asIndex, imageRefsPath, opts)
		},
	}
	cmd.Flags().BoolVar(&asIndex, "index", false, "Push a multi-manifest OCI layout as an index (OCI layout dirs only)")
	cmd.Flags().StringVar(&imageRefsPath, "image-refs", "", "Persist the pushed reference (with digest) to this file")
	return cmd
}

func runPush(ctx context.Context, w io.Writer, path, tagRef string, asIndex bool, imageRefsPath string, opts *registry.Options) error {
	// Validate tagRef before reading any OCI layout from disk - layouts can
	// be tens of GB, and a typo in the destination ref should not require
	// loading the source first.
	parsed, err := name.ParseReference(tagRef, opts.Name...)
	if err != nil {
		return fmt.Errorf("parse reference %q: %w", tagRef, err)
	}

	obj, err := imageio.LoadLocal(path, asIndex)
	if err != nil {
		return err
	}

	digest, err := registry.Push(ctx, tagRef, obj, opts)
	if err != nil {
		return err
	}

	fullRef := parsed.Context().Digest(digest.String()).String()

	if imageRefsPath != "" {
		if err := os.WriteFile(imageRefsPath, []byte(fullRef), 0o600); err != nil {
			return fmt.Errorf("write image refs %s: %w", imageRefsPath, err)
		}
	}
	_, err = fmt.Fprintln(w, fullRef)
	return err
}
