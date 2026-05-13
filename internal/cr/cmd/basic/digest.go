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
	"errors"
	"fmt"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/imageio"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

func NewDigestCmd(opts *registry.Options) *cobra.Command {
	var (
		tarballPath string
		fullRef     bool
	)
	cmd := &cobra.Command{
		Use:   "digest [IMAGE]",
		Short: "Print the digest of an image",
		Long: `By default, fetches the digest of IMAGE from the registry. With --tarball,
reads it from a local tarball instead; IMAGE then becomes optional and
selects an entry by tag (the first entry is used if omitted).

--full-ref is incompatible with --tarball.`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: completion.ImageRef(),
		RunE: func(cmd *cobra.Command, args []string) error {
			if fullRef && tarballPath != "" {
				return errors.New("--full-ref cannot be combined with --tarball")
			}
			if tarballPath == "" && len(args) == 0 {
				return errors.New("image reference required when --tarball is not used")
			}

			digest, err := resolveDigest(cmd.Context(), tarballPath, args, opts)
			if err != nil {
				return err
			}

			w := cmd.OutOrStdout()
			if !fullRef {
				_, err = fmt.Fprintln(w, digest)
				return err
			}
			// fullRef branch is reachable only when tarballPath == "" (rejected above)
			// and len(args) > 0 (rejected above when tarballPath is also empty).
			ref, err := name.ParseReference(args[0], opts.Name...)
			if err != nil {
				return fmt.Errorf("parse reference %q: %w", args[0], err)
			}
			_, err = fmt.Fprintln(w, ref.Context().Digest(digest))
			return err
		},
	}
	cmd.Flags().StringVar(&tarballPath, "tarball", "", "Read the digest from a local tarball instead of the registry")
	cmd.Flags().BoolVar(&fullRef, "full-ref", false, "Print the full image reference with digest (registry/repo@sha256:...); incompatible with --tarball")
	return cmd
}

func resolveDigest(ctx context.Context, tarballPath string, args []string, opts *registry.Options) (string, error) {
	if tarballPath == "" {
		return registry.FetchDigest(ctx, args[0], opts)
	}

	tag := ""
	if len(args) > 0 {
		tag = args[0]
	}
	img, err := imageio.LoadTarball(tarballPath, tag)
	if err != nil {
		return "", err
	}
	d, err := img.Digest()
	if err != nil {
		return "", fmt.Errorf("compute digest: %w", err)
	}
	return d.String(), nil
}
