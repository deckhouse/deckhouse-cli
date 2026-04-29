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
	"fmt"

	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/image"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/imageio"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

func NewPullCmd(opts *registry.Options) *cobra.Command {
	var (
		cachePath string
		format    string
	)
	cmd := &cobra.Command{
		Use:   "pull IMAGE... PATH",
		Short: "Pull one or more remote images to a local path",
		Long: `Pull one or more images and save them to PATH. PATH is a tarball file
for formats "tarball"/"legacy", or a directory for format "oci".

Formats:
  tarball (default)  docker-compatible multi-image tarball
  legacy             single-image format compatible with "docker load" (tags only, digests not preserved)
  oci                OCI image-layout directory; keeps all platforms unless --platform is set

When to use:
  tarball  - default. Best for shipping one or more images as a single
             file: "docker load" / "podman load" reads it natively.
             Multi-arch indices flatten to one platform - pin it with
             --platform to avoid an ambiguity error.
  oci      - prefer when the destination is OCI-aware tooling (skopeo,
             crane, buildkit, another registry via "cr push --index").
             Preserves the full multi-arch index without --platform.
             Resumable: re-running the same pull skips already-downloaded
             blobs.
  legacy   - last-resort compatibility with very old "docker load" or
             tooling that rejects newer manifests. Lossy: digests are
             not preserved, single-image only. Avoid unless you have a
             specific consumer that fails on tarball.

On interruption (Ctrl+C):
  --format oci       Layer-level resume. Rerun the same pull; existing valid blobs are skipped
                     and in-flight layer temp-files are cleaned up automatically.
  --format tarball|  No resume. Rerun replaces the partial archive from scratch.
   legacy
  --cache-path       Layer cache is self-healing: corrupt partial entries are detected on
                     next access and re-downloaded.
`,
		Args:              cobra.MinimumNArgs(2),
		ValidArgsFunction: completion.ImageRef(),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPull(cmd, args, format, cachePath, opts)
		},
	}
	cmd.Flags().StringVarP(&cachePath, "cache-path", "c", "", "Cache image layers under this directory (reused between pulls)")
	cmd.Flags().StringVar(&format, "format", imageio.PullFormatTarball,
		fmt.Sprintf("Output format (one of: %s, %s, %s)", imageio.PullFormatTarball, imageio.PullFormatLegacy, imageio.PullFormatOCI))
	_ = cmd.RegisterFlagCompletionFunc("format", completion.Static(completion.PullFormats()...))
	return cmd
}

func runPull(cmd *cobra.Command, args []string, format, cachePath string, opts *registry.Options) error {
	if err := validatePullFormat(format); err != nil {
		return err
	}

	srcList, dst := args[:len(args)-1], args[len(args)-1]

	keepIndex := format == imageio.PullFormatOCI
	resolved, err := image.Resolve(cmd.Context(), srcList, keepIndex, cachePath, opts)
	if err != nil {
		return err
	}

	switch format {
	case imageio.PullFormatTarball:
		return imageio.SaveTarball(dst, resolved.Images)
	case imageio.PullFormatLegacy:
		return imageio.SaveLegacy(dst, resolved.Images)
	case imageio.PullFormatOCI:
		return imageio.SaveOCI(dst, resolved.Images, resolved.Indices)
	default:
		// Pre-validated via validatePullFormat.
		return fmt.Errorf("unsupported --format %q", format)
	}
}

func validatePullFormat(format string) error {
	switch format {
	case imageio.PullFormatTarball, imageio.PullFormatLegacy, imageio.PullFormatOCI:
		return nil
	default:
		return fmt.Errorf("invalid --format %q (valid: %q, %q, %q)",
			format, imageio.PullFormatTarball, imageio.PullFormatLegacy, imageio.PullFormatOCI)
	}
}
