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
	"io"
	"os"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/spf13/cobra"

	"github.com/deckhouse/deckhouse-cli/internal/cr/cmd/completion"
	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

// NewExportCmd mirrors crane export: writes the merged filesystem of IMAGE
// as a verbatim tar stream to TARBALL (or stdout when TARBALL is "-" or omitted).
//
// Verbatim semantics: linknames are preserved as recorded in layers (absolute
// targets stay absolute), whiteouts are filtered via mutate.Extract's reverse
// iteration. For a sanitized direct-to-disk variant use `cr fs extract -o DIR`.
func NewExportCmd(opts *registry.Options) *cobra.Command {
	return &cobra.Command{
		Use:   "export IMAGE [TARBALL]",
		Short: "Export the filesystem of an image as a tarball",
		Long: `Export writes the merged filesystem of IMAGE as a tar stream to TARBALL
(default: "-" = stdout). Output is byte-for-byte equivalent to crane export:
linknames are not rewritten, whiteouts are filtered.

For a directory-target extraction with symlink/path-traversal safety checks,
use "d8 cr fs extract".

Examples:
  d8 cr export alpine:3.19 -          # stream to stdout
  d8 cr export alpine:3.19 fs.tar     # write to file
  d8 cr export alpine:3.19 - | tar tf -   # list contents
`,
		Args:              cobra.RangeArgs(1, 2),
		ValidArgsFunction: completion.ImageThenPath(),
		RunE: func(cmd *cobra.Command, args []string) error {
			dst := "-"
			if len(args) > 1 {
				dst = args[1]
			}
			return runExport(cmd, args[0], dst, opts)
		},
	}
}

func runExport(cmd *cobra.Command, src, dst string, opts *registry.Options) error {
	img, err := registry.Fetch(cmd.Context(), src, opts)
	if err != nil {
		return err
	}

	w, closeFn, err := openExportSink(cmd, dst)
	if err != nil {
		return err
	}

	exportErr := exportImage(img, w)
	closeErr := closeFn()
	if exportErr != nil {
		// File-sink target is now a half-written tar that callers would
		// likely consume by mistake (`tar tf` happily reads short streams).
		// Remove it so the user sees a clean failure, not a corrupt artifact.
		if dst != "-" {
			_ = os.Remove(dst)
		}
		return exportErr
	}
	return closeErr
}

// exportImage mirrors crane's pkg/crane.Export
// (https://pkg.go.dev/github.com/google/go-containerregistry/pkg/crane#Export):
// for single-layer images whose only layer is non-OCI media (e.g. arbitrary
// blob wrappers), it dumps the uncompressed contents directly. Otherwise it
// streams the merged filesystem via mutate.Extract.
//
// Functionally identical to upstream; we keep our own copy so the export
// command stays in our domain layer (no pkg/crane dependency) and to ensure
// rc.Close() is honoured in both branches.
func exportImage(img v1.Image, w io.Writer) error {
	layers, err := img.Layers()
	if err != nil {
		return fmt.Errorf("get layers: %w", err)
	}
	if len(layers) == 1 {
		mt, err := layers[0].MediaType()
		if err != nil {
			return fmt.Errorf("media type: %w", err)
		}
		if !mt.IsLayer() {
			rc, err := layers[0].Uncompressed()
			if err != nil {
				return fmt.Errorf("uncompress: %w", err)
			}
			defer rc.Close()
			if _, err := io.Copy(w, rc); err != nil {
				return fmt.Errorf("copy: %w", err)
			}
			return nil
		}
	}
	rc := mutate.Extract(img)
	defer rc.Close()
	if _, err := io.Copy(w, rc); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	return nil
}

func openExportSink(cmd *cobra.Command, dst string) (io.Writer, func() error, error) {
	if dst == "-" {
		return cmd.OutOrStdout(), func() error { return nil }, nil
	}
	f, err := os.Create(dst)
	if err != nil {
		return nil, nil, fmt.Errorf("create %s: %w", dst, err)
	}
	return f, func() error {
		if err := f.Close(); err != nil {
			return fmt.Errorf("close %s: %w", dst, err)
		}
		return nil
	}, nil
}
