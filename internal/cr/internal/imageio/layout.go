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

package imageio

import (
	"fmt"
	"os"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/partial"
)

// SaveOCI appends images and indices to an OCI image-layout directory at
// path, creating it if missing.
func SaveOCI(path string, imgs map[string]v1.Image, idxs map[string]v1.ImageIndex) error {
	p, err := openOrCreateLayout(path)
	if err != nil {
		return err
	}
	for refStr, img := range imgs {
		if err := p.AppendImage(img); err != nil {
			return fmt.Errorf("append image %s: %w", refStr, err)
		}
	}
	for refStr, idx := range idxs {
		if err := p.AppendIndex(idx); err != nil {
			return fmt.Errorf("append index %s: %w", refStr, err)
		}
	}
	return nil
}

// LoadLocal inspects path and returns a v1.Image or v1.ImageIndex.
//
//	path is a file          -> docker tarball, returns v1.Image
//	path is an OCI layout   -> contents determine the type:
//	                           - exactly one image manifest                 -> v1.Image
//	                           - exactly one nested index                   -> v1.ImageIndex (unwrapped, --index optional)
//	                           - several entries with asIndex = true        -> v1.ImageIndex (the layout's top-level index)
//	                           - several entries without asIndex            -> error (ambiguous)
func LoadLocal(path string, asIndex bool) (partial.WithRawManifest, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", path, err)
	}
	if !stat.IsDir() {
		return LoadTarball(path, "")
	}

	idx, err := layout.ImageIndexFromPath(path)
	if err != nil {
		return nil, fmt.Errorf("read OCI layout %s: %w", path, err)
	}

	manifest, err := idx.IndexManifest()
	if err != nil {
		return nil, fmt.Errorf("read index manifest: %w", err)
	}

	// Single-entry layout: unwrap regardless of asIndex. The layout's
	// top-level index.json is a "directory of contents" pointer, not the
	// thing the user intends to publish. Pushing it as-is would store a
	// redundant 1-entry wrapper in the registry (an index whose only
	// manifest is the real index/image), and subsequent pulls would
	// preserve that extra layer. asIndex stays meaningful only for layouts
	// that actually need a fresh index built from multiple entries.
	if len(manifest.Manifests) == 1 {
		desc := manifest.Manifests[0]
		switch {
		case desc.MediaType.IsImage():
			return idx.Image(desc.Digest)
		case desc.MediaType.IsIndex():
			return idx.ImageIndex(desc.Digest)
		default:
			return nil, fmt.Errorf("layout %s contains non-image entry (mediaType %q)", path, desc.MediaType)
		}
	}

	if !asIndex {
		return nil, fmt.Errorf("layout %s contains %d entries; pass --index to push as an index", path, len(manifest.Manifests))
	}
	return idx, nil
}

func openOrCreateLayout(path string) (layout.Path, error) {
	stat, err := os.Stat(path)
	switch {
	case err != nil && !os.IsNotExist(err):
		return "", fmt.Errorf("stat %s: %w", path, err)
	case err == nil && !stat.IsDir():
		return "", fmt.Errorf("--format oci requires a directory, got file %s", path)
	}

	if err == nil {
		// Existing directory: take it only if it is already a valid OCI
		// layout, or if it is empty. A non-empty directory that is not a
		// layout (the user mistyped a destination, e.g. ~/Documents) must
		// not be silently overwritten with layout.Write.
		if p, lerr := layout.FromPath(path); lerr == nil {
			return p, nil
		}
		entries, rerr := os.ReadDir(path)
		if rerr != nil {
			return "", fmt.Errorf("read %s: %w", path, rerr)
		}
		if len(entries) > 0 {
			return "", fmt.Errorf("%s exists, is not an OCI layout, and is not empty; refusing to overwrite", path)
		}
	}

	p, err := layout.Write(path, empty.Index)
	if err != nil {
		return "", fmt.Errorf("create OCI layout %s: %w", path, err)
	}
	return p, nil
}
