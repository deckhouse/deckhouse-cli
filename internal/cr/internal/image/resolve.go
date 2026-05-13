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

package image

import (
	"context"
	"fmt"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/cache"

	"github.com/deckhouse/deckhouse-cli/internal/cr/internal/registry"
)

// ResolvedSources holds per-reference results of resolving a list of remote
// sources. For indices that need to stay multi-arch (OCI layout without
// --platform) we store them in Indices; everything else ends up in Images
// already resolved to a single manifest.
type ResolvedSources struct {
	Images  map[string]v1.Image
	Indices map[string]v1.ImageIndex
}

// Resolve fetches each reference in srcList and classifies the result.
//
// keepMultiArchIndex = true tells the resolver to keep an OCI index as an
// index when no --platform is pinned (pull-to-OCI layout wants full indices).
// Otherwise every source resolves to a single v1.Image for the current or
// pinned platform.
//
// cachePath, when non-empty, wraps each returned image with a filesystem
// cache so layers re-used across pulls are kept on disk.
//
// Duplicate srcList entries return an error rather than collapsing into
// a single map slot - otherwise downstream tarballs/layouts would silently
// drop one copy and surprise the user (`cr pull foo:1 foo:1 dst.tar` would
// yield a single-image tar, not two).
func Resolve(ctx context.Context, srcList []string, keepMultiArchIndex bool, cachePath string, opts *registry.Options) (*ResolvedSources, error) {
	if opts == nil {
		return nil, fmt.Errorf("resolve: registry options must not be nil")
	}

	seen := make(map[string]struct{}, len(srcList))
	for _, src := range srcList {
		if _, dup := seen[src]; dup {
			return nil, fmt.Errorf("duplicate source reference %q", src)
		}
		seen[src] = struct{}{}
	}

	out := &ResolvedSources{
		Images:  map[string]v1.Image{},
		Indices: map[string]v1.ImageIndex{},
	}

	// Build the cache once and reuse it - NewFilesystemCache opens the
	// directory each call, no need to pay that per source.
	var fsCache cache.Cache
	if cachePath != "" {
		fsCache = cache.NewFilesystemCache(cachePath)
	}

	for _, src := range srcList {
		desc, err := registry.FetchDescriptor(ctx, src, opts)
		if err != nil {
			return nil, err
		}

		if keepMultiArchIndex && desc.MediaType.IsIndex() && opts.Platform == nil {
			idx, err := desc.ImageIndex()
			if err != nil {
				return nil, fmt.Errorf("read index %s: %w", src, err)
			}
			if fsCache != nil {
				// Without this, --cache-path was a no-op for OCI pulls of
				// multi-arch images (the most common shape, e.g. alpine).
				idx = cache.ImageIndex(idx, fsCache)
			}
			out.Indices[src] = idx
			continue
		}

		img, err := desc.Image()
		if err != nil {
			return nil, fmt.Errorf("read image %s: %w", src, err)
		}
		if fsCache != nil {
			img = cache.Image(img, fsCache)
		}
		out.Images[src] = img
	}
	return out, nil
}
