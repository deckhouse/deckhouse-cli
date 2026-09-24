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

package registry

import (
	"context"
	"fmt"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// Fetch resolves ref to a single image.
//
// For a multi-arch index the platform pinned on Options wins; without one the
// underlying library falls back to a hardcoded linux/amd64 rather than the
// host's platform, so commands that must match the caller's architecture have
// to pass --platform.
func Fetch(ctx context.Context, ref string, opts *Options) (v1.Image, error) {
	client, id, err := clientForRef(ref, opts)
	if err != nil {
		return nil, err
	}

	img, err := client.GetImage(ctx, id, opts.imageGetOptions()...)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", ref, err)
	}

	return img, nil
}

// FetchIndex resolves ref to a multi-arch index, resolving nothing. An image
// reference is an error - the caller asked for an index.
func FetchIndex(ctx context.Context, ref string, opts *Options) (v1.ImageIndex, error) {
	client, id, err := clientForRef(ref, opts)
	if err != nil {
		return nil, err
	}

	idx, err := client.GetIndex(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fetch index %s: %w", ref, err)
	}

	return idx, nil
}

// IsIndex reports whether ref is a multi-arch index rather than a single image,
// which is what pull needs to know before deciding what to write to disk.
func IsIndex(ctx context.Context, ref string, opts *Options) (bool, error) {
	client, id, err := clientForRef(ref, opts)
	if err != nil {
		return false, err
	}

	// No platform here on purpose: the question is what the registry serves for
	// this reference, and resolving a platform first would answer "image" for
	// every index.
	res, err := client.GetManifest(ctx, id)
	if err != nil {
		return false, fmt.Errorf("inspect %s: %w", ref, err)
	}

	return res.GetMediaType().IsIndex(), nil
}
