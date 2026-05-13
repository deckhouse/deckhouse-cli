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

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// SaveTarball writes imgs to path as a modern docker-compatible tarball
// (references may be digests or tagged names). An empty map is rejected:
// the resulting file would have no manifest.json and LoadTarball could
// not read it back, so callers see a clean error instead of a malformed
// artifact.
func SaveTarball(path string, imgs map[string]v1.Image) error {
	if len(imgs) == 0 {
		return fmt.Errorf("save tarball %s: at least one image is required", path)
	}
	refToImage := make(map[name.Reference]v1.Image, len(imgs))
	for refStr, img := range imgs {
		ref, err := name.ParseReference(refStr)
		if err != nil {
			return fmt.Errorf("parse reference %q: %w", refStr, err)
		}
		refToImage[ref] = img
	}
	if err := tarball.MultiRefWriteToFile(path, refToImage); err != nil {
		return fmt.Errorf("write tarball %s: %w", path, err)
	}
	return nil
}

// SaveLegacy writes imgs to path as a legacy docker-format tarball, which
// understands only tagged references (no digests) but is readable by old
// `docker load` versions. Empty maps are rejected for the same reason as
// SaveTarball.
func SaveLegacy(path string, imgs map[string]v1.Image) error {
	if len(imgs) == 0 {
		return fmt.Errorf("save legacy tarball %s: at least one image is required", path)
	}
	tagToImage := make(map[name.Tag]v1.Image, len(imgs))
	for refStr, img := range imgs {
		tag, err := name.NewTag(refStr)
		if err != nil {
			return fmt.Errorf("parse tag %q (legacy tarballs require tagged refs): %w", refStr, err)
		}
		tagToImage[tag] = img
	}
	if err := tarball.MultiWriteToFile(path, tagToImage); err != nil {
		return fmt.Errorf("write legacy tarball %s: %w", path, err)
	}
	return nil
}

// LoadTarball reads a docker-format tarball from path. When tag is non-empty
// it picks the matching manifest entry; otherwise the first/only entry.
func LoadTarball(path, tag string) (v1.Image, error) {
	if tag == "" {
		img, err := tarball.ImageFromPath(path, nil)
		if err != nil {
			return nil, fmt.Errorf("load tarball %s: %w", path, err)
		}
		return img, nil
	}
	ref, err := name.NewTag(tag)
	if err != nil {
		return nil, fmt.Errorf("parse tag %q: %w", tag, err)
	}
	img, err := tarball.ImageFromPath(path, &ref)
	if err != nil {
		return nil, fmt.Errorf("load tarball %s with tag %s: %w", path, tag, err)
	}
	return img, nil
}
