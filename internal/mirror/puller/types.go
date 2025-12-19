/*
Copyright 2025 Flant JSC

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

package puller

import (
	"context"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/pkg"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// ImageGetter is a function type for getting images from the registry
type ImageGetter func(ctx context.Context, tag string, opts ...registry.ImageGetOption) (pkg.RegistryImage, error)

// PullConfig encapsulates the configuration for pulling images
type PullConfig struct {
	Name             string
	ImageSet         map[string]*ImageMeta
	Layout           *regimage.ImageLayout
	AllowMissingTags bool
	GetterService    pkg.BasicService
}

// ImageMeta represents metadata for an image
type ImageMeta struct {
	ImageRepo       string
	ImageTag        string
	Digest          *v1.Hash
	Version         string
	TagReference    string
	DigestReference string
}

// NewImageMeta creates a new ImageMeta instance
func NewImageMeta(version string, tagReference string, digest *v1.Hash) *ImageMeta {
	imageRepo, tag := SplitImageRefByRepoAndTag(tagReference)

	return &ImageMeta{
		ImageRepo:       imageRepo,
		ImageTag:        tag,
		Digest:          digest,
		Version:         version,
		TagReference:    tagReference,
		DigestReference: imageRepo + "@" + digest.String(),
	}
}

// SplitImageRefByRepoAndTag splits an image reference into repository and tag parts
// For digest references (repo@sha256:abc), returns just the hex part as tag
func SplitImageRefByRepoAndTag(imageReferenceString string) (string, string) {
	splitIndex := strings.LastIndex(imageReferenceString, ":")
	repo := imageReferenceString[:splitIndex]
	tag := imageReferenceString[splitIndex+1:]

	if strings.HasSuffix(repo, "@sha256") {
		repo = strings.TrimSuffix(repo, "@sha256")
		// Return just the hex digest without @sha256: prefix
		// This makes it a valid registry tag
	}

	return repo, tag
}
