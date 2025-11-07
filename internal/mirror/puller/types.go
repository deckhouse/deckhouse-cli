package puller

import (
	"context"
	"strings"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// ImageGetter is a function type for getting images from the registry
type ImageGetter func(ctx context.Context, tag string) (pkg.RegistryImage, error)

// PullConfig encapsulates the configuration for pulling images
type PullConfig struct {
	Name             string
	ImageSet         map[string]*ImageMeta
	Layout           *registry.ImageLayout
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
func SplitImageRefByRepoAndTag(imageReferenceString string) (repo, tag string) {
	splitIndex := strings.LastIndex(imageReferenceString, ":")
	repo = imageReferenceString[:splitIndex]
	tag = imageReferenceString[splitIndex+1:]

	if strings.HasSuffix(repo, "@sha256") {
		repo = strings.TrimSuffix(repo, "@sha256")
		tag = "@sha256:" + tag
	}

	return repo, tag
}
