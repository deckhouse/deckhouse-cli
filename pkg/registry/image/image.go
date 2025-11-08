package image

import (
	"fmt"
	"io"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse-cli/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
)

type Image struct {
	v1.Image

	metadata *ImageMeta
}

type ImageMeta struct {
	TagReference    string
	DigestReference string

	Digest *v1.Hash
}

func (m *ImageMeta) GetTagReference() string {
	return m.TagReference
}

func (m *ImageMeta) GetDigestReference() string {
	return m.DigestReference
}

func (m *ImageMeta) GetDigest() *v1.Hash {
	return m.Digest
}

func NewImageMeta(tagReference string, digestReference string, digest *v1.Hash) *ImageMeta {
	return &ImageMeta{
		TagReference:    tagReference,
		DigestReference: digestReference,
		Digest:          digest,
	}
}

type ImageOption func(img *Image)

func WithMetadata(metadata *ImageMeta) ImageOption {
	return func(img *Image) {
		img.metadata = metadata
	}
}

// WithFetchingMetadata enables fetching and filling image metadata (digest, digest reference) during NewImage call
func WithFetchingMetadata(tagReference string) ImageOption {
	return func(img *Image) {
		img.metadata = &ImageMeta{
			TagReference: tagReference,
		}
	}
}

func NewImage(img v1.Image, opts ...ImageOption) (*Image, error) {
	image := &Image{Image: img}

	for _, opt := range opts {
		opt(image)
	}

	// for fetching metadata
	if image.metadata != nil && image.metadata.Digest != nil {
		imageRepo, _ := registry.SplitImageRefByRepoAndTag(image.metadata.TagReference)

		digest, err := img.Digest()
		if err != nil {
			return nil, fmt.Errorf("get image digest: %w", err)
		}

		image.metadata.DigestReference = imageRepo + "@" + digest.String()
		image.metadata.Digest = &digest
	}

	return image, nil
}

// Extract flattens the image to a single layer and returns ReadCloser for fetching the content
// The repository is determined by the chained WithSegment() calls
func (i *Image) Extract() io.ReadCloser {
	return mutate.Extract(i)
}

func (i *Image) GetMetadata() (pkg.ImageMeta, error) {
	if i.metadata == nil {
		return nil, registry.ErrImageMetaNotFound
	}

	return i.metadata, nil
}

func (i *Image) SetMetadata(metadata pkg.ImageMeta) {
	i.metadata = metadata.(*ImageMeta)
}

func (i *Image) GetTagReference() (string, error) {
	if i.metadata == nil {
		return "", registry.ErrImageMetaNotFound
	}

	return i.metadata.TagReference, nil
}

func (i *Image) GetDigestReference() (string, error) {
	if i.metadata == nil {
		return "", registry.ErrImageMetaNotFound
	}

	return i.metadata.DigestReference, nil
}
