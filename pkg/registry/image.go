package registry

import (
	"io"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
)

type Image struct {
	v1.Image
	reference string
}

type ImageOption func(img *Image)

func WithReference(ref string) ImageOption {
	return func(img *Image) {
		img.reference = ref
	}
}

func NewImage(img v1.Image, opts ...ImageOption) *Image {
	image := &Image{Image: img}
	for _, opt := range opts {
		opt(image)
	}

	return image
}

// Extract flattens the image to a single layer and returns ReadCloser for fetching the content
// The repository is determined by the chained WithSegment() calls
func (i *Image) Extract() io.ReadCloser {
	return mutate.Extract(i)
}

func (i *Image) GetReference() string {
	return i.reference
}
