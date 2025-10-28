package registry

import (
	"io"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
)

type Image struct {
	v1.Image
}

func NewImage(img v1.Image) *Image {
	return &Image{Image: img}
}

// Extract flattens the image to a single layer and returns ReadCloser for fetching the content
// The repository is determined by the chained WithSegment() calls
func (i *Image) Extract() io.ReadCloser {
	return mutate.Extract(i)
}
