package registry

import (
	"fmt"
	"io"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	ocitools "github.com/sylabs/oci-tools/pkg/mutate"
)

type Image struct {
	v1.Image
}

func NewImage(img v1.Image) *Image {
	return &Image{Image: img}
}

// Extract flattens the image to a single layer and returns ReadCloser for fetching the content
// The repository is determined by the chained WithSegment() calls
func (i *Image) Extract() (io.ReadCloser, error) {
	flattenedImage, err := ocitools.Squash(i.Image)
	if err != nil {
		return nil, fmt.Errorf("flattening image to a single layer: %w", err)
	}

	imageLayers, err := flattenedImage.Layers()
	if err != nil {
		return nil, fmt.Errorf("getting the image's layers: %w", err)
	}

	if len(imageLayers) != 1 {
		return nil, fmt.Errorf("unexpected number of layers: %w", err)
	}

	rc, err := imageLayers[0].Uncompressed()
	if err != nil {
		return nil, fmt.Errorf("uncompress the layer: %w", err)
	}

	return rc, nil
}
