package pkg

import (
	"context"
	"io"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// RegistryClient defines the contract for interacting with container registries
type RegistryClient interface {
	// GetManifest retrieves the manifest for a specific image tag
	GetManifest(ctx context.Context, repository, tag string) (*remote.Descriptor, error)

	// GetImage retrieves an image for a specific reference
	GetImage(ctx context.Context, repository, tag string) (v1.Image, error)

	// GetImageConfig retrieves the image config file containing labels and metadata
	GetImageConfig(ctx context.Context, repository, tag string) (*v1.ConfigFile, error)

	// GetImageLayers retrieves all layers of an image
	GetImageLayers(ctx context.Context, repository, tag string) ([]v1.Layer, error)

	// GetLabel retrieves a specific label from image metadata
	GetLabel(ctx context.Context, repository, tag, labelKey string) (string, bool, error)

	// ExtractImageLayers retrieves uncompressed layer streams for extraction
	ExtractImageLayers(ctx context.Context, repository, tag string, handler func(LayerStream) error) error
}

// LayerStream provides access to a single layer stream for extraction
type LayerStream interface {
	// GetIndex returns the current layer index (1-based)
	GetIndex() int
	// GetTotal returns the total number of layers
	GetTotal() int
	// GetReader returns the reader for the layer content
	GetReader() io.ReadCloser
}
