package plugins

import (
	"context"
	"fmt"
	"io"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// RegistryClient provides methods to interact with container registries
type RegistryClient struct {
	registry string
	auth     authn.Authenticator
	options  []remote.Option
}

// NewRegistryClient creates a new container registry client using go-containerregistry
func NewRegistryClient(registry, username, password string) *RegistryClient {
	var auth authn.Authenticator

	if username != "" && password != "" {
		auth = &authn.Basic{
			Username: username,
			Password: password,
		}
	} else {
		auth = authn.Anonymous
	}

	options := []remote.Option{
		remote.WithAuth(auth),
	}

	return &RegistryClient{
		registry: registry,
		auth:     auth,
		options:  options,
	}
}

// GetManifest retrieves the manifest for a specific image tag
func (c *RegistryClient) GetManifest(ctx context.Context, repository, tag string) (*remote.Descriptor, error) {
	ref, err := name.ParseReference(fmt.Sprintf("%s/%s:%s", c.registry, repository, tag))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))
	desc, err := remote.Get(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}

	return desc, nil
}

// GetImage retrieves an image for a specific reference
func (c *RegistryClient) GetImage(ctx context.Context, repository, tag string) (v1.Image, error) {
	ref, err := name.ParseReference(fmt.Sprintf("%s/%s:%s", c.registry, repository, tag))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))
	img, err := remote.Image(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	return img, nil
}

// GetImageConfig retrieves the image config file containing labels and metadata
func (c *RegistryClient) GetImageConfig(ctx context.Context, repository, tag string) (*v1.ConfigFile, error) {
	img, err := c.GetImage(ctx, repository, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	configFile, err := img.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("failed to get image config: %w", err)
	}

	return configFile, nil
}

// GetImageLayers retrieves all layers of an image
func (c *RegistryClient) GetImageLayers(ctx context.Context, repository, tag string) ([]v1.Layer, error) {
	img, err := c.GetImage(ctx, repository, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("failed to get image layers: %w", err)
	}

	return layers, nil
}

// GetLabel retrieves a specific label from image metadata
func (c *RegistryClient) GetLabel(ctx context.Context, repository, tag, labelKey string) (string, bool, error) {
	configFile, err := c.GetImageConfig(ctx, repository, tag)
	if err != nil {
		return "", false, err
	}

	if configFile.Config.Labels == nil {
		return "", false, nil
	}

	value, exists := configFile.Config.Labels[labelKey]
	return value, exists, nil
}

// LayerStream represents a single layer stream for extraction
type LayerStream struct {
	Index  int
	Total  int
	Reader io.ReadCloser
}

// ExtractImageLayers retrieves uncompressed layer streams for extraction
// The caller is responsible for closing each LayerStream.Reader
func (c *RegistryClient) ExtractImageLayers(ctx context.Context, repository, tag string, handler func(*LayerStream) error) error {
	layers, err := c.GetImageLayers(ctx, repository, tag)
	if err != nil {
		return fmt.Errorf("failed to get image layers: %w", err)
	}

	total := len(layers)
	for i, layer := range layers {
		// Get the layer as an uncompressed tar stream
		reader, err := layer.Uncompressed()
		if err != nil {
			return fmt.Errorf("failed to uncompress layer %d: %w", i, err)
		}

		// Create layer stream
		stream := &LayerStream{
			Index:  i + 1,
			Total:  total,
			Reader: reader,
		}

		// Pass to handler
		err = handler(stream)
		reader.Close()

		if err != nil {
			return fmt.Errorf("failed to handle layer %d: %w", i, err)
		}
	}

	return nil
}
