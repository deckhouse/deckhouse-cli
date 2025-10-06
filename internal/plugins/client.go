package plugins

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse/pkg/log"
)

// RegistryClient provides methods to interact with container registries
type RegistryClient struct {
	registry string
	auth     authn.Authenticator
	options  []remote.Option
	log      *log.Logger
}

// NewRegistryClient creates a new container registry client using go-containerregistry
func NewRegistryClient(registry, username, password string, logger *log.Logger) *RegistryClient {
	var auth authn.Authenticator

	if username != "" && password != "" {
		auth = &authn.Basic{
			Username: username,
			Password: password,
		}
		logger.Debug("Registry client initialized with authentication", slog.String("registry", registry), slog.String("username", username))
	} else {
		auth = authn.Anonymous
		logger.Debug("Registry client initialized with anonymous access", slog.String("registry", registry))
	}

	options := []remote.Option{
		remote.WithAuth(auth),
	}

	return &RegistryClient{
		registry: registry,
		auth:     auth,
		options:  options,
		log:      logger,
	}
}

// GetManifest retrieves the manifest for a specific image tag
func (c *RegistryClient) GetManifest(ctx context.Context, repository, tag string) (*remote.Descriptor, error) {
	c.log.Debug("Getting manifest", slog.String("repository", repository), slog.String("tag", tag))

	ref, err := name.ParseReference(fmt.Sprintf("%s/%s:%s", c.registry, repository, tag))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))
	desc, err := remote.Get(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}

	c.log.Debug("Manifest retrieved successfully", slog.String("repository", repository), slog.String("tag", tag))

	return desc, nil
}

// GetImage retrieves an image for a specific reference
func (c *RegistryClient) GetImage(ctx context.Context, repository, tag string) (v1.Image, error) {
	c.log.Debug("Getting image", slog.String("repository", repository), slog.String("tag", tag))

	ref, err := name.ParseReference(fmt.Sprintf("%s/%s:%s", c.registry, repository, tag))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))
	img, err := remote.Image(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	c.log.Debug("Image retrieved successfully", slog.String("repository", repository), slog.String("tag", tag))

	return img, nil
}

// GetImageConfig retrieves the image config file containing labels and metadata
func (c *RegistryClient) GetImageConfig(ctx context.Context, repository, tag string) (*v1.ConfigFile, error) {
	c.log.Debug("Getting image config", slog.String("repository", repository), slog.String("tag", tag))

	img, err := c.GetImage(ctx, repository, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	configFile, err := img.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("failed to get image config: %w", err)
	}

	c.log.Debug("Image config retrieved successfully", slog.String("repository", repository), slog.String("tag", tag))

	return configFile, nil
}

// GetImageLayers retrieves all layers of an image
func (c *RegistryClient) GetImageLayers(ctx context.Context, repository, tag string) ([]v1.Layer, error) {
	c.log.Debug("Getting image layers", slog.String("repository", repository), slog.String("tag", tag))

	img, err := c.GetImage(ctx, repository, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("failed to get image layers: %w", err)
	}

	c.log.Debug("Image layers retrieved successfully", slog.String("repository", repository), slog.String("tag", tag), slog.Int("count", len(layers)))

	return layers, nil
}

// GetLabel retrieves a specific label from image metadata
func (c *RegistryClient) GetLabel(ctx context.Context, repository, tag, labelKey string) (string, bool, error) {
	c.log.Debug("Getting label", slog.String("repository", repository), slog.String("tag", tag), slog.String("label", labelKey))

	configFile, err := c.GetImageConfig(ctx, repository, tag)
	if err != nil {
		return "", false, err
	}

	if configFile.Config.Labels == nil {
		c.log.Debug("No labels found in image", slog.String("repository", repository), slog.String("tag", tag))
		return "", false, nil
	}

	value, exists := configFile.Config.Labels[labelKey]

	c.log.Debug("Label lookup result", slog.String("repository", repository), slog.String("tag", tag), slog.String("label", labelKey), slog.Bool("exists", exists))

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
	c.log.Debug("Extracting image layers", slog.String("repository", repository), slog.String("tag", tag))

	layers, err := c.GetImageLayers(ctx, repository, tag)
	if err != nil {
		return fmt.Errorf("failed to get image layers: %w", err)
	}

	total := len(layers)

	c.log.Debug("Starting layer extraction", slog.String("repository", repository), slog.String("tag", tag), slog.Int("total_layers", total))

	for i, layer := range layers {
		c.log.Debug("Processing layer", slog.Int("index", i+1), slog.Int("total", total))

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

		c.log.Debug("Layer processed successfully", slog.Int("index", i+1), slog.Int("total", total))
	}

	c.log.Debug("All layers extracted successfully", slog.String("repository", repository), slog.String("tag", tag), slog.Int("total_layers", total))

	return nil
}
