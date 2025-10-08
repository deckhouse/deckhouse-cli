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

package registry

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse/pkg/log"
)

// Client provides methods to interact with container registries
type Client struct {
	registryHost string // e.g., "registry.deckhouse.io"
	scopePath    string // e.g., "deckhouse/ee/modules" (built from chained WithScope calls)
	options      []remote.Option
	log          *log.Logger
}

// Ensure Client implements pkg.RegistryInterface
var _ pkg.RegistryClient = (*Client)(nil)

// NewClientWithOptions creates a new container registry client with advanced options
func NewClientWithOptions(opts *ClientOptions) *Client {
	auth := buildAuthenticator(opts)
	remoteOptions := buildRemoteOptions(auth, opts)

	return &Client{
		registryHost: opts.RegistryHost,
		scopePath:    "",
		options:      remoteOptions,
		log:          opts.Logger,
	}
}

// WithScope creates a new client with an additional scope path segment
// This method can be chained to build complex paths:
// client.WithScope("deckhouse").WithScope("ee").WithScope("modules")
func (c *Client) WithScope(scope string) pkg.RegistryClient {
	newScopePath := scope
	if c.scopePath != "" {
		newScopePath = fmt.Sprintf("%s/%s", c.scopePath, scope)
	}

	return &Client{
		registryHost: c.registryHost,
		scopePath:    newScopePath,
		options:      c.options,
		log:          c.log,
	}
}

// GetRegistry returns the full registry path (host + scope)
func (c *Client) GetRegistry() string {
	if c.scopePath == "" {
		return c.registryHost
	}

	return fmt.Sprintf("%s/%s", c.registryHost, c.scopePath)
}

// GetManifest retrieves the manifest for a specific image tag
// The repository is determined by the chained WithScope() calls
func (c *Client) GetManifest(ctx context.Context, tag string) (*remote.Descriptor, error) {
	c.log.Debug("Getting manifest", slog.String("scope", c.scopePath), slog.String("tag", tag))

	ref, err := name.ParseReference(fmt.Sprintf("%s/%s:%s", c.registryHost, c.scopePath, tag))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))
	desc, err := remote.Get(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}

	c.log.Debug("Manifest retrieved successfully", slog.String("scope", c.scopePath), slog.String("tag", tag))

	return desc, nil
}

// GetImage retrieves an image for a specific reference
// The repository is determined by the chained WithScope() calls
func (c *Client) GetImage(ctx context.Context, tag string) (v1.Image, error) {
	c.log.Debug("Getting image", slog.String("scope", c.scopePath), slog.String("tag", tag))

	ref, err := name.ParseReference(fmt.Sprintf("%s/%s:%s", c.registryHost, c.scopePath, tag))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))
	img, err := remote.Image(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	c.log.Debug("Image retrieved successfully", slog.String("scope", c.scopePath), slog.String("tag", tag))

	return img, nil
}

// GetImageConfig retrieves the image config file containing labels and metadata
// The repository is determined by the chained WithScope() calls
func (c *Client) GetImageConfig(ctx context.Context, tag string) (*v1.ConfigFile, error) {
	c.log.Debug("Getting image config", slog.String("scope", c.scopePath), slog.String("tag", tag))

	img, err := c.GetImage(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	configFile, err := img.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("failed to get image config: %w", err)
	}

	c.log.Debug("Image config retrieved successfully", slog.String("scope", c.scopePath), slog.String("tag", tag))

	return configFile, nil
}

// GetImageLayers retrieves all layers of an image
// The repository is determined by the chained WithScope() calls
func (c *Client) GetImageLayers(ctx context.Context, tag string) ([]v1.Layer, error) {
	c.log.Debug("Getting image layers", slog.String("scope", c.scopePath), slog.String("tag", tag))

	img, err := c.GetImage(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("failed to get image layers: %w", err)
	}

	c.log.Debug("Image layers retrieved successfully", slog.String("scope", c.scopePath), slog.String("tag", tag), slog.Int("count", len(layers)))

	return layers, nil
}

// GetLabel retrieves a specific label from image metadata
// The repository is determined by the chained WithScope() calls
func (c *Client) GetLabel(ctx context.Context, tag, labelKey string) (string, bool, error) {
	c.log.Debug("Getting label", slog.String("scope", c.scopePath), slog.String("tag", tag), slog.String("label", labelKey))

	configFile, err := c.GetImageConfig(ctx, tag)
	if err != nil {
		return "", false, err
	}

	if configFile.Config.Labels == nil {
		c.log.Debug("No labels found in image", slog.String("scope", c.scopePath), slog.String("tag", tag))
		return "", false, nil
	}

	value, exists := configFile.Config.Labels[labelKey]

	c.log.Debug("Label lookup result", slog.String("scope", c.scopePath), slog.String("tag", tag), slog.String("label", labelKey), slog.Bool("exists", exists))

	return value, exists, nil
}

// LayerStream represents a single layer stream for extraction
type LayerStream struct {
	index  int
	total  int
	reader io.ReadCloser
}

// Ensure LayerStream implements pkg.LayerStreamInterface
var _ pkg.LayerStream = (*LayerStream)(nil)

// GetIndex returns the current layer index (1-based)
func (ls *LayerStream) GetIndex() int {
	return ls.index
}

// GetTotal returns the total number of layers
func (ls *LayerStream) GetTotal() int {
	return ls.total
}

// GetReader returns the reader for the layer content
func (ls *LayerStream) GetReader() io.ReadCloser {
	return ls.reader
}

// NewLayerStream creates a new LayerStream
func NewLayerStream(index, total int, reader io.ReadCloser) *LayerStream {
	return &LayerStream{
		index:  index,
		total:  total,
		reader: reader,
	}
}

// ExtractImageLayers retrieves uncompressed layer streams for extraction
// The repository is determined by the chained WithScope() calls
// The caller is responsible for closing each LayerStream.Reader
func (c *Client) ExtractImageLayers(ctx context.Context, tag string, handler func(pkg.LayerStream) error) error {
	c.log.Debug("Extracting image layers", slog.String("scope", c.scopePath), slog.String("tag", tag))

	layers, err := c.GetImageLayers(ctx, tag)
	if err != nil {
		return fmt.Errorf("failed to get image layers: %w", err)
	}

	total := len(layers)

	c.log.Debug("Starting layer extraction", slog.String("scope", c.scopePath), slog.String("tag", tag), slog.Int("total_layers", total))

	for i, layer := range layers {
		c.log.Debug("Processing layer", slog.Int("index", i+1), slog.Int("total", total))

		// Get the layer as an uncompressed tar stream
		reader, err := layer.Uncompressed()
		if err != nil {
			return fmt.Errorf("failed to uncompress layer %d: %w", i, err)
		}

		// Create layer stream
		stream := NewLayerStream(i+1, total, reader)

		// Pass to handler
		err = handler(stream)
		reader.Close()

		if err != nil {
			return fmt.Errorf("failed to handle layer %d: %w", i, err)
		}

		c.log.Debug("Layer processed successfully", slog.Int("index", i+1), slog.Int("total", total))
	}

	c.log.Debug("All layers extracted successfully", slog.String("scope", c.scopePath), slog.String("tag", tag), slog.Int("total_layers", total))

	return nil
}

// ListTags lists all tags for the current scope
// The repository is determined by the chained WithScope() calls
func (c *Client) ListTags(ctx context.Context) ([]string, error) {
	c.log.Debug("Listing tags", slog.String("scope", c.scopePath))

	ref, err := name.ParseReference(fmt.Sprintf("%s/%s", c.registryHost, c.scopePath))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	repo := ref.Context()
	opts := append(c.options, remote.WithContext(ctx))

	tags, err := remote.List(repo, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to list tags: %w", err)
	}

	c.log.Debug("Tags listed successfully", slog.String("scope", c.scopePath), slog.Int("count", len(tags)))

	return tags, nil
}

// ListRepositories lists all sub-repositories under the current scope
// The scope is determined by the chained WithScope() calls
// Returns repository names (tags) under the current scope
func (c *Client) ListRepositories(ctx context.Context) ([]string, error) {
	fullRegistry := c.GetRegistry()
	c.log.Debug("Listing repositories", slog.String("base_registry", fullRegistry))

	// Use the current scope path to list sub-repositories
	// For example, if scope is "deckhouse/ee/modules"
	// this will list all tags/sub-paths under that repository
	ref, err := name.ParseReference(fullRegistry)
	if err != nil {
		return nil, fmt.Errorf("failed to parse registry reference: %w", err)
	}

	repo := ref.Context()
	c.log.Debug("Listing tags for base repository", slog.String("repository", repo.String()))

	opts := append(c.options, remote.WithContext(ctx))

	// List "tags" which actually represent sub-repositories in this case
	tags, err := remote.List(repo, opts...)
	if err != nil {
		c.log.Debug("Failed to list repository tags", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to list repositories: %w", err)
	}

	c.log.Debug("Repositories listed successfully", slog.String("scope", c.scopePath), slog.Int("total", len(tags)))

	return tags, nil
}
