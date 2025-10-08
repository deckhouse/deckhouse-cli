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
	registry string
	options  []remote.Option
	log      *log.Logger
}

// Ensure Client implements pkg.RegistryInterface
var _ pkg.RegistryClient = (*Client)(nil)

// NewClientWithOptions creates a new container registry client with advanced options
func NewClientWithOptions(opts *ClientOptions) *Client {
	auth := buildAuthenticator(opts)
	remoteOptions := buildRemoteOptions(auth, opts)

	return &Client{
		registry: opts.Registry,
		options:  remoteOptions,
		log:      opts.Logger,
	}
}

// GetManifest retrieves the manifest for a specific image tag
func (c *Client) GetManifest(ctx context.Context, repository, tag string) (*remote.Descriptor, error) {
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
func (c *Client) GetImage(ctx context.Context, repository, tag string) (v1.Image, error) {
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
func (c *Client) GetImageConfig(ctx context.Context, repository, tag string) (*v1.ConfigFile, error) {
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
func (c *Client) GetImageLayers(ctx context.Context, repository, tag string) ([]v1.Layer, error) {
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
func (c *Client) GetLabel(ctx context.Context, repository, tag, labelKey string) (string, bool, error) {
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
// The caller is responsible for closing each LayerStream.Reader
func (c *Client) ExtractImageLayers(ctx context.Context, repository, tag string, handler func(pkg.LayerStream) error) error {
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
		stream := NewLayerStream(i+1, total, reader)

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

// ListTags lists all tags for a specific repository
func (c *Client) ListTags(ctx context.Context, repository string) ([]string, error) {
	c.log.Debug("Listing tags", slog.String("repository", repository))

	ref, err := name.ParseReference(fmt.Sprintf("%s/%s", c.registry, repository))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	repo := ref.Context()
	opts := append(c.options, remote.WithContext(ctx))

	tags, err := remote.List(repo, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to list tags: %w", err)
	}

	c.log.Debug("Tags listed successfully", slog.String("repository", repository), slog.Int("count", len(tags)))

	return tags, nil
}

// ListRepositories lists all repositories under a specific path prefix
// This uses remote.List() on the base registry path to list sub-repositories/tags
func (c *Client) ListRepositories(ctx context.Context, pathPrefix string) ([]string, error) {
	c.log.Debug("Listing repositories", slog.String("pathPrefix", pathPrefix), slog.String("base_registry", c.registry))

	// Use the base registry path to list sub-repositories
	// For example, if c.registry is "registry.deckhouse.io/deckhouse/ee/modules"
	// this will list all tags/sub-paths under that repository
	ref, err := name.ParseReference(c.registry)
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

	c.log.Debug("Base repository listing returned", slog.Int("total_items", len(tags)))

	// Build repository paths (without registry hostname)
	// Extract the repository path from c.registry (remove registry hostname)
	// For example: "registry.deckhouse.io/deckhouse/ee/modules" -> "deckhouse/ee/modules"
	baseRepoPath := repo.RepositoryStr()

	var repos []string
	for _, tag := range tags {
		// Construct path: base_repo_path/tag
		fullPath := fmt.Sprintf("%s/%s", baseRepoPath, tag)
		repos = append(repos, fullPath)
	}

	c.log.Debug("Constructed repository paths", slog.Int("total", len(repos)), slog.String("base_path", baseRepoPath))

	// Filter repositories by path prefix if specified
	var filtered []string
	if pathPrefix != "" {
		for _, repoPath := range repos {
			if len(repoPath) >= len(pathPrefix) && repoPath[:len(pathPrefix)] == pathPrefix {
				filtered = append(filtered, repoPath)
			}
		}
	} else {
		filtered = repos
	}

	c.log.Debug("Repositories listed successfully", slog.String("pathPrefix", pathPrefix), slog.Int("total", len(repos)), slog.Int("filtered", len(filtered)))

	return filtered, nil
}
