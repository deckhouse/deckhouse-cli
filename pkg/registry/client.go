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
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg"
)

// Client provides methods to interact with container registries
type Client struct {
	registryHost        string   // e.g., "registry.deckhouse.io"
	segments            []string // e.g., [deckhouse,ee,modules] (built from chained WithSegment calls)
	constructedSegments string   // cached joined segments for scope path
	options             []remote.Option
	log                 *log.Logger
}

// Ensure Client implements pkg.RegistryInterface
var _ pkg.RegistryClient = (*Client)(nil)

// NewClientWithOptions creates a new container registry client with advanced options
func NewClientWithOptions(registry string, opts *ClientOptions) *Client {
	// Ensure logger first before using it
	logger := ensureLogger(opts.Logger)

	remoteOptions := buildRemoteOptions(opts.Auth, opts)

	if opts.TLSSkipVerify {
		logger.Debug("TLS certificate verification disabled",
			slog.String("registry", registry))
	}

	if opts.Insecure {
		logger.Debug("Insecure HTTP mode enabled",
			slog.String("registry", registry))
	}

	registry = strings.TrimSuffix(registry, "/")

	return &Client{
		registryHost: registry,
		options:      remoteOptions,
		log:          logger,
	}
}

// WithSegment creates a new client with an additional scope path segment
// This method can be chained to build complex paths:
// client.WithSegment("deckhouse").WithSegment("ee").WithSegment("modules")
func (c *Client) WithSegment(segments ...string) pkg.RegistryClient {
	for idx, scope := range segments {
		segments[idx] = strings.TrimPrefix(scope, "/")
		segments[idx] = strings.TrimSuffix(scope, "/")
	}

	if len(segments) == 0 {
		return c
	}

	return &Client{
		registryHost: c.registryHost,
		segments:     append(c.segments, segments...),
		options:      c.options,
		log:          c.log,
	}
}

// GetRegistry returns the full registry path (host + scope)
func (c *Client) GetRegistry() string {
	if len(c.segments) == 0 {
		return c.registryHost
	}

	if c.constructedSegments == "" {
		c.constructedSegments = filepath.Join(c.segments...)
	}

	return filepath.Join(c.registryHost, c.constructedSegments)
}

// The repository is determined by the chained WithSegment() calls
func (c *Client) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	fullRegistry := c.GetRegistry()

	logentry := c.log.With(
		slog.String("registry_host", c.registryHost),
		slog.String("scope", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Getting manifest")

	ref, err := name.ParseReference(fmt.Sprintf("%s:%s", fullRegistry, tag))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))

	head, err := remote.Head(ref, opts...)
	if err == nil {
		return &head.Digest, nil
	}

	desc, err := remote.Get(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}

	logentry.Debug("Manifest retrieved successfully")

	return &desc.Digest, nil
}

// GetManifest retrieves the manifest for a specific image tag
// The repository is determined by the chained WithSegment() calls
func (c *Client) GetManifest(ctx context.Context, tag string) ([]byte, error) {
	fullRegistry := c.GetRegistry()

	logentry := c.log.With(
		slog.String("registry_host", c.registryHost),
		slog.String("scope", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Getting manifest")

	ref, err := name.ParseReference(fmt.Sprintf("%s:%s", fullRegistry, tag))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))
	desc, err := remote.Get(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}

	logentry.Debug("Manifest retrieved successfully")

	return desc.Manifest, nil
}

// GetImage retrieves an remote image for a specific reference
// Do not return remote image to avoid drop connection with context cancelation.
// It will be in use while passed context will be alive.
// The repository is determined by the chained WithSegment() calls
func (c *Client) GetImage(ctx context.Context, tag string) (pkg.RegistryImage, error) {
	fullRegistry := c.GetRegistry()

	logentry := c.log.With(
		slog.String("registry_host", c.registryHost),
		slog.String("scope", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Getting image")

	ref, err := name.ParseReference(fmt.Sprintf("%s:%s", fullRegistry, tag))
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))
	img, err := remote.Image(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	logentry.Debug("Image retrieved successfully")

	return &Image{Image: img}, nil
}

// PushImage pushes an image to the registry at the specified tag
// The repository is determined by the chained WithSegment() calls
func (c *Client) PushImage(ctx context.Context, tag string, img pkg.RegistryImage) error {
	fullRegistry := c.GetRegistry()
	logentry := c.log.With(
		slog.String("registry_host", c.registryHost),
		slog.String("scope", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Pushing image")

	ref, err := name.ParseReference(fmt.Sprintf("%s:%s", fullRegistry, tag))
	if err != nil {
		return fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))

	if err := remote.Write(ref, img, opts...); err != nil {
		return fmt.Errorf("failed to push image: %w", err)
	}

	logentry.Debug("Image pushed successfully")

	return nil
}

// GetImageConfig retrieves the image config file containing labels and metadata
// The repository is determined by the chained WithSegment() calls
func (c *Client) GetImageConfig(ctx context.Context, tag string) (*v1.ConfigFile, error) {
	logentry := c.log.With(
		slog.String("registry_host", c.registryHost),
		slog.String("scope", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Getting image config")

	img, err := c.GetImage(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	configFile, err := img.ConfigFile()
	if err != nil {
		return nil, fmt.Errorf("failed to get image config: %w", err)
	}

	logentry.Debug("Image config retrieved successfully")

	return configFile, nil
}

// GetImageLayers retrieves all layers of an image
// The repository is determined by the chained WithSegment() calls
func (c *Client) GetImageLayers(ctx context.Context, tag string) ([]v1.Layer, error) {
	logentry := c.log.With(
		slog.String("registry_host", c.registryHost),
		slog.String("scope", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Getting image layers")

	img, err := c.GetImage(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, fmt.Errorf("failed to get image layers: %w", err)
	}

	logentry.Debug("Image layers retrieved successfully", slog.Int("count", len(layers)))

	return layers, nil
}

// GetLabel retrieves a specific label from remote image metadata
// If you want to get several labels, consider using GetImageConfig to reduce API calls
// The repository is determined by the chained WithSegment() calls
func (c *Client) GetLabel(ctx context.Context, tag, labelKey string) (string, bool, error) {
	logentry := c.log.With(
		slog.String("registry_host", c.registryHost),
		slog.String("scope", c.constructedSegments),
		slog.String("tag", tag),
		slog.String("label", labelKey),
	)

	logentry.Debug("Getting label")

	configFile, err := c.GetImageConfig(ctx, tag)
	if err != nil {
		return "", false, err
	}

	if configFile.Config.Labels == nil {
		logentry.Debug("No labels found in image")
		return "", false, nil
	}

	value, exists := configFile.Config.Labels[labelKey]

	logentry.Debug("Label lookup result", slog.Bool("exists", exists))

	return value, exists, nil
}

// ListTags lists all tags for the current scope
// The repository is determined by the chained WithSegment() calls
func (c *Client) ListTags(ctx context.Context) ([]string, error) {
	fullRegistry := c.GetRegistry()

	logentry := c.log.With(
		slog.String("registry_host", c.registryHost),
		slog.String("scope", c.constructedSegments),
	)

	logentry.Debug("Listing tags")

	ref, err := name.ParseReference(fullRegistry)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	repo := ref.Context()
	opts := append(c.options, remote.WithContext(ctx))

	tags, err := remote.List(repo, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to list tags: %w", err)
	}

	logentry.Debug("Tags listed successfully", slog.Int("count", len(tags)))

	return tags, nil
}

// ListRepositories lists all sub-repositories under the current scope
// The scope is determined by the chained WithSegment() calls
// Returns repository names (tags) under the current scope
func (c *Client) ListRepositories(ctx context.Context) ([]string, error) {
	fullRegistry := c.GetRegistry()

	logentry := c.log.With(
		slog.String("registry_host", c.registryHost),
		slog.String("scope", c.constructedSegments),
	)

	logentry.Debug("Listing repositories")

	// Use the current scope path to list sub-repositories
	// For example, if scope is "deckhouse/ee/modules"
	// this will list all tags/sub-paths under that repository
	ref, err := name.ParseReference(fullRegistry)
	if err != nil {
		return nil, fmt.Errorf("failed to parse registry reference: %w", err)
	}

	repo := ref.Context()
	logentry.Debug("Listing tags for base repository", slog.String("repository", repo.String()))

	opts := append(c.options, remote.WithContext(ctx))

	// List "tags" which actually represent sub-repositories in this case
	tags, err := remote.List(repo, opts...)
	if err != nil {
		logentry.Debug("Failed to list repository tags", slog.String("error", err.Error()))
		return nil, fmt.Errorf("failed to list repositories: %w", err)
	}

	logentry.Debug("Repositories listed successfully", slog.Int("total", len(tags)))

	return tags, nil
}
