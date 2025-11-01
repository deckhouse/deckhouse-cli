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
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"

	"github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg"
)

var ErrImageNotFound = errors.New("image not found")

// Client provides methods to interact with container registries
type Client struct {
	registryHost            string    // e.g., "registry.deckhouse.io"
	segments                []string  // e.g., [deckhouse,ee,modules] (built from chained WithSegment calls)
	constructedSegments     string    // cached joined segments for scope path
	constructedSegmentsOnce sync.Once // ensures constructedSegments is computed only once
	options                 []remote.Option
	logger                  *log.Logger
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
		logger:       logger,
	}
}

// WithSegment creates a new client with an additional scope path segment
// This method can be chained to build complex paths:
// client.WithSegment("deckhouse").WithSegment("ee").WithSegment("modules")
func (c *Client) WithSegment(segments ...string) pkg.RegistryClient {
	for idx, scope := range segments {
		segments[idx] = strings.TrimSuffix(strings.TrimPrefix(scope, "/"), "/")
	}

	if len(segments) == 0 {
		return c
	}

	return &Client{
		registryHost: c.registryHost,
		segments:     append(append([]string(nil), c.segments...), segments...),
		options:      c.options,
		logger:       c.logger,
	}
}

// GetRegistry returns the full registry path (host + scope)
func (c *Client) GetRegistry() string {
	if len(c.segments) == 0 {
		return c.registryHost
	}

	c.constructedSegmentsOnce.Do(func() {
		c.constructedSegments = path.Join(c.segments...)
	})

	return path.Join(c.registryHost, c.constructedSegments)
}

// The repository is determined by the chained WithSegment() calls
func (c *Client) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	fullRegistry := c.GetRegistry()

	logentry := c.logger.With(
		slog.String("registry_host", c.registryHost),
		slog.String("segments", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Getting manifest")

	ref, err := name.ParseReference(fullRegistry + ":" + tag)
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

	logentry := c.logger.With(
		slog.String("registry_host", c.registryHost),
		slog.String("segments", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Getting manifest")

	ref, err := name.ParseReference(fullRegistry + ":" + tag)
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

	logentry := c.logger.With(
		slog.String("registry_host", c.registryHost),
		slog.String("segments", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Getting image")

	imagepath := fullRegistry + ":" + tag
	if strings.Contains(tag, "@sha256:") {
		logentry.Debug("tag contains digest reference")
		imagepath = fullRegistry + tag
	}

	ref, err := name.ParseReference(imagepath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))
	img, err := remote.Image(ref, opts...)
	if err != nil {
		var transportErr *transport.Error
		if errors.As(err, &transportErr) && transportErr.StatusCode == 404 {
			// Image not found, which is expected for non-vulnerable images
			return nil, fmt.Errorf("%w: %w", err, ErrImageNotFound)
		}

		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	logentry.Debug("Image retrieved successfully")

	digest, err := img.Digest()
	if err != nil {
		return nil, fmt.Errorf("failed to get image digest: %w", err)
	}

	imgOpts := []ImageOption{}

	imgOpts = append(imgOpts, WithTagReference(ref.String()))
	imgOpts = append(imgOpts, WithDigestReference(fullRegistry+digest.String()))

	return NewImage(img, imgOpts...), nil
}

// PushImage pushes an image to the registry at the specified tag
// The repository is determined by the chained WithSegment() calls
func (c *Client) PushImage(ctx context.Context, tag string, img pkg.RegistryImage) error {
	fullRegistry := c.GetRegistry()

	logentry := c.logger.With(
		slog.String("registry_host", c.registryHost),
		slog.String("segments", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Pushing image")

	ref, err := name.ParseReference(fullRegistry + ":" + tag)
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
	_ = c.GetRegistry()

	logentry := c.logger.With(
		slog.String("registry_host", c.registryHost),
		slog.String("segments", c.constructedSegments),
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

// ListTags lists all tags for the current scope
// The repository is determined by the chained WithSegment() calls
func (c *Client) ListTags(ctx context.Context) ([]string, error) {
	fullRegistry := c.GetRegistry()

	logentry := c.logger.With(
		slog.String("registry_host", c.registryHost),
		slog.String("segments", c.constructedSegments),
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

	logentry := c.logger.With(
		slog.String("registry_host", c.registryHost),
		slog.String("segments", c.constructedSegments),
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

// CheckImageExists checks if a specific image exists in the registry
// If image not found, return an error
// The repository is determined by the chained WithSegment() calls
func (c *Client) CheckImageExists(ctx context.Context, tag string) error {
	fullRegistry := c.GetRegistry()

	logentry := c.logger.With(
		slog.String("registry_host", c.registryHost),
		slog.String("segments", c.constructedSegments),
		slog.String("tag", tag),
	)

	logentry.Debug("Checking if image exists")

	ref, err := name.ParseReference(fullRegistry + ":" + tag)
	if err != nil {
		return fmt.Errorf("failed to parse reference: %w", err)
	}

	opts := append(c.options, remote.WithContext(ctx))

	_, err = remote.Head(ref, opts...)
	if err != nil {
		var transportErr *transport.Error
		if errors.As(err, &transportErr) && transportErr.StatusCode == 404 {
			// Image not found, which is expected for non-vulnerable images
			return ErrImageNotFound
		}

		logentry.Debug("get Head error", log.Err(err))
	}

	if err != nil {
		_, err = remote.Get(ref, opts...)
	}

	if err != nil {
		var transportErr *transport.Error
		if errors.As(err, &transportErr) && transportErr.StatusCode == 404 {
			// Image not found, which is expected for non-vulnerable images
			return ErrImageNotFound
		}

		logentry.Debug("get Get error", log.Err(err))

		return err
	}

	logentry.Debug("Image exists")

	return nil
}
