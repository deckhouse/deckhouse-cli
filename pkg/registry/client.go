package client

import (
	"context"

	"github.com/deckhouse/deckhouse/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// Client defines the contract for interacting with container registries
type Client interface {
	// WithSegment creates a new client with an additional scope path segment
	// This method can be chained to build complex paths
	WithSegment(segments ...string) Client

	// GetRegistry returns the full registry path (host + scope)
	GetRegistry() string

	// GetDigest retrieves the digest for a specific image tag
	// The repository is determined by the chained WithSegment() calls
	GetDigest(ctx context.Context, tag string) (*v1.Hash, error)

	// GetManifest retrieves the manifest for a specific image tag
	// The repository is determined by the chained WithSegment() calls
	GetManifest(ctx context.Context, tag string) (registry.ManifestResult, error)

	// GetImageConfig retrieves the image config file containing labels and metadata
	// The repository is determined by the chained WithSegment() calls
	GetImageConfig(ctx context.Context, tag string) (*v1.ConfigFile, error)

	// CheckImageExists checks if a specific image exists in the registry
	// If image not found, return an error
	// The repository is determined by the chained WithSegment() calls
	CheckImageExists(ctx context.Context, tag string) error

	// GetImage retrieves an remote image for a specific reference
	// Do not return remote image to avoid drop connection with context cancelation.
	// It will be in use while passed context will be alive.
	// The repository is determined by the chained WithSegment() calls
	GetImage(ctx context.Context, tag string, opts ...registry.ImageGetOption) (registry.Image, error)

	// PushImage pushes an image to the registry at the specified tag
	// The repository is determined by the chained WithSegment() calls
	PushImage(ctx context.Context, tag string, img v1.Image, opts ...registry.ImagePushOption) error

	// ListTags retrieves tags for the current scope with pagination
	// The repository is determined by the chained WithSegment() calls
	ListTags(ctx context.Context, opts ...registry.ListTagsOption) ([]string, error)

	// ListRepositories retrieves sub-repositories under the current scope with pagination
	// The scope is determined by the chained WithSegment() calls
	ListRepositories(ctx context.Context, opts ...registry.ListRepositoriesOption) ([]string, error)

	// DeleteTag deletes a specific tag from the repository.
	// Returns ErrImageNotFound if the tag does not exist.
	// The repository is determined by the chained WithSegment() calls.
	DeleteTag(ctx context.Context, tag string) error

	// TagImage adds a new tag pointing to the same manifest as sourceTag without
	// re-uploading any layers (single manifest PUT).
	// Standard promotion pattern: e.g. :latest → :v1.2.3.
	// The repository is determined by the chained WithSegment() calls.
	TagImage(ctx context.Context, sourceTag, destTag string) error
}
