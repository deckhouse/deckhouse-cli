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

package pkg

import (
	"context"
	"io"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type RegistryImage interface {
	v1.Image
	Extract() io.ReadCloser
	GetTagReference() string
}

// RegistryClient defines the contract for interacting with container registries
type RegistryClient interface {
	// WithSegment creates a new client with an additional scope path segment
	// This method can be chained to build complex paths
	WithSegment(segments ...string) RegistryClient

	// GetRegistry returns the full registry path (host + scope)
	GetRegistry() string

	// GetDigest retrieves the digest for a specific image tag
	// The repository is determined by the chained WithSegment() calls
	GetDigest(ctx context.Context, tag string) (*v1.Hash, error)

	// GetManifest retrieves the manifest for a specific image tag
	// The repository is determined by the chained WithSegment() calls
	GetManifest(ctx context.Context, tag string) ([]byte, error)

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
	GetImage(ctx context.Context, tag string) (RegistryImage, error)

	// PushImage pushes an image to the registry at the specified tag
	// The repository is determined by the chained WithSegment() calls
	PushImage(ctx context.Context, tag string, img RegistryImage) error

	// ListTags retrieves all available tags for the current scope
	// The repository is determined by the chained WithSegment() calls
	ListTags(ctx context.Context) ([]string, error)

	// ListRepositories retrieves all sub-repositories under the current scope
	// The scope is determined by the chained WithSegment() calls
	ListRepositories(ctx context.Context) ([]string, error)
}
