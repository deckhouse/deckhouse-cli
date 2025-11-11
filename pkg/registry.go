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

	"github.com/deckhouse/deckhouse-cli/internal/progress"
)

type ImageMeta interface {
	GetTagReference() string
	GetDigestReference() string
	GetDigest() *v1.Hash
}

type ClientImage interface {
	v1.Image
	Extract() io.ReadCloser
}

type RegistryImage interface {
	v1.Image
	Extract() io.ReadCloser
	GetMetadata() (ImageMeta, error)
	SetMetadata(metadata ImageMeta)
}

// ImageGetOption is some configuration that modifies options for a get request.
type ImageGetOption interface {
	// ApplyToImageGet applies this configuration to the given image get options.
	ApplyToImageGet(*ImageGetOptions)
}

type ImageGetOptions struct {
	ProgressBar *progress.DownloadBar
}

// ImagePutOption is some configuration that modifies options for a put request.
type ImagePutOption interface {
	// ApplyToImagePut applies this configuration to the given image put options.
	ApplyToImagePut(*ImagePutOptions)
}

type ImagePutOptions struct {
	ProgressBar *progress.UploadBar
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
	GetImage(ctx context.Context, tag string, opts ...ImageGetOption) (ClientImage, error)

	// PushImage pushes an image to the registry at the specified tag
	// The repository is determined by the chained WithSegment() calls
	PushImage(ctx context.Context, tag string, img v1.Image, opts ...ImagePutOption) error

	// ListTags retrieves all available tags for the current scope
	// The repository is determined by the chained WithSegment() calls
	ListTags(ctx context.Context) ([]string, error)

	// ListRepositories retrieves all sub-repositories under the current scope
	// The scope is determined by the chained WithSegment() calls
	ListRepositories(ctx context.Context) ([]string, error)
}

// BasicService defines common registry operations with standardized logging
type BasicService interface {
	GetImage(ctx context.Context, tag string, opts ...ImageGetOption) (RegistryImage, error)
	GetDigest(ctx context.Context, tag string) (*v1.Hash, error)
	CheckImageExists(ctx context.Context, tag string) error
	ListTags(ctx context.Context) ([]string, error)
}
