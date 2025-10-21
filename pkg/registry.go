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
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// RegistryClient defines the contract for interacting with container registries
type RegistryClient interface {
	// WithScope creates a new client with an additional scope path segment
	// This method can be chained to build complex paths
	WithScope(scope string) RegistryClient

	// GetRegistry returns the full registry path (host + scope)
	GetRegistry() string

	// GetManifest retrieves the manifest for a specific image tag
	// The repository is determined by the chained WithScope() calls
	GetManifest(ctx context.Context, tag string) (*remote.Descriptor, error)

	// GetImage retrieves an image for a specific reference
	// The repository is determined by the chained WithScope() calls
	GetImage(ctx context.Context, tag string) (v1.Image, error)

	// GetImageConfig retrieves the image config file containing labels and metadata
	// The repository is determined by the chained WithScope() calls
	GetImageConfig(ctx context.Context, tag string) (*v1.ConfigFile, error)

	// GetImageLayers retrieves all layers of an image
	// The repository is determined by the chained WithScope() calls
	GetImageLayers(ctx context.Context, tag string) ([]v1.Layer, error)

	// GetLabel retrieves a specific label from image metadata
	// The repository is determined by the chained WithScope() calls
	GetLabel(ctx context.Context, tag, labelKey string) (string, bool, error)

	// ExtractImageLayers retrieves uncompressed layer streams for extraction
	// The repository is determined by the chained WithScope() calls
	ExtractImageLayers(ctx context.Context, tag string, handler func(LayerStream) error) error

	// ListTags retrieves all available tags for the current scope
	// The repository is determined by the chained WithScope() calls
	ListTags(ctx context.Context) ([]string, error)

	// ListRepositories retrieves all sub-repositories under the current scope
	// The scope is determined by the chained WithScope() calls
	ListRepositories(ctx context.Context) ([]string, error)

	// PushImage pushes an image to the registry at the specified tag
	// The repository is determined by the chained WithScope() calls
	PushImage(ctx context.Context, tag string, img v1.Image) error
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
