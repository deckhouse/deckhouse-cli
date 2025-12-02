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

package usecase

import (
	"context"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/deckhouse/deckhouse-cli/pkg"
)

// =============================================================================
// Secondary Ports (Output) - interfaces for infrastructure dependencies
// =============================================================================

// ImageGetter provides operations to retrieve images from a registry
type ImageGetter interface {
	// GetImage retrieves an image by tag or digest reference
	GetImage(ctx context.Context, ref string) (pkg.RegistryImage, error)
	// GetDigest retrieves only the digest for a tag
	GetDigest(ctx context.Context, tag string) (*v1.Hash, error)
	// CheckImageExists verifies if an image exists in the registry
	CheckImageExists(ctx context.Context, tag string) error
}

// ImageLister provides operations to list images in a registry
type ImageLister interface {
	// ListTags returns all available tags in the repository
	ListTags(ctx context.Context) ([]string, error)
}

// ImageService combines getter and lister capabilities
type ImageService interface {
	ImageGetter
	ImageLister
}

// DeckhouseRegistryService provides access to Deckhouse-specific registry services
type DeckhouseRegistryService interface {
	// GetRoot returns the base registry URL
	GetRoot() string
	// Deckhouse returns the main Deckhouse image service
	Deckhouse() DeckhouseImageService
	// Modules returns the modules service
	Modules() ModulesRegistryService
	// Security returns the security databases service
	Security() SecurityRegistryService
}

// DeckhouseImageService provides operations for Deckhouse platform images
type DeckhouseImageService interface {
	ImageService
	// ReleaseChannels returns the release channels service
	ReleaseChannels() ReleaseChannelService
	// Installer returns the installer images service
	Installer() ImageService
	// StandaloneInstaller returns the standalone installer images service
	StandaloneInstaller() ImageService
}

// ReleaseChannelService provides operations for release channel images
type ReleaseChannelService interface {
	ImageService
	// GetMetadata retrieves release channel metadata (version, suspend status)
	GetMetadata(ctx context.Context, tag string) (*ReleaseChannelMetadata, error)
}

// ReleaseChannelMetadata contains release channel information
type ReleaseChannelMetadata struct {
	Version string
	Suspend bool
}

// ModulesRegistryService provides operations for Deckhouse modules
type ModulesRegistryService interface {
	ImageLister
	// Module returns a service for a specific module
	Module(name string) ModuleService
}

// ModuleService provides operations for a single module
type ModuleService interface {
	ImageService
	// ReleaseChannels returns the module's release channels service
	ReleaseChannels() ImageService
	// Extra returns the module's extra images service
	Extra() ImageService
}

// SecurityRegistryService provides operations for security databases
type SecurityRegistryService interface {
	// Database returns a service for a specific security database
	Database(name string) ImageService
}

// =============================================================================
// Image Layout Ports - interfaces for OCI image layout operations
// =============================================================================

// ImageLayout provides operations for managing OCI image layouts
type ImageLayout interface {
	// AddImage adds an image to the layout with the specified tag
	AddImage(img pkg.RegistryImage, tag string) error
	// GetImage retrieves an image from the layout by tag
	GetImage(tag string) (pkg.RegistryImage, error)
	// TagImage creates an additional tag for an existing image by digest
	TagImage(digest v1.Hash, tag string) error
}

// =============================================================================
// Bundle Ports - interfaces for bundle operations
// =============================================================================

// BundlePacker packs OCI layouts into tar bundles
type BundlePacker interface {
	// Pack creates a tar bundle from the source directory
	Pack(ctx context.Context, sourceDir, bundleName string) error
}

// =============================================================================
// Logger Port - interface for logging
// =============================================================================

// Logger provides logging capabilities with process tracking
type Logger interface {
	// Info logs an informational message
	Info(msg string)
	// Infof logs a formatted informational message
	Infof(format string, args ...interface{})
	// Warn logs a warning message
	Warn(msg string)
	// Warnf logs a formatted warning message
	Warnf(format string, args ...interface{})
	// Debug logs a debug message
	Debug(msg string)
	// Debugf logs a formatted debug message
	Debugf(format string, args ...interface{})
	// Process wraps an operation with start/end logging
	Process(name string, fn func() error) error
}

// =============================================================================
// Puller Port - interface for image pulling operations
// =============================================================================

// ImagePuller handles pulling images from registry to layout
type ImagePuller interface {
	// PullToLayout pulls images from registry and stores them in the layout
	PullToLayout(ctx context.Context, config PullImageConfig) error
}

// PullImageConfig configures an image pull operation
type PullImageConfig struct {
	// Name is a human-readable name for logging
	Name string
	// ImageRefs is the list of image references to pull
	ImageRefs []string
	// Layout is the destination layout
	Layout ImageLayout
	// Source is the registry service to pull from
	Source ImageGetter
	// AllowMissing allows missing images without error
	AllowMissing bool
}

// =============================================================================
// Adapters - helper types for existing implementations
// =============================================================================

// ImageMeta holds metadata for an image being processed
type ImageMeta struct {
	// Tag is the image tag
	Tag string
	// Digest is the image digest
	Digest *v1.Hash
	// DigestReference is the full digest reference (repo@sha256:...)
	DigestReference string
}

// NewImageMeta creates a new ImageMeta
func NewImageMeta(tag string, digestRef string, digest *v1.Hash) *ImageMeta {
	return &ImageMeta{
		Tag:             tag,
		Digest:          digest,
		DigestReference: digestRef,
	}
}
