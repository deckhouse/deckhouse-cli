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
	"github.com/Masterminds/semver/v3"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/modules"
)

// PullConfig contains all configuration for the pull operation
type PullOpts struct {
	// WorkingDir is the temporary directory for intermediate files
	WorkingDir string
	// BundleDir is the directory to store the final bundle
	BundleDir string
	// BundleChunkSize is the max size of bundle chunks in bytes (0 = no chunking)
	BundleChunkSize int64

	// SkipPlatform skips pulling platform images
	SkipPlatform bool
	// SkipModules skips pulling module images
	SkipModules bool
	// SkipSecurity skips pulling security databases
	SkipSecurity bool

	// TargetTag specifies a specific tag to mirror instead of automatic version detection
	TargetTag string
	// SinceVersion specifies the minimum version to start mirroring from
	SinceVersion *semver.Version

	// ModuleFilter is the filter for module selection (whitelist/blacklist)
	ModuleFilter *modules.Filter
	// OnlyExtraImages pulls only extra images for modules (without main module images)
	OnlyExtraImages bool

	// DoGOSTDigests enables GOST digest calculation for bundles
	DoGOSTDigests bool
}

// PlatformOpts contains configuration specific to platform operations
type PlatformOpts struct {
	// TargetTag specifies a specific tag to mirror
	TargetTag string
	// SinceVersion specifies the minimum version
	SinceVersion *semver.Version
	// BundleDir is the directory to store the bundle
	BundleDir string
	// BundleChunkSize is the max chunk size
	BundleChunkSize int64
}

// ModulesOpts contains configuration specific to modules operations
type ModulesOpts struct {
	// Filter is the module filter
	Filter *modules.Filter
	// OnlyExtraImages pulls only extra images
	OnlyExtraImages bool
	// BundleDir is the directory to store the bundle
	BundleDir string
	// BundleChunkSize is the max chunk size
	BundleChunkSize int64
}

// SecurityOpts contains configuration specific to security operations
type SecurityOpts struct {
	// BundleDir is the directory to store the bundle
	BundleDir string
	// BundleChunkSize is the max chunk size
	BundleChunkSize int64
}

// NewPlatformOpts creates PlatformOpts from PullOpts
func (c *PullOpts) NewPlatformOpts() *PlatformOpts {
	return &PlatformOpts{
		TargetTag:       c.TargetTag,
		SinceVersion:    c.SinceVersion,
		BundleDir:       c.BundleDir,
		BundleChunkSize: c.BundleChunkSize,
	}
}

// NewModulesOpts creates ModulesOpts from PullOpts
func (c *PullOpts) NewModulesOpts() *ModulesOpts {
	return &ModulesOpts{
		Filter:          c.ModuleFilter,
		OnlyExtraImages: c.OnlyExtraImages,
		BundleDir:       c.BundleDir,
		BundleChunkSize: c.BundleChunkSize,
	}
}

// NewSecurityOpts creates SecurityOpts from PullOpts
func (c *PullOpts) NewSecurityOpts() *SecurityOpts {
	return &SecurityOpts{
		BundleDir:       c.BundleDir,
		BundleChunkSize: c.BundleChunkSize,
	}
}

// RegistryOpts contains common registry configuration
type RegistryOpts struct {
	// Host is the registry host (e.g., "registry.example.com")
	Host string
	// Path is the base path in the registry (e.g., "deckhouse")
	Path string
	// Insecure allows HTTP connections
	Insecure bool
	// SkipTLSVerify skips TLS certificate verification
	SkipTLSVerify bool
	// Username for authentication
	Username string
	// Password for authentication
	Password string
}
