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

package pull

import (
	"github.com/Masterminds/semver/v3"
	"github.com/google/go-containerregistry/pkg/authn"

	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
	libmodules "github.com/deckhouse/deckhouse-cli/pkg/libmirror/modules"
)

// Config holds all configuration for the pull command
type Config struct {
	// Registry configuration
	Registry RegistryConfig

	// Bundle configuration
	BundleDir       string
	BundleChunkSize int64

	// Working directory for temporary files
	WorkingDir string

	// Skip flags
	SkipPlatform bool
	SkipModules  bool
	SkipSecurity bool

	// Version selection
	TargetTag    string
	SinceVersion *semver.Version

	// Module filtering
	ModuleFilter    *libmodules.Filter
	OnlyExtraImages bool

	// Additional options
	DoGOSTDigests bool
}

// RegistryConfig holds registry-related configuration
type RegistryConfig struct {
	URL           string
	Insecure      bool
	SkipTLSVerify bool
	Auth          authn.Authenticator
}

// NewConfigFromFlags creates Config from CLI flags
func NewConfigFromFlags() (*Config, error) {
	// Create module filter
	var moduleFilter *libmodules.Filter
	var err error

	if pullflags.ModulesWhitelist != nil {
		moduleFilter, err = libmodules.NewFilter(pullflags.ModulesWhitelist, libmodules.FilterTypeWhitelist)
	} else {
		moduleFilter, err = libmodules.NewFilter(pullflags.ModulesBlacklist, libmodules.FilterTypeBlacklist)
	}
	if err != nil {
		return nil, err
	}

	return &Config{
		Registry: RegistryConfig{
			URL:           pullflags.SourceRegistryRepo,
			Insecure:      pullflags.Insecure,
			SkipTLSVerify: pullflags.TLSSkipVerify,
			Auth:          getAuthProvider(),
		},

		BundleDir:       pullflags.ImagesBundlePath,
		BundleChunkSize: pullflags.ImagesBundleChunkSizeGB * 1000 * 1000 * 1000,
		WorkingDir:      pullflags.TempDir,

		SkipPlatform: pullflags.NoPlatform,
		SkipModules:  pullflags.NoModules,
		SkipSecurity: pullflags.NoSecurityDB,

		TargetTag:    pullflags.DeckhouseTag,
		SinceVersion: pullflags.SinceVersion,

		ModuleFilter:    moduleFilter,
		OnlyExtraImages: pullflags.OnlyExtraImages,

		DoGOSTDigests: pullflags.DoGOSTDigest,
	}, nil
}

// ToPullOpts converts Config to usecase.PullOpts
func (c *Config) ToPullOpts() *usecase.PullOpts {
	return &usecase.PullOpts{
		WorkingDir:      c.WorkingDir,
		BundleDir:       c.BundleDir,
		BundleChunkSize: c.BundleChunkSize,

		SkipPlatform: c.SkipPlatform,
		SkipModules:  c.SkipModules,
		SkipSecurity: c.SkipSecurity,

		TargetTag:    c.TargetTag,
		SinceVersion: c.SinceVersion,

		ModuleFilter:    c.ModuleFilter,
		OnlyExtraImages: c.OnlyExtraImages,

		DoGOSTDigests: c.DoGOSTDigests,
	}
}

func getAuthProvider() authn.Authenticator {
	if pullflags.SourceRegistryLogin != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: pullflags.SourceRegistryLogin,
			Password: pullflags.SourceRegistryPassword,
		})
	}

	if pullflags.DeckhouseLicenseToken != "" {
		return authn.FromConfig(authn.AuthConfig{
			Username: "license-token",
			Password: pullflags.DeckhouseLicenseToken,
		})
	}

	return authn.Anonymous
}

