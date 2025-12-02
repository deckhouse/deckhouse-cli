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

package push

import (
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/authn"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
)

// Config holds all configuration for the push command
type Config struct {
	// Registry configuration
	Registry RegistryConfig

	// Bundle configuration
	BundleDir string

	// Working directory for temporary files
	WorkingDir string

	// ModulesPathSuffix is the path suffix for modules
	ModulesPathSuffix string

	// Parallelism configuration
	BlobParallelism  int
	ImageParallelism int
}

// RegistryConfig holds registry-related configuration
type RegistryConfig struct {
	Host          string
	Path          string
	Insecure      bool
	SkipTLSVerify bool
	Auth          authn.Authenticator
}

// NewConfigFromFlags creates Config from CLI flags
func NewConfigFromFlags() *Config {
	var auth authn.Authenticator
	if RegistryUsername != "" {
		auth = authn.FromConfig(authn.AuthConfig{
			Username: RegistryUsername,
			Password: RegistryPassword,
		})
	}

	return &Config{
		Registry: RegistryConfig{
			Host:          RegistryHost,
			Path:          RegistryPath,
			Insecure:      Insecure,
			SkipTLSVerify: TLSSkipVerify,
			Auth:          auth,
		},

		BundleDir:         ImagesBundlePath,
		WorkingDir:        filepath.Join(TempDir, "push"),
		ModulesPathSuffix: ModulesPathSuffix,

		BlobParallelism:  4,
		ImageParallelism: 1,
	}
}

// ToPushOpts converts Config to usecase.PushOpts
func (c *Config) ToPushOpts() *usecase.PushOpts {
	return &usecase.PushOpts{
		BundleDir:         c.BundleDir,
		WorkingDir:        c.WorkingDir,
		RegistryHost:      c.Registry.Host,
		RegistryPath:      c.Registry.Path,
		ModulesPathSuffix: c.ModulesPathSuffix,
		BlobParallelism:   c.BlobParallelism,
		ImageParallelism:  c.ImageParallelism,
	}
}

