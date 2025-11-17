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

package plugins

import (
	"log/slog"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	d8flags "github.com/deckhouse/deckhouse-cli/cmd/plugins/flags"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/client"
	"github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

func (pc *PluginsCommand) initPluginServices() {
	pc.logger.Debug("Initializing plugin services")

	// Extract registry host from the source registry repo
	// SourceRegistryRepo can be:
	// - Just hostname: "registry.deckhouse.io"
	// - Full path: "registry.deckhouse.io/deckhouse/ee/plugins"
	sourceRepo := d8flags.SourcePluginRegistryRepo
	registryHost := sourceRepo

	// If it's just a hostname (no slashes), use it directly
	// Otherwise parse to extract the hostname
	if containsSlash(registryHost) {
		// Has path components, parse to extract registry
		ref, err := name.ParseReference(registryHost)
		if err == nil {
			registryHost = ref.Context().RegistryStr()
			pc.logger.Debug("Extracted registry from path",
				slog.String("extracted", registryHost))
		}
	}

	auth := getPluginRegistryAuthProvider(registryHost, pc.logger)

	pc.logger.Debug("Creating plugin registry client",
		slog.String("registry_host", registryHost),
		slog.Bool("insecure", d8flags.Insecure),
		slog.Bool("tls_skip_verify", d8flags.TLSSkipVerify))

	// Create base client with registry host only
	pc.pluginRegistryClient = client.NewClientWithOptions(sourceRepo, &client.Options{
		Auth:          auth,
		Insecure:      d8flags.Insecure,
		TLSSkipVerify: d8flags.TLSSkipVerify,
		Logger:        pc.logger.Named("registry-client"),
	})

	pc.logger.Debug("Creating plugin service with scoped client",
		slog.String("scope_path", strings.TrimPrefix(sourceRepo, sourceRepo)))

	registryService := service.NewService(
		pc.pluginRegistryClient,
		pc.logger.Named("registry-service"),
	)

	pc.service = registryService.PluginService()

	pc.logger.Debug("Plugin services initialized successfully")
}

// containsSlash checks if a string contains a forward slash
func containsSlash(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return true
		}
	}
	return false
}

func getPluginRegistryAuthProvider(registryHost string, logger *dkplog.Logger) authn.Authenticator {
	// Priority 1: Explicit username/password from flags
	if d8flags.SourceRegistryLogin != "" {
		logger.Debug("Using explicit credentials from flags",
			slog.String("username", d8flags.SourceRegistryLogin))
		return authn.FromConfig(authn.AuthConfig{
			Username: d8flags.SourceRegistryLogin,
			Password: d8flags.SourceRegistryPassword,
		})
	}

	// Priority 2: License token from flags
	if d8flags.DeckhouseLicenseToken != "" {
		logger.Debug("Using license token from flags")
		return authn.FromConfig(authn.AuthConfig{
			Username: "license-token",
			Password: d8flags.DeckhouseLicenseToken,
		})
	}

	// Priority 3: Try to get credentials from Docker config (~/.docker/config.json)
	// Parse the registry hostname to create a Registry object for DefaultKeychain
	var reg name.Registry
	var err error

	// If registryHost contains a slash, it might be a full path - extract just the host
	if containsSlash(registryHost) {
		ref, parseErr := name.ParseReference(registryHost)
		if parseErr == nil {
			reg = ref.Context().Registry
		} else {
			// Fallback: just use the part before the first slash
			idx := 0
			for i := 0; i < len(registryHost); i++ {
				if registryHost[i] == '/' {
					idx = i
					break
				}
			}
			reg, err = name.NewRegistry(registryHost[:idx])
		}
	} else {
		// Just a hostname, parse it directly
		reg, err = name.NewRegistry(registryHost)
	}

	if err == nil {
		auth, err := authn.DefaultKeychain.Resolve(reg)
		if err == nil && auth != authn.Anonymous {
			// Verify that auth is not anonymous by trying to get the config
			cfg, err := auth.Authorization()
			if err == nil && (cfg.Username != "" || cfg.Password != "" || cfg.Auth != "" || cfg.IdentityToken != "") {
				logger.Debug("Using credentials from Docker config",
					slog.String("registry", reg.String()))
				return auth
			}
		}
	}

	// Priority 4: Anonymous access
	logger.Debug("Using anonymous access for registry",
		slog.String("registry", registryHost))
	return authn.Anonymous
}
