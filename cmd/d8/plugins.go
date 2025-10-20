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

package main

import (
	"log/slog"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	d8flags "github.com/deckhouse/deckhouse-cli/cmd/d8/flags"
	"github.com/deckhouse/deckhouse-cli/pkg/registry"
	intplugins "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

const (
	// Registry hostnames
	devRegistryHost        = "dev-registry.deckhouse.io"
	productionRegistryHost = "registry.deckhouse.io"
)

func (r *RootCommand) initPluginServices() {
	r.logger.Debug("Initializing plugin services")

	// Extract registry host from the source registry repo
	// SourceRegistryRepo can be:
	// - Just hostname: "registry.deckhouse.io"
	// - Full path: "registry.deckhouse.io/deckhouse/ee"
	registryHost := d8flags.SourceRegistryRepo

	// If it's just a hostname (no slashes), use it directly
	// Otherwise parse to extract the hostname
	if len(registryHost) > 0 && registryHost[0] != '/' {
		// Try to parse it - if it has a path component, extract the registry
		// We need to add a dummy path to force it to be treated as a registry URL
		testRef := registryHost
		if !containsSlash(registryHost) {
			// Just a hostname, use it as-is
			r.logger.Debug("Using hostname as registry", slog.String("host", registryHost))
		} else {
			// Has path components, parse to extract registry
			ref, err := name.ParseReference(registryHost)
			if err == nil {
				registryHost = ref.Context().RegistryStr()
				r.logger.Debug("Extracted registry from path",
					slog.String("original", testRef),
					slog.String("extracted", registryHost))
			}
		}
	}

	auth := getPluginRegistryAuthProvider(registryHost, r.logger)

	r.logger.Debug("Creating plugin registry client",
		slog.String("registry_host", registryHost),
		slog.Bool("insecure", d8flags.Insecure),
		slog.Bool("tls_skip_verify", d8flags.TLSSkipVerify))

	// Create base client with registry host only
	baseClient := registry.NewClientWithOptions(&registry.ClientOptions{
		RegistryHost:  registryHost,
		Auth:          auth,
		Insecure:      d8flags.Insecure,
		TLSSkipVerify: d8flags.TLSSkipVerify,
		Logger:        r.logger.Named("registry-client"),
	})

	// Build scoped client using dynamic path based on registry
	pluginPath := getPluginRegistryPath(registryHost, r.logger)
	r.pluginRegistryClient = buildScopedClient(baseClient, pluginPath)

	r.logger.Debug("Creating plugin service with scoped client",
		slog.String("scope_path", strings.Join(pluginPath, "/")))

	r.registryService = intplugins.NewService(
		r.pluginRegistryClient,
		r.logger.Named("registry-service"),
	)

	r.logger.Debug("Plugin services initialized successfully")
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

// getPluginRegistryPath determines plugin path based on registry host
func getPluginRegistryPath(registryHost string, logger *dkplog.Logger) []string {
	logger.Debug("Determining plugin registry path",
		slog.String("registry_host", registryHost))

	// Check by hostname
	switch {
	case strings.Contains(registryHost, devRegistryHost):
		logger.Debug("Using dev-registry path")
		return []string{"deckhouse", "foxtrot", "external-modules"}

	case strings.Contains(registryHost, productionRegistryHost):
		logger.Debug("Using production registry path")
		return []string{"deckhouse", "ee", "modules"}

	default:
		// Default for unknown registries
		logger.Debug("Unknown registry, using default production path")
		return []string{"deckhouse", "ee", "modules"}
	}
}

// buildScopedClient builds scoped client from base client and path
func buildScopedClient(baseClient *registry.Client, path []string) *registry.Client {
	var scopedClient *registry.Client = baseClient

	for _, scope := range path {
		// WithScope returns pkg.RegistryClient interface, need type assertion
		scopedClient = scopedClient.WithScope(scope).(*registry.Client)
	}

	return scopedClient
}
