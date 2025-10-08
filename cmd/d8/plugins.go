/*
Copyright 2024 Flant JSC

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

	d8flags "github.com/deckhouse/deckhouse-cli/cmd/d8/flags"
	"github.com/deckhouse/deckhouse-cli/pkg/registry"
	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"

	intplugins "github.com/deckhouse/deckhouse-cli/internal/plugins"
)

func (r *RootCommand) initPluginServices() {
	r.logger.Debug("Initializing plugin services")

	// Use the full source registry repo path for plugins
	// This includes the ee/modules path which is needed for catalog API access
	pluginRegistry := d8flags.SourceRegistryRepo

	auth := getPluginRegistryAuthProvider(d8flags.SourceRegistryRepo, r.logger)

	r.logger.Debug("Creating plugin registry client",
		slog.String("registry", pluginRegistry),
		slog.Bool("insecure", d8flags.Insecure),
		slog.Bool("tls_skip_verify", d8flags.TLSSkipVerify))

	r.pluginRegistryClient = registry.NewClientWithOptions(&registry.ClientOptions{
		// TODO: change postfix to new registry
		Registry:      pluginRegistry + "/modules",
		Auth:          auth,
		Insecure:      d8flags.Insecure,
		TLSSkipVerify: d8flags.TLSSkipVerify,
		Logger:        r.logger.Named("registry-client"),
	})

	r.logger.Debug("Creating plugin service")
	r.pluginService = intplugins.NewPluginService(
		r.pluginRegistryClient,
		r.logger.Named("plugin-service"),
	)

	r.logger.Debug("Plugin services initialized successfully")
}

func getPluginRegistryAuthProvider(registryHost string, logger *dkplog.Logger) authn.Authenticator {
	// Priority 1: Explicit username/password from flags
	if d8flags.SourceRegistryLogin != "" {
		logger.Info("Using explicit credentials from flags",
			slog.String("username", d8flags.SourceRegistryLogin))
		return authn.FromConfig(authn.AuthConfig{
			Username: d8flags.SourceRegistryLogin,
			Password: d8flags.SourceRegistryPassword,
		})
	}

	// Priority 2: License token from flags
	if d8flags.DeckhouseLicenseToken != "" {
		logger.Info("Using license token from flags")
		return authn.FromConfig(authn.AuthConfig{
			Username: "license-token",
			Password: d8flags.DeckhouseLicenseToken,
		})
	}

	// Priority 3: Try to get credentials from Docker config (~/.docker/config.json)
	// This will automatically use credentials corresponding to the registry URL
	// Parse the registry host from the full repository path (e.g., "registry.deckhouse.io/deckhouse/ee" -> "registry.deckhouse.io")
	ref, err := name.ParseReference(registryHost)
	if err == nil {
		reg := ref.Context().Registry
		auth, err := authn.DefaultKeychain.Resolve(reg)
		if err == nil && auth != authn.Anonymous {
			// Verify that auth is not anonymous by trying to get the config
			cfg, err := auth.Authorization()
			if err == nil && (cfg.Username != "" || cfg.Password != "" || cfg.Auth != "" || cfg.IdentityToken != "") {
				logger.Info("Using credentials from Docker config",
					slog.String("registry", reg.String()))
				return auth
			}
		}
	}

	// Priority 4: Anonymous access
	logger.Info("Using anonymous access for registry",
		slog.String("registry", registryHost))
	return authn.Anonymous
}
