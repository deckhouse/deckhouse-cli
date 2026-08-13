/*
Copyright 2026 Flant JSC

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

package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/deckhouse/deckhouse/pkg/log"
	client "github.com/deckhouse/deckhouse/pkg/registry"
)

// CLI plugins are standalone d8 subcommand binaries published as OCI images
// under the deckhouse-cli/plugins catalog at the bare registry root, outside
// the edition segment (like the installer). The catalog is a directory-as-tags
// index: ListTags on deckhouse-cli/plugins returns plugin names, ListTags on
// deckhouse-cli/plugins/<name> returns its published versions.
const (
	deckhouseCLISegment = "deckhouse-cli"
	pluginsSegment      = "plugins"

	pluginsServiceName = "plugins"
	pluginServiceName  = "plugin"

	// pluginContractAnnotation carries the plugin contract as base64 JSON on
	// the image manifest (or on the index / its children for multi-platform
	// plugins). Same convention as the internal/plugins sources.
	pluginContractAnnotation = "contract"
)

// PluginsService is scoped to the plugins catalog
// (<root>/deckhouse-cli/plugins). ListTags on it enumerates plugin names.
type PluginsService struct {
	client client.Client

	*BasicService

	services map[string]*PluginService

	logger *log.Logger
}

// NewPluginsService creates a new plugins catalog service.
func NewPluginsService(client client.Client, logger *log.Logger) *PluginsService {
	return &PluginsService{
		client: client,

		BasicService: NewBasicService(pluginsServiceName, client, logger),
		services:     make(map[string]*PluginService),

		logger: logger,
	}
}

// Plugin returns the service scoped to one plugin repository
// (deckhouse-cli/plugins/<name>).
func (s *PluginsService) Plugin(pluginName string) *PluginService {
	if s.services == nil {
		s.services = make(map[string]*PluginService)
	}

	if _, exists := s.services[pluginName]; !exists {
		s.services[pluginName] = NewPluginService(s.client.WithSegment(pluginName), s.logger)
	}

	return s.services[pluginName]
}

// GetRoot returns the full registry path of the plugins catalog.
func (s *PluginsService) GetRoot() string {
	return s.client.GetRegistry()
}

// PluginService provides operations for a single plugin repository. ListTags
// returns the plugin's published versions.
type PluginService struct {
	client client.Client

	*BasicService

	logger *log.Logger
}

// NewPluginService creates a service for a single plugin repository.
func NewPluginService(client client.Client, logger *log.Logger) *PluginService {
	return &PluginService{
		client: client,

		BasicService: NewBasicService(pluginServiceName, client, logger),

		logger: logger,
	}
}

// GetRoot returns the full registry path of the plugin repository.
func (s *PluginService) GetRoot() string {
	return s.client.GetRegistry()
}

// GetManifest returns the raw manifest structure for tag. A multi-platform
// plugin resolves to an index whose children carry per-platform manifests.
func (s *PluginService) GetManifest(ctx context.Context, tag string) (client.ManifestResult, error) {
	logger := s.logger.With(slog.String("service", pluginServiceName), slog.String("tag", tag))

	logger.Debug("Getting manifest")

	result, err := s.client.GetManifest(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get manifest: %w", err)
	}

	logger.Debug("Manifest retrieved successfully")

	return result, nil
}

// ContractAnnotation returns the base64-encoded plugin contract for tag. The
// contract may sit on the index or on its (identical) child manifests: the
// index is read first, and the first child is followed only when the index
// carries none. An empty string means the image ships no contract.
func (s *PluginService) ContractAnnotation(ctx context.Context, tag string) (string, error) {
	result, err := s.GetManifest(ctx, tag)
	if err != nil {
		return "", err
	}

	if !result.GetMediaType().IsIndex() {
		man, err := result.GetManifest()
		if err != nil {
			return "", fmt.Errorf("read manifest: %w", err)
		}

		return man.GetAnnotations()[pluginContractAnnotation], nil
	}

	index, err := result.GetIndexManifest()
	if err != nil {
		return "", fmt.Errorf("read index manifest: %w", err)
	}

	if encoded := index.GetAnnotations()[pluginContractAnnotation]; encoded != "" {
		return encoded, nil
	}

	children := index.GetManifests()
	if len(children) == 0 {
		return "", nil
	}

	child, err := s.GetManifest(ctx, "@"+children[0].GetDigest().String())
	if err != nil {
		return "", fmt.Errorf("get first child manifest: %w", err)
	}

	childManifest, err := child.GetManifest()
	if err != nil {
		return "", fmt.Errorf("read child manifest: %w", err)
	}

	return childManifest.GetAnnotations()[pluginContractAnnotation], nil
}
