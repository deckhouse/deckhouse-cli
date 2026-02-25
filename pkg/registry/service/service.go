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

package service

import (
	"strings"

	"github.com/deckhouse/deckhouse-cli/pkg"
	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"
)

const (
	moduleSegment   = "modules"
	pluginSegment   = "plugins"
	securitySegment = "security"

	securityServiceName = "security"
)

// Service provides high-level registry operations using a registry client
type Service struct {
	client registry.Client

	modulesService   *ModulesService
	pluginService    *PluginService
	deckhouseService *DeckhouseService
	security         *SecurityServices
	installer        *InstallerServices

	logger *log.Logger
}

// NewService creates a new registry service with the given client and logger
func NewService(client registry.Client, edition pkg.Edition, logger *log.Logger) *Service {
	s := &Service{
		client: client,
		logger: logger,
	}

	s.modulesService = NewModulesService(client.WithSegment(edition.String(), moduleSegment), logger.Named("modules"))
	s.deckhouseService = NewDeckhouseService(client.WithSegment(edition.String()), logger.Named("deckhouse"))
	s.security = NewSecurityServices(securityServiceName, client.WithSegment(edition.String(), securitySegment), logger.Named("security"))

	// services that are not scoped by edition
	s.pluginService = NewPluginService(client.WithSegment(pluginSegment), logger.Named("plugins"))
	s.installer = NewInstallerServices(installerServiceName, client.WithSegment("installer"), logger.Named("installer"))

	return s
}

// GetRoot gets path of the registry root
func (s *Service) GetRoot() string {
	return s.client.GetRegistry()
}

// ModuleService returns the module service
func (s *Service) ModuleService() *ModulesService {
	return s.modulesService
}

// PluginService returns the plugin service
func (s *Service) PluginService() *PluginService {
	return s.pluginService
}

// DeckhouseService returns the deckhouse service
func (s *Service) DeckhouseService() *DeckhouseService {
	return s.deckhouseService
}

func (s *Service) Security() *SecurityServices {
	return s.security
}

func (s *Service) InstallerService() *InstallerServices {
	return s.installer
}

// GetEditionFromRegistryPath cuts the edition from the registry path
// returns the path without the edition and the edition
// this is needed because of the different paths for the installer images in the registry
// example:
// registry.deckhouse.ru/deckhouse/ee/ -> registry.deckhouse.ru/deckhouse, ee
// myregistry.ru/deckhouse/ -> myregistry.ru/deckhouse, ""
func GetEditionFromRegistryPath(registryRepo string) (string, pkg.Edition) {
	// strip last slash
	registry := strings.TrimSuffix(registryRepo, "/")

	// split by /
	parts := strings.Split(registry, "/")

	// get last element
	lastPart := parts[len(parts)-1]

	// if not edition, return registry + /installer
	edition := pkg.Edition(lastPart)
	if !edition.IsValid() {
		return registry, pkg.NoEdition
	}

	// cut edition
	newparts := parts[:len(parts)-1]

	// join back together
	newregistry := strings.Join(newparts, "/")

	return newregistry, edition
}
