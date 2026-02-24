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

type ServiceOption func(*Service)

func WithEdition(edition string) ServiceOption {
	return func(s *Service) {
		s.client = s.client.WithSegment(edition)
	}
}

// NewService creates a new registry service with the given client and logger
func NewService(client registry.Client, logger *log.Logger, opts ...ServiceOption) *Service {
	s := &Service{
		client: client,
		logger: logger,
	}

	// before options parse to skip edition segment from registry path
	installerClient := client.WithSegment("installer")

	for _, opt := range opts {
		opt(s)
	}

	s.modulesService = NewModulesService(client.WithSegment(moduleSegment), logger.Named("modules"))
	s.pluginService = NewPluginService(client.WithSegment(pluginSegment), logger.Named("plugins"))
	s.deckhouseService = NewDeckhouseService(client, logger.Named("deckhouse"))
	s.security = NewSecurityServices(securityServiceName, client.WithSegment(securitySegment), logger.Named("security"))
	s.installer = NewInstallerServices(installerServiceName, installerClient, logger.Named("installer"))

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
