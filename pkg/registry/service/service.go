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

	"github.com/deckhouse/deckhouse-cli/pkg"
)

// Service provides high-level registry operations using a registry client
type Service struct {
	client pkg.RegistryClient

	moduleService    *ModuleService
	pluginService    *PluginService
	deckhouseService *DeckhouseService

	logger *log.Logger
}

// NewService creates a new registry service with the given client and logger
func NewService(client pkg.RegistryClient, logger *log.Logger) *Service {
	s := &Service{
		client: client,
		logger: logger,
	}

	s.moduleService = NewModuleService(client.WithSegment("modules"), logger.Named("modules"))
	s.pluginService = NewPluginService(client.WithSegment("modules"), logger.Named("plugins"))
	s.deckhouseService = NewDeckhouseService(client, logger.Named("deckhouse"))

	return s
}

// ModuleService returns the module service
func (s *Service) ModuleService() *ModuleService {
	return s.moduleService
}

// PluginService returns the plugin service
func (s *Service) PluginService() *PluginService {
	return s.pluginService
}

// DeckhouseService returns the deckhouse service
func (s *Service) DeckhouseService() *DeckhouseService {
	return s.deckhouseService
}
