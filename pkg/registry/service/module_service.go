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
	moduleReleaseChannelsSegment = "release"
	moduleExtraSegment           = "extra"

	modulesServiceName               = "modules"
	moduleServiceName                = "module"
	moduleReleaseChannelsServiceName = "module_release_channel"
	moduleExtraServiceName           = "module_extra"
)

// ModuleService provides high-level operations for module management
type ModuleService struct {
	client registry.Client

	*BasicService
	moduleReleaseChannels *ModuleReleaseService
	extra                 *BasicService

	logger *log.Logger
}

// NewModuleService creates a new module service
func NewModuleService(client registry.Client, logger *log.Logger) *ModuleService {
	return &ModuleService{
		client: client,

		BasicService:          NewBasicService(moduleServiceName, client, logger),
		moduleReleaseChannels: NewModuleReleaseService(NewBasicService(moduleReleaseChannelsServiceName, client.WithSegment(moduleReleaseChannelsSegment), logger)),
		extra:                 NewBasicService(moduleExtraServiceName, client.WithSegment(moduleExtraSegment), logger),

		logger: logger,
	}
}

func (s *ModuleService) ReleaseChannels() *ModuleReleaseService {
	return s.moduleReleaseChannels
}

func (s *ModuleService) Extra() *BasicService {
	return s.extra
}

type ModulesService struct {
	client registry.Client

	*BasicService

	services map[string]*ModuleService

	logger *log.Logger
}

func NewModulesService(client registry.Client, logger *log.Logger) *ModulesService {
	return &ModulesService{
		client: client,

		BasicService: NewBasicService(modulesServiceName, client, logger),
		services:     make(map[string]*ModuleService),

		logger: logger,
	}
}

func (s *ModulesService) Module(moduleName string) *ModuleService {
	if s.services == nil {
		s.services = make(map[string]*ModuleService)
	}

	if _, exists := s.services[moduleName]; !exists {
		moduleClient := s.client.WithSegment(moduleName)
		s.services[moduleName] = NewModuleService(moduleClient, s.logger)
	}

	return s.services[moduleName]
}

type ModuleReleaseService struct {
	*BasicService
}

func NewModuleReleaseService(basicService *BasicService) *ModuleReleaseService {
	return &ModuleReleaseService{
		BasicService: basicService,
	}
}
