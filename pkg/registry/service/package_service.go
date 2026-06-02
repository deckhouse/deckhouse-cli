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
	client "github.com/deckhouse/deckhouse/pkg/registry"
)

// Packages are structurally identical to modules, but they live under the
// packages/ catalog segment and expose their release-channel metadata under a
// version/ segment instead of release/.
const (
	packageVersionChannelsSegment = "version"
	packageExtraSegment           = "extra"

	packagesServiceName               = "packages"
	packageServiceName                = "package"
	packageVersionChannelsServiceName = "package_version_channel"
	packageExtraServiceName           = "package_extra"
)

// PackageService provides high-level operations for a single package.
type PackageService struct {
	client client.Client

	*BasicService
	packageVersionChannels *PackageVersionService
	extra                  *BasicService
	extraImages            map[string]*BasicService

	logger *log.Logger
}

// NewPackageService creates a new package service.
func NewPackageService(client client.Client, logger *log.Logger) *PackageService {
	return &PackageService{
		client: client,

		BasicService:           NewBasicService(packageServiceName, client, logger),
		packageVersionChannels: NewPackageVersionService(NewBasicService(packageVersionChannelsServiceName, client.WithSegment(packageVersionChannelsSegment), logger)),
		extra:                  NewBasicService(packageExtraServiceName, client.WithSegment(packageExtraSegment), logger),
		extraImages:            make(map[string]*BasicService),

		logger: logger,
	}
}

// VersionChannels returns the service scoped to packages/<package>/version.
func (s *PackageService) VersionChannels() *PackageVersionService {
	return s.packageVersionChannels
}

func (s *PackageService) Extra() *BasicService {
	return s.extra
}

// ExtraImage returns a BasicService scoped to a specific extra image (e.g., packages/<package>/extra/<extraName>).
func (s *PackageService) ExtraImage(extraName string) *BasicService {
	if s.extraImages == nil {
		s.extraImages = make(map[string]*BasicService)
	}

	if _, exists := s.extraImages[extraName]; !exists {
		extraClient := s.client.WithSegment(packageExtraSegment).WithSegment(extraName)
		s.extraImages[extraName] = NewBasicService(packageExtraServiceName+"/"+extraName, extraClient, s.logger)
	}

	return s.extraImages[extraName]
}

type PackagesService struct {
	client client.Client

	*BasicService

	services map[string]*PackageService

	logger *log.Logger
}

func NewPackagesService(client client.Client, logger *log.Logger) *PackagesService {
	return &PackagesService{
		client: client,

		BasicService: NewBasicService(packagesServiceName, client, logger),
		services:     make(map[string]*PackageService),

		logger: logger,
	}
}

func (s *PackagesService) Package(packageName string) *PackageService {
	if s.services == nil {
		s.services = make(map[string]*PackageService)
	}

	if _, exists := s.services[packageName]; !exists {
		packageClient := s.client.WithSegment(packageName)
		s.services[packageName] = NewPackageService(packageClient, s.logger)
	}

	return s.services[packageName]
}

type PackageVersionService struct {
	*BasicService
}

func NewPackageVersionService(basicService *BasicService) *PackageVersionService {
	return &PackageVersionService{
		BasicService: basicService,
	}
}
