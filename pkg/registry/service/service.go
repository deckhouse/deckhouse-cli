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

	"github.com/deckhouse/deckhouse/pkg/log"
	client "github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/pkg"
)

const (
	moduleSegment   = "modules"
	packageSegment  = "packages"
	securitySegment = "security"

	securityServiceName = "security"
)

// Service provides high-level registry operations using a registry client
type Service struct {
	client client.Client
	// editionBase is the client scoped to the edition sub-path (or equal to
	// client when no edition is configured). Services that live under the
	// edition path (deckhouse, modules, security) are built on top of it.
	editionBase client.Client

	modulesService   *ModulesService
	packagesService  *PackagesService
	deckhouseService *DeckhouseService
	security         *SecurityServices
	installer        *InstallerServices

	// modulesSegment is the registry path segment where modules live, relative
	// to the edition root. Defaults to "modules"; empty means the edition root.
	modulesSegment string

	logger *log.Logger
}

// Option configures a Service at construction time.
type Option func(*Service)

// WithModulesPathSuffix sets the registry path segment where modules live,
// relative to the edition root. Empty suffix keeps the default "modules";
// "/" places modules at the edition root. Leading and trailing slashes are ignored.
func WithModulesPathSuffix(suffix string) Option {
	return func(s *Service) {
		s.modulesSegment = normalizeModulesSegment(suffix)
	}
}

// normalizeModulesSegment resolves the modules path segment from a suffix flag
// value. Empty value keeps the default; surrounding slashes are trimmed so "/"
// yields an empty segment (modules at the edition root).
func normalizeModulesSegment(suffix string) string {
	if suffix == "" {
		return moduleSegment
	}

	return strings.Trim(suffix, "/")
}

// scopeSegment scopes the client by each non-empty component of a
// slash-separated segment. An empty segment leaves the client unchanged.
func scopeSegment(c client.Client, segment string) client.Client {
	for _, seg := range strings.Split(segment, "/") {
		if seg == "" {
			continue
		}

		c = c.WithSegment(seg)
	}

	return c
}

// NewService creates a new registry service with the given client and logger
func NewService(c client.Client, edition pkg.Edition, logger *log.Logger, opts ...Option) *Service {
	s := &Service{
		client:         c,
		logger:         logger,
		modulesSegment: moduleSegment,
	}

	for _, opt := range opts {
		opt(s)
	}

	// base is scoped to the edition sub-path when an edition is specified.
	// When edition is NoEdition the client already points at the correct root.
	var base client.Client
	if edition == pkg.NoEdition {
		base = c
	} else {
		base = c.WithSegment(edition.String())
	}

	s.editionBase = base

	s.modulesService = NewModulesService(scopeSegment(base, s.modulesSegment), logger.Named("modules"))
	s.packagesService = NewPackagesService(base.WithSegment(packageSegment), logger.Named("packages"))
	s.deckhouseService = NewDeckhouseService(base, logger.Named("deckhouse"))
	s.security = NewSecurityServices(securityServiceName, base.WithSegment(securitySegment), logger.Named("security"))

	// services that are not scoped by edition
	s.installer = NewInstallerServices(installerServiceName, c.WithSegment("installer"), logger.Named("installer"))

	return s
}

// GetRoot gets path of the registry root, without the edition segment.
// This is the non-edition-scoped root (e.g. "registry.deckhouse.ru/deckhouse")
// used for services that live outside the edition sub-tree (installer, plugins).
func (s *Service) GetRoot() string {
	return s.client.GetRegistry()
}

// GetEditionRoot returns the registry root scoped to the edition sub-path
// (e.g. "registry.deckhouse.ru/deckhouse/fe"). When no edition is configured
// the result equals GetRoot(). Use this when building references for services
// that live under the edition sub-tree (deckhouse, modules, security).
func (s *Service) GetEditionRoot() string {
	return s.editionBase.GetRegistry()
}

// ModuleService returns the module service
func (s *Service) ModuleService() *ModulesService {
	return s.modulesService
}

// GetModulesSegment returns the registry path segment where modules live,
// relative to the edition root (default "modules", empty when modules are at
// the edition root). Callers building module references must use it so their
// paths match the scope of ModuleService.
func (s *Service) GetModulesSegment() string {
	return s.modulesSegment
}

// PackageService returns the packages service
func (s *Service) PackageService() *PackagesService {
	return s.packagesService
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
