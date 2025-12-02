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

package adapters

import (
	"context"
	"errors"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
	"github.com/deckhouse/deckhouse-cli/pkg"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// convertError translates infrastructure errors to domain errors
func convertError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, client.ErrImageNotFound) {
		return usecase.ErrImageNotFound
	}
	return err
}

// Compile-time interface checks
var (
	_ usecase.DeckhouseRegistryService = (*RegistryServiceAdapter)(nil)
	_ usecase.DeckhouseImageService    = (*DeckhouseServiceAdapter)(nil)
	_ usecase.ReleaseChannelService    = (*ReleaseChannelAdapter)(nil)
	_ usecase.ImageService             = (*BasicServiceAdapter)(nil)
	_ usecase.ModulesRegistryService   = (*ModulesServiceAdapter)(nil)
	_ usecase.ModuleService            = (*ModuleServiceAdapter)(nil)
	_ usecase.SecurityRegistryService  = (*SecurityServiceAdapter)(nil)
)

// RegistryServiceAdapter adapts registryservice.Service to usecase.DeckhouseRegistryService
type RegistryServiceAdapter struct {
	svc *registryservice.Service
}

// NewRegistryServiceAdapter creates a new adapter for the registry service
func NewRegistryServiceAdapter(svc *registryservice.Service) *RegistryServiceAdapter {
	return &RegistryServiceAdapter{svc: svc}
}

func (a *RegistryServiceAdapter) GetRoot() string {
	return a.svc.GetRoot()
}

func (a *RegistryServiceAdapter) Deckhouse() usecase.DeckhouseImageService {
	return NewDeckhouseServiceAdapter(a.svc.DeckhouseService())
}

func (a *RegistryServiceAdapter) Modules() usecase.ModulesRegistryService {
	return NewModulesServiceAdapter(a.svc.ModuleService())
}

func (a *RegistryServiceAdapter) Security() usecase.SecurityRegistryService {
	return NewSecurityServiceAdapter(a.svc.Security())
}

// DeckhouseServiceAdapter adapts registryservice.DeckhouseService to usecase.DeckhouseImageService
type DeckhouseServiceAdapter struct {
	svc *registryservice.DeckhouseService
}

func NewDeckhouseServiceAdapter(svc *registryservice.DeckhouseService) *DeckhouseServiceAdapter {
	return &DeckhouseServiceAdapter{svc: svc}
}

func (a *DeckhouseServiceAdapter) GetImage(ctx context.Context, ref string) (pkg.RegistryImage, error) {
	return a.svc.GetImage(ctx, ref)
}

func (a *DeckhouseServiceAdapter) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	return a.svc.GetDigest(ctx, tag)
}

func (a *DeckhouseServiceAdapter) CheckImageExists(ctx context.Context, tag string) error {
	return a.svc.CheckImageExists(ctx, tag)
}

func (a *DeckhouseServiceAdapter) ListTags(ctx context.Context) ([]string, error) {
	return a.svc.ListTags(ctx)
}

func (a *DeckhouseServiceAdapter) ReleaseChannels() usecase.ReleaseChannelService {
	return NewReleaseChannelAdapter(a.svc.ReleaseChannels())
}

func (a *DeckhouseServiceAdapter) Installer() usecase.ImageService {
	return NewBasicServiceAdapter(a.svc.Installer())
}

func (a *DeckhouseServiceAdapter) StandaloneInstaller() usecase.ImageService {
	return NewBasicServiceAdapter(a.svc.StandaloneInstaller())
}

// ReleaseChannelAdapter adapts registryservice.DeckhouseReleaseService to usecase.ReleaseChannelService
type ReleaseChannelAdapter struct {
	svc *registryservice.DeckhouseReleaseService
}

func NewReleaseChannelAdapter(svc *registryservice.DeckhouseReleaseService) *ReleaseChannelAdapter {
	return &ReleaseChannelAdapter{svc: svc}
}

func (a *ReleaseChannelAdapter) GetImage(ctx context.Context, ref string) (pkg.RegistryImage, error) {
	img, err := a.svc.GetImage(ctx, ref)
	return img, convertError(err)
}

func (a *ReleaseChannelAdapter) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	hash, err := a.svc.GetDigest(ctx, tag)
	return hash, convertError(err)
}

func (a *ReleaseChannelAdapter) CheckImageExists(ctx context.Context, tag string) error {
	return convertError(a.svc.CheckImageExists(ctx, tag))
}

func (a *ReleaseChannelAdapter) ListTags(ctx context.Context) ([]string, error) {
	tags, err := a.svc.ListTags(ctx)
	return tags, convertError(err)
}

func (a *ReleaseChannelAdapter) GetMetadata(ctx context.Context, tag string) (*usecase.ReleaseChannelMetadata, error) {
	meta, err := a.svc.GetMetadata(ctx, tag)
	if err != nil {
		return nil, convertError(err)
	}
	return &usecase.ReleaseChannelMetadata{
		Version: meta.Version,
		Suspend: meta.Suspend,
	}, nil
}

// BasicServiceAdapter adapts registryservice.BasicService to usecase.ImageService
type BasicServiceAdapter struct {
	svc *registryservice.BasicService
}

func NewBasicServiceAdapter(svc *registryservice.BasicService) *BasicServiceAdapter {
	return &BasicServiceAdapter{svc: svc}
}

func (a *BasicServiceAdapter) GetImage(ctx context.Context, ref string) (pkg.RegistryImage, error) {
	return a.svc.GetImage(ctx, ref)
}

func (a *BasicServiceAdapter) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	return a.svc.GetDigest(ctx, tag)
}

func (a *BasicServiceAdapter) CheckImageExists(ctx context.Context, tag string) error {
	return a.svc.CheckImageExists(ctx, tag)
}

func (a *BasicServiceAdapter) ListTags(ctx context.Context) ([]string, error) {
	return a.svc.ListTags(ctx)
}

// ModulesServiceAdapter adapts registryservice.ModulesService to usecase.ModulesRegistryService
type ModulesServiceAdapter struct {
	svc *registryservice.ModulesService
}

func NewModulesServiceAdapter(svc *registryservice.ModulesService) *ModulesServiceAdapter {
	return &ModulesServiceAdapter{svc: svc}
}

func (a *ModulesServiceAdapter) ListTags(ctx context.Context) ([]string, error) {
	return a.svc.ListTags(ctx)
}

func (a *ModulesServiceAdapter) Module(name string) usecase.ModuleService {
	return NewModuleServiceAdapter(a.svc.Module(name))
}

// ModuleServiceAdapter adapts registryservice.ModuleService to usecase.ModuleService
type ModuleServiceAdapter struct {
	svc *registryservice.ModuleService
}

func NewModuleServiceAdapter(svc *registryservice.ModuleService) *ModuleServiceAdapter {
	return &ModuleServiceAdapter{svc: svc}
}

func (a *ModuleServiceAdapter) GetImage(ctx context.Context, ref string) (pkg.RegistryImage, error) {
	return a.svc.GetImage(ctx, ref)
}

func (a *ModuleServiceAdapter) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	return a.svc.GetDigest(ctx, tag)
}

func (a *ModuleServiceAdapter) CheckImageExists(ctx context.Context, tag string) error {
	return a.svc.CheckImageExists(ctx, tag)
}

func (a *ModuleServiceAdapter) ListTags(ctx context.Context) ([]string, error) {
	return a.svc.ListTags(ctx)
}

func (a *ModuleServiceAdapter) ReleaseChannels() usecase.ImageService {
	return NewModuleReleaseServiceAdapter(a.svc.ReleaseChannels())
}

// ModuleReleaseServiceAdapter adapts registryservice.ModuleReleaseService to usecase.ImageService
type ModuleReleaseServiceAdapter struct {
	svc *registryservice.ModuleReleaseService
}

func NewModuleReleaseServiceAdapter(svc *registryservice.ModuleReleaseService) *ModuleReleaseServiceAdapter {
	return &ModuleReleaseServiceAdapter{svc: svc}
}

func (a *ModuleReleaseServiceAdapter) GetImage(ctx context.Context, ref string) (pkg.RegistryImage, error) {
	return a.svc.GetImage(ctx, ref)
}

func (a *ModuleReleaseServiceAdapter) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	return a.svc.GetDigest(ctx, tag)
}

func (a *ModuleReleaseServiceAdapter) CheckImageExists(ctx context.Context, tag string) error {
	return a.svc.CheckImageExists(ctx, tag)
}

func (a *ModuleReleaseServiceAdapter) ListTags(ctx context.Context) ([]string, error) {
	return a.svc.ListTags(ctx)
}

func (a *ModuleServiceAdapter) Extra() usecase.ImageService {
	return NewBasicServiceAdapter(a.svc.Extra())
}

// SecurityServiceAdapter adapts registryservice.SecurityServices to usecase.SecurityRegistryService
type SecurityServiceAdapter struct {
	svc *registryservice.SecurityServices
}

func NewSecurityServiceAdapter(svc *registryservice.SecurityServices) *SecurityServiceAdapter {
	return &SecurityServiceAdapter{svc: svc}
}

func (a *SecurityServiceAdapter) Database(name string) usecase.ImageService {
	return NewBasicServiceAdapter(a.svc.Security(name))
}
