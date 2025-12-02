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

package adapters

import (
	"context"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/deckhouse/deckhouse-cli/internal/status/domain"
	"github.com/deckhouse/deckhouse-cli/internal/status/objects/clusteralerts"
	cnimodules "github.com/deckhouse/deckhouse-cli/internal/status/objects/cni_modules"
	deckhouseedition "github.com/deckhouse/deckhouse-cli/internal/status/objects/edition"
	"github.com/deckhouse/deckhouse-cli/internal/status/objects/masters"
	deckhousepods "github.com/deckhouse/deckhouse-cli/internal/status/objects/pods"
	deckhousequeue "github.com/deckhouse/deckhouse-cli/internal/status/objects/queue"
	deckhouseregistry "github.com/deckhouse/deckhouse-cli/internal/status/objects/registry"
	deckhousereleases "github.com/deckhouse/deckhouse-cli/internal/status/objects/releases"
	deckhousesettings "github.com/deckhouse/deckhouse-cli/internal/status/objects/settings"
	"github.com/deckhouse/deckhouse-cli/internal/status/tools/statusresult"
	"github.com/deckhouse/deckhouse-cli/internal/status/usecase"
)

// StatusProviderFactory creates status providers from K8s clients
type StatusProviderFactory struct {
	kubeCl     kubernetes.Interface
	dynamicCl  dynamic.Interface
	restConfig *rest.Config
}

// NewStatusProviderFactory creates a new StatusProviderFactory
func NewStatusProviderFactory(kubeCl kubernetes.Interface, dynamicCl dynamic.Interface, restConfig *rest.Config) *StatusProviderFactory {
	return &StatusProviderFactory{
		kubeCl:     kubeCl,
		dynamicCl:  dynamicCl,
		restConfig: restConfig,
	}
}

// CreateMastersProvider creates masters status provider
func (f *StatusProviderFactory) CreateMastersProvider() usecase.StatusProvider {
	return &mastersProvider{kubeCl: f.kubeCl}
}

// CreateDeckhousePodsProvider creates deckhouse pods status provider
func (f *StatusProviderFactory) CreateDeckhousePodsProvider() usecase.StatusProvider {
	return &deckhousePodsProvider{kubeCl: f.kubeCl}
}

// CreateReleasesProvider creates releases status provider
func (f *StatusProviderFactory) CreateReleasesProvider() usecase.StatusProvider {
	return &releasesProvider{dynamicCl: f.dynamicCl}
}

// CreateEditionProvider creates edition status provider
func (f *StatusProviderFactory) CreateEditionProvider() usecase.StatusProvider {
	return &editionProvider{kubeCl: f.kubeCl}
}

// CreateSettingsProvider creates settings status provider
func (f *StatusProviderFactory) CreateSettingsProvider() usecase.StatusProvider {
	return &settingsProvider{dynamicCl: f.dynamicCl}
}

// CreateRegistryProvider creates registry status provider
func (f *StatusProviderFactory) CreateRegistryProvider() usecase.StatusProvider {
	return &registryProvider{kubeCl: f.kubeCl}
}

// CreateClusterAlertsProvider creates cluster alerts status provider
func (f *StatusProviderFactory) CreateClusterAlertsProvider() usecase.StatusProvider {
	return &clusterAlertsProvider{dynamicCl: f.dynamicCl}
}

// CreateCNIModulesProvider creates CNI modules status provider
func (f *StatusProviderFactory) CreateCNIModulesProvider() usecase.StatusProvider {
	return &cniModulesProvider{dynamicCl: f.dynamicCl}
}

// CreateQueueProvider creates queue status provider
func (f *StatusProviderFactory) CreateQueueProvider() usecase.StatusProvider {
	return &queueProvider{kubeCl: f.kubeCl, restConfig: f.restConfig}
}

// Provider implementations

type mastersProvider struct {
	kubeCl kubernetes.Interface
}

func (p *mastersProvider) GetStatus(ctx context.Context) domain.StatusSection {
	result := masters.Status(ctx, p.kubeCl)
	return convertResult(result)
}

type deckhousePodsProvider struct {
	kubeCl kubernetes.Interface
}

func (p *deckhousePodsProvider) GetStatus(ctx context.Context) domain.StatusSection {
	result := deckhousepods.Status(ctx, p.kubeCl)
	return convertResult(result)
}

type releasesProvider struct {
	dynamicCl dynamic.Interface
}

func (p *releasesProvider) GetStatus(ctx context.Context) domain.StatusSection {
	result := deckhousereleases.Status(ctx, p.dynamicCl)
	return convertResult(result)
}

type editionProvider struct {
	kubeCl kubernetes.Interface
}

func (p *editionProvider) GetStatus(ctx context.Context) domain.StatusSection {
	result := deckhouseedition.Status(ctx, p.kubeCl)
	return convertResult(result)
}

type settingsProvider struct {
	dynamicCl dynamic.Interface
}

func (p *settingsProvider) GetStatus(ctx context.Context) domain.StatusSection {
	result := deckhousesettings.Status(ctx, p.dynamicCl)
	return convertResult(result)
}

type registryProvider struct {
	kubeCl kubernetes.Interface
}

func (p *registryProvider) GetStatus(ctx context.Context) domain.StatusSection {
	result := deckhouseregistry.Status(ctx, p.kubeCl)
	return convertResult(result)
}

type clusterAlertsProvider struct {
	dynamicCl dynamic.Interface
}

func (p *clusterAlertsProvider) GetStatus(ctx context.Context) domain.StatusSection {
	result := clusteralerts.Status(ctx, p.dynamicCl)
	return convertResult(result)
}

type cniModulesProvider struct {
	dynamicCl dynamic.Interface
}

func (p *cniModulesProvider) GetStatus(ctx context.Context) domain.StatusSection {
	result := cnimodules.Status(ctx, p.dynamicCl)
	return convertResult(result)
}

type queueProvider struct {
	kubeCl     kubernetes.Interface
	restConfig *rest.Config
}

func (p *queueProvider) GetStatus(ctx context.Context) domain.StatusSection {
	result := deckhousequeue.Status(ctx, p.kubeCl, p.restConfig)
	return convertResult(result)
}

func convertResult(result statusresult.StatusResult) domain.StatusSection {
	return domain.StatusSection{
		Title:  result.Title,
		Output: result.Output,
	}
}

