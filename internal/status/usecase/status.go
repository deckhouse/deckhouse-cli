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

package usecase

import (
	"context"

	"github.com/deckhouse/deckhouse-cli/internal/status/domain"
)

// StatusUseCase collects cluster status from all providers
type StatusUseCase struct {
	masters       StatusProvider
	deckhousePods StatusProvider
	releases      StatusProvider
	edition       StatusProvider
	settings      StatusProvider
	registry      StatusProvider
	clusterAlerts StatusProvider
	cniModules    StatusProvider
	queue         StatusProvider
}

// NewStatusUseCase creates a new StatusUseCase
func NewStatusUseCase(
	masters StatusProvider,
	deckhousePods StatusProvider,
	releases StatusProvider,
	edition StatusProvider,
	settings StatusProvider,
	registry StatusProvider,
	clusterAlerts StatusProvider,
	cniModules StatusProvider,
	queue StatusProvider,
) *StatusUseCase {
	return &StatusUseCase{
		masters:       masters,
		deckhousePods: deckhousePods,
		releases:      releases,
		edition:       edition,
		settings:      settings,
		registry:      registry,
		clusterAlerts: clusterAlerts,
		cniModules:    cniModules,
		queue:         queue,
	}
}

// Execute collects status from all providers
func (uc *StatusUseCase) Execute(ctx context.Context) *domain.StatusReport {
	return &domain.StatusReport{
		Masters:        uc.masters.GetStatus(ctx),
		DeckhousePods:  uc.deckhousePods.GetStatus(ctx),
		Releases:       uc.releases.GetStatus(ctx),
		Edition:        uc.edition.GetStatus(ctx),
		Settings:       uc.settings.GetStatus(ctx),
		Registry:       uc.registry.GetStatus(ctx),
		ClusterAlerts:  uc.clusterAlerts.GetStatus(ctx),
		CNIModules:     uc.cniModules.GetStatus(ctx),
		Queue:          uc.queue.GetStatus(ctx),
	}
}

