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

// StatusProvider provides a single status section
type StatusProvider interface {
	GetStatus(ctx context.Context) domain.StatusSection
}

// MastersStatusProvider provides masters status
type MastersStatusProvider interface {
	StatusProvider
}

// DeckhousePodsStatusProvider provides deckhouse pods status
type DeckhousePodsStatusProvider interface {
	StatusProvider
}

// ReleasesStatusProvider provides releases status
type ReleasesStatusProvider interface {
	StatusProvider
}

// EditionStatusProvider provides edition status
type EditionStatusProvider interface {
	StatusProvider
}

// SettingsStatusProvider provides settings status
type SettingsStatusProvider interface {
	StatusProvider
}

// RegistryStatusProvider provides registry status
type RegistryStatusProvider interface {
	StatusProvider
}

// ClusterAlertsStatusProvider provides cluster alerts status
type ClusterAlertsStatusProvider interface {
	StatusProvider
}

// CNIModulesStatusProvider provides CNI modules status
type CNIModulesStatusProvider interface {
	StatusProvider
}

// QueueStatusProvider provides queue status
type QueueStatusProvider interface {
	StatusProvider
}

