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

package mirror

import (
	"context"
	"fmt"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/modules"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/platform"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/security"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

type PullService struct {
	registryService *registryservice.Service

	platformService *platform.Service
	securityService *security.Service
	modulesService  *modules.Service

	// layout manages the OCI image layouts for different components
	layout *ImageLayouts

	// logger is for internal debug logging
	logger *dkplog.Logger
	// userLogger is for user-facing informational messages
	userLogger *log.SLogger
}

func NewPullService(
	registryService *registryservice.Service,
	tmpDir string,
	targetTag string,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *PullService {
	return &PullService{
		registryService: registryService,

		platformService: platform.NewService(registryService, nil, tmpDir, targetTag, logger, userLogger),
		securityService: security.NewService(registryService, tmpDir, logger, userLogger),
		modulesService:  modules.NewService(registryService, tmpDir, logger, userLogger),

		layout: NewImageLayouts(),

		logger:     logger,
		userLogger: userLogger,
	}
}

// Pull
func (svc *PullService) Pull(ctx context.Context) error {

	err := svc.modulesService.PullModules(ctx)
	if err != nil {
		return fmt.Errorf("pull modules: %w", err)
	}

	return nil
	err = svc.platformService.PullPlatform(ctx)
	if err != nil {
		return fmt.Errorf("pull platform: %w", err)
	}

	err = svc.securityService.PullSecurity(ctx)
	if err != nil {
		return fmt.Errorf("pull security databases: %w", err)
	}

	err = svc.modulesService.PullModules(ctx)
	if err != nil {
		return fmt.Errorf("pull modules: %w", err)
	}

	return nil
}
