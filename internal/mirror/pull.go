package mirror

import (
	"context"
	"fmt"

	"github.com/Masterminds/semver/v3"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/platform"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	dkplog "github.com/deckhouse/deckhouse/pkg/log"
)

type PullService struct {
	// moduleService handles module-related registry operations
	moduleService *registryservice.ModuleService
	// deckhouseService handles Deckhouse platform registry operations
	deckhouseService *registryservice.DeckhouseService

	platformService *platform.Service

	// layout manages the OCI image layouts for different components
	layout *ImageLayouts

	// sinceVersion specifies the minimum version to start mirroring from (optional)
	sinceVersion *semver.Version
	// targetTag specifies a specific tag to mirror instead of determining versions automatically
	targetTag string

	// logger is for internal debug logging
	logger *dkplog.Logger
	// userLogger is for user-facing informational messages
	userLogger *log.SLogger
}

func NewPullService(
	registryService *registryservice.Service,
	tmpDir string,
	targetTag string,
	ignoreSuspendedChannels bool,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *PullService {
	return &PullService{
		moduleService:    registryService.ModuleService(),
		deckhouseService: registryService.DeckhouseService(),

		platformService: platform.NewService(registryService.DeckhouseService(), nil, tmpDir, targetTag, ignoreSuspendedChannels, logger.Named("pull"), userLogger),

		layout: NewImageLayouts(),

		logger:     logger,
		userLogger: userLogger,
	}
}

// Pull
func (svc *PullService) Pull(ctx context.Context) error {
	err := svc.platformService.PullPlatform(ctx)
	if err != nil {
		return fmt.Errorf("pull platform: %w", err)
	}

	return nil
}
