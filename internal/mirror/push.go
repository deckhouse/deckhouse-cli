package mirror

import (
	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

type PushService struct {
	registryService registryservice.Service

	logger     *dkplog.Logger
	userLogger *log.SLogger
}

func NewPushService(registryService registryservice.Service, logger *dkplog.Logger, userLogger *log.SLogger) *PushService {
	return &PushService{
		registryService: registryService,
		logger:          logger,
		userLogger:      userLogger,
	}
}
