package mirror

import (
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
	dkplog "github.com/deckhouse/deckhouse/pkg/log"
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
