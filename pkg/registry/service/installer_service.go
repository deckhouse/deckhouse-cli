package service

import (
	"github.com/deckhouse/deckhouse/pkg/log"
	client "github.com/deckhouse/deckhouse/pkg/registry"
)

type InstallerServices struct {
	*BasicService

	client client.Client

	logger *log.Logger
}

func NewInstallerServices(name string, client client.Client, logger *log.Logger) *InstallerServices {
	return &InstallerServices{
		BasicService: NewBasicService(name, client, logger),
		client:       client,
		logger:       logger,
	}
}
