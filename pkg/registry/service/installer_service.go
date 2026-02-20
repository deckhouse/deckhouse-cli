package service

import (
	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"
)

type InstallerServices struct {
	*BasicService

	client registry.Client

	logger *log.Logger
}

func NewInstallerServices(name string, client registry.Client, logger *log.Logger) *InstallerServices {
	return &InstallerServices{
		BasicService: NewBasicService(name, client, logger),
		client:       client,
		logger:       logger,
	}
}

// func (s *InstallerServices) Installer(imageName string) *BasicService {
// 	if service, exists := s.securityServices[imageName]; exists {
// 		return service
// 	}

// 	s.securityServices[imageName] = NewBasicService(s.name+" "+imageName, s.client.WithSegment(imageName), s.logger)

// 	return s.securityServices[imageName]
// }
