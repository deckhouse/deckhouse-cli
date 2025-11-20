package service

import (
	"github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"
)

type SecurityServices struct {
	name             string
	client           registry.Client
	securityServices map[string]*BasicService
	logger           *log.Logger
}

func NewSecurityServices(name string, client registry.Client, logger *log.Logger) *SecurityServices {
	return &SecurityServices{
		name:             name,
		securityServices: map[string]*BasicService{},
		client:           client,
		logger:           logger,
	}
}

func (s *SecurityServices) Security(imageName string) *BasicService {
	if service, exists := s.securityServices[imageName]; exists {
		return service
	}

	s.securityServices[imageName] = NewBasicService(s.name+" "+imageName, s.client.WithSegment(imageName), s.logger)

	return s.securityServices[imageName]
}
