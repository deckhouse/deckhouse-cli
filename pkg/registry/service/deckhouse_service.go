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

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	"github.com/deckhouse/deckhouse/pkg/log"
	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/deckhouse/deckhouse-cli/pkg"
)

const (
	deckhouseReleaseChannelsSegment = "release-channel"
	installerSegment                = "install"
	installStandaloneSegment        = "install-standalone"

	deckhouseServiceName                = "deckhouse"
	deckhouseReleaseChannelsServiceName = "deckhouse_release_channel"
	installerServiceName                = "installer"
	standaloneInstallerServiceName      = "standalone_installer"
)

// DeckhouseService provides high-level operations for Deckhouse platform management
type DeckhouseService struct {
	client pkg.RegistryClient

	*BasicService
	deckhouseReleaseChannels *DeckhouseReleaseService
	installer                *BasicService
	standaloneInstaller      *BasicService

	logger *log.Logger
}

// NewDeckhouseService creates a new deckhouse service
func NewDeckhouseService(client pkg.RegistryClient, logger *log.Logger) *DeckhouseService {
	return &DeckhouseService{
		client: client,

		BasicService:             NewBasicService(deckhouseServiceName, client, logger),
		deckhouseReleaseChannels: NewDeckhouseReleaseService(NewBasicService(deckhouseReleaseChannelsServiceName, client.WithSegment(deckhouseReleaseChannelsSegment), logger)),
		installer:                NewBasicService(installerServiceName, client.WithSegment(installerSegment), logger),
		standaloneInstaller:      NewBasicService(standaloneInstallerServiceName, client.WithSegment(installStandaloneSegment), logger),

		logger: logger,
	}
}

func (s *DeckhouseService) ReleaseChannels() *DeckhouseReleaseService {
	return s.deckhouseReleaseChannels
}

func (s *DeckhouseService) Installer() *BasicService {
	return s.installer
}

func (s *DeckhouseService) StandaloneInstaller() *BasicService {
	return s.standaloneInstaller
}

// GetRoot gets path of the registry root
func (s *DeckhouseService) GetRoot() string {
	return s.client.GetRegistry()
}

func (s *DeckhouseService) GetImage(ctx context.Context, tag string, opts ...pkg.ImageGetOption) (pkg.RegistryImage, error) {
	logger := s.logger.With("tag", tag)

	logger.Debug("Getting image")

	img, err := s.client.GetImage(ctx, tag, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	logger.Debug("Image retrieved successfully")

	return img, nil
}

func (s *DeckhouseService) GetDigest(ctx context.Context, tag string) (*v1.Hash, error) {
	logger := s.logger.With("tag", tag)

	logger.Debug("Getting digest")

	hash, err := s.client.GetDigest(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	logger.Debug("Digest retrieved successfully")

	return hash, nil
}

type DeckhouseReleaseService struct {
	*BasicService
}

func NewDeckhouseReleaseService(basicService *BasicService) *DeckhouseReleaseService {
	return &DeckhouseReleaseService{
		BasicService: basicService,
	}
}

type DeckhouseReleaseMetadata struct {
	Version string
	Suspend bool
}

func (s *DeckhouseReleaseService) GetMetadata(ctx context.Context, tag string) (*DeckhouseReleaseMetadata, error) {
	logger := s.logger.With(slog.String("service", s.name), slog.String("tag", tag))

	logger.Debug("Getting metadata")

	img, err := s.client.GetImage(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	meta, err := extractDeckhouseReleaseMetadata(img.Extract())
	if err != nil {
		return nil, fmt.Errorf("failed to extract metadata: %w", err)
	}

	return meta, nil
}

func extractDeckhouseReleaseMetadata(rc io.ReadCloser) (*DeckhouseReleaseMetadata, error) {
	var meta = new(DeckhouseReleaseMetadata)

	defer rc.Close()

	drr := &deckhouseReleaseReader{
		versionReader: bytes.NewBuffer(nil),
	}

	err := drr.untarMetadata(rc)
	if err != nil {
		return nil, err
	}

	type versionStruct struct {
		Version string `json:"version"`
		Suspend bool   `json:"suspend"`
	}

	var version versionStruct
	if drr.versionReader.Len() > 0 {
		err = json.NewDecoder(drr.versionReader).Decode(&version)
		if err != nil {
			return nil, err
		}

		meta.Version = version.Version
		meta.Suspend = version.Suspend
	}

	return meta, nil
}
