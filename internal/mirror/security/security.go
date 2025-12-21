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

package security

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// Options contains configuration options for the security service
type Options struct {
	// BundleDir is the directory to store the bundle
	BundleDir string
	// BundleChunkSize is the max size of bundle chunks in bytes (0 = no chunking)
	BundleChunkSize int64
}

type Service struct {
	// securityService handles Deckhouse security registry operations
	securityService *registryservice.SecurityServices
	// layout manages the OCI image layouts for security components
	layout *ImageLayouts
	// downloadList manages the list of images to be downloaded
	downloadList *ImageDownloadList
	// pullerService handles the pulling of images
	pullerService *puller.PullerService

	// options contains service configuration
	options *Options

	// logger is for internal debug logging
	logger *dkplog.Logger
	// userLogger is for user-facing informational messages
	userLogger *log.SLogger
}

func NewService(
	registryService *registryservice.Service,
	workingDir string,
	options *Options,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	userLogger.Infof("Creating OCI Image Layouts for Security")

	if options == nil {
		options = &Options{}
	}

	// workingDir is the root where we create layouts
	// Layouts will be created at workingDir/security/trivy-db, etc.
	layout, err := createOCIImageLayoutsForSecurity(workingDir)
	if err != nil {
		//TODO: handle error
		userLogger.Warnf("Create OCI Image Layouts: %v", err)
	}

	return &Service{
		securityService: registryService.Security(),
		layout:          layout,
		downloadList:    NewImageDownloadList(registryService.GetRoot()),
		pullerService:   puller.NewPullerService(logger, userLogger),
		options:         options,
		logger:          logger,
		userLogger:      userLogger,
	}
}

// PullSecurity pulls the security databases
// It validates access to the registry and pulls the security database images
func (svc *Service) PullSecurity(ctx context.Context) error {
	err := svc.validateSecurityAccess(ctx)
	if err != nil {
		return fmt.Errorf("validate security access: %w", err)
	}

	err = svc.pullSecurityDatabases(ctx)
	if err != nil {
		return fmt.Errorf("pull security databases: %w", err)
	}

	return nil
}

// validateSecurityAccess validates access to the security registry
// It checks if the security database image exists in the source registry
func (svc *Service) validateSecurityAccess(ctx context.Context) error {
	svc.logger.Debug("Validating access to the security registry")

	// Add timeout to prevent hanging on slow/unreachable registries
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// For specific tags, check if the tag exists
	err := svc.securityService.Security(internal.SecurityTrivyDBSegment).CheckImageExists(ctx, "2")
	if errors.Is(err, client.ErrImageNotFound) {
		svc.userLogger.Warnf("Skipping pull of security databases: %v", err)

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to check tag exists: %w", err)
	}

	return nil
}

func (svc *Service) pullSecurityDatabases(ctx context.Context) error {
	logger := svc.userLogger

	// Fill download list with security images
	svc.downloadList.FillSecurityImages()

	err := logger.Process("Pull Security Databases", func() error {
		for securityName, imageSet := range svc.downloadList.Security {
			config := puller.PullConfig{
				Name:             "Security Databases " + securityName,
				ImageSet:         imageSet,
				Layout:           svc.layout.Security[securityName],
				AllowMissingTags: true, // Allow missing security database images
				GetterService:    svc.securityService.Security(securityName),
			}

			err := svc.pullerService.PullImages(ctx, config)
			if err != nil {
				return fmt.Errorf("pull security database images: %w", err)
			}

			svc.userLogger.InfoLn()
		}

		return nil
	})
	if err != nil {
		return err
	}

	err = logger.Process("Processing security image indexes", func() error {
		for _, l := range svc.layout.AsList() {
			err = layouts.SortIndexManifests(l)
			if err != nil {
				return fmt.Errorf("sorting index manifests of %s: %w", l, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("processing security image indexes: %w", err)
	}

	if err := logger.Process("Pack security images into security.tar", func() error {
		bundleChunkSize := svc.options.BundleChunkSize
		bundleDir := svc.options.BundleDir

		var security io.Writer = chunked.NewChunkedFileWriter(
			bundleChunkSize,
			bundleDir,
			"security.tar",
		)

		if bundleChunkSize == 0 {
			security, err = os.Create(filepath.Join(bundleDir, "security.tar"))
			if err != nil {
				return fmt.Errorf("create security.tar: %w", err)
			}
		}

		if err := bundle.Pack(context.Background(), svc.layout.workingDir, security); err != nil {
			return fmt.Errorf("pack security.tar: %w", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func createOCIImageLayoutsForSecurity(
	rootFolder string,
) (*ImageLayouts, error) {
	layouts := NewImageLayouts(rootFolder)

	mirrorTypes := []internal.MirrorType{
		internal.MirrorTypeSecurityTrivyDBSegment,
		internal.MirrorTypeSecurityTrivyBDUSegment,
		internal.MirrorTypeSecurityTrivyJavaDBSegment,
		internal.MirrorTypeSecurityTrivyChecksSegment,
	}

	for _, mtype := range mirrorTypes {
		err := layouts.setLayoutByMirrorType(rootFolder, mtype)
		if err != nil {
			return nil, fmt.Errorf("set layout by mirror type %v: %w", mtype, err)
		}
	}

	return layouts, nil
}
