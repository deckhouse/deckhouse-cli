/*
Copyright 2026 Flant JSC

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

package installer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

const defaultTargetTag = "latest"

type Options struct {
	// TargetTag specifies a specific tag to mirror instead of determining versions automatically
	// it can be:
	// semver f.e. vX.Y.Z
	// channel f.e. alpha/beta/stable
	// any other tag
	TargetTag string
	// BundleDir is the directory to store the bundle
	BundleDir string
	// BundleChunkSize is the max size of bundle chunks in bytes (0 = no chunking)
	BundleChunkSize int64
}
type Service struct {
	// registryService handles Deckhouse installer registry operations
	registryService *registryservice.Service
	// layout manages the OCI image layouts for installer components
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
	userLogger.Infof("Creating OCI Image Layouts for Installer")

	// workingDir is the root where we create layouts
	// Layouts will be created at workingDir/installer/
	layout, err := NewImageLayouts(filepath.Join(workingDir, "installer"))
	if err != nil {
		//TODO: handle error
		userLogger.Warnf("Create OCI Image Layouts: %v", err)
	}

	if options == nil {
		options = &Options{}
	}

	return &Service{
		registryService: registryService,
		layout:          layout,
		downloadList:    NewImageDownloadList(registryService.GetRoot()),
		pullerService:   puller.NewPullerService(logger, userLogger),
		options:         options,
		logger:          logger,
		userLogger:      userLogger,
	}
}

// PullInstaller pulls the installer image
// It validates access to the registry and pulls the image
func (svc *Service) PullInstaller(ctx context.Context) error {
	err := svc.validateInstallerAccess(ctx)
	if err != nil {
		return fmt.Errorf("validate installer access: %w", err)
	}

	tagsToMirror := svc.findTagsToMirror(ctx)

	svc.downloadList.FillInstallerImages(tagsToMirror)

	err = svc.pullInstaller(ctx)
	if err != nil {
		return fmt.Errorf("pull installer: %w", err)
	}

	return nil
}

func (svc *Service) validateInstallerAccess(ctx context.Context) error {
	targetTag := defaultTargetTag

	if svc.options.TargetTag != "" {
		targetTag = svc.options.TargetTag
	}

	svc.logger.Debug("Validating access to the installer registry", slog.String("tag", targetTag))

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	err := svc.registryService.InstallerService().CheckImageExists(ctx, targetTag)
	if err != nil {
		return fmt.Errorf("failed to check installer tag %q exists in registry: %w", targetTag, err)
	}

	return nil
}

func (svc *Service) findTagsToMirror(_ context.Context) []string {
	targetTag := defaultTargetTag

	if svc.options.TargetTag != "" {
		targetTag = svc.options.TargetTag
	}

	return []string{targetTag}
}

func (svc *Service) pullInstaller(ctx context.Context) error {
	logger := svc.userLogger

	err := logger.Process("Pull installer", func() error {
		config := puller.PullConfig{
			Name:             "installer",
			ImageSet:         svc.downloadList.Installer,
			Layout:           svc.layout.image,
			AllowMissingTags: svc.options.TargetTag == "",
			GetterService:    svc.registryService.InstallerService(),
		}

		return svc.pullerService.PullImages(ctx, config)
	})
	if err != nil {
		return fmt.Errorf("pull installer: %w", err)
	}

	if err := logger.Process("Pack Deckhouse images into platform.tar", func() error {
		bundleChunkSize := svc.options.BundleChunkSize
		bundleDir := svc.options.BundleDir

		var installer io.Writer = chunked.NewChunkedFileWriter(
			bundleChunkSize,
			bundleDir,
			"installer.tar",
		)

		if bundleChunkSize == 0 {
			installer, err = os.Create(filepath.Join(bundleDir, "installer.tar"))
			if err != nil {
				return fmt.Errorf("create installer.tar: %w", err)
			}
		}

		if err := bundle.Pack(context.Background(), svc.layout.workingDir, installer); err != nil {
			return fmt.Errorf("pack installer.tar: %w", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}
