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

package modules

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/validation"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

type Service struct {
	// deckhouseService handles Deckhouse platform registry operations
	deckhouseService *registryservice.DeckhouseService
	// layout manages the OCI image layouts for different components
	layout *ImageLayouts
	// downloadList manages the list of images to be downloaded
	downloadList *ImageDownloadList
	// pullerService handles the pulling of images
	pullerService *puller.PullerService

	// rootURL is the base registry URL for modules images
	rootURL string

	// logger is for internal debug logging
	logger *dkplog.Logger
	// userLogger is for user-facing informational messages
	userLogger *log.SLogger
}

func NewService(
	deckhouseService *registryservice.DeckhouseService,
	workingDir string,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	userLogger.Infof("Creating OCI Image Layouts for Modules")

	tmpDir := filepath.Join(workingDir, "modules")

	layout, err := createOCIImageLayoutsForModules(tmpDir)
	if err != nil {
		//TODO: handle error
		userLogger.Warnf("Create OCI Image Layouts: %v", err)
	}

	rootURL := deckhouseService.GetRoot()

	return &Service{
		deckhouseService: deckhouseService,
		layout:           layout,
		downloadList:     NewImageDownloadList(rootURL),
		pullerService:    puller.NewPullerService(deckhouseService, logger, userLogger),
		rootURL:          rootURL,
		logger:           logger,
		userLogger:       userLogger,
	}
}

// PullModules pulls the Deckhouse modules
// It validates access to the registry and pulls the module images
func (svc *Service) PullModules(ctx context.Context, modules []string) error {
	err := svc.validateModulesAccess(ctx)
	if err != nil {
		return fmt.Errorf("validate modules access: %w", err)
	}

	err = svc.pullModules(ctx, modules)
	if err != nil {
		return fmt.Errorf("pull modules: %w", err)
	}

	return nil
}

// validateModulesAccess validates access to the modules registry
// It checks if the modules registry is accessible
func (svc *Service) validateModulesAccess(ctx context.Context) error {
	svc.logger.Debug("Validating access to the modules registry")

	// Add timeout to prevent hanging on slow/unreachable registries
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	modulesRepo := filepath.Join(svc.deckhouseService.GetRoot(), internal.ModulesSegment)
	validator := validation.NewRemoteRegistryAccessValidator()
	err := validator.ValidateListAccessForRepo(ctx, modulesRepo, []validation.Option{}...) // TODO: add options
	if err != nil {
		return fmt.Errorf("modules registry is not accessible: %w", err)
	}

	return nil
}

func (svc *Service) pullModules(ctx context.Context, modules []string) error {
	logger := svc.userLogger

	// Fill download list with modules images
	svc.downloadList.FillModulesImages(modules)

	// Pull modules images
	err := logger.Process("Pull Modules", func() error {
		config := puller.PullConfig{
			Name:             "Modules",
			ImageSet:         svc.downloadList.Modules,
			Layout:           svc.layout.Modules,
			AllowMissingTags: true, // Allow missing module images
			GetterService:    svc.deckhouseService,
		}

		return svc.pullerService.PullImages(ctx, config)
	})
	if err != nil {
		return err
	}

	// Pull modules release channels
	err = logger.Process("Pull Modules Release Channels", func() error {
		config := puller.PullConfig{
			Name:             "Modules Release Channels",
			ImageSet:         svc.downloadList.ModulesReleaseChannels,
			Layout:           svc.layout.ModulesReleaseChannels,
			AllowMissingTags: true,
			GetterService:    svc.deckhouseService,
		}

		return svc.pullerService.PullImages(ctx, config)
	})
	if err != nil {
		return err
	}

	// TODO: Pull modules extra images if needed

	err = logger.Process("Processing modules image indexes", func() error {
		for _, l := range svc.layout.AsList() {
			err = layouts.SortIndexManifests(l)
			if err != nil {
				return fmt.Errorf("sorting index manifests of %s: %w", l, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("processing modules image indexes: %w", err)
	}

	if err := logger.Process("Pack modules images into modules.tar", func() error {
		bundleChunkSize := pullflags.ImagesBundleChunkSizeGB * 1000 * 1000 * 1000
		bundleDir := pullflags.ImagesBundlePath

		var modulesBundle io.Writer = chunked.NewChunkedFileWriter(
			bundleChunkSize,
			bundleDir,
			"modules.tar",
		)

		if bundleChunkSize == 0 {
			modulesBundle, err = os.Create(filepath.Join(bundleDir, "modules.tar"))
			if err != nil {
				return fmt.Errorf("create modules.tar: %w", err)
			}
		}

		if err := bundle.Pack(context.Background(), svc.layout.workingDir, modulesBundle); err != nil {
			return fmt.Errorf("pack modules.tar: %w", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func createOCIImageLayoutsForModules(
	rootFolder string,
) (*ImageLayouts, error) {
	layouts := NewImageLayouts(rootFolder)

	mirrorTypes := []internal.MirrorType{
		internal.MirrorTypeModules,
		internal.MirrorTypeModulesReleaseChannels,
		internal.MirrorTypeModulesExtra,
	}

	for _, mtype := range mirrorTypes {
		err := layouts.setLayoutByMirrorType(rootFolder, mtype)
		if err != nil {
			return nil, fmt.Errorf("set layout by mirror type %v: %w", mtype, err)
		}
	}

	return layouts, nil
}
