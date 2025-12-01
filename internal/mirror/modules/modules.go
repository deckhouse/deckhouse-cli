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
	pullflags "github.com/deckhouse/deckhouse-cli/internal/mirror/cmd/pull/flags"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

type Service struct {
	workingDir string

	// modulesService handles Deckhouse platform registry operations
	modulesService *registryservice.ModulesService
	// layout manages the OCI image layouts for different components
	layout *ModulesImageLayouts
	// modulesDownloadList manages the list of images to be downloaded
	modulesDownloadList *ModulesDownloadList
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
	registryService *registryservice.Service,
	workingDir string,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	userLogger.Infof("Creating OCI Image Layouts for Modules")

	rootURL := registryService.GetRoot()

	return &Service{
		workingDir:          workingDir,
		modulesService:      registryService.ModuleService(),
		modulesDownloadList: NewModulesDownloadList(rootURL),
		pullerService:       puller.NewPullerService(logger, userLogger),
		rootURL:             rootURL,
		logger:              logger,
		userLogger:          userLogger,
	}
}

// PullModules pulls the Deckhouse modules
// It validates access to the registry and pulls the module images
func (svc *Service) PullModules(ctx context.Context) error {
	err := svc.validateModulesAccess(ctx)
	if err != nil {
		return fmt.Errorf("validate modules access: %w", err)
	}

	err = svc.pullModules(ctx)
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

	// For specific tags, check if the tag exists
	_, err := svc.modulesService.ListTags(ctx)
	if errors.Is(err, client.ErrImageNotFound) {
		svc.userLogger.Warnf("Skipping pull of modules: %v", err)

		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to check modules lists: %w", err)
	}

	return nil
}

func (svc *Service) pullModules(ctx context.Context) error {
	logger := svc.userLogger

	tmpDir := filepath.Join(svc.workingDir, "modules")

	modules, err := svc.modulesService.ListTags(ctx)
	if err != nil {
		return fmt.Errorf("list modules: %w", err)
	}

	for _, module := range modules {
		logger.Infof("Module found: %s", module)
	}

	moduleImagesLayout, err := createOCIImageLayoutsForModules(tmpDir, modules)
	if err != nil {
		return fmt.Errorf("create OCI image layouts for modules: %w", err)
	}
	svc.layout = moduleImagesLayout

	// Fill download list with modules images
	svc.modulesDownloadList.FillModulesImages(modules)

	err = logger.Process("Pull Modules", func() error {
		for _, module := range modules {
			config := puller.PullConfig{
				Name:             module + " release channels",
				ImageSet:         svc.modulesDownloadList.Module(module).ModuleReleaseChannels,
				Layout:           svc.layout.Module(module).ModulesReleaseChannels,
				AllowMissingTags: true,
				GetterService:    svc.modulesService.Module(module).ReleaseChannels(),
			}

			err = svc.pullerService.PullImages(ctx, config)
			if err != nil {
				return err
			}

			// TODO:
			// we must extract module images tags from release channels before pulling module images

			// Pull modules images
			config = puller.PullConfig{
				Name:             module,
				ImageSet:         svc.modulesDownloadList.Module(module).Module,
				Layout:           svc.layout.Module(module).Modules,
				AllowMissingTags: true, // Allow missing module images
				GetterService:    svc.modulesService.Module(module),
			}

			err := svc.pullerService.PullImages(ctx, config)
			if err != nil {
				return err
			}

			config = puller.PullConfig{
				Name:             module + " extra",
				ImageSet:         svc.modulesDownloadList.Module(module).ModuleExtra,
				Layout:           svc.layout.Module(module).ModulesExtra,
				AllowMissingTags: true,
				GetterService:    svc.modulesService.Module(module).Extra(),
			}

			err = svc.pullerService.PullImages(ctx, config)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

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
	modules []string,
) (*ModulesImageLayouts, error) {
	layouts := NewModulesImageLayouts(rootFolder)

	for _, moduleName := range modules {
		moduleLayouts, err := createOCIImageLayoutsForModule(
			filepath.Join(rootFolder, moduleName),
		)
		if err != nil {
			return nil, fmt.Errorf("create OCI image layouts for module %s: %w", moduleName, err)
		}
		layouts.list[moduleName] = moduleLayouts
	}

	return layouts, nil
}

func createOCIImageLayoutsForModule(
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
