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
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
	libmodules "github.com/deckhouse/deckhouse-cli/pkg/libmirror/modules"
)

// Compile-time interface check
var _ usecase.ModulesPuller = (*ModulesService)(nil)

// ModulesService handles pulling Deckhouse module images using Clean Architecture
type ModulesService struct {
	// Dependencies (injected via interfaces)
	registry     usecase.ModulesRegistryService
	rootURL      string
	bundlePacker usecase.BundlePacker
	logger       usecase.Logger

	// Internal state
	layouts      *ModulesLayouts
	downloadList *ModulesDownloadListNew

	// Configuration
	opts *usecase.ModulesOpts
}

// NewModulesService creates a new modules service with injected dependencies
func NewModulesService(
	registry usecase.DeckhouseRegistryService,
	bundlePacker usecase.BundlePacker,
	logger usecase.Logger,
	opts *usecase.ModulesOpts,
) *ModulesService {
	if opts == nil {
		opts = &usecase.ModulesOpts{}
	}

	// Create default filter if not provided
	if opts.Filter == nil {
		opts.Filter, _ = libmodules.NewFilter(nil, libmodules.FilterTypeBlacklist)
	}

	rootURL := registry.GetRoot()

	return &ModulesService{
		registry:     registry.Modules(),
		rootURL:      rootURL,
		bundlePacker: bundlePacker,
		logger:       logger,
		downloadList: NewModulesDownloadListNew(rootURL),
		opts:         opts,
	}
}

// Pull implements usecase.ModulesPuller
func (s *ModulesService) Pull(ctx context.Context) error {
	// Validate access to registry
	if err := s.validateAccess(ctx); err != nil {
		return fmt.Errorf("validate access: %w", err)
	}

	// List and filter modules
	modules, err := s.listAndFilterModules(ctx)
	if err != nil {
		return fmt.Errorf("list modules: %w", err)
	}

	if len(modules) == 0 {
		s.logger.Warn("No modules to pull after filtering")
		return nil
	}

	s.logger.Infof("Found %d modules to pull", len(modules))

	// Initialize layouts for filtered modules
	if err := s.initLayouts(modules); err != nil {
		return fmt.Errorf("init layouts: %w", err)
	}

	// Pull each module
	for i, mod := range modules {
		s.logger.Infof("[%d/%d] Processing module: %s", i+1, len(modules), mod.Name)

		if err := s.pullModule(ctx, mod); err != nil {
			return fmt.Errorf("pull module %s: %w", mod.Name, err)
		}
	}

	// Pack modules into bundles
	for _, mod := range modules {
		bundleName := fmt.Sprintf("module-%s.tar", mod.Name)
		moduleLayout := s.layouts.Module(mod.Name)
		if moduleLayout == nil {
			continue
		}

		if err := s.bundlePacker.Pack(ctx, moduleLayout.WorkingDir(), bundleName); err != nil {
			return fmt.Errorf("pack module %s: %w", mod.Name, err)
		}
	}

	return nil
}

func (s *ModulesService) validateAccess(ctx context.Context) error {
	s.logger.Debug("Validating access to modules registry")

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	_, err := s.registry.ListTags(ctx)
	if err != nil {
		if errors.Is(err, client.ErrImageNotFound) {
			return fmt.Errorf("modules not found in registry")
		}
		return fmt.Errorf("list modules: %w", err)
	}

	return nil
}

// moduleInfo holds information about a module to be pulled
type moduleInfo struct {
	Name         string
	RegistryPath string
}

func (s *ModulesService) listAndFilterModules(ctx context.Context) ([]moduleInfo, error) {
	moduleNames, err := s.registry.ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("list module names: %w", err)
	}

	filtered := make([]moduleInfo, 0)
	for _, name := range moduleNames {
		mod := &libmodules.Module{
			Name:         name,
			RegistryPath: s.rootURL + "/modules/" + name,
		}

		if s.opts.Filter.Match(mod) {
			filtered = append(filtered, moduleInfo{
				Name:         name,
				RegistryPath: mod.RegistryPath,
			})
			s.logger.Debugf("Module %s matched filter", name)
		} else {
			s.logger.Debugf("Module %s filtered out", name)
		}
	}

	return filtered, nil
}

func (s *ModulesService) initLayouts(modules []moduleInfo) error {
	s.logger.Info("Creating OCI Image Layouts for modules")

	moduleNames := make([]string, len(modules))
	for i, m := range modules {
		moduleNames[i] = m.Name
	}

	layouts, err := NewModulesLayouts(s.opts.BundleDir, moduleNames)
	if err != nil {
		return fmt.Errorf("create layouts: %w", err)
	}

	s.layouts = layouts
	return nil
}

func (s *ModulesService) pullModule(ctx context.Context, mod moduleInfo) error {
	moduleService := s.registry.Module(mod.Name)
	moduleLayout := s.layouts.Module(mod.Name)
	if moduleLayout == nil {
		return fmt.Errorf("no layout for module %s", mod.Name)
	}

	// Initialize download list for this module
	downloadList := s.downloadList.ForModule(mod.Name)

	// Determine if we should pull release channels
	shouldPullChannels := s.opts.Filter.ShouldMirrorReleaseChannels(mod.Name)

	var versions []string

	if shouldPullChannels && !s.opts.OnlyExtraImages {
		// Pull release channels
		channelVersions, err := s.pullReleaseChannels(ctx, mod.Name, moduleService, moduleLayout, downloadList)
		if err != nil {
			return fmt.Errorf("pull release channels: %w", err)
		}
		versions = append(versions, channelVersions...)
	}

	// Add versions from filter constraints
	filterMod := &libmodules.Module{Name: mod.Name, RegistryPath: mod.RegistryPath}
	filterVersions := s.opts.Filter.VersionsToMirror(filterMod)
	versions = append(versions, filterVersions...)
	versions = dedupeStrings(versions)

	// Pull main module images (unless OnlyExtraImages)
	if !s.opts.OnlyExtraImages && len(versions) > 0 {
		if err := s.pullModuleImages(ctx, mod.Name, versions, moduleService, moduleLayout, downloadList); err != nil {
			return fmt.Errorf("pull module images: %w", err)
		}
	}

	// Pull extra images
	if err := s.pullExtraImages(ctx, mod.Name, versions, moduleService, moduleLayout, downloadList); err != nil {
		return fmt.Errorf("pull extra images: %w", err)
	}

	return nil
}

func (s *ModulesService) pullReleaseChannels(
	ctx context.Context,
	moduleName string,
	moduleService usecase.ModuleService,
	moduleLayout *ModuleLayout,
	downloadList *ModuleDownloadList,
) ([]string, error) {
	versions := make([]string, 0)

	return versions, s.logger.Process(fmt.Sprintf("Pull %s release channels", moduleName), func() error {
		releaseChannelService := moduleService.ReleaseChannels()

		for _, channel := range internal.GetAllDefaultReleaseChannels() {
			ref := s.rootURL + "/modules/" + moduleName + "/release:" + channel
			downloadList.ReleaseChannels[ref] = struct{}{}

			img, err := releaseChannelService.GetImage(ctx, channel)
			if err != nil {
				if errors.Is(err, client.ErrImageNotFound) {
					s.logger.Debugf("Release channel %s not found for %s", channel, moduleName)
					continue
				}
				return fmt.Errorf("get release channel %s: %w", channel, err)
			}

			if err := moduleLayout.ReleaseChannels().AddImage(img, channel); err != nil {
				return fmt.Errorf("add release channel to layout: %w", err)
			}

			// Extract version from release channel image
			version, err := extractVersionFromImage(img)
			if err != nil {
				s.logger.Debugf("Failed to extract version from %s/%s: %v", moduleName, channel, err)
				continue
			}

			if version != "" {
				versions = append(versions, "v"+version)
			}
		}

		return nil
	})
}

func (s *ModulesService) pullModuleImages(
	ctx context.Context,
	moduleName string,
	versions []string,
	moduleService usecase.ModuleService,
	moduleLayout *ModuleLayout,
	downloadList *ModuleDownloadList,
) error {
	return s.logger.Process(fmt.Sprintf("Pull %s images", moduleName), func() error {
		for _, version := range versions {
			ref := s.rootURL + "/modules/" + moduleName + ":" + version
			downloadList.Images[ref] = struct{}{}

			img, err := moduleService.GetImage(ctx, version)
			if err != nil {
				if errors.Is(err, client.ErrImageNotFound) {
					s.logger.Warnf("Module image %s:%s not found", moduleName, version)
					continue
				}
				return fmt.Errorf("get module image %s: %w", version, err)
			}

			if err := moduleLayout.Module().AddImage(img, version); err != nil {
				return fmt.Errorf("add module image to layout: %w", err)
			}
		}

		return nil
	})
}

func (s *ModulesService) pullExtraImages(
	ctx context.Context,
	moduleName string,
	versions []string,
	moduleService usecase.ModuleService,
	moduleLayout *ModuleLayout,
	downloadList *ModuleDownloadList,
) error {
	// Find extra images from module versions
	extraImages := s.findExtraImages(ctx, moduleName, versions, moduleService)

	if len(extraImages) == 0 {
		return nil
	}

	return s.logger.Process(fmt.Sprintf("Pull %s extra images", moduleName), func() error {
		extraService := moduleService.Extra()

		for imageRef := range extraImages {
			downloadList.ExtraImages[imageRef] = struct{}{}

			_, tag := splitModuleRef(imageRef)

			img, err := extraService.GetImage(ctx, tag)
			if err != nil {
				if errors.Is(err, client.ErrImageNotFound) {
					s.logger.Warnf("Extra image %s not found", imageRef)
					continue
				}
				return fmt.Errorf("get extra image %s: %w", imageRef, err)
			}

			if err := moduleLayout.Extra().AddImage(img, tag); err != nil {
				return fmt.Errorf("add extra image to layout: %w", err)
			}
		}

		return nil
	})
}

func (s *ModulesService) findExtraImages(
	ctx context.Context,
	moduleName string,
	versions []string,
	moduleService usecase.ModuleService,
) map[string]struct{} {
	extraImages := make(map[string]struct{})

	for _, version := range versions {
		if strings.Contains(version, "@sha256:") {
			continue
		}

		tag := version
		if strings.Contains(version, ":") {
			parts := strings.SplitN(version, ":", 2)
			tag = parts[1]
		}

		img, err := moduleService.GetImage(ctx, tag)
		if err != nil {
			s.logger.Debugf("Failed to get module image %s:%s for extra images: %v", moduleName, tag, err)
			continue
		}

		extras, err := extractExtraImagesFromModule(img)
		if err != nil {
			continue
		}

		for imageName, imageTag := range extras {
			fullRef := s.rootURL + "/modules/" + moduleName + "/extra/" + imageName + ":" + imageTag
			extraImages[fullRef] = struct{}{}
		}
	}

	return extraImages
}

// Helper functions

type imageExtractor interface {
	Extract() io.ReadCloser
}

func extractVersionFromImage(img imageExtractor) (string, error) {
	rc := img.Extract()
	defer rc.Close()

	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return "", fmt.Errorf("version.json not found")
		}
		if err != nil {
			return "", err
		}

		if hdr.Name == "version.json" {
			var v struct {
				Version string `json:"version"`
			}
			if err := json.NewDecoder(tr).Decode(&v); err != nil {
				return "", err
			}
			return v.Version, nil
		}
	}
}

func extractExtraImagesFromModule(img imageExtractor) (map[string]string, error) {
	rc := img.Extract()
	defer rc.Close()

	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("extra_images.json not found")
		}
		if err != nil {
			return nil, err
		}

		if hdr.Name == "extra_images.json" {
			var raw map[string]interface{}
			if err := json.NewDecoder(tr).Decode(&raw); err != nil {
				return nil, err
			}

			result := make(map[string]string)
			for name, value := range raw {
				switch v := value.(type) {
				case string:
					result[name] = v
				case float64:
					result[name] = fmt.Sprintf("%.0f", v)
				case int:
					result[name] = fmt.Sprintf("%d", v)
				}
			}
			return result, nil
		}
	}
}

func splitModuleRef(ref string) (repo, tag string) {
	for i := len(ref) - 1; i >= 0; i-- {
		if ref[i] == ':' {
			return ref[:i], ref[i+1:]
		}
		if ref[i] == '@' {
			return ref[:i], ref[i:]
		}
	}
	return ref, ""
}

func dedupeStrings(items []string) []string {
	seen := make(map[string]struct{})
	result := make([]string, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item]; !ok {
			seen[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}

