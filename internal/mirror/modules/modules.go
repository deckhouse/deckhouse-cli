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
	"os"
	"path/filepath"
	"strings"
	"time"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/layouts"
	libmodules "github.com/deckhouse/deckhouse-cli/pkg/libmirror/modules"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	registryservice "github.com/deckhouse/deckhouse-cli/pkg/registry/service"
)

// Options contains configuration options for the modules service
type Options struct {
	// Filter is the module filter (whitelist/blacklist)
	Filter *libmodules.Filter
	// OnlyExtraImages pulls only extra images without main module images
	OnlyExtraImages bool
	// BundleDir is the directory to store the bundle
	BundleDir string
	// BundleChunkSize is the max size of bundle chunks in bytes (0 = no chunking)
	BundleChunkSize int64
}

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

	// options contains service configuration
	options *Options

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
	options *Options,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *Service {
	userLogger.Infof("Creating OCI Image Layouts for Modules")

	if options == nil {
		options = &Options{}
	}

	// Create default filter (blacklist with no items = accept all)
	if options.Filter == nil {
		filter, _ := libmodules.NewFilter(nil, libmodules.FilterTypeBlacklist)
		options.Filter = filter
	}

	rootURL := registryService.GetRoot()

	return &Service{
		workingDir:          workingDir,
		modulesService:      registryService.ModuleService(),
		modulesDownloadList: NewModulesDownloadList(rootURL),
		pullerService:       puller.NewPullerService(logger, userLogger),
		options:             options,
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

// moduleData represents a module with its metadata
type moduleData struct {
	name         string
	registryPath string
}

func (svc *Service) pullModules(ctx context.Context) error {
	logger := svc.userLogger

	tmpDir := filepath.Join(svc.workingDir, "modules")

	// List all available modules
	moduleNames, err := svc.modulesService.ListTags(ctx)
	if err != nil {
		return fmt.Errorf("list modules: %w", err)
	}

	if len(moduleNames) == 0 {
		logger.WarnLn("Modules were not found, check your source repository address and modules path suffix")
		return nil
	}

	// Filter modules according to whitelist/blacklist
	filteredModules := make([]moduleData, 0)
	for _, moduleName := range moduleNames {
		mod := &libmodules.Module{
			Name:         moduleName,
			RegistryPath: filepath.Join(svc.rootURL, "modules", moduleName),
		}
		if svc.options.Filter.Match(mod) {
			filteredModules = append(filteredModules, moduleData{
				name:         moduleName,
				registryPath: mod.RegistryPath,
			})
			logger.Infof("Module found: %s", moduleName)
		} else {
			logger.Debugf("Module %s filtered out", moduleName)
		}
	}

	if len(filteredModules) == 0 {
		logger.WarnLn("No modules matched the filter criteria")
		return nil
	}

	logger.Infof("Repo contains %d modules to pull", len(filteredModules))

	// Create OCI image layouts for filtered modules
	moduleImagesLayout, err := createOCIImageLayoutsForModules(tmpDir, getModuleNames(filteredModules))
	if err != nil {
		return fmt.Errorf("create OCI image layouts for modules: %w", err)
	}
	svc.layout = moduleImagesLayout

	processName := "Pull Modules"
	if svc.options.OnlyExtraImages {
		processName = "Pull Extra Images"
	}

	err = logger.Process(processName, func() error {
		for i, module := range filteredModules {
			logger.Infof("[%d/%d] Processing module: %s", i+1, len(filteredModules), module.name)

			if err := svc.pullSingleModule(ctx, module); err != nil {
				return fmt.Errorf("pull module %s: %w", module.name, err)
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

	// Apply channel aliases if needed (not for OnlyExtraImages mode)
	if !svc.options.OnlyExtraImages {
		for _, module := range filteredModules {
			if err := svc.applyChannelAliases(module.name); err != nil {
				return fmt.Errorf("apply channel aliases for module %s: %w", module.name, err)
			}
		}
	}

	// Pack each module into separate tar
	if err := svc.packModules(filteredModules); err != nil {
		return err
	}

	return nil
}

func (svc *Service) pullSingleModule(ctx context.Context, module moduleData) error {
	logger := svc.userLogger

	// Initialize download list for this module
	downloadList := NewImageDownloadList(filepath.Join(svc.rootURL, "modules", module.name))
	svc.modulesDownloadList.list[module.name] = downloadList

	// Determine which release channels to pull based on filter
	shouldPullReleaseChannels := svc.options.Filter.ShouldMirrorReleaseChannels(module.name)

	// Get module release channel versions for image discovery
	moduleVersions := make([]string, 0)

	if shouldPullReleaseChannels && !svc.options.OnlyExtraImages {
		// Fill release channels
		for _, channel := range internal.GetAllDefaultReleaseChannels() {
			downloadList.ModuleReleaseChannels[svc.rootURL+"/modules/"+module.name+"/release:"+channel] = nil
		}

		// Pull release channels first to get version information
		config := puller.PullConfig{
			Name:             module.name + " release channels",
			ImageSet:         downloadList.ModuleReleaseChannels,
			Layout:           svc.layout.Module(module.name).ModulesReleaseChannels,
			AllowMissingTags: true,
			GetterService:    svc.modulesService.Module(module.name).ReleaseChannels(),
		}

		if err := svc.pullerService.PullImages(ctx, config); err != nil {
			return fmt.Errorf("pull release channels: %w", err)
		}

		// Extract versions from pulled release channels
		moduleVersions = svc.extractVersionsFromReleaseChannels(ctx, module.name)
	}

	// Check for explicit version constraints from filter
	mod := &libmodules.Module{
		Name:         module.name,
		RegistryPath: module.registryPath,
	}

	// Get specific versions to mirror from filter (for whitelist with version constraints)
	filterVersions := svc.options.Filter.VersionsToMirror(mod)
	if len(filterVersions) > 0 {
		moduleVersions = append(moduleVersions, filterVersions...)
	}

	// Deduplicate versions
	moduleVersions = deduplicateStrings(moduleVersions)

	// Skip main module images if only pulling extra images
	if !svc.options.OnlyExtraImages {
		// Fill module images for each version
		for _, version := range moduleVersions {
			downloadList.Module[svc.rootURL+"/modules/"+module.name+":"+version] = nil
		}

		// Pull module images
		if len(downloadList.Module) > 0 {
			config := puller.PullConfig{
				Name:             module.name + " images",
				ImageSet:         downloadList.Module,
				Layout:           svc.layout.Module(module.name).Modules,
				AllowMissingTags: true,
				GetterService:    svc.modulesService.Module(module.name),
			}

			if err := svc.pullerService.PullImages(ctx, config); err != nil {
				return fmt.Errorf("pull module images: %w", err)
			}
		}
	}

	// Extract and pull extra images from module versions
	extraImages, err := svc.findExtraImages(ctx, module.name, moduleVersions)
	if err != nil {
		logger.Warnf("Failed to find extra images for %s: %v", module.name, err)
	}

	if len(extraImages) > 0 {
		for img := range extraImages {
			downloadList.ModuleExtra[img] = nil
		}

		config := puller.PullConfig{
			Name:             module.name + " extra",
			ImageSet:         downloadList.ModuleExtra,
			Layout:           svc.layout.Module(module.name).ModulesExtra,
			AllowMissingTags: true,
			GetterService:    svc.modulesService.Module(module.name).Extra(),
		}

		if err := svc.pullerService.PullImages(ctx, config); err != nil {
			return fmt.Errorf("pull extra images: %w", err)
		}
	}

	// Find and pull VEX images for all module images
	if err := svc.pullVexImages(ctx, module.name, downloadList); err != nil {
		logger.Warnf("Failed to pull VEX images for %s: %v", module.name, err)
	}

	return nil
}

// extractVersionsFromReleaseChannels extracts version tags from pulled release channel images
func (svc *Service) extractVersionsFromReleaseChannels(ctx context.Context, moduleName string) []string {
	versions := make([]string, 0)

	for _, channel := range internal.GetAllDefaultReleaseChannels() {
		img, err := svc.modulesService.Module(moduleName).ReleaseChannels().GetImage(ctx, channel)
		if err != nil {
			svc.logger.Debug(fmt.Sprintf("Failed to get release channel image for %s/%s: %v", moduleName, channel, err))
			continue
		}

		// Extract version.json from release channel image
		versionJSON, err := extractVersionJSON(img)
		if err != nil {
			svc.logger.Debug(fmt.Sprintf("Failed to extract version.json for %s/%s: %v", moduleName, channel, err))
			continue
		}

		if versionJSON.Version != "" {
			versions = append(versions, "v"+versionJSON.Version)
		}
	}

	return versions
}

type versionJSON struct {
	Version string `json:"version"`
}

// extractVersionJSON extracts version.json from an image using the Extract method
func extractVersionJSON(img interface{ Extract() io.ReadCloser }) (*versionJSON, error) {
	rc := img.Extract()
	defer rc.Close()

	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("version.json not found in image")
		}
		if err != nil {
			return nil, err
		}

		if hdr.Name == "version.json" {
			var version versionJSON
			if err := json.NewDecoder(tr).Decode(&version); err != nil {
				return nil, fmt.Errorf("parse version.json: %w", err)
			}
			return &version, nil
		}
	}
}

// findExtraImages finds extra images from module images
func (svc *Service) findExtraImages(ctx context.Context, moduleName string, versions []string) (map[string]struct{}, error) {
	extraImages := make(map[string]struct{})

	for _, version := range versions {
		// Skip digest references
		if strings.Contains(version, "@sha256:") {
			continue
		}

		tag := version
		if strings.Contains(version, ":") {
			parts := strings.SplitN(version, ":", 2)
			tag = parts[1]
		}

		img, err := svc.modulesService.Module(moduleName).GetImage(ctx, tag)
		if err != nil {
			svc.logger.Debug(fmt.Sprintf("Failed to get module image %s:%s: %v", moduleName, tag, err))
			continue
		}

		// Try to extract extra_images.json
		extraImagesJSON, err := extractExtraImagesJSON(img)
		if err != nil {
			continue // No extra_images.json in this version
		}

		for imageName, tagValue := range extraImagesJSON {
			var imageTag string
			switch v := tagValue.(type) {
			case float64:
				imageTag = fmt.Sprintf("%.0f", v)
			case int:
				imageTag = fmt.Sprintf("%d", v)
			case string:
				imageTag = v
			default:
				continue
			}

			fullImagePath := svc.rootURL + "/modules/" + moduleName + "/extra/" + imageName + ":" + imageTag
			extraImages[fullImagePath] = struct{}{}
		}
	}

	return extraImages, nil
}

// extractExtraImagesJSON extracts extra_images.json from an image
func extractExtraImagesJSON(img interface{ Extract() io.ReadCloser }) (map[string]interface{}, error) {
	rc := img.Extract()
	defer rc.Close()

	tr := tar.NewReader(rc)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("extra_images.json not found in image")
		}
		if err != nil {
			return nil, err
		}

		if hdr.Name == "extra_images.json" {
			var extraImages map[string]interface{}
			if err := json.NewDecoder(tr).Decode(&extraImages); err != nil {
				return nil, fmt.Errorf("parse extra_images.json: %w", err)
			}
			return extraImages, nil
		}
	}
}

// pullVexImages finds and pulls VEX attestation images for module images
func (svc *Service) pullVexImages(ctx context.Context, moduleName string, downloadList *ImageDownloadList) error {
	allImages := make([]string, 0)

	for img := range downloadList.Module {
		allImages = append(allImages, img)
	}
	for img := range downloadList.ModuleExtra {
		allImages = append(allImages, img)
	}

	for _, img := range allImages {
		vexImageName, err := svc.findVexImage(ctx, moduleName, img)
		if err != nil {
			svc.logger.Debug(fmt.Sprintf("Failed to find VEX image for %s: %v", img, err))
			continue
		}
		if vexImageName != "" {
			svc.logger.Debug(fmt.Sprintf("Found VEX image: %s", vexImageName))
			downloadList.Module[vexImageName] = nil
		}
	}

	return nil
}

// findVexImage checks if a VEX attestation image exists for the given image
func (svc *Service) findVexImage(ctx context.Context, moduleName string, imageRef string) (string, error) {
	// VEX image reference format: sha256-xxx.att
	vexImageName := strings.Replace(strings.Replace(imageRef, "@sha256:", "@sha256-", 1), "@sha256", ":sha256", 1) + ".att"

	// Extract tag from vex image name
	splitIndex := strings.LastIndex(vexImageName, ":")
	if splitIndex == -1 {
		return "", nil
	}
	tag := vexImageName[splitIndex+1:]

	err := svc.modulesService.Module(moduleName).CheckImageExists(ctx, tag)
	if errors.Is(err, client.ErrImageNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return vexImageName, nil
}

// applyChannelAliases applies release channel tags to images with exact tag constraints
func (svc *Service) applyChannelAliases(moduleName string) error {
	constraint, ok := svc.options.Filter.GetConstraint(moduleName)
	if !ok || !constraint.IsExact() {
		return nil
	}

	exact, ok := constraint.(*libmodules.ExactTagConstraint)
	if !ok {
		return nil
	}

	moduleLayout := svc.layout.Module(moduleName)
	if moduleLayout == nil || moduleLayout.ModulesReleaseChannels == nil {
		return nil
	}

	desc, err := layouts.FindImageDescriptorByTag(moduleLayout.ModulesReleaseChannels.Path(), exact.Tag())
	if err != nil {
		if errors.Is(err, layouts.ErrImageNotFound) {
			return nil
		}
		return err
	}

	if exact.HasChannelAlias() {
		if err := layouts.TagImage(moduleLayout.ModulesReleaseChannels.Path(), desc.Digest, exact.Channel()); err != nil {
			return err
		}
	} else {
		// Tag all channels with this version
		for _, channel := range append(internal.GetAllDefaultReleaseChannels(), internal.LTSChannel) {
			if err := layouts.TagImage(moduleLayout.ModulesReleaseChannels.Path(), desc.Digest, channel); err != nil {
				return err
			}
		}
	}

	return nil
}

func (svc *Service) packModules(modules []moduleData) error {
	logger := svc.userLogger

	bundleDir := svc.options.BundleDir
	bundleChunkSize := svc.options.BundleChunkSize

	for _, module := range modules {
		pkgName := "module-" + module.name + ".tar"

		if err := logger.Process(fmt.Sprintf("Pack %s", pkgName), func() error {
			moduleLayout := svc.layout.Module(module.name)
			if moduleLayout == nil {
				return fmt.Errorf("no layout found for module %s", module.name)
			}

			var pkg io.Writer = chunked.NewChunkedFileWriter(bundleChunkSize, bundleDir, pkgName)
			if bundleChunkSize == 0 {
				f, err := os.Create(filepath.Join(bundleDir, pkgName))
				if err != nil {
					return fmt.Errorf("create %s: %w", pkgName, err)
				}
				pkg = f
			}

			// Pack from the module's working directory
			moduleDir := filepath.Join(svc.layout.workingDir, module.name)
			if err := bundle.Pack(context.Background(), moduleDir, pkg); err != nil {
				return fmt.Errorf("pack module %s: %w", pkgName, err)
			}

			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func getModuleNames(modules []moduleData) []string {
	names := make([]string, len(modules))
	for i, m := range modules {
		names[i] = m.name
	}
	return names
}

func deduplicateStrings(items []string) []string {
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
