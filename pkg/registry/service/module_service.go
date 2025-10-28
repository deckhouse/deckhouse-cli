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
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/pkg"
)

// ModuleService provides high-level operations for module management
type ModuleService struct {
	client pkg.RegistryClient
	logger *log.Logger
}

// NewModuleService creates a new module service
func NewModuleService(client pkg.RegistryClient, logger *log.Logger) *ModuleService {
	return &ModuleService{
		client: client,
		logger: logger,
	}
}

// ListModules lists all available module names from the registry
func (s *ModuleService) ListModules(ctx context.Context) ([]string, error) {
	s.logger.Debug("Listing all modules")

	moduleNames, err := s.client.ListRepositories(ctx)
	if err != nil {
		s.logger.Warn("Failed to list repositories from registry", "error", err.Error())
		return nil, fmt.Errorf("failed to list repositories: %w", err)
	}

	s.logger.Debug("Modules listed successfully", "count", len(moduleNames))

	return moduleNames, nil
}

// GetModuleInfo gets information about a specific module
func (s *ModuleService) GetModuleInfo(ctx context.Context, moduleName, tag string) (interface{}, error) {
	s.logger.Debug("Getting module info", "module", moduleName, "tag", tag)

	moduleClient := s.client.WithSegment(moduleName)

	// Get some label, e.g., disable.message
	info, exists, err := moduleClient.GetLabel(ctx, tag, "disable.message")
	if err != nil {
		return nil, fmt.Errorf("failed to get module info: %w", err)
	}

	if !exists {
		return nil, fmt.Errorf("module info not found")
	}

	return info, nil
}

// ExtractModule downloads the module image and extracts it to the specified location
func (s *ModuleService) ExtractModule(ctx context.Context, moduleName, tag, destination string) error {
	s.logger.Debug("Extracting module", "module", moduleName, "tag", tag, "destination", destination)

	if err := os.MkdirAll(destination, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	moduleClient := s.client.WithSegment(moduleName)

	img, err := moduleClient.GetImage(ctx, tag)
	if err != nil {
		return fmt.Errorf("failed to get image: %w", err)
	}

	err = s.extractTar(img.Extract(), destination)
	if err != nil {
		return fmt.Errorf("failed to extract tar: %w", err)
	}

	return nil
}

// extractTar extracts a tar archive to the destination directory
func (s *ModuleService) extractTar(r io.Reader, destination string) error {
	tr := tar.NewReader(r)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		target := filepath.Join(destination, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", target, err)
			}
		case tar.TypeReg:
			file, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", target, err)
			}
			if _, err := io.Copy(file, tr); err != nil {
				file.Close()
				return fmt.Errorf("failed to write file %s: %w", target, err)
			}
			file.Close()
		}
	}
	return nil
}

// ListModuleTags lists all available tags for a specific module
func (s *ModuleService) ListModuleTags(ctx context.Context, moduleName string) ([]string, error) {
	s.logger.Debug("Listing module tags", "module", moduleName)

	moduleClient := s.client.WithSegment(moduleName)

	tags, err := moduleClient.ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list tags for module %s: %w", moduleName, err)
	}

	s.logger.Debug("Module tags listed successfully", "module", moduleName, "count", len(tags))

	return tags, nil
}
