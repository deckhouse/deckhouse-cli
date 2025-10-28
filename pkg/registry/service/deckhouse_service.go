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

// DeckhouseService provides high-level operations for Deckhouse platform management
type DeckhouseService struct {
	client pkg.RegistryClient
	logger *log.Logger
}

// NewDeckhouseService creates a new deckhouse service
func NewDeckhouseService(client pkg.RegistryClient, logger *log.Logger) *DeckhouseService {
	return &DeckhouseService{
		client: client,
		logger: logger,
	}
}

// ListReleases lists all available Deckhouse releases
func (s *DeckhouseService) ListReleases(ctx context.Context) ([]string, error) {
	s.logger.Debug("Listing Deckhouse releases")

	// Assume releases are under a specific path, e.g., scoped to "releases"
	releaseClient := s.client.WithSegment("releases")

	releaseNames, err := releaseClient.ListTags(ctx)
	if err != nil {
		s.logger.Warn("Failed to list releases from registry", "error", err.Error())
		return nil, fmt.Errorf("failed to list releases: %w", err)
	}

	s.logger.Debug("Releases listed successfully", "count", len(releaseNames))

	return releaseNames, nil
}

// GetReleaseInfo gets information about a specific Deckhouse release
func (s *DeckhouseService) GetReleaseInfo(ctx context.Context, releaseTag string) (interface{}, error) {
	s.logger.Debug("Getting release info", "release", releaseTag)

	releaseClient := s.client.WithSegment("releases")

	imageConfig, err := releaseClient.GetImageConfig(ctx, releaseTag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image config: %w", err)
	}

	// Get some label, e.g., version info
	version, exists := imageConfig.Config.Labels["version"]
	if !exists {
		return nil, fmt.Errorf("version info not found")
	}

	return version, nil
}

// ExtractRelease downloads the Deckhouse release image and extracts it to the specified location
func (s *DeckhouseService) ExtractRelease(ctx context.Context, releaseTag, destination string) error {
	s.logger.Debug("Extracting release", "release", releaseTag, "destination", destination)

	if err := os.MkdirAll(destination, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	releaseClient := s.client.WithSegment("releases")
	img, err := releaseClient.GetImage(ctx, releaseTag)
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
func (s *DeckhouseService) extractTar(r io.Reader, destination string) error {
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

// ListReleaseTags lists all available tags for Deckhouse releases
func (s *DeckhouseService) ListReleaseTags(ctx context.Context) ([]string, error) {
	s.logger.Debug("Listing release tags")

	releaseClient := s.client.WithSegment("releases")

	tags, err := releaseClient.ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list release tags: %w", err)
	}

	s.logger.Debug("Release tags listed successfully", "count", len(tags))

	return tags, nil
}
