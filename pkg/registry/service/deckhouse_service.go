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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse/pkg/log"
	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/deckhouse/deckhouse-cli/pkg"
)

const (
	releaseSegment           = "release-channel"
	installerSegment         = "install"
	installStandaloneSegment = "install-standalone"
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

// GetRoot gets information about a specific Deckhouse release
func (s *DeckhouseService) GetRoot() string {
	return s.client.GetRegistry()
}

// GetReleaseInfo gets information about a specific Deckhouse release
func (s *DeckhouseService) GetReleaseInfo(ctx context.Context, releaseTag string) (interface{}, error) {
	s.logger.Debug("Getting release info", "release", releaseTag)

	releaseClient := s.client.WithSegment(releaseSegment)

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

func (s *DeckhouseService) GetImage(ctx context.Context, tag string) (pkg.RegistryImage, error) {
	logger := s.logger.With("tag", tag)

	logger.Debug("Getting image")

	img, err := s.client.GetImage(ctx, tag)
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

func (s *DeckhouseService) GetReleaseImage(ctx context.Context, releaseTag string) (pkg.RegistryImage, error) {
	logger := s.logger.With("release", releaseTag)

	logger.Debug("Getting release image")

	releaseClient := s.client.WithSegment(releaseSegment)

	img, err := releaseClient.GetImage(ctx, releaseTag)
	if err != nil {
		return nil, fmt.Errorf("failed to get release image: %w", err)
	}

	logger.Debug("Release image retrieved successfully")

	return img, nil
}

func (s *DeckhouseService) GetInstallerImage(ctx context.Context, tag string) (pkg.RegistryImage, error) {
	logger := s.logger.With("tag", tag)

	logger.Debug("Getting installer image")

	releaseClient := s.client.WithSegment(installerSegment)

	img, err := releaseClient.GetImage(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get installer image: %w", err)
	}

	logger.Debug("Installer image retrieved successfully")

	return img, nil
}

func (s *DeckhouseService) GetInstallStandaloneImage(ctx context.Context, tag string) (pkg.RegistryImage, error) {
	logger := s.logger.With("tag", tag)

	logger.Debug("Getting install standalone image")

	releaseClient := s.client.WithSegment(installStandaloneSegment)

	img, err := releaseClient.GetImage(ctx, tag)
	if err != nil {
		return nil, fmt.Errorf("failed to get install standalone image: %w", err)
	}

	logger.Debug("Install standalone image retrieved successfully")

	return img, nil
}

type DeckhouseReleaseMetadata struct {
	Version string
	Suspend bool
}

func (s *DeckhouseService) GetReleaseMetadata(ctx context.Context, releaseTag string) (*DeckhouseReleaseMetadata, error) {
	logger := s.logger.With("release", releaseTag)

	logger.Debug("Getting release metadata")

	releaseClient := s.client.WithSegment(releaseSegment)

	img, err := releaseClient.GetImage(ctx, releaseTag)
	if err != nil {
		return nil, fmt.Errorf("failed to get image config: %w", err)
	}

	meta, err := extractDeckhouseReleaseMetadata(img.Extract())
	if err != nil {
		return nil, fmt.Errorf("failed to extract release metadata: %w", err)
	}

	return meta, nil
}

// ExtractRelease downloads the Deckhouse release image and extracts it to the specified location
func (s *DeckhouseService) ExtractRelease(ctx context.Context, releaseTag, destination string) error {
	s.logger.Debug("Extracting release", "release", releaseTag, "destination", destination)

	if err := os.MkdirAll(destination, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	releaseClient := s.client.WithSegment(releaseSegment)

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

// ListReleaseTags lists all available tags for Deckhouse releases
func (s *DeckhouseService) ListReleaseTags(ctx context.Context) ([]string, error) {
	s.logger.Debug("Listing release tags")

	releaseClient := s.client.WithSegment(releaseSegment)

	tags, err := releaseClient.ListTags(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list release tags: %w", err)
	}

	s.logger.Debug("Release tags listed successfully", "count", len(tags))

	return tags, nil
}

func (s *DeckhouseService) CheckTagExists(ctx context.Context, tag string) error {
	logger := s.logger.With("tag", tag)

	logger.Debug("Checking if tag exists")

	err := s.client.CheckImageExists(ctx, tag)
	if err != nil {
		return fmt.Errorf("failed to check if tag exists: %w", err)
	}

	s.logger.Debug("Tag existence check completed")

	return nil
}

func (s *DeckhouseService) CheckReleaseExists(ctx context.Context, tag string) error {
	logger := s.logger.With("tag", tag)

	logger.Debug("Checking if release exists")

	releaseClient := s.client.WithSegment(releaseSegment)

	err := releaseClient.CheckImageExists(ctx, tag)
	if err != nil {
		return fmt.Errorf("failed to check if release exists: %w", err)
	}

	s.logger.Debug("Release existence check completed")

	return nil
}
