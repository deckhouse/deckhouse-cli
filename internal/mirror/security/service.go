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
	"time"

	"github.com/deckhouse/deckhouse/pkg/registry/client"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
)

// Compile-time interface check
var _ usecase.SecurityPuller = (*SecurityService)(nil)

// SecurityService handles pulling security database images using Clean Architecture
type SecurityService struct {
	// Dependencies (injected via interfaces)
	registry     usecase.SecurityRegistryService
	rootURL      string
	bundlePacker usecase.BundlePacker
	logger       usecase.Logger

	// Internal state
	layout       *SecurityLayouts
	downloadList *SecurityDownloadList

	// Configuration
	opts *usecase.SecurityOpts
}

// NewSecurityService creates a new security service with injected dependencies
func NewSecurityService(
	registry usecase.DeckhouseRegistryService,
	bundlePacker usecase.BundlePacker,
	logger usecase.Logger,
	opts *usecase.SecurityOpts,
) *SecurityService {
	if opts == nil {
		opts = &usecase.SecurityOpts{}
	}

	rootURL := registry.GetRoot()

	return &SecurityService{
		registry:     registry.Security(),
		rootURL:      rootURL,
		bundlePacker: bundlePacker,
		logger:       logger,
		downloadList: NewSecurityDownloadList(rootURL),
		opts:         opts,
	}
}

// Pull implements usecase.SecurityPuller
func (s *SecurityService) Pull(ctx context.Context) error {
	// Initialize layouts
	if err := s.initLayouts(); err != nil {
		return fmt.Errorf("init layouts: %w", err)
	}

	// Validate access to registry
	if err := s.validateAccess(ctx); err != nil {
		// If security databases are not available, just warn and continue
		s.logger.Warnf("Security databases not available: %v", err)
		return nil
	}

	// Fill download list
	s.downloadList.Fill()

	// Pull images
	if err := s.pullAllImages(ctx); err != nil {
		return fmt.Errorf("pull images: %w", err)
	}

	// Pack bundle
	if err := s.bundlePacker.Pack(ctx, s.layout.WorkingDir(), "security.tar"); err != nil {
		return fmt.Errorf("pack bundle: %w", err)
	}

	return nil
}

func (s *SecurityService) initLayouts() error {
	s.logger.Info("Creating OCI Image Layouts for security databases")

	layouts, err := NewSecurityLayouts(s.opts.BundleDir)
	if err != nil {
		return fmt.Errorf("create layouts: %w", err)
	}

	s.layout = layouts
	return nil
}

func (s *SecurityService) validateAccess(ctx context.Context) error {
	s.logger.Debug("Validating access to security registry")

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// Check if trivy-db exists (primary security database)
	if err := s.registry.Database(internal.SecurityTrivyDBSegment).CheckImageExists(ctx, "2"); err != nil {
		if errors.Is(err, client.ErrImageNotFound) {
			return fmt.Errorf("trivy-db not found in registry")
		}
		return fmt.Errorf("check trivy-db: %w", err)
	}

	return nil
}

func (s *SecurityService) pullAllImages(ctx context.Context) error {
	databases := []struct {
		name   string
		layout securityLayoutGetter
	}{
		{internal.SecurityTrivyDBSegment, func() *securityImageLayout { return s.layout.TrivyDB() }},
		{internal.SecurityTrivyBDUSegment, func() *securityImageLayout { return s.layout.TrivyBDU() }},
		{internal.SecurityTrivyJavaDBSegment, func() *securityImageLayout { return s.layout.TrivyJavaDB() }},
		{internal.SecurityTrivyChecksSegment, func() *securityImageLayout { return s.layout.TrivyChecks() }},
	}

	for _, db := range databases {
		if err := s.pullSecurityDatabase(ctx, db.name, db.layout()); err != nil {
			// Log warning but continue with other databases
			s.logger.Warnf("Failed to pull %s: %v", db.name, err)
		}
	}

	return nil
}

type securityLayoutGetter func() *securityImageLayout

func (s *SecurityService) pullSecurityDatabase(ctx context.Context, dbName string, layout *securityImageLayout) error {
	return s.logger.Process(fmt.Sprintf("Pull %s", dbName), func() error {
		imageRefs, ok := s.downloadList.Databases[dbName]
		if !ok || len(imageRefs) == 0 {
			s.logger.Debugf("No images to pull for %s", dbName)
			return nil
		}

		dbService := s.registry.Database(dbName)

		for ref := range imageRefs {
			_, tag := splitSecurityRef(ref)

			s.logger.Infof("Pulling %s:%s", dbName, tag)

			img, err := dbService.GetImage(ctx, tag)
			if err != nil {
				if errors.Is(err, client.ErrImageNotFound) {
					s.logger.Warnf("Image %s:%s not found, skipping", dbName, tag)
					continue
				}
				return fmt.Errorf("get image %s:%s: %w", dbName, tag, err)
			}

			if err := layout.imageLayout.AddImage(img, tag); err != nil {
				return fmt.Errorf("add image to layout: %w", err)
			}
		}

		return nil
	})
}

func splitSecurityRef(ref string) (repo, tag string) {
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

