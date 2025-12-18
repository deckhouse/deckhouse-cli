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

package mirror

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/go-containerregistry/pkg/v1/layout"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/pusher"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

const (
	defaultDirPermissions = 0755

	platformPackage = "platform"
	securityPackage = "security"
	modulesPrefix   = "module-"
)

// PushServiceOptions contains configuration options for PushService
type PushServiceOptions struct {
	// BundleDir is the directory containing the bundle to push
	BundleDir string
	// WorkingDir is the temporary directory for unpacking bundles
	WorkingDir string
}

// PushService orchestrates pushing Deckhouse components to registry
type PushService struct {
	client     registry.Client
	options    *PushServiceOptions
	pusher     *pusher.Service
	logger     *dkplog.Logger
	userLogger *log.SLogger
}

// NewPushService creates a new PushService
func NewPushService(
	client registry.Client,
	options *PushServiceOptions,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *PushService {
	if options == nil {
		options = &PushServiceOptions{}
	}

	return &PushService{
		client:     client,
		options:    options,
		pusher:     pusher.NewService(logger, userLogger),
		logger:     logger,
		userLogger: userLogger,
	}
}

// Push uploads Deckhouse components to registry
// It unpacks all packages into a unified directory structure and pushes
// each OCI layout based on its path (path = registry segment)
func (svc *PushService) Push(ctx context.Context) error {
	// Create unified directory for unpacking
	unifiedDir := filepath.Join(svc.options.WorkingDir, "unified")
	if err := os.MkdirAll(unifiedDir, defaultDirPermissions); err != nil {
		return fmt.Errorf("create unified directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(unifiedDir); err != nil {
			svc.logger.Warn("Failed to cleanup unified directory",
				slog.String("path", unifiedDir),
				slog.Any("error", err))
		}
	}()

	// Unpack all packages into unified structure
	if err := svc.unpackPlatform(ctx, unifiedDir); err != nil {
		return fmt.Errorf("unpack platform: %w", err)
	}

	if err := svc.unpackSecurity(ctx, unifiedDir); err != nil {
		return fmt.Errorf("unpack security: %w", err)
	}

	if err := svc.unpackModules(ctx, unifiedDir); err != nil {
		return fmt.Errorf("unpack modules: %w", err)
	}

	// Push all layouts recursively
	return svc.userLogger.Process("Push to registry", func() error {
		return svc.pushDir(ctx, unifiedDir, svc.client)
	})
}

// pushDir recursively walks directory and pushes each OCI layout
// The relative path from root becomes the registry segment
func (svc *PushService) pushDir(ctx context.Context, rootDir string, client registry.Client) error {
	var layouts []string

	// First, collect all layouts
	err := filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() != "index.json" {
			return nil
		}
		layouts = append(layouts, filepath.Dir(path))
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan layouts: %w", err)
	}

	if len(layouts) == 0 {
		svc.userLogger.InfoLn("No layouts to push")
		return nil
	}

	// Sort for predictable output
	slices.Sort(layouts)

	svc.userLogger.Infof("Found %d layouts to push", len(layouts))

	// Push each layout
	for _, layoutDir := range layouts {
		// Skip empty layouts (no images)
		hasImages, err := svc.layoutHasImages(layoutDir)
		if err != nil {
			svc.logger.Warn("Failed to check layout", slog.String("path", layoutDir), slog.Any("error", err))
			continue
		}
		if !hasImages {
			continue
		}

		relPath, _ := filepath.Rel(rootDir, layoutDir)

		// Build registry segment from relative path
		segment := ""
		if relPath != "." {
			segment = relPath
		}

		targetClient := client
		if segment != "" {
			// WithSegment expects single segment, but we have path like "modules/virtualization"
			// Split and apply each segment
			for _, seg := range strings.Split(segment, string(os.PathSeparator)) {
				targetClient = targetClient.WithSegment(seg)
			}
		}

		svc.userLogger.Infof("Pushing %s", targetClient.GetRegistry())

		if err := svc.pusher.PushLayout(ctx, layout.Path(layoutDir), targetClient); err != nil {
			return fmt.Errorf("push layout %q: %w", relPath, err)
		}
	}

	return nil
}

// layoutHasImages checks if an OCI layout has any images to push
func (svc *PushService) layoutHasImages(layoutDir string) (bool, error) {
	layoutPath := layout.Path(layoutDir)
	index, err := layoutPath.ImageIndex()
	if err != nil {
		return false, fmt.Errorf("read index: %w", err)
	}

	indexManifest, err := index.IndexManifest()
	if err != nil {
		return false, fmt.Errorf("parse index manifest: %w", err)
	}

	return len(indexManifest.Manifests) > 0, nil
}

// unpackPlatform unpacks platform.tar to root of unified directory
// platform.tar contains: index.json, install/, install-standalone/, release-channel/
func (svc *PushService) unpackPlatform(ctx context.Context, unifiedDir string) error {
	if !svc.pusher.PackageExists(svc.options.BundleDir, platformPackage) {
		svc.userLogger.InfoLn("Platform package not found, skipping")
		return nil
	}

	return svc.userLogger.Process("Unpack platform", func() error {
		pkg, err := svc.pusher.OpenPackage(svc.options.BundleDir, platformPackage)
		if err != nil {
			return fmt.Errorf("open package: %w", err)
		}
		defer pkg.Close()

		// Platform unpacks directly to root
		if err := bundle.Unpack(ctx, pkg, unifiedDir); err != nil {
			return fmt.Errorf("unpack: %w", err)
		}
		return nil
	})
}

// unpackSecurity unpacks security.tar to unified directory
// security.tar contains: security/trivy-db/, security/trivy-java-db/, etc.
// These paths already include "security/" prefix, so unpack to root
func (svc *PushService) unpackSecurity(ctx context.Context, unifiedDir string) error {
	if !svc.pusher.PackageExists(svc.options.BundleDir, securityPackage) {
		svc.userLogger.InfoLn("Security package not found, skipping")
		return nil
	}

	return svc.userLogger.Process("Unpack security", func() error {
		pkg, err := svc.pusher.OpenPackage(svc.options.BundleDir, securityPackage)
		if err != nil {
			return fmt.Errorf("open package: %w", err)
		}
		defer pkg.Close()

		// Security tar already has security/ prefix inside
		if err := bundle.Unpack(ctx, pkg, unifiedDir); err != nil {
			return fmt.Errorf("unpack: %w", err)
		}
		return nil
	})
}

// unpackModules unpacks all module-*.tar files
// Each module tar contains: module/, release/, extra/
// We need to transform paths:
//   - module/ -> modules/{name}/
//   - release/ -> modules/{name}/release/
//   - extra/ -> modules/{name}/extra/
func (svc *PushService) unpackModules(ctx context.Context, unifiedDir string) error {
	// Find all module packages
	modulePackages, err := svc.findModulePackages()
	if err != nil {
		return fmt.Errorf("find module packages: %w", err)
	}

	if len(modulePackages) == 0 {
		svc.userLogger.InfoLn("No module packages found, skipping")
		return nil
	}

	slices.Sort(modulePackages)
	svc.userLogger.Infof("Found %d module packages", len(modulePackages))

	for _, moduleName := range modulePackages {
		if err := svc.unpackModule(ctx, unifiedDir, moduleName); err != nil {
			return fmt.Errorf("unpack module %s: %w", moduleName, err)
		}
	}

	return nil
}

func (svc *PushService) unpackModule(ctx context.Context, unifiedDir, moduleName string) error {
	packageName := modulesPrefix + moduleName

	return svc.userLogger.Process(fmt.Sprintf("Unpack module %s", moduleName), func() error {
		pkg, err := svc.pusher.OpenPackage(svc.options.BundleDir, packageName)
		if err != nil {
			return fmt.Errorf("open package: %w", err)
		}
		defer pkg.Close()

		// Unpack to temp directory first
		tempDir := filepath.Join(svc.options.WorkingDir, "temp-"+moduleName)
		if err := os.MkdirAll(tempDir, defaultDirPermissions); err != nil {
			return fmt.Errorf("create temp dir: %w", err)
		}
		defer os.RemoveAll(tempDir)

		if err := bundle.Unpack(ctx, pkg, tempDir); err != nil {
			return fmt.Errorf("unpack: %w", err)
		}

		// Transform paths and move to unified directory
		// module/ -> modules/{name}/
		// release/ -> modules/{name}/release/
		// extra/ -> modules/{name}/extra/
		targetDir := filepath.Join(unifiedDir, "modules", moduleName)
		if err := os.MkdirAll(targetDir, defaultDirPermissions); err != nil {
			return fmt.Errorf("create target dir: %w", err)
		}

		// Move module/ contents to modules/{name}/
		moduleDir := filepath.Join(tempDir, "module")
		if _, err := os.Stat(moduleDir); err == nil {
			if err := copyDir(moduleDir, targetDir); err != nil {
				return fmt.Errorf("copy module layout: %w", err)
			}
		}

		// Move release/ to modules/{name}/release/
		releaseDir := filepath.Join(tempDir, "release")
		if _, err := os.Stat(releaseDir); err == nil {
			targetRelease := filepath.Join(targetDir, "release")
			if err := copyDir(releaseDir, targetRelease); err != nil {
				return fmt.Errorf("copy release layout: %w", err)
			}
		}

		// Move extra/ to modules/{name}/extra/
		extraDir := filepath.Join(tempDir, "extra")
		if _, err := os.Stat(extraDir); err == nil {
			targetExtra := filepath.Join(targetDir, "extra")
			if err := copyDir(extraDir, targetExtra); err != nil {
				return fmt.Errorf("copy extra layout: %w", err)
			}
		}

		return nil
	})
}

func (svc *PushService) findModulePackages() ([]string, error) {
	entries, err := os.ReadDir(svc.options.BundleDir)
	if err != nil {
		return nil, fmt.Errorf("read bundle dir: %w", err)
	}

	modules := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, modulesPrefix) {
			continue
		}

		// Extract module name: "module-virtualization.tar" -> "virtualization"
		moduleName := strings.TrimPrefix(name, modulesPrefix)
		moduleName = strings.TrimSuffix(moduleName, ".tar")

		// Handle chunked files: "module-virtualization.tar.chunk000" -> "virtualization"
		if idx := strings.Index(moduleName, ".tar.chunk"); idx != -1 {
			moduleName = moduleName[:idx]
		}

		modules = append(modules, moduleName)
	}

	// Deduplicate (in case of chunked files)
	slices.Sort(modules)
	return slices.Compact(modules), nil
}

// copyDir copies directory contents recursively
func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, _ := filepath.Rel(src, path)
		targetPath := filepath.Join(dst, relPath)

		if d.IsDir() {
			return os.MkdirAll(targetPath, defaultDirPermissions)
		}

		// Copy file
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}

		return os.WriteFile(targetPath, data, 0644)
	})
}
