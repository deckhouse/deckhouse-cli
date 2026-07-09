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
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/random"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	client "github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/pusher"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

const (
	dirPermissions = 0755
)

// PushServiceOptions contains configuration options for PushService
type PushServiceOptions struct {
	// Packages is the list of tar/chunked package archive paths to push.
	Packages []string
	// WorkingDir is the temporary directory for unpacking bundles
	WorkingDir string
}

// PushService handles pushing OCI layouts to registry.
// It treats the layout structure as the source of truth - the relative path of each layout
// becomes the registry segment directly.
//
// Expected layout structure (after unpack):
//
//	<root>/
//	├── index.json                     # Deckhouse main images
//	├── blobs/
//	├── install/                       # Deckhouse Install
//	│   ├── index.json
//	│   └── blobs/
//	├── install-standalone/            # Deckhouse Standalone Install
//	├── release-channel/               # Deckhouse release channels
//	├── security/                      # Security databases
//	│   ├── trivy-db/
//	│   ├── trivy-bdu/
//	│   ├── trivy-java-db/
//	│   └── trivy-checks/
//	├── modules/                       # Modules
//	│   └── <module-name>/
//	│       ├── index.json
//	│       ├── release/
//	│       └── <extra-name>/
//	└── packages/                      # Packages
//	    └── <package-name>/
//	        ├── index.json
//	        ├── version/
//	        └── <extra-name>/
type PushService struct {
	client     client.Client
	options    *PushServiceOptions
	pusher     *pusher.Service
	logger     *dkplog.Logger
	userLogger *log.SLogger
}

// NewPushService creates a new PushService
func NewPushService(
	client client.Client,
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

// Push uploads all OCI layouts from the bundle to the registry.
// It unpacks all packages into a unified directory and pushes each layout
// using its relative path as the registry segment.
//
// The key principle: no path transformations. Whatever path the layout has
// in the unpacked directory becomes its path in the registry.
func (svc *PushService) Push(ctx context.Context) error {
	// Create unified directory for unpacking
	dirPath := filepath.Join(svc.options.WorkingDir, "unified")
	if err := os.MkdirAll(dirPath, dirPermissions); err != nil {
		return fmt.Errorf("create unified directory: %w", err)
	}

	defer func() {
		if err := os.RemoveAll(dirPath); err != nil {
			svc.logger.Warn("Failed to cleanup unified directory",
				slog.String("path", dirPath),
				slog.Any("error", err))
		}
	}()

	// Unpack all packages into unified structure
	if err := svc.unpackAllPackages(ctx, dirPath); err != nil {
		return fmt.Errorf("unpack packages: %w", err)
	}

	// Push all layouts recursively
	if err := svc.userLogger.Process("Push to registry", func() error {
		return svc.pushAllLayouts(ctx, dirPath)
	}); err != nil {
		return err
	}

	// Create modules index (deckhouse/modules:<module-name> tags for discovery)
	if err := svc.userLogger.Process("Create modules index", func() error {
		return svc.createModulesIndex(ctx, dirPath)
	}); err != nil {
		return err
	}

	// Create packages index (deckhouse/packages:<package-name> tags for discovery)
	return svc.userLogger.Process("Create packages index", func() error {
		return svc.createPackagesIndex(ctx, dirPath)
	})
}

// unpackAllPackages unpacks all tar packages into the unified directory.
// All packages are unpacked to the same root - the structure inside each tar
// should already have the correct paths.
func (svc *PushService) unpackAllPackages(ctx context.Context, dirPath string) error {
	if len(svc.options.Packages) == 0 {
		return fmt.Errorf("no packages found to push")
	}

	packages := slices.Clone(svc.options.Packages)
	slices.Sort(packages)
	// Drop duplicate archive paths so the same package is not unpacked twice
	// (which would redo work and overwrite files in the unified directory).
	packages = slices.Compact(packages)

	svc.userLogger.Infof("Found %d packages to unpack", len(packages))

	for _, pkgPath := range packages {
		pkgName := packageNameFromPath(pkgPath)
		if err := svc.unpackPackage(ctx, dirPath, pkgPath, pkgName); err != nil {
			// Log warning but continue with other packages
			svc.userLogger.Warnf("Failed to unpack %s: %v", pkgName, err)
		}
	}

	return nil
}

// packageNameFromPath derives the package name (used for legacy module detection
// during unpack) from a package archive path by stripping its directory and the
// .tar suffix. Callers must pass canonical .tar paths - chunked packages are
// already collapsed to their .tar name before reaching this point.
func packageNameFromPath(pkgPath string) string {
	return strings.TrimSuffix(filepath.Base(pkgPath), ".tar")
}

// unpackPackage unpacks a single package archive into the unified directory.
func (svc *PushService) unpackPackage(ctx context.Context, dirPath, pkgPath, pkgName string) error {
	return svc.userLogger.Process(fmt.Sprintf("Unpack %s", pkgName), func() error {
		pkg, err := openPackage(pkgPath)
		if err != nil {
			return fmt.Errorf("open package: %w", err)
		}
		defer pkg.Close()

		// Unpack directly to unified directory
		if err := bundle.Unpack(ctx, pkg, dirPath, pkgName); err != nil {
			return fmt.Errorf("unpack: %w", err)
		}

		return nil
	})
}

// openPackage opens a package archive by path, trying the path as a plain .tar
// first, then falling back to the chunked format.
func openPackage(pkgPath string) (io.ReadCloser, error) {
	pkg, err := os.Open(pkgPath)
	if err == nil {
		return pkg, nil
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open tar package %q: %w", pkgPath, err)
	}

	// Try chunked format: chunks live next to the package as <name>.tar.NNNN.chunk
	return chunked.Open(filepath.Dir(pkgPath), filepath.Base(pkgPath))
}

// pushAllLayouts recursively walks the directory and pushes each OCI layout found.
// The relative path from root becomes the registry segment.
func (svc *PushService) pushAllLayouts(ctx context.Context, rootDir string) error {
	layouts, err := svc.findLayouts(rootDir)
	if err != nil {
		return fmt.Errorf("scan layouts in %q: %w", rootDir, err)
	}

	if len(layouts) == 0 {
		svc.userLogger.InfoLn("No layouts to push")
		return nil
	}

	svc.userLogger.Infof("Found %d layouts to push", len(layouts))

	for _, layoutDir := range layouts {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := svc.pushSingleLayout(ctx, rootDir, layoutDir); err != nil {
			return err
		}
	}

	return nil
}

// findLayouts finds all OCI layouts in the directory by looking for index.json files.
func (svc *PushService) findLayouts(rootDir string) ([]string, error) {
	var layouts []string

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
		return nil, err
	}

	slices.Sort(layouts)

	return layouts, nil
}

// pushSingleLayout pushes a single OCI layout to the registry.
func (svc *PushService) pushSingleLayout(ctx context.Context, rootDir, layoutDir string) error {
	// Check if layout has any images
	hasImages, err := svc.layoutHasImages(layoutDir)
	if err != nil {
		svc.logger.Warn("Failed to check layout",
			slog.String("path", layoutDir),
			slog.Any("error", err))

		return nil
	}

	if !hasImages {
		return nil
	}

	// Build registry segment from relative path
	relPath, _ := filepath.Rel(rootDir, layoutDir)

	segment := ""
	if relPath != "." {
		segment = relPath
	}
	// support old behavior when modules stored as "module-<name>.tar"
	if strings.HasPrefix(layoutDir, "module-") {
		segment = internal.ModulesSegment
	}

	// Create client with appropriate segments
	targetClient := svc.client

	if segment != "" {
		// Apply each path component as a segment
		for _, seg := range strings.Split(segment, string(os.PathSeparator)) {
			targetClient = targetClient.WithSegment(seg)
		}
	}

	svc.userLogger.Infof("Pushing %s", targetClient.GetRegistry())

	if err := svc.pusher.PushLayout(ctx, layout.Path(layoutDir), targetClient); err != nil {
		return fmt.Errorf("push layout %q to registry %s: %w", relPath, targetClient.GetRegistry(), err)
	}

	return nil
}

// layoutHasImages checks if an OCI layout has any images to push.
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

// createModulesIndex creates the modules index in the registry.
// This pushes a small random image for each module with tag = module name
// to deckhouse/modules repo, enabling module discovery via ListTags.
func (svc *PushService) createModulesIndex(ctx context.Context, rootDir string) error {
	modulesDir := filepath.Join(rootDir, internal.ModulesSegment)

	// Check if modules directory exists
	entries, err := os.ReadDir(modulesDir)
	if err != nil {
		if os.IsNotExist(err) {
			svc.userLogger.InfoLn("No modules directory found, skipping modules index")
			return nil
		}

		return fmt.Errorf("read modules directory %q: %w", modulesDir, err)
	}

	// Find all module directories
	var moduleNames []string

	for _, entry := range entries {
		if entry.IsDir() {
			moduleNames = append(moduleNames, entry.Name())
		}
	}

	if len(moduleNames) == 0 {
		svc.userLogger.InfoLn("No modules found, skipping modules index")
		return nil
	}

	slices.Sort(moduleNames)
	svc.userLogger.Infof("Creating modules index with %d modules", len(moduleNames))

	// Get client scoped to modules repo
	modulesClient := svc.client.WithSegment(internal.ModulesSegment)

	// Push a small random image for each module with tag = module name
	for _, moduleName := range moduleNames {
		if err := ctx.Err(); err != nil {
			return err
		}

		svc.userLogger.Infof("Creating index tag: %s:%s", modulesClient.GetRegistry(), moduleName)

		// Create minimal random image (32 bytes, 1 layer)
		img, err := random.Image(32, 1)
		if err != nil {
			return fmt.Errorf("create random image for module discovery tag %s: %w", moduleName, err)
		}

		// Push with module name as tag
		if err := modulesClient.PushImage(ctx, moduleName, img); err != nil {
			return fmt.Errorf("push module index tag %s to registry %s: %w", moduleName, modulesClient.GetRegistry(), err)
		}
	}

	svc.userLogger.Infof("Modules index created successfully")

	return nil
}

// createPackagesIndex creates the packages index in the registry.
// This pushes a small random image for each package with tag = package name
// to deckhouse/packages repo, enabling package discovery via ListTags.
func (svc *PushService) createPackagesIndex(ctx context.Context, rootDir string) error {
	packagesDir := filepath.Join(rootDir, internal.PackagesSegment)

	// Check if packages directory exists
	entries, err := os.ReadDir(packagesDir)
	if err != nil {
		if os.IsNotExist(err) {
			svc.userLogger.InfoLn("No packages directory found, skipping packages index")
			return nil
		}

		return fmt.Errorf("read packages directory %q: %w", packagesDir, err)
	}

	// Find all package directories
	var packageNames []string

	for _, entry := range entries {
		if entry.IsDir() {
			packageNames = append(packageNames, entry.Name())
		}
	}

	if len(packageNames) == 0 {
		svc.userLogger.InfoLn("No packages found, skipping packages index")
		return nil
	}

	slices.Sort(packageNames)
	svc.userLogger.Infof("Creating packages index with %d packages", len(packageNames))

	// Get client scoped to packages repo
	packagesClient := svc.client.WithSegment(internal.PackagesSegment)

	// Push a small random image for each package with tag = package name
	for _, packageName := range packageNames {
		if err := ctx.Err(); err != nil {
			return err
		}

		svc.userLogger.Infof("Creating index tag: %s:%s", packagesClient.GetRegistry(), packageName)

		// Create minimal random image (32 bytes, 1 layer)
		img, err := random.Image(32, 1)
		if err != nil {
			return fmt.Errorf("create random image for package discovery tag %s: %w", packageName, err)
		}

		// Push with package name as tag
		if err := packagesClient.PushImage(ctx, packageName, img); err != nil {
			return fmt.Errorf("push package index tag %s to registry %s: %w", packageName, packagesClient.GetRegistry(), err)
		}
	}

	svc.userLogger.Infof("Packages index created successfully")

	return nil
}
