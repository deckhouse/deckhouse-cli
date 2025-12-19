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
	"github.com/deckhouse/deckhouse/pkg/registry"

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
	// BundleDir is the directory containing the bundle to push
	BundleDir string
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
//	└── modules/                       # Modules
//	    └── <module-name>/
//	        ├── index.json
//	        ├── release/
//	        └── <extra-name>/
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
	return svc.userLogger.Process("Create modules index", func() error {
		return svc.createModulesIndex(ctx, dirPath)
	})
}

// unpackAllPackages unpacks all tar packages from bundle directory into unified directory.
// All packages are unpacked to the same root - the structure inside each tar
// should already have the correct paths.
func (svc *PushService) unpackAllPackages(ctx context.Context, dirPath string) error {
	entries, err := os.ReadDir(svc.options.BundleDir)
	if err != nil {
		return fmt.Errorf("read bundle dir: %w", err)
	}

	packages := svc.findPackages(entries)
	if len(packages) == 0 {
		return fmt.Errorf("no packages found in bundle directory")
	}

	svc.userLogger.Infof("Found %d packages to unpack", len(packages))

	for _, pkgName := range packages {
		if err := svc.unpackPackage(ctx, dirPath, pkgName); err != nil {
			// Log warning but continue with other packages
			svc.userLogger.Warnf("Failed to unpack %s: %v", pkgName, err)
		}
	}

	return nil
}

// findPackages finds all package names (without .tar extension) in the bundle directory.
// It handles both regular .tar files and chunked packages (.tar.chunk000).
func (svc *PushService) findPackages(entries []os.DirEntry) []string {
	packagesSet := make(map[string]struct{})

	for _, entry := range entries {
		name := entry.Name()

		// Handle regular tar files
		if strings.HasSuffix(name, ".tar") {
			pkgName := strings.TrimSuffix(name, ".tar")
			packagesSet[pkgName] = struct{}{}
			continue
		}

		// Handle chunked files (e.g., "platform.tar.chunk000")
		if idx := strings.Index(name, ".tar.chunk"); idx != -1 {
			pkgName := name[:idx]
			packagesSet[pkgName] = struct{}{}
		}
	}

	packages := make([]string, 0, len(packagesSet))
	for pkg := range packagesSet {
		packages = append(packages, pkg)
	}
	slices.Sort(packages)

	return packages
}

// unpackPackage unpacks a single package to the unified directory.
func (svc *PushService) unpackPackage(ctx context.Context, dirPath, pkgName string) error {
	return svc.userLogger.Process(fmt.Sprintf("Unpack %s", pkgName), func() error {
		pkg, err := svc.openPackage(pkgName)
		if err != nil {
			return fmt.Errorf("open package: %w", err)
		}
		defer pkg.Close()

		// Unpack directly to unified directory - no path transformations
		if err := bundle.Unpack(ctx, pkg, dirPath); err != nil {
			return fmt.Errorf("unpack: %w", err)
		}

		return nil
	})
}

// openPackage opens a package file, trying .tar first, then chunked format.
func (svc *PushService) openPackage(pkgName string) (io.ReadCloser, error) {
	tarPath := filepath.Join(svc.options.BundleDir, pkgName+".tar")

	pkg, err := os.Open(tarPath)
	if err == nil {
		return pkg, nil
	}

	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open %s: %w", tarPath, err)
	}

	// Try chunked format
	return chunked.Open(svc.options.BundleDir, pkgName+".tar")
}

// pushAllLayouts recursively walks the directory and pushes each OCI layout found.
// The relative path from root becomes the registry segment.
func (svc *PushService) pushAllLayouts(ctx context.Context, rootDir string) error {
	layouts, err := svc.findLayouts(rootDir)
	if err != nil {
		return fmt.Errorf("scan layouts: %w", err)
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
		return fmt.Errorf("push layout %q: %w", relPath, err)
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
		return fmt.Errorf("read modules directory: %w", err)
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
			return fmt.Errorf("create random image for module %s: %w", moduleName, err)
		}

		// Push with module name as tag
		if err := modulesClient.PushImage(ctx, moduleName, img); err != nil {
			return fmt.Errorf("push module index tag %s: %w", moduleName, err)
		}
	}

	svc.userLogger.Infof("Modules index created successfully")
	return nil
}
