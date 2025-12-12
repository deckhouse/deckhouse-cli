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

package pusher

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/go-containerregistry/pkg/v1/layout"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/errorutil"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
)

const (
	defaultDirPermissions = 0755
	pushRetryAttempts     = 4
	pushRetryDelay        = 3 * time.Second
)

// LayoutMapping defines the mapping between bundle layout path and registry segment
type LayoutMapping struct {
	// LayoutPath is the path within the unpacked package (e.g., "module", "release")
	LayoutPath string
	// Segment is the registry path segment to push to (e.g., "", "release")
	Segment string
}

// PackagePushConfig defines the configuration for pushing a package
type PackagePushConfig struct {
	// PackageName is the name of the package file (without .tar extension)
	PackageName string
	// ProcessName is the name shown in logs (e.g., "Push Deckhouse platform")
	ProcessName string
	// WorkingDir is the temp directory for unpacking
	WorkingDir string
	// BundleDir is the directory containing the bundle
	BundleDir string
	// Client is the registry client to use for pushing
	Client registry.Client
	// MandatoryLayouts returns paths that must exist after unpacking (for validation)
	// Key is description, value is path. Used if MandatoryLayoutsFunc is nil.
	MandatoryLayouts map[string]string
	// MandatoryLayoutsFunc dynamically builds the mandatory layouts for validation
	// If set, MandatoryLayouts field is ignored. packageDir is the unpacked path.
	MandatoryLayoutsFunc func(packageDir string) map[string]string
	// Layouts is the static list of layouts to push (used if LayoutsFunc is nil)
	Layouts []LayoutMapping
	// LayoutsFunc dynamically builds the layouts list after unpacking
	// If set, Layouts field is ignored. packageDir is the unpacked path.
	LayoutsFunc func(packageDir string) []LayoutMapping
}

// Service handles the pushing of images to the registry
type Service struct {
	logger     *dkplog.Logger
	userLogger *log.SLogger
}

// NewService creates a new pusher service
func NewService(logger *dkplog.Logger, userLogger *log.SLogger) *Service {
	return &Service{
		logger:     logger,
		userLogger: userLogger,
	}
}

// PackageExists checks if a package exists (tar or chunked)
func (s *Service) PackageExists(bundleDir, pkgName string) bool {
	packagePath := filepath.Join(bundleDir, pkgName+".tar")
	if _, err := os.Stat(packagePath); err == nil {
		return true
	}
	// Check for chunked package
	if _, err := os.Stat(packagePath + ".chunk000"); err == nil {
		return true
	}
	return false
}

// PushPackage handles the common flow of pushing a package:
// 1. Check if package exists
// 2. Create temp directory
// 3. Unpack package
// 4. Validate structure
// 5. Push all layouts
// 6. Cleanup temp directory
func (s *Service) PushPackage(ctx context.Context, config PackagePushConfig) error {
	// Check if package exists
	if !s.PackageExists(config.BundleDir, config.PackageName) {
		s.userLogger.Infof("%s package is not present, skipping", config.PackageName)
		return nil
	}

	return s.userLogger.Process(config.ProcessName, func() error {
		return s.pushPackageInternal(ctx, config)
	})
}

func (s *Service) pushPackageInternal(ctx context.Context, config PackagePushConfig) error {
	// Create temp directory
	packageDir := filepath.Join(config.WorkingDir, config.PackageName)
	if err := os.MkdirAll(packageDir, defaultDirPermissions); err != nil {
		return fmt.Errorf("create temp directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(packageDir); err != nil {
			s.logger.Warn("Failed to cleanup temp directory",
				slog.String("path", packageDir),
				slog.Any("error", err))
		}
	}()

	// Open and unpack
	pkg, err := s.OpenPackage(config.BundleDir, config.PackageName)
	if err != nil {
		return fmt.Errorf("open package: %w", err)
	}
	defer pkg.Close()

	s.userLogger.InfoLn("Unpacking package")
	if err := bundle.Unpack(ctx, pkg, packageDir); err != nil {
		return fmt.Errorf("unpack package: %w", err)
	}

	// Validate structure (dynamic or static)
	mandatoryLayouts := config.MandatoryLayouts
	if config.MandatoryLayoutsFunc != nil {
		mandatoryLayouts = config.MandatoryLayoutsFunc(packageDir)
	}
	if len(mandatoryLayouts) > 0 {
		s.userLogger.InfoLn("Validating package structure")
		if err := bundle.ValidateUnpackedPackage(mandatoryLayouts); err != nil {
			return fmt.Errorf("invalid package structure: %w", err)
		}
	}

	// Get layouts to push (dynamic or static)
	layouts := config.Layouts
	if config.LayoutsFunc != nil {
		layouts = config.LayoutsFunc(packageDir)
	}

	// Push layouts
	for _, layoutMapping := range layouts {
		layoutFullPath := filepath.Join(packageDir, layoutMapping.LayoutPath)

		// Check if layout exists
		if _, err := os.Stat(filepath.Join(layoutFullPath, "index.json")); os.IsNotExist(err) {
			s.logger.Debug("Layout does not exist, skipping", slog.String("layout", layoutMapping.LayoutPath))
			continue
		}

		client := config.Client
		if layoutMapping.Segment != "" {
			client = client.WithSegment(layoutMapping.Segment)
		}

		repoRef := client.GetRegistry()
		s.userLogger.InfoLn("Pushing", repoRef)

		if err := s.PushLayout(ctx, layout.Path(layoutFullPath), client); err != nil {
			return fmt.Errorf("push layout %q: %w", layoutMapping.LayoutPath, err)
		}
	}

	return nil
}

// PushLayout pushes all images from an OCI layout to the registry
func (s *Service) PushLayout(ctx context.Context, layoutPath layout.Path, client registry.Client) error {
	index, err := layoutPath.ImageIndex()
	if err != nil {
		return fmt.Errorf("read OCI image index: %w", err)
	}

	indexManifest, err := index.IndexManifest()
	if err != nil {
		return fmt.Errorf("parse OCI image index manifest: %w", err)
	}

	if len(indexManifest.Manifests) == 0 {
		s.userLogger.InfoLn("No images to push")
		return nil
	}

	s.userLogger.Infof("Pushing %d images", len(indexManifest.Manifests))

	for i, manifest := range indexManifest.Manifests {
		tag := manifest.Annotations["io.deckhouse.image.short_tag"]
		if tag == "" {
			s.logger.Warn("Skipping image without short_tag annotation", slog.String("digest", manifest.Digest.String()))
			continue
		}

		s.userLogger.Infof("[%d / %d] Pushing image %s:%s", i+1, len(indexManifest.Manifests), client.GetRegistry(), tag)

		img, err := index.Image(manifest.Digest)
		if err != nil {
			return fmt.Errorf("read image %s: %w", tag, err)
		}

		err = retry.RunTaskWithContext(
			ctx, silentLogger{}, "push",
			task.WithConstantRetries(pushRetryAttempts, pushRetryDelay, func(ctx context.Context) error {
				if err := client.PushImage(ctx, tag, img); err != nil {
					if errorutil.IsTrivyMediaTypeNotAllowedError(err) {
						return fmt.Errorf(errorutil.CustomTrivyMediaTypesWarning)
					}
					return fmt.Errorf("write %s:%s to registry: %w", client.GetRegistry(), tag, err)
				}
				return nil
			}),
		)
		if err != nil {
			return fmt.Errorf("push image %s: %w", tag, err)
		}
	}

	return nil
}

// OpenPackage opens a package file, trying .tar first, then chunked
func (s *Service) OpenPackage(bundleDir, pkgName string) (io.ReadCloser, error) {
	p := filepath.Join(bundleDir, pkgName+".tar")
	pkg, err := os.Open(p)
	switch {
	case os.IsNotExist(err):
		return s.openChunkedPackage(bundleDir, pkgName)
	case err != nil:
		return nil, fmt.Errorf("read bundle package %s: %w", pkgName, err)
	}

	return pkg, nil
}

func (s *Service) openChunkedPackage(bundleDir, pkgName string) (io.ReadCloser, error) {
	pkg, err := chunked.Open(bundleDir, pkgName+".tar")
	if err != nil {
		return nil, fmt.Errorf("open bundle package %q: %w", pkgName, err)
	}

	return pkg, nil
}

// silentLogger suppresses retry task logging
type silentLogger struct{}

func (silentLogger) Debugf(_ string, _ ...interface{})        {}
func (silentLogger) DebugLn(_ ...interface{})                 {}
func (silentLogger) Infof(_ string, _ ...interface{}) {}
func (silentLogger) InfoLn(_ ...interface{})                  {}
func (silentLogger) Warnf(_ string, _ ...interface{})         {}
func (silentLogger) WarnLn(_ ...interface{})                  {}
func (silentLogger) Process(_ string, _ func() error) error   { return nil }
