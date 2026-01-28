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
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/errorutil"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/retry/task"
)

const (
	pushRetryAttempts = 4
	pushRetryDelay    = 3 * time.Second
)

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
		return nil
	}

	s.userLogger.Infof("Pushing %d images", len(indexManifest.Manifests))

	for i, manifest := range indexManifest.Manifests {
		tag := manifest.Annotations["io.deckhouse.image.short_tag"]
		if tag == "" {
			s.logger.Warn("Skipping image without short_tag annotation", slog.String("digest", manifest.Digest.String()))
			continue
		}

		img, err := index.Image(manifest.Digest)
		if err != nil {
			return fmt.Errorf("read image %s from layout %s: %w", tag, layoutPath, err)
		}

		imageReferenceString := fmt.Sprintf("%s:%s", client.GetRegistry(), tag)
		err = retry.RunTask(
			ctx,
			s.userLogger,
			fmt.Sprintf("[%d / %d] Pushing %s", i+1, len(indexManifest.Manifests), imageReferenceString),
			task.WithConstantRetries(pushRetryAttempts, pushRetryDelay, func(ctx context.Context) error {
				if err := client.PushImage(ctx, tag, img); err != nil {
					if errorutil.IsTrivyMediaTypeNotAllowedError(err) {
						return fmt.Errorf(errorutil.CustomTrivyMediaTypesWarning)
					}
					return fmt.Errorf("write %s:%s to registry: %w", client.GetRegistry(), tag, err)
				}
				return nil
			}))
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
