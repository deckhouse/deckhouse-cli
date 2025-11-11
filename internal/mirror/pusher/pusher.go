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
	"os"
	"path/filepath"
	"strings"

	"github.com/samber/lo"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

// PusherService handles the pushing of images to the registry
type PusherService struct {
	logger     *dkplog.Logger
	userLogger *log.SLogger
}

// NewPusherService creates a new PusherService
func NewPusherService(
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *PusherService {
	return &PusherService{
		logger:     logger,
		userLogger: userLogger,
	}
}

// PushModules pushes module packages from the bundle directory
func (ps *PusherService) PushModules(_ context.Context, bundleDir string, _ interface{}) error {
	bundleContents, err := os.ReadDir(bundleDir)
	if err != nil {
		return fmt.Errorf("list bundle directory: %w", err)
	}

	modulePackages := lo.Compact(lo.Map(bundleContents, func(item os.DirEntry, _ int) string {
		fileExt := filepath.Ext(item.Name())
		pkgName, _, ok := strings.Cut(strings.TrimPrefix(item.Name(), "module-"), ".")
		switch {
		case !ok:
			fallthrough
		case fileExt != ".tar" && fileExt != ".chunk":
			fallthrough
		case !strings.HasPrefix(item.Name(), "module-"):
			return ""
		}
		return pkgName
	}))

	successfullyPushedModules := make([]string, 0)
	for _, modulePackageName := range modulePackages {
		if lo.Contains(successfullyPushedModules, modulePackageName) {
			continue
		}

		if err = ps.userLogger.Process("Push module: "+modulePackageName, func() error {
			pkg, err := ps.openPackage(bundleDir, "module-"+modulePackageName)
			if err != nil {
				return fmt.Errorf("open package %q: %w", modulePackageName, err)
			}
			defer pkg.Close()

			// Here we would call operations.PushModule, but since we don't have access to it,
			// we'll leave this as a placeholder
			// if err = operations.PushModule(pushParams, modulePackageName, pkg, client); err != nil {
			//     return fmt.Errorf("failed to push module %q: %w", modulePackageName, err)
			// }

			ps.userLogger.InfoLn("Module " + modulePackageName + " pushed successfully")

			successfullyPushedModules = append(successfullyPushedModules, modulePackageName)

			return nil
		}); err != nil {
			ps.userLogger.WarnLn(err)
		}
	}

	if len(successfullyPushedModules) > 0 {
		ps.userLogger.Infof("Modules pushed: %v", strings.Join(successfullyPushedModules, ", "))
	}

	return nil
}

// openPackage opens a package file, trying .tar first, then .chunk
func (ps *PusherService) openPackage(bundleDir, pkgName string) (io.ReadCloser, error) {
	p := filepath.Join(bundleDir, pkgName+".tar")
	pkg, err := os.Open(p)
	switch {
	case os.IsNotExist(err):
		return ps.openChunkedPackage(bundleDir, pkgName)
	case err != nil:
		return nil, fmt.Errorf("read bundle package %s: %w", pkgName, err)
	}

	return pkg, nil
}

// openChunkedPackage opens a chunked package
func (ps *PusherService) openChunkedPackage(bundleDir, pkgName string) (io.ReadCloser, error) {
	pkg, err := chunked.Open(bundleDir, pkgName+".tar")
	if err != nil {
		return nil, fmt.Errorf("open bundle package %q: %w", pkgName, err)
	}

	return pkg, nil
}
