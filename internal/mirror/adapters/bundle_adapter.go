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

package adapters

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/chunked"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/bundle"
)

// Compile-time interface check
var _ usecase.BundlePacker = (*BundlePackerAdapter)(nil)

// BundlePackerAdapter adapts the bundle package to usecase.BundlePacker
type BundlePackerAdapter struct {
	bundleDir   string
	chunkSize   int64
	logger      usecase.Logger
}

// NewBundlePackerAdapter creates a new bundle packer adapter
func NewBundlePackerAdapter(bundleDir string, chunkSize int64, logger usecase.Logger) *BundlePackerAdapter {
	return &BundlePackerAdapter{
		bundleDir: bundleDir,
		chunkSize: chunkSize,
		logger:    logger,
	}
}

func (a *BundlePackerAdapter) Pack(ctx context.Context, sourceDir, bundleName string) error {
	return a.logger.Process(fmt.Sprintf("Pack %s", bundleName), func() error {
		var writer io.Writer
		var err error

		if a.chunkSize > 0 {
			writer = chunked.NewChunkedFileWriter(a.chunkSize, a.bundleDir, bundleName)
		} else {
			writer, err = os.Create(filepath.Join(a.bundleDir, bundleName))
			if err != nil {
				return fmt.Errorf("create %s: %w", bundleName, err)
			}
		}

		if err := bundle.Pack(ctx, sourceDir, writer); err != nil {
			return fmt.Errorf("pack %s: %w", bundleName, err)
		}

		return nil
	})
}

