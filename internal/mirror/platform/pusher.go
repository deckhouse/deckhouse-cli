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

package platform

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/operations"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

// Compile-time interface check
var _ usecase.PlatformPusher = (*PlatformPushService)(nil)

// PlatformPushService handles pushing platform images to registry
type PlatformPushService struct {
	bundleOpener BundleOpener
	pusher       LegacyPusher
	logger       usecase.Logger
	opts         *PushOptions
}

// BundleOpener opens bundle packages
type BundleOpener interface {
	Open(pkgName string) (io.ReadCloser, error)
}

// LegacyPusher wraps the legacy push operations
type LegacyPusher interface {
	PushPlatform(pkg io.ReadCloser) error
}

// PushOptions contains options for push service
type PushOptions struct {
	BundleDir  string
	WorkingDir string
}

// NewPlatformPushService creates a new platform push service
func NewPlatformPushService(
	bundleOpener BundleOpener,
	pusher LegacyPusher,
	logger usecase.Logger,
	opts *PushOptions,
) *PlatformPushService {
	return &PlatformPushService{
		bundleOpener: bundleOpener,
		pusher:       pusher,
		logger:       logger,
		opts:         opts,
	}
}

// Push implements usecase.PlatformPusher
func (s *PlatformPushService) Push(ctx context.Context) error {
	pkg, err := s.bundleOpener.Open("platform")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.logger.Info("Platform package not found, skipping")
			return nil
		}
		return fmt.Errorf("open platform bundle: %w", err)
	}
	defer pkg.Close()

	if err := s.pusher.PushPlatform(pkg); err != nil {
		return fmt.Errorf("push platform: %w", err)
	}

	return nil
}

// LegacyPlatformPusher wraps the legacy operations.PushDeckhousePlatform
type LegacyPlatformPusher struct {
	params *params.PushParams
	client registry.Client
}

// NewLegacyPlatformPusher creates a new legacy platform pusher
func NewLegacyPlatformPusher(params *params.PushParams, client registry.Client) *LegacyPlatformPusher {
	return &LegacyPlatformPusher{
		params: params,
		client: client,
	}
}

func (p *LegacyPlatformPusher) PushPlatform(pkg io.ReadCloser) error {
	return operations.PushDeckhousePlatform(p.params, pkg, p.client)
}

