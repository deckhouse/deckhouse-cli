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
	"io"
	"os"

	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/operations"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

// Compile-time interface check
var _ usecase.SecurityPusher = (*SecurityPushService)(nil)

// SecurityPushService handles pushing security database images to registry
type SecurityPushService struct {
	bundleOpener BundleOpener
	pusher       LegacySecurityPusher
	logger       usecase.Logger
}

// BundleOpener opens bundle packages
type BundleOpener interface {
	Open(pkgName string) (io.ReadCloser, error)
}

// LegacySecurityPusher wraps the legacy push operations
type LegacySecurityPusher interface {
	PushSecurity(pkg io.ReadCloser) error
}

// NewSecurityPushService creates a new security push service
func NewSecurityPushService(
	bundleOpener BundleOpener,
	pusher LegacySecurityPusher,
	logger usecase.Logger,
) *SecurityPushService {
	return &SecurityPushService{
		bundleOpener: bundleOpener,
		pusher:       pusher,
		logger:       logger,
	}
}

// Push implements usecase.SecurityPusher
func (s *SecurityPushService) Push(ctx context.Context) error {
	pkg, err := s.bundleOpener.Open("security")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.logger.Info("Security package not found, skipping")
			return nil
		}
		return fmt.Errorf("open security bundle: %w", err)
	}
	defer pkg.Close()

	if err := s.pusher.PushSecurity(pkg); err != nil {
		return fmt.Errorf("push security: %w", err)
	}

	return nil
}

// LegacySecurityPusherImpl wraps the legacy operations.PushSecurityDatabases
type LegacySecurityPusherImpl struct {
	params *params.PushParams
	client registry.Client
}

// NewLegacySecurityPusher creates a new legacy security pusher
func NewLegacySecurityPusher(params *params.PushParams, client registry.Client) *LegacySecurityPusherImpl {
	return &LegacySecurityPusherImpl{
		params: params,
		client: client,
	}
}

func (p *LegacySecurityPusherImpl) PushSecurity(pkg io.ReadCloser) error {
	return operations.PushSecurityDatabases(p.params, pkg, p.client)
}

