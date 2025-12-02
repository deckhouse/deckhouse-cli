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

package modules

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/samber/lo"

	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/operations"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/usecase"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/operations/params"
)

// Compile-time interface check
var _ usecase.ModulesPusher = (*ModulesPushService)(nil)

// ModulesPushService handles pushing module images to registry
type ModulesPushService struct {
	bundleOpener BundleOpener
	pusher       LegacyModulePusher
	logger       usecase.Logger
	opts         *ModulesPushOptions
}

// BundleOpener opens bundle packages
type BundleOpener interface {
	Open(pkgName string) (io.ReadCloser, error)
}

// LegacyModulePusher wraps the legacy push operations
type LegacyModulePusher interface {
	PushModule(moduleName string, pkg io.ReadCloser) error
}

// ModulesPushOptions contains options for modules push
type ModulesPushOptions struct {
	BundleDir string
}

// NewModulesPushService creates a new modules push service
func NewModulesPushService(
	bundleOpener BundleOpener,
	pusher LegacyModulePusher,
	logger usecase.Logger,
	opts *ModulesPushOptions,
) *ModulesPushService {
	return &ModulesPushService{
		bundleOpener: bundleOpener,
		pusher:       pusher,
		logger:       logger,
		opts:         opts,
	}
}

// Push implements usecase.ModulesPusher
func (s *ModulesPushService) Push(ctx context.Context) error {
	moduleNames, err := s.findModulePackages()
	if err != nil {
		return fmt.Errorf("find module packages: %w", err)
	}

	if len(moduleNames) == 0 {
		s.logger.Info("No module packages found")
		return nil
	}

	s.logger.Infof("Found %d module packages to push", len(moduleNames))

	pushed := make([]string, 0)
	for _, moduleName := range moduleNames {
		if lo.Contains(pushed, moduleName) {
			continue
		}

		if err := s.pushModule(ctx, moduleName); err != nil {
			s.logger.Warnf("Failed to push module %s: %v", moduleName, err)
			continue
		}

		pushed = append(pushed, moduleName)
	}

	if len(pushed) > 0 {
		s.logger.Infof("Modules pushed: %s", strings.Join(pushed, ", "))
	}

	return nil
}

func (s *ModulesPushService) findModulePackages() ([]string, error) {
	entries, err := os.ReadDir(s.opts.BundleDir)
	if err != nil {
		return nil, fmt.Errorf("read bundle dir: %w", err)
	}

	modules := lo.Compact(lo.Map(entries, func(item os.DirEntry, _ int) string {
		ext := filepath.Ext(item.Name())
		if ext != ".tar" && ext != ".chunk" {
			return ""
		}
		if !strings.HasPrefix(item.Name(), "module-") {
			return ""
		}

		name, _, ok := strings.Cut(strings.TrimPrefix(item.Name(), "module-"), ".")
		if !ok {
			return ""
		}
		return name
	}))

	return modules, nil
}

func (s *ModulesPushService) pushModule(ctx context.Context, moduleName string) error {
	return s.logger.Process("Push module: "+moduleName, func() error {
		pkg, err := s.bundleOpener.Open("module-" + moduleName)
		if err != nil {
			return fmt.Errorf("open module bundle: %w", err)
		}
		defer pkg.Close()

		if err := s.pusher.PushModule(moduleName, pkg); err != nil {
			return fmt.Errorf("push module: %w", err)
		}

		return nil
	})
}

// LegacyModulePusherImpl wraps the legacy operations.PushModule
type LegacyModulePusherImpl struct {
	params *params.PushParams
	client registry.Client
}

// NewLegacyModulePusher creates a new legacy module pusher
func NewLegacyModulePusher(params *params.PushParams, client registry.Client) *LegacyModulePusherImpl {
	return &LegacyModulePusherImpl{
		params: params,
		client: client,
	}
}

func (p *LegacyModulePusherImpl) PushModule(moduleName string, pkg io.ReadCloser) error {
	return operations.PushModule(p.params, moduleName, pkg, p.client)
}

