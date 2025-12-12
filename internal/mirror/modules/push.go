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
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	dkplog "github.com/deckhouse/deckhouse/pkg/log"
	"github.com/deckhouse/deckhouse/pkg/registry"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/pusher"
	"github.com/deckhouse/deckhouse-cli/pkg/libmirror/util/log"
)

// PushOptions contains options for pushing module images
type PushOptions struct {
	BundleDir  string
	WorkingDir string
}

// PushService handles pushing module images to registry
type PushService struct {
	client        registry.Client
	pusherService *pusher.Service
	options       *PushOptions
	logger        *dkplog.Logger
	userLogger    *log.SLogger
}

// NewPushService creates a new modules push service
func NewPushService(
	client registry.Client,
	options *PushOptions,
	logger *dkplog.Logger,
	userLogger *log.SLogger,
) *PushService {
	if options == nil {
		options = &PushOptions{}
	}

	return &PushService{
		client:        client,
		pusherService: pusher.NewService(logger, userLogger),
		options:       options,
		logger:        logger,
		userLogger:    userLogger,
	}
}

// Push pushes all module packages to the registry
func (svc *PushService) Push(ctx context.Context) error {
	modulePackages, err := svc.findModulePackages()
	if err != nil {
		return fmt.Errorf("find module packages: %w", err)
	}

	if len(modulePackages) == 0 {
		svc.userLogger.InfoLn("No module packages found, skipping")
		return nil
	}

	pushed := make(map[string]struct{})
	for _, moduleName := range modulePackages {
		if _, ok := pushed[moduleName]; ok {
			continue
		}

		if err := svc.pushModule(ctx, moduleName); err != nil {
			svc.userLogger.WarnLn(err)
			continue
		}
		pushed[moduleName] = struct{}{}
	}

	if len(pushed) > 0 {
		names := make([]string, 0, len(pushed))
		for name := range pushed {
			names = append(names, name)
		}
		slices.Sort(names)
		svc.userLogger.Infof("Modules pushed: %s", strings.Join(names, ", "))
	}

	return nil
}

func (svc *PushService) findModulePackages() ([]string, error) {
	entries, err := os.ReadDir(svc.options.BundleDir)
	if err != nil {
		return nil, fmt.Errorf("list bundle directory: %w", err)
	}

	modules := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()

		// Skip non-module files
		if !strings.HasPrefix(name, "module-") {
			continue
		}

		// Only process .tar and .chunk files
		ext := filepath.Ext(name)
		if ext != ".tar" && ext != ".chunk" {
			continue
		}

		// Extract module name: "module-foo.tar" -> "foo"
		// Handle chunked files: "module-foo.tar.chunk000" -> "foo"
		moduleName := strings.TrimPrefix(name, "module-")
		moduleName = strings.TrimSuffix(moduleName, ext)
		moduleName = strings.TrimSuffix(moduleName, ".tar")

		modules = append(modules, moduleName)
	}

	return modules, nil
}

func (svc *PushService) pushModule(ctx context.Context, moduleName string) error {
	return svc.pusherService.PushPackage(ctx, pusher.PackagePushConfig{
		PackageName: "module-" + moduleName,
		ProcessName: "Push module: " + moduleName,
		WorkingDir:  filepath.Join(svc.options.WorkingDir, "modules"),
		BundleDir:   svc.options.BundleDir,
		Client:      svc.client.WithSegment(moduleName),
		// New pull creates: module/, release/, extra/
		MandatoryLayoutsFunc: func(packageDir string) map[string]string {
			return map[string]string{
				"module root layout":             filepath.Join(packageDir, "module"),
				"module release channels layout": filepath.Join(packageDir, "release"),
			}
		},
		// Dynamic layout discovery after unpacking
		LayoutsFunc: svc.buildModuleLayouts,
	})
}

// buildModuleLayouts returns the list of layouts for a module, including dynamic extra discovery
func (svc *PushService) buildModuleLayouts(packageDir string) []pusher.LayoutMapping {
	layouts := []pusher.LayoutMapping{
		{LayoutPath: "module", Segment: ""},         // Root module images
		{LayoutPath: "release", Segment: "release"}, // Release channels
	}

	// Check if extra directory exists
	extraDir := filepath.Join(packageDir, "extra")
	if _, err := os.Stat(extraDir); os.IsNotExist(err) {
		return layouts
	}

	// Add root extra layout
	layouts = append(layouts, pusher.LayoutMapping{
		LayoutPath: "extra",
		Segment:    "extra",
	})

	// Discover nested extra layouts
	entries, err := os.ReadDir(extraDir)
	if err != nil {
		svc.logger.Warn("Error reading extra dir", slog.Any("error", err))
		return layouts
	}

	for _, entry := range entries {
		if entry.IsDir() {
			svc.logger.Debug("Found extra layout", slog.String("layout", entry.Name()))
			layouts = append(layouts, pusher.LayoutMapping{
				LayoutPath: path.Join("extra", entry.Name()),
				Segment:    path.Join("extra", entry.Name()),
			})
		}
	}

	return layouts
}
