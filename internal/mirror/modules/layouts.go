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
	"fmt"
	"path/filepath"

	"github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

// ModuleLayout holds layouts for a single module
type ModuleLayout struct {
	workingDir      string
	module          *image.ImageLayout
	releaseChannels *image.ImageLayout
	extra           *image.ImageLayout
}

// NewModuleLayout creates layouts for a single module
func NewModuleLayout(moduleDir string) (*ModuleLayout, error) {
	module, err := image.NewImageLayout(filepath.Join(moduleDir, "module"))
	if err != nil {
		return nil, fmt.Errorf("create module layout: %w", err)
	}

	releaseChannels, err := image.NewImageLayout(filepath.Join(moduleDir, "release"))
	if err != nil {
		return nil, fmt.Errorf("create release-channel layout: %w", err)
	}

	extra, err := image.NewImageLayout(filepath.Join(moduleDir, "extra"))
	if err != nil {
		return nil, fmt.Errorf("create extra layout: %w", err)
	}

	return &ModuleLayout{
		workingDir:      moduleDir,
		module:          module,
		releaseChannels: releaseChannels,
		extra:           extra,
	}, nil
}

func (l *ModuleLayout) WorkingDir() string {
	return l.workingDir
}

func (l *ModuleLayout) Module() *image.ImageLayout {
	return l.module
}

func (l *ModuleLayout) ReleaseChannels() *image.ImageLayout {
	return l.releaseChannels
}

func (l *ModuleLayout) Extra() *image.ImageLayout {
	return l.extra
}

func (l *ModuleLayout) AsList() []*image.ImageLayout {
	return []*image.ImageLayout{
		l.module,
		l.releaseChannels,
		l.extra,
	}
}

// ModulesLayouts manages layouts for all modules
type ModulesLayouts struct {
	workingDir string
	modules    map[string]*ModuleLayout
}

// NewModulesLayouts creates layouts for multiple modules
func NewModulesLayouts(workingDir string, moduleNames []string) (*ModulesLayouts, error) {
	modulesDir := filepath.Join(workingDir, "modules")

	layouts := &ModulesLayouts{
		workingDir: modulesDir,
		modules:    make(map[string]*ModuleLayout),
	}

	for _, name := range moduleNames {
		moduleLayout, err := NewModuleLayout(filepath.Join(modulesDir, name))
		if err != nil {
			return nil, fmt.Errorf("create layout for module %s: %w", name, err)
		}
		layouts.modules[name] = moduleLayout
	}

	return layouts, nil
}

func (l *ModulesLayouts) WorkingDir() string {
	return l.workingDir
}

func (l *ModulesLayouts) Module(name string) *ModuleLayout {
	return l.modules[name]
}

func (l *ModulesLayouts) AllModules() map[string]*ModuleLayout {
	return l.modules
}

