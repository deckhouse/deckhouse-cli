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

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

type ModulesDownloadList struct {
	rootURL string
	list    map[string]*ImageDownloadList
}

func NewModulesDownloadList(rootURL string) *ModulesDownloadList {
	return &ModulesDownloadList{
		rootURL: rootURL,
		list:    make(map[string]*ImageDownloadList),
	}
}

func (l *ModulesDownloadList) Module(moduleName string) *ImageDownloadList {
	return l.list[moduleName]
}

func (l *ModulesDownloadList) FillModulesImages(modules []string) {
	for _, moduleName := range modules {
		list := NewImageDownloadList(filepath.Join(l.rootURL, moduleName))
		list.FillForTag("")
		l.list[moduleName] = list
	}
}

type ImageDownloadList struct {
	rootURL string

	Module                map[string]*puller.ImageMeta
	ModuleReleaseChannels map[string]*puller.ImageMeta
	ModuleExtra           map[string]*puller.ImageMeta
}

func NewImageDownloadList(rootURL string) *ImageDownloadList {
	return &ImageDownloadList{
		rootURL: rootURL,

		Module:                make(map[string]*puller.ImageMeta),
		ModuleReleaseChannels: make(map[string]*puller.ImageMeta),
		ModuleExtra:           make(map[string]*puller.ImageMeta),
	}
}

func (l *ImageDownloadList) FillForTag(tag string) {
	// If we are to pull only the specific requested version, we should not pull any release channels at all.
	if tag != "" {
		return
	}

	for _, channel := range internal.GetAllDefaultReleaseChannels() {
		l.ModuleReleaseChannels[l.rootURL+":"+channel] = nil
	}
}

type ModulesImageLayouts struct {
	platform   v1.Platform
	workingDir string

	list map[string]*ImageLayouts
}

func NewModulesImageLayouts(rootFolder string) *ModulesImageLayouts {
	l := &ModulesImageLayouts{
		workingDir: rootFolder,
		platform:   v1.Platform{Architecture: "amd64", OS: "linux"},
		list:       make(map[string]*ImageLayouts),
	}

	return l
}

func (l *ModulesImageLayouts) Module(moduleName string) *ImageLayouts {
	return l.list[moduleName]
}

// AsList returns a list of layout.Path's from all modules. Undefined path's are not included in the list.
func (l *ModulesImageLayouts) AsList() []layout.Path {
	var paths []layout.Path
	for _, imgLayout := range l.list {
		if imgLayout != nil {
			paths = append(paths, imgLayout.AsList()...)
		}
	}
	return paths
}

type ImageLayouts struct {
	platform   v1.Platform
	workingDir string

	// Modules is the main module image layout (modules/<name>/)
	Modules *regimage.ImageLayout
	// ModulesReleaseChannels is the release channel layout (modules/<name>/release/)
	ModulesReleaseChannels *regimage.ImageLayout
	// ExtraImages holds layouts for each extra image (modules/<name>/<extra-name>/)
	// Key is the extra image name (e.g., "scanner", "enforcer")
	ExtraImages map[string]*regimage.ImageLayout
}

func NewImageLayouts(rootFolder string) *ImageLayouts {
	l := &ImageLayouts{
		workingDir:  rootFolder,
		platform:    v1.Platform{Architecture: "amd64", OS: "linux"},
		ExtraImages: make(map[string]*regimage.ImageLayout),
	}

	return l
}

func (l *ImageLayouts) setLayoutByMirrorType(rootFolder string, mirrorType internal.MirrorType) error {
	layoutPath := filepath.Join(rootFolder, internal.InstallPathByMirrorType(mirrorType))

	layout, err := regimage.NewImageLayout(layoutPath)
	if err != nil {
		return fmt.Errorf("failed to create image layout: %w", err)
	}

	switch mirrorType {
	case internal.MirrorTypeModules:
		l.Modules = layout
	case internal.MirrorTypeModulesReleaseChannels:
		l.ModulesReleaseChannels = layout
	default:
		return fmt.Errorf("wrong mirror type in modules image layout: %v", mirrorType)
	}

	return nil
}

// GetOrCreateExtraLayout returns or creates a layout for a specific extra image.
// Extra images are stored under: modules/<name>/extra/<extra-name>/
func (l *ImageLayouts) GetOrCreateExtraLayout(extraName string) (*regimage.ImageLayout, error) {
	if existing, ok := l.ExtraImages[extraName]; ok {
		return existing, nil
	}

	// Create layout at modules/<module-name>/extra/<extra-name>/
	layoutPath := filepath.Join(l.workingDir, "extra", extraName)
	layout, err := regimage.NewImageLayout(layoutPath)
	if err != nil {
		return nil, fmt.Errorf("create extra image layout for %s: %w", extraName, err)
	}

	l.ExtraImages[extraName] = layout
	return layout, nil
}

// AsList returns a list of layout.Path's in it. Undefined path's are not included in the list.
func (l *ImageLayouts) AsList() []layout.Path {
	paths := make([]layout.Path, 0)
	if l.Modules != nil {
		paths = append(paths, l.Modules.Path())
	}
	if l.ModulesReleaseChannels != nil {
		paths = append(paths, l.ModulesReleaseChannels.Path())
	}
	// Add all extra image layouts
	for _, extraLayout := range l.ExtraImages {
		if extraLayout != nil {
			paths = append(paths, extraLayout.Path())
		}
	}
	return paths
}
