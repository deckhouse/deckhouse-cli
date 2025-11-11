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

type ImageDownloadList struct {
	rootURL string

	Modules                map[string]*puller.ImageMeta
	ModulesReleaseChannels map[string]*puller.ImageMeta
	ModulesExtra           map[string]*puller.ImageMeta
}

func NewImageDownloadList(rootURL string) *ImageDownloadList {
	return &ImageDownloadList{
		rootURL: rootURL,

		Modules:                make(map[string]*puller.ImageMeta),
		ModulesReleaseChannels: make(map[string]*puller.ImageMeta),
		ModulesExtra:           make(map[string]*puller.ImageMeta),
	}
}

func (l *ImageDownloadList) FillModulesImages(modules []string) {
	for _, module := range modules {
		l.Modules[filepath.Join(l.rootURL, internal.ModulesSegment, module)+":latest"] = nil
		l.ModulesReleaseChannels[filepath.Join(l.rootURL, internal.ModulesSegment, module, internal.ModulesReleasesSegment)+":latest"] = nil
	}
}

func (l *ImageDownloadList) FillForTag(tag string) {
	// If we are to pull only the specific requested version, we should not pull any release channels at all.
	if tag != "" {
		return
	}

	// For modules, release channels might be handled differently
	// TODO: implement if needed
}

type ImageLayouts struct {
	platform   v1.Platform
	workingDir string

	Modules                *regimage.ImageLayout
	ModulesReleaseChannels *regimage.ImageLayout
	ModulesExtra           *regimage.ImageLayout
}

func NewImageLayouts(rootFolder string) *ImageLayouts {
	l := &ImageLayouts{
		workingDir: rootFolder,
		platform:   v1.Platform{Architecture: "amd64", OS: "linux"},
	}

	return l
}

func (l *ImageLayouts) setLayoutByMirrorType(rootFolder string, mirrorType internal.MirrorType) error {
	layoutPath := filepath.Join(rootFolder, internal.InstallSegmentByMirrorType(mirrorType))

	layout, err := regimage.NewImageLayout(layoutPath)
	if err != nil {
		return fmt.Errorf("failed to create image layout: %w", err)
	}

	switch mirrorType {
	case internal.MirrorTypeModules:
		l.Modules = layout
	case internal.MirrorTypeModulesReleaseChannels:
		l.ModulesReleaseChannels = layout
	case internal.MirrorTypeModulesExtra:
		l.ModulesExtra = layout
	default:
		return fmt.Errorf("wrong mirror type in modules image layout: %v", mirrorType)
	}

	return nil
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
	if l.ModulesExtra != nil {
		paths = append(paths, l.ModulesExtra.Path())
	}
	return paths
}
