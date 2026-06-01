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

package packages

import (
	"fmt"
	"path/filepath"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

type PackagesDownloadList struct {
	rootURL string
	list    map[string]*ImageDownloadList
}

func NewPackagesDownloadList(rootURL string) *PackagesDownloadList {
	return &PackagesDownloadList{
		rootURL: rootURL,
		list:    make(map[string]*ImageDownloadList),
	}
}

func (l *PackagesDownloadList) Package(packageName string) *ImageDownloadList {
	return l.list[packageName]
}

type ImageDownloadList struct {
	rootURL string

	Package                map[string]*puller.ImageMeta
	PackageVersionChannels map[string]*puller.ImageMeta
	PackageExtra           map[string]*puller.ImageMeta
}

func NewImageDownloadList(rootURL string) *ImageDownloadList {
	return &ImageDownloadList{
		rootURL: rootURL,

		Package:                make(map[string]*puller.ImageMeta),
		PackageVersionChannels: make(map[string]*puller.ImageMeta),
		PackageExtra:           make(map[string]*puller.ImageMeta),
	}
}

type PackagesImageLayouts struct {
	platform   v1.Platform
	workingDir string

	list map[string]*ImageLayouts
}

func NewPackagesImageLayouts(rootFolder string) *PackagesImageLayouts {
	l := &PackagesImageLayouts{
		workingDir: rootFolder,
		platform:   v1.Platform{Architecture: "amd64", OS: "linux"},
		list:       make(map[string]*ImageLayouts),
	}

	return l
}

func (l *PackagesImageLayouts) Package(packageName string) *ImageLayouts {
	return l.list[packageName]
}

// AsList returns a list of layout.Path's from all packages. Undefined path's are not included in the list.
func (l *PackagesImageLayouts) AsList() []layout.Path {
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

	// Packages is the main package image layout (packages/<name>/)
	Packages *regimage.ImageLayout
	// PackageVersionChannels is the version channel layout (packages/<name>/version/)
	PackageVersionChannels *regimage.ImageLayout
	// ExtraImages holds layouts for each extra image (packages/<name>/extra/<extra-name>/)
	// Key is the extra image name.
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
	case internal.MirrorTypePackages:
		l.Packages = layout
	case internal.MirrorTypePackagesVersionChannels:
		l.PackageVersionChannels = layout
	default:
		return fmt.Errorf("wrong mirror type in packages image layout: %v", mirrorType)
	}

	return nil
}

// GetOrCreateExtraLayout returns or creates a layout for a specific extra image.
// Extra images are stored under: packages/<name>/extra/<extra-name>/
func (l *ImageLayouts) GetOrCreateExtraLayout(extraName string) (*regimage.ImageLayout, error) {
	if existing, ok := l.ExtraImages[extraName]; ok {
		return existing, nil
	}

	// Create layout at packages/<package-name>/extra/<extra-name>/
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
	if l.Packages != nil {
		paths = append(paths, l.Packages.Path())
	}

	if l.PackageVersionChannels != nil {
		paths = append(paths, l.PackageVersionChannels.Path())
	}
	// Add all extra image layouts
	for _, extraLayout := range l.ExtraImages {
		if extraLayout != nil {
			paths = append(paths, extraLayout.Path())
		}
	}

	return paths
}

// HasImages reports whether any sub-layout of this package contains at least
// one image manifest. Returns false when all layouts are empty (i.e. the
// package was discovered but no images were pulled into it).
func (l *ImageLayouts) HasImages() bool {
	for _, lp := range l.AsList() {
		index, err := lp.ImageIndex()
		if err != nil {
			continue
		}

		manifest, err := index.IndexManifest()
		if err != nil {
			continue
		}

		if len(manifest.Manifests) > 0 {
			return true
		}
	}

	return false
}
