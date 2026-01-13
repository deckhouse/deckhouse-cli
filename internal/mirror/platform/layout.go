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
	"fmt"
	"path"
	"reflect"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

type ImageDownloadList struct {
	rootURL string

	Deckhouse                  map[string]*puller.ImageMeta
	DeckhouseExtra             map[string]*puller.ImageMeta
	DeckhouseInstall           map[string]*puller.ImageMeta
	DeckhouseInstallStandalone map[string]*puller.ImageMeta
	DeckhouseReleaseChannel    map[string]*puller.ImageMeta
}

func NewImageDownloadList(rootURL string) *ImageDownloadList {
	return &ImageDownloadList{
		rootURL: rootURL,

		Deckhouse:                  make(map[string]*puller.ImageMeta),
		DeckhouseExtra:             make(map[string]*puller.ImageMeta),
		DeckhouseInstall:           make(map[string]*puller.ImageMeta),
		DeckhouseInstallStandalone: make(map[string]*puller.ImageMeta),
		DeckhouseReleaseChannel:    make(map[string]*puller.ImageMeta),
	}
}

func (l *ImageDownloadList) FillDeckhouseImages(deckhouseVersions []string) {
	for _, version := range deckhouseVersions {
		l.Deckhouse[l.rootURL+":"+version] = nil
		l.DeckhouseInstall[path.Join(l.rootURL, internal.InstallSegment)+":"+version] = nil
		l.DeckhouseInstallStandalone[path.Join(l.rootURL, internal.InstallStandaloneSegment)+":"+version] = nil
		// Also add version tags to release-channel (e.g., release-channel:v1.74.0)
		l.DeckhouseReleaseChannel[path.Join(l.rootURL, internal.ReleaseChannelSegment)+":"+version] = nil
	}
}

func (l *ImageDownloadList) FillForTag(tag string) {
	// If we are to pull only the specific requested version, we should not pull any release channels at all.
	if tag != "" {
		return
	}

	for _, channel := range internal.GetAllDefaultReleaseChannels() {
		l.Deckhouse[l.rootURL+":"+channel] = nil
		l.DeckhouseInstall[path.Join(l.rootURL, internal.InstallSegment)+":"+channel] = nil
		l.DeckhouseInstallStandalone[path.Join(l.rootURL, internal.InstallStandaloneSegment)+":"+channel] = nil
		key := path.Join(l.rootURL, internal.ReleaseChannelSegment) + ":" + channel
		if _, exists := l.DeckhouseReleaseChannel[key]; !exists {
			l.DeckhouseReleaseChannel[key] = nil
		}
	}
}

type ImageLayouts struct {
	platform   v1.Platform
	workingDir string

	Deckhouse                  *regimage.ImageLayout
	DeckhouseInstall           *regimage.ImageLayout
	DeckhouseInstallStandalone *regimage.ImageLayout
	DeckhouseReleaseChannel    *regimage.ImageLayout
}

func NewImageLayouts(rootFolder string) *ImageLayouts {
	l := &ImageLayouts{
		workingDir: rootFolder,
		platform:   v1.Platform{Architecture: "amd64", OS: "linux"},
	}

	return l
}

func (l *ImageLayouts) setLayoutByMirrorType(rootFolder string, mirrorType internal.MirrorType) error {
	layoutPath := path.Join(rootFolder, internal.InstallPathByMirrorType(mirrorType))

	layout, err := regimage.NewImageLayout(layoutPath)
	if err != nil {
		return fmt.Errorf("failed to create image layout: %w", err)
	}

	switch mirrorType {
	case internal.MirrorTypeDeckhouse:
		l.Deckhouse = layout
	case internal.MirrorTypeDeckhouseReleaseChannels:
		l.DeckhouseReleaseChannel = layout
	case internal.MirrorTypeDeckhouseInstall:
		l.DeckhouseInstall = layout
	case internal.MirrorTypeDeckhouseInstallStandalone:
		l.DeckhouseInstallStandalone = layout
	default:
		return fmt.Errorf("wrong mirror type in platform image layout: %v", mirrorType)
	}

	return nil
}

// AsList returns a list of layout.Path's in it. Undefined path's are not included in the list.
func (l *ImageLayouts) AsList() []layout.Path {
	layoutsValue := reflect.ValueOf(l).Elem()
	layoutPathType := reflect.TypeOf(layout.Path(""))

	paths := make([]layout.Path, 0)
	for i := 0; i < layoutsValue.NumField(); i++ {
		if layoutsValue.Field(i).Type() != layoutPathType {
			continue
		}

		if pathValue := layoutsValue.Field(i).String(); pathValue != "" {
			paths = append(paths, layout.Path(pathValue))
		}
	}

	return paths
}
