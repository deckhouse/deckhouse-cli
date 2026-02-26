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

package installer

import (
	"fmt"
	"path/filepath"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

type ImageDownloadList struct {
	rootURL string

	Installer map[string]*puller.ImageMeta
}

func NewImageDownloadList(rootURL string) *ImageDownloadList {
	return &ImageDownloadList{
		rootURL: rootURL,

		Installer: make(map[string]*puller.ImageMeta),
	}
}

func (l *ImageDownloadList) FillInstallerImages(tagsToMirror []string) {
	for _, tag := range tagsToMirror {
		l.Installer[l.rootURL+":"+tag] = nil
	}
}

type ImageLayouts struct {
	platform   v1.Platform
	workingDir string
	image      *regimage.ImageLayout
}

func NewImageLayouts(rootFolder string) (*ImageLayouts, error) {
	layoutPath := filepath.Join(rootFolder, "installer")
	image, err := regimage.NewImageLayout(layoutPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create image layout: %w", err)
	}

	l := &ImageLayouts{
		workingDir: rootFolder,
		platform:   v1.Platform{Architecture: "amd64", OS: "linux"},
		image:      image,
	}

	return l, nil
}

// AsList returns a list of layout.Path's in it. Undefined path's are not included in the list.
func (l *ImageLayouts) AsList() []layout.Path {
	return []layout.Path{l.image.Path()}
}
