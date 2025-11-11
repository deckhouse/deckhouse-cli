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
	"fmt"
	"path"
	"path/filepath"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/layout"

	"github.com/deckhouse/deckhouse-cli/internal"
	"github.com/deckhouse/deckhouse-cli/internal/mirror/puller"
	regimage "github.com/deckhouse/deckhouse-cli/pkg/registry/image"
)

const (
	TrivyDBName     = "trivy-db"
	TrivyBDUName    = "trivy-bdu"
	TrivyJavaDBName = "trivy-java-db"
	TrivyChecksName = "trivy-checks"
)

type ImageDownloadList struct {
	rootURL string

	Security map[string]*puller.ImageMeta
}

func NewImageDownloadList(rootURL string) *ImageDownloadList {
	return &ImageDownloadList{
		rootURL:  rootURL,
		Security: make(map[string]*puller.ImageMeta),
	}
}

func (l *ImageDownloadList) FillSecurityImages() {
	imageReferences := []string{
		path.Join(l.rootURL, internal.SecuritySegment, TrivyDBName) + ":2",
		path.Join(l.rootURL, internal.SecuritySegment, TrivyBDUName) + ":1",
		path.Join(l.rootURL, internal.SecuritySegment, TrivyJavaDBName) + ":1",
		path.Join(l.rootURL, internal.SecuritySegment, TrivyChecksName) + ":0",
	}

	for _, imageRef := range imageReferences {
		l.Security[imageRef] = nil
	}
}

type ImageLayouts struct {
	platform   v1.Platform
	workingDir string

	Security *regimage.ImageLayout
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
	case internal.MirrorTypeSecurity:
		l.Security = layout
	default:
		return fmt.Errorf("wrong mirror type in security image layout: %v", mirrorType)
	}

	return nil
}

// AsList returns a list of layout.Path's in it. Undefined path's are not included in the list.
func (l *ImageLayouts) AsList() []layout.Path {
	paths := make([]layout.Path, 0)
	if l.Security != nil {
		paths = append(paths, l.Security.Path())
	}
	return paths
}
