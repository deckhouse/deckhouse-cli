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

type ImageDownloadList struct {
	rootURL string

	Security map[string]map[string]*puller.ImageMeta
}

func NewImageDownloadList(rootURL string) *ImageDownloadList {
	return &ImageDownloadList{
		rootURL:  rootURL,
		Security: make(map[string]map[string]*puller.ImageMeta),
	}
}

func (l *ImageDownloadList) FillSecurityImages() {
	imageReferences := map[string]string{
		internal.SecurityTrivyDBSegment:     path.Join(l.rootURL, internal.SecuritySegment, internal.SecurityTrivyDBSegment) + ":2",
		internal.SecurityTrivyBDUSegment:    path.Join(l.rootURL, internal.SecuritySegment, internal.SecurityTrivyBDUSegment) + ":1",
		internal.SecurityTrivyJavaDBSegment: path.Join(l.rootURL, internal.SecuritySegment, internal.SecurityTrivyJavaDBSegment) + ":1",
		internal.SecurityTrivyChecksSegment: path.Join(l.rootURL, internal.SecuritySegment, internal.SecurityTrivyChecksSegment) + ":0",
	}

	for name, ref := range imageReferences {
		l.Security[name] = map[string]*puller.ImageMeta{
			ref: nil,
		}
	}
}

type ImageLayouts struct {
	platform   v1.Platform
	workingDir string

	Security map[string]*regimage.ImageLayout
}

func NewImageLayouts(rootFolder string) *ImageLayouts {
	l := &ImageLayouts{
		workingDir: rootFolder,
		platform:   v1.Platform{Architecture: "amd64", OS: "linux"},
		Security:   make(map[string]*regimage.ImageLayout, 1),
	}

	return l
}

// TODO: maybe make mirrorType security (like a group)
// and for loop with security names inside?
func (l *ImageLayouts) setLayoutByMirrorType(rootFolder string, mirrorType internal.MirrorType) error {
	layoutPath := filepath.Join(rootFolder, internal.InstallPathByMirrorType(mirrorType))

	layout, err := regimage.NewImageLayout(layoutPath)
	if err != nil {
		return fmt.Errorf("failed to create image layout: %w", err)
	}

	switch mirrorType {
	case internal.MirrorTypeSecurityTrivyDBSegment:
		l.Security[internal.SecurityTrivyDBSegment] = layout
	case internal.MirrorTypeSecurityTrivyBDUSegment:
		l.Security[internal.SecurityTrivyBDUSegment] = layout
	case internal.MirrorTypeSecurityTrivyJavaDBSegment:
		l.Security[internal.SecurityTrivyJavaDBSegment] = layout
	case internal.MirrorTypeSecurityTrivyChecksSegment:
		l.Security[internal.SecurityTrivyChecksSegment] = layout
	default:
		return fmt.Errorf("wrong mirror type in security image layout: %v", mirrorType)
	}

	return nil
}

// AsList returns a list of layout.Path's in it. Undefined path's are not included in the list.
func (l *ImageLayouts) AsList() []layout.Path {
	paths := make([]layout.Path, 0, len(l.Security))
	for _, layout := range l.Security {
		paths = append(paths, layout.Path())
	}
	return paths
}
