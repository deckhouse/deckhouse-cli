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
	"github.com/deckhouse/deckhouse-cli/internal"
)

// DownloadList tracks images to be downloaded for platform
type DownloadList struct {
	rootURL string

	// ReleaseChannels holds release channel image references
	ReleaseChannels map[string]struct{}
	// Installers holds installer image references
	Installers map[string]struct{}
	// StandaloneInstallers holds standalone installer image references
	StandaloneInstallers map[string]struct{}
	// Images holds main Deckhouse image references
	Images map[string]struct{}
}

// NewDownloadList creates a new download list
func NewDownloadList(rootURL string) *DownloadList {
	return &DownloadList{
		rootURL:              rootURL,
		ReleaseChannels:      make(map[string]struct{}),
		Installers:           make(map[string]struct{}),
		StandaloneInstallers: make(map[string]struct{}),
		Images:               make(map[string]struct{}),
	}
}

// FillDeckhouseImages populates the download list with images for the given tags
func (dl *DownloadList) FillDeckhouseImages(tags []string) {
	for _, tag := range tags {
		// Main deckhouse images
		dl.Images[dl.rootURL+":"+tag] = struct{}{}

		// Installers
		dl.Installers[dl.rootURL+"/install:"+tag] = struct{}{}
		dl.StandaloneInstallers[dl.rootURL+"/install-standalone:"+tag] = struct{}{}
	}

	// Release channels
	for _, channel := range internal.GetAllDefaultReleaseChannels() {
		dl.ReleaseChannels[dl.rootURL+"/release-channel:"+channel] = struct{}{}
	}
}

// FillForTag populates additional images for a specific tag
func (dl *DownloadList) FillForTag(tag string) {
	if tag == "" {
		return
	}

	// For specific tag, also add release channel with that tag
	dl.ReleaseChannels[dl.rootURL+"/release-channel:"+tag] = struct{}{}
}

